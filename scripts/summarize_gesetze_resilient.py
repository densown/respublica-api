#!/usr/bin/env python3
"""
Resiliente Variante von summarize_gesetze.py:
- Commit per Row (kein Datenverlust bei Crash)
- Retry mit Backoff bei HTTP 429 (Rate Limit)
- Progress alle 50 Eintraege ins Log
- Saubere Signal-Behandlung
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"
DIFF_PREVIEW_LEN = 2000
PAUSE_SEC = 8.0
MAX_RETRIES = 5
BACKOFF_BASE = 10  # Sekunden, wird exponentiell

UA = "gesetze-summarize/1.1-resilient"

_running = True

def _signal_handler(signum, frame):
    global _running
    print(f"\n[SIGNAL] {signum} empfangen, beende sauber nach aktuellem Eintrag...", flush=True)
    _running = False

signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def load_env() -> None:
    load_dotenv(ROOT / ".env")


def connect():
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "respublica_gesetze")
    if not user:
        print("Fehler: DB_USER fehlt in .env", file=sys.stderr)
        sys.exit(1)
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=True,  # WICHTIG: per-row commit
    )


def build_user_content(kuerzel: str, diff_text: str) -> str:
    snippet = diff_text[:DIFF_PREVIEW_LEN]
    return (
        "Du bist ein Journalist der komplexe Gesetzesaenderungen fuer normale Buerger erklaert.\n"
        "Fasse diese Gesetzesaenderung in 2-3 Saetzen zusammen. Erklaere was sich geaendert hat\n"
        "und was das fuer den Alltag der Buerger bedeutet. Sei konkret und verstaendlich.\n"
        f"Gesetz: {kuerzel}\n"
        "Aenderung (git diff Format):\n"
        f"{snippet}"
    )


def groq_chat_completion(api_key: str, user_content: str) -> str:
    body: dict[str, Any] = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": user_content}],
        "temperature": 0.4,
        "max_tokens": 1024,
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        GROQ_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": UA,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    choices = payload.get("choices") or []
    if not choices:
        err = payload.get("error") or payload
        raise RuntimeError(f"Keine Antwort von Groq: {err!r}")

    msg = choices[0].get("message") or {}
    content = msg.get("content")
    if not content or not isinstance(content, str):
        raise RuntimeError("Antwort ohne Text")
    return content.strip()


def groq_with_retry(api_key: str, user_content: str, aid: int) -> str | None:
    """Wiederholt bei 429 mit Backoff. Respektiert retry-after Header (max 60s).
    Bei Anti-Abuse (retry-after > 60 oder x-should-retry=false) sofort skippen."""
    for attempt in range(MAX_RETRIES):
        try:
            return groq_chat_completion(api_key, user_content)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            if e.code == 429:
                retry_after = e.headers.get("retry-after", "")
                should_retry = e.headers.get("x-should-retry", "true").lower()
                # Anti-Abuse-Erkennung: lange Pause oder explizit kein retry
                try:
                    ra_int = int(retry_after) if retry_after else 0
                except ValueError:
                    ra_int = 0
                if ra_int > 60 or should_retry == "false":
                    print(f"  [ABUSE-LOCK] id={aid} retry-after={retry_after}s should-retry={should_retry}, GIVING UP NOW", flush=True)
                    return None
                # Normal: respect retry-after wenn vorhanden, sonst exp backoff
                wait = ra_int if ra_int > 0 else BACKOFF_BASE * (2 ** attempt)
                print(f"  [429] id={aid} attempt={attempt+1}/{MAX_RETRIES}, warte {wait}s...", flush=True)
                time.sleep(wait)
                continue
            print(f"HTTP-Fehler id={aid}: {e.code} {err_body[:500]}", file=sys.stderr, flush=True)
            return None
        except Exception as e:
            print(f"Groq-Fehler id={aid}: {e}", file=sys.stderr, flush=True)
            return None
    print(f"Aufgegeben nach {MAX_RETRIES} retries id={aid}", file=sys.stderr, flush=True)
    return None


def main() -> int:
    load_env()
    # Single-Process-Lock
    pid_file = ROOT / "logs" / "summarize_gesetze.pid"
    my_pid = os.getpid()
    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            if old_pid != my_pid:  # nicht die eigene PID
                os.kill(old_pid, 0)  # signal 0 = check ob Prozess existiert
                print(f"FEHLER: Anderer Prozess laeuft bereits (PID {old_pid}). Abbruch.", file=sys.stderr)
                return 1
        except (OSError, ValueError):
            pass  # PID-File stale, ok
    pid_file.write_text(str(my_pid))


    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        print("Fehler: GROQ_API_KEY fehlt in .env", file=sys.stderr)
        return 1

    conn = connect()
    generiert = 0
    fehler = 0
    start = time.time()
    
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT a.id, a.diff, g.kuerzel AS kuerzel
            FROM aenderungen a
            INNER JOIN gesetze g ON g.id = a.gesetz_id
            WHERE (a.zusammenfassung IS NULL OR TRIM(a.zusammenfassung) = '')
              AND a.diff IS NOT NULL
              AND TRIM(a.diff) != ''
            """
        )
        rows = cur.fetchall()
        total = len(rows)
        print(f"[START] {total} Eintraege ohne summary mit diff", flush=True)

        for i, row in enumerate(rows):
            if not _running:
                print(f"[STOP] Nach Signal sauber beendet bei {i}/{total}", flush=True)
                break
            
            if i:
                time.sleep(PAUSE_SEC)

            aid = row["id"]
            diff_text = row["diff"] or ""
            kuerzel = row["kuerzel"] or ""
            user_content = build_user_content(kuerzel, diff_text)

            text = groq_with_retry(api_key, user_content, aid)
            
            if not text:
                fehler += 1
                continue

            # Per-row UPDATE + autocommit
            cur.execute(
                "UPDATE aenderungen SET zusammenfassung = %s WHERE id = %s",
                (text, aid),
            )
            if cur.rowcount:
                generiert += 1

            # Progress alle 50 Eintraege
            if (i + 1) % 50 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed * 60  # pro Minute
                eta_min = (total - i - 1) / rate if rate > 0 else 0
                print(
                    f"[{i+1}/{total}] ok={generiert} fehler={fehler} "
                    f"rate={rate:.1f}/min eta={eta_min:.0f}min",
                    flush=True,
                )

    finally:
        conn.close()

    elapsed = time.time() - start
    print(f"\n[FERTIG] generiert={generiert} fehler={fehler} dauer={elapsed/60:.1f}min", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
