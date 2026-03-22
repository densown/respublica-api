#!/usr/bin/env python3
"""
Generiert deutschsprachige Kurz-Zusammenfassungen für aenderungen ohne zusammenfassung
über die Groq API (OpenAI-kompatibel) und speichert sie in der Datenbank.
"""

from __future__ import annotations

import json
import os
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
PAUSE_SEC = 2.0

UA = "gesetze-summarize/1.0"


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
        autocommit=False,
    )


def build_user_content(kuerzel: str, diff_text: str) -> str:
    snippet = diff_text[:DIFF_PREVIEW_LEN]
    return (
        "Du bist ein Journalist der komplexe Gesetzesänderungen für normale Bürger erklärt.\n"
        "Fasse diese Gesetzesänderung in 2-3 Sätzen zusammen. Erkläre was sich geändert hat\n"
        "und was das für den Alltag der Bürger bedeutet. Sei konkret und verständlich.\n"
        f"Gesetz: {kuerzel}\n"
        "Änderung (git diff Format):\n"
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


def main() -> int:
    load_env()
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        print("Fehler: GROQ_API_KEY fehlt in .env", file=sys.stderr)
        return 1

    conn = connect()
    generiert = 0
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

        for i, row in enumerate(rows):
            if i:
                time.sleep(PAUSE_SEC)

            aid = row["id"]
            diff_text = row["diff"] or ""
            kuerzel = row["kuerzel"] or ""
            user_content = build_user_content(kuerzel, diff_text)

            try:
                text = groq_chat_completion(api_key, user_content)
            except urllib.error.HTTPError as e:
                err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
                print(
                    f"HTTP-Fehler id={aid}: {e.code} {err_body[:500]}",
                    file=sys.stderr,
                )
                continue
            except Exception as e:
                print(f"Groq-Fehler id={aid}: {e}", file=sys.stderr)
                continue

            if not text:
                print(f"Leere Antwort id={aid}", file=sys.stderr)
                continue

            cur.execute(
                "UPDATE aenderungen SET zusammenfassung = %s WHERE id = %s",
                (text, aid),
            )
            if cur.rowcount:
                generiert += 1

        conn.commit()
    finally:
        conn.close()

    print(f"Zusammenfassungen generiert: {generiert}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
