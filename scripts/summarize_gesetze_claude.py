#!/usr/bin/env python3
"""
Generiert DE+EN Zusammenfassungen fuer aenderungen via Claude CLI (claude --print).
Nutzt Claude Max Plan (kein API-Key noetig, ANTHROPIC_API_KEY wird aus env entfernt).

Modi:
  --limit N     Nur N Eintraege verarbeiten (Default: alle)
  --all         Auch bereits-ok Eintraege neu generieren
  --dry-run     Nur Prompt anzeigen, kein DB-Update
  --quiet       Nur Progress-Marker, kein Eintrag-Log

Cron-tauglich: Exit 0 bei Erfolg, Exit 1 bei Fehler.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db as lib_get_db
from lib.env import load_env
from lib.log import acquire_lock as lib_acquire_lock, release_lock as lib_release_lock

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "summarize_gesetze_claude.log"
LOCK_NAME = "summarize_gesetze_claude"

DIFF_PREVIEW_LEN = 50000   # max 50 KB Diff im Prompt (vs Groqs 2 KB)
TIMEOUT_SEC = 180

_running = True

def _signal_handler(signum, frame):
    global _running
    log(f"[SIGNAL] {signum} empfangen, beende sauber nach aktuellem Eintrag...")
    _running = False

signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def log(msg, also_stdout=True):
    line = f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}'
    if also_stdout:
        print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def get_db():
    load_env()
    return lib_get_db(autocommit=True)  # per-row commit


def acquire_lock():
    return lib_acquire_lock(LOCK_NAME)


def release_lock():
    lib_release_lock(LOCK_NAME)


def build_prompt(kuerzel: str, titel: str, diff_text: str) -> str:
    snippet = diff_text[:DIFF_PREVIEW_LEN]
    truncated_note = ""
    if len(diff_text) > DIFF_PREVIEW_LEN:
        truncated_note = f"\n\n[Hinweis: Diff war urspruenglich {len(diff_text)} Zeichen, hier auf {DIFF_PREVIEW_LEN} gekuerzt.]"
    
    return f"""Du bist ein Journalist der komplexe Gesetzesaenderungen fuer normale Buerger erklaert.

Ich gebe dir eine Gesetzesaenderung im git-diff Format. Antworte AUSSCHLIESSLICH mit einem JSON-Objekt (kein Markdown, kein Codeblock, kein Text davor/danach).

Das JSON muss exakt diese zwei Schluessel haben:
- "zusammenfassung_de": Zusammenfassung in 2-3 Saetzen auf Deutsch.
- "zusammenfassung_en": Summary in 2-3 sentences in English.

Regeln:
- Erklaere was sich konkret geaendert hat und was das fuer den Alltag der Buerger bedeutet.
- Vermeide juristisches Fachvokabular wo moeglich.
- Keine Phrasen wie "Hier ist die Zusammenfassung" oder "Die Aenderung betrifft".
- Steige direkt mit dem Inhalt ein.

--- AENDERUNG ---
Gesetz-Kuerzel: {kuerzel}
Gesetzes-Titel: {titel}

Diff (Git-Format):
{snippet}{truncated_note}
--- ENDE ---

JSON:"""


def call_claude(prompt: str) -> str | None:
    try:
        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)  # Max Plan, nicht API
        result = subprocess.run(
            ["claude", "--print", "-p", prompt],
            capture_output=True, text=True, timeout=TIMEOUT_SEC, env=env,
        )
        if result.returncode != 0:
            log(f"  claude stderr: {result.stderr.strip()[:300]}")
            return None
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log(f"  claude timeout ({TIMEOUT_SEC}s)")
        return None
    except FileNotFoundError:
        log("  claude CLI nicht gefunden")
        return None


def parse_response(raw: str | None) -> tuple[str, str] | None:
    if not raw:
        return None
    text = raw.strip()
    # Codeblock-Markdown wegmachen falls vorhanden
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    # JSON-Objekt im Text suchen
    match = re.search(r'\{[^{}]*"zusammenfassung_de"[^{}]*\}', text, re.DOTALL)
    if match:
        text = match.group(0)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        log(f"  JSON parse error: {e}")
        log(f"  Rohtext: {text[:300]}")
        return None
    z_de = (data.get("zusammenfassung_de") or "").strip()
    z_en = (data.get("zusammenfassung_en") or "").strip()
    if not z_de or not z_en:
        log(f"  Fehlende Felder: de={bool(z_de)}, en={bool(z_en)}")
        return None
    return z_de, z_en


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Max Eintraege")
    ap.add_argument("--all", action="store_true", help="Auch quality_ok=1 erneut")
    ap.add_argument("--dry-run", action="store_true", help="Nur Prompt zeigen")
    ap.add_argument("--quiet", action="store_true", help="Weniger Log-Spam")
    ap.add_argument("--lobby-only", action="store_true", help="Nur Gesetze mit Lobby-Aktivitaet")
    args = ap.parse_args()

    if not acquire_lock():
        return 1

    try:
        db = get_db()
        cur = db.cursor()
        
        where = "1=1" if args.all else "a.quality_ok = 0"
        lobby_filter = ""
        if args.lobby_only:
            lobby_filter = " AND EXISTS (SELECT 1 FROM lobby_gesetze lg WHERE lg.gesetz_id = a.gesetz_id)"
        cur.execute(f"""
            SELECT a.id, g.kuerzel, COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), g.name, g.kuerzel) AS titel, a.diff
            FROM aenderungen a
            INNER JOIN gesetze g ON g.id = a.gesetz_id
            WHERE {where}
              AND a.diff IS NOT NULL
              AND TRIM(a.diff) != ''
              {lobby_filter}
            ORDER BY a.id DESC
        """)
        rows = cur.fetchall()
        
        if args.limit:
            rows = rows[:args.limit]
        
        total = len(rows)
        log(f"[START] {total} Aenderungen zu verarbeiten (limit={args.limit}, all={args.all})")
        
        if total == 0:
            log("[FERTIG] Nichts zu tun.")
            return 0
        
        ok = fail = 0
        start = time.time()
        
        for i, (aid, kuerzel, titel, diff_text) in enumerate(rows):
            if not _running:
                log(f"[STOP] Sauberer Abbruch bei {i}/{total}")
                break
            
            if not args.quiet:
                log(f"  [{i+1}/{total}] id={aid} kuerzel={kuerzel}")
            
            prompt = build_prompt(kuerzel or "", titel or "", diff_text or "")
            
            if args.dry_run:
                print(prompt[:1000])
                print("...")
                if i >= 2:
                    break
                continue
            
            raw = call_claude(prompt)
            parsed = parse_response(raw)
            
            if parsed is None:
                fail += 1
                if not args.quiet:
                    log(f"  FEHLER id={aid}")
                continue
            
            z_de, z_en = parsed
            try:
                cur.execute(
                    "UPDATE aenderungen SET zusammenfassung = %s, zusammenfassung_en = %s, quality_ok = 1 WHERE id = %s",
                    (z_de, z_en, aid),
                )
                ok += 1
                if not args.quiet:
                    log(f"  OK id={aid}")
            except Exception as e:
                fail += 1
                log(f"  DB-Fehler id={aid}: {e}")
            
            # Progress alle 25 Eintraege
            if (i + 1) % 25 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed * 60
                eta_min = (total - i - 1) / rate if rate > 0 else 0
                log(f"[PROGRESS {i+1}/{total}] ok={ok} fail={fail} rate={rate:.1f}/min eta={eta_min:.0f}min")
        
        elapsed = time.time() - start
        log(f"[FERTIG] generiert={ok} fehler={fail} dauer={elapsed/60:.1f}min")
        cur.close()
        db.close()
        return 0 if fail == 0 or ok > 0 else 1
    
    finally:
        release_lock()


if __name__ == "__main__":
    sys.exit(main())
