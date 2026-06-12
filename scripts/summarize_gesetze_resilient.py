#!/usr/bin/env python3
"""
Resiliente Variante des Gesetze-Summarizers (Produktions-Cron 07:00):
- Commit per Row (kein Datenverlust bei Crash)
- Retry mit Backoff bei HTTP 429 via lib.groq
- Progress alle 50 Eintraege ins Log
- Saubere Signal-Behandlung
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env
from lib.groq import GroqError, chat_completion
from lib.log import acquire_lock, install_signal_handlers, release_lock

GROQ_MODEL = "llama-3.3-70b-versatile"
DIFF_PREVIEW_LEN = 2000
PAUSE_SEC = 8.0
QUOTA_ABORT_AFTER = 10  # Abbruch nach so vielen Quota-Fehlern in Folge

LOCK_NAME = "summarize_gesetze"

_running = True


def _stop() -> None:
    global _running
    _running = False


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


def main() -> int:
    ap = argparse.ArgumentParser(description="Generiert fehlende Zusammenfassungen via Groq")
    ap.add_argument("--limit", type=int, default=None, help="Max Eintraege (Default: alle)")
    args = ap.parse_args()

    load_env()
    install_signal_handlers(_stop)

    if not acquire_lock(LOCK_NAME):
        return 1

    if not os.environ.get("GROQ_API_KEY", "").strip():
        print("Fehler: GROQ_API_KEY fehlt in .env", file=sys.stderr)
        release_lock(LOCK_NAME)
        return 1

    conn = get_db(autocommit=True)  # WICHTIG: per-row commit
    generiert = 0
    fehler = 0
    consecutive_quota_errors = 0
    start = time.time()

    try:
        cur = conn.cursor(dictionary=True)
        limit_clause = f"LIMIT {int(args.limit)}" if args.limit else ""
        cur.execute(
            f"""
            SELECT a.id, a.diff, g.kuerzel AS kuerzel
            FROM aenderungen a
            INNER JOIN gesetze g ON g.id = a.gesetz_id
            WHERE (a.zusammenfassung IS NULL OR TRIM(a.zusammenfassung) = '')
              AND a.diff IS NOT NULL
              AND TRIM(a.diff) != ''
            {limit_clause}
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

            text = None
            try:
                text = chat_completion(
                    [{"role": "user", "content": user_content}],
                    model=GROQ_MODEL,
                    max_tokens=1024,
                    temperature=0.4,
                )
                consecutive_quota_errors = 0
            except GroqError as e:
                print(f"Groq-Fehler id={aid}: {e}", file=sys.stderr, flush=True)
                fehler += 1
                if e.quota_exhausted:
                    consecutive_quota_errors += 1
                    if consecutive_quota_errors >= QUOTA_ABORT_AFTER:
                        print(
                            f"[QUOTA] Tagesbudget erschoepft nach {generiert} generierten, beende Lauf",
                            flush=True,
                        )
                        break
                else:
                    consecutive_quota_errors = 0

            if text:
                # Per-row UPDATE + autocommit
                cur.execute(
                    "UPDATE aenderungen SET zusammenfassung = %s WHERE id = %s",
                    (text, aid),
                )
                if cur.rowcount:
                    generiert += 1

            # Progress alle 50 verarbeitete Eintraege (ok + fehler)
            verarbeitet = generiert + fehler
            if verarbeitet and verarbeitet % 50 == 0:
                elapsed = time.time() - start
                rate = verarbeitet / elapsed * 60  # pro Minute
                eta_min = (total - i - 1) / rate if rate > 0 else 0
                print(
                    f"[{verarbeitet}/{total}] ok={generiert} fehler={fehler} "
                    f"rate={rate:.1f}/min eta={eta_min:.0f}min",
                    flush=True,
                )

    finally:
        conn.close()
        release_lock(LOCK_NAME)

    elapsed = time.time() - start
    print(f"\n[FERTIG] generiert={generiert} fehler={fehler} dauer={elapsed/60:.1f}min", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
