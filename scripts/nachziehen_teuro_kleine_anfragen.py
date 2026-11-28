#!/usr/bin/env python3
"""Einmalig: Haushaltsnotation "T Euro" in bestehenden Zusammenfassungen umrechnen.

Zusammenfassungen vor dem 28.11.2026 enthalten "2.727,50 T Euro" (Tausend Euro),
Leser lesen Billionen. Neue Auswertungen rechnen seit summarize_kleine_anfragen.py
automatisch um, dieses Skript zieht die gespeicherten Zeilen nach: zusammenfassung,
zusammenfassung_en, schlagzeile und die Kernzahl-Chips. Zitate bleiben woertlich.
Kein Claude-Aufruf, keine Neuauswertung.

  python scripts/nachziehen_teuro_kleine_anfragen.py --probelauf   # zeigt vorher/nachher
  python scripts/nachziehen_teuro_kleine_anfragen.py               # schreibt
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock  # noqa: E402
from summarize_kleine_anfragen import kernzahl_umrechnen, teuro_umrechnen  # noqa: E402

LOCK_NAME = "nachziehen_teuro_kleine_anfragen"
log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


def umrechnen(z: dict) -> dict | None:
    """Neue Werte fuer eine Zeile oder None, wenn nichts zu tun ist."""
    neu = {
        "zusammenfassung": teuro_umrechnen(z["zusammenfassung"] or "", "de"),
        "zusammenfassung_en": teuro_umrechnen(z["zusammenfassung_en"] or "", "en"),
        "schlagzeile": teuro_umrechnen(z["schlagzeile"] or "", "de"),
    }
    try:
        kern = json.loads(z["kernzahlen"] or "[]")
    except (TypeError, json.JSONDecodeError):
        kern = []
    neu["kernzahlen"] = json.dumps([kernzahl_umrechnen(k) for k in kern if isinstance(k, dict)], ensure_ascii=False)
    alt = {k: (z[k] or "") for k in ("zusammenfassung", "zusammenfassung_en", "schlagzeile")}
    alt["kernzahlen"] = json.dumps([k for k in kern if isinstance(k, dict)], ensure_ascii=False)
    return neu if neu != alt else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--probelauf", action="store_true", help="nur anzeigen, nichts schreiben")
    args = ap.parse_args()

    load_env()
    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)
    conn = get_db(autocommit=False)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT id, dokumentnummer, zusammenfassung, zusammenfassung_en, schlagzeile, kernzahlen
                 FROM kleine_anfragen
                WHERE zusammenfassung IS NOT NULL
                  AND (zusammenfassung REGEXP 'T ?(Euro|€)|TEUR|Tsd\\\\.? ?Euro|Tausend Euro'
                       OR zusammenfassung_en REGEXP 'T ?(Euro|€)|TEUR|thousand euro'
                       OR schlagzeile REGEXP 'T ?(Euro|€)|TEUR|Tsd\\\\.? ?Euro|Tausend Euro'
                       OR kernzahlen REGEXP 'T ?(Euro|€)|TEUR|Tsd\\\\.? ?Euro|Tausend Euro')"""
        )
        zeilen = cur.fetchall()
        geaendert = 0
        for z in zeilen:
            if not _running:
                break
            neu = umrechnen(z)
            if not neu:
                continue
            geaendert += 1
            if args.probelauf:
                print(json.dumps({"dokument": z["dokumentnummer"], "vorher": z["schlagzeile"],
                                  "nachher": neu["schlagzeile"], "zusammenfassung": neu["zusammenfassung"]},
                                 ensure_ascii=False))
                continue
            cur.execute(
                """UPDATE kleine_anfragen SET zusammenfassung = %s, zusammenfassung_en = %s,
                          schlagzeile = %s, kernzahlen = %s WHERE id = %s""",
                (neu["zusammenfassung"], neu["zusammenfassung_en"], neu["schlagzeile"], neu["kernzahlen"], z["id"]),
            )
        if not args.probelauf:
            conn.commit()
        log.info("%d Zeilen mit T Euro gefunden, %d %s", len(zeilen), geaendert,
                 "wuerden geaendert" if args.probelauf else "geaendert")
        return 0
    finally:
        cur.close()
        conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
