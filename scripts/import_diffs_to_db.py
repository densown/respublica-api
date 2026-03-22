#!/usr/bin/env python3
"""
Liest die Tages-Diff-JSON (data/diffs/YYYY-MM-DD.json), legt Gesetze an und
schreibt Änderungen nach MariaDB. Wiederholter Lauf am selben Tag ist idempotent
(keine doppelten Zeilen in aenderungen, sofern (gesetz_id, datum, diff) eindeutig ist).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DIFFS_DIR = ROOT / "data" / "diffs"


def law_slug_and_folder(rel_path: str) -> tuple[str, str]:
    """z. B. m/milog/index.md -> ('milog', 'm/milog')."""
    parts = Path(rel_path).parts
    if len(parts) >= 2:
        return parts[1], f"{parts[0]}/{parts[1]}"
    if parts:
        return parts[0], parts[0]
    return rel_path, rel_path


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


def main() -> int:
    load_env()

    today = date.today().isoformat()
    json_path = DIFFS_DIR / f"{today}.json"
    if not json_path.is_file():
        print(f"Keine Datei gefunden: {json_path}", file=sys.stderr)
        return 1

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    files = payload.get("files") or []
    if not isinstance(files, list):
        print('Ungültiges JSON: "files" muss eine Liste sein.', file=sys.stderr)
        return 1

    conn = connect()
    neue_gesetze = 0
    neue_aenderungen = 0

    try:
        cur = conn.cursor()
        for entry in files:
            if not isinstance(entry, dict):
                continue
            rel_path = entry.get("path")
            diff_text = entry.get("diff")
            if not isinstance(rel_path, str) or not isinstance(diff_text, str):
                continue

            kuerzel, pfad = law_slug_and_folder(rel_path)

            cur.execute(
                "INSERT IGNORE INTO gesetze (kuerzel, pfad) VALUES (%s, %s)",
                (kuerzel, pfad),
            )
            neue_gesetze += cur.rowcount

            cur.execute(
                "SELECT id FROM gesetze WHERE kuerzel = %s LIMIT 1",
                (kuerzel,),
            )
            row = cur.fetchone()
            if not row:
                continue
            gesetz_id = row[0]

            cur.execute(
                """
                INSERT INTO aenderungen (gesetz_id, datum, diff)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM aenderungen
                    WHERE gesetz_id <=> %s AND datum = %s AND diff <=> %s
                )
                """,
                (gesetz_id, today, diff_text, gesetz_id, today, diff_text),
            )
            neue_aenderungen += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    print(
        f"Fertig: {neue_gesetze} neue Gesetze, {neue_aenderungen} neue Änderungen "
        f"gespeichert ({today})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
