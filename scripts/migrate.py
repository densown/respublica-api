#!/usr/bin/env python3
"""Einfacher, idempotenter SQL-Migrations-Runner (M-015).

Wendet ``migrations/*.sql`` in sortierter Reihenfolge an und protokolliert jede
Datei in der Tabelle ``schema_migrations``. Bereits angewandte Dateien werden
uebersprungen — der Runner ist damit idempotent.

  python scripts/migrate.py              # wendet ausstehende Migrationen an
  python scripts/migrate.py --status     # zeigt angewandt / ausstehend
  python scripts/migrate.py --baseline   # markiert ALLE vorhandenen Dateien als
                                          # angewandt, OHNE sie auszufuehren
                                          # (einmalig zur Adoption des Ist-Stands)

Hinweis: MySQL-DDL committet implizit; der Runner faehrt deshalb pro Datei
sequentiell und protokolliert erst nach erfolgreicher Ausfuehrung.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename VARCHAR(255) NOT NULL PRIMARY KEY,
    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def migration_files() -> list[Path]:
    # nur Top-Level *.sql (migrations/_disabled/ wird bewusst ignoriert)
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def applied_set(cur) -> set[str]:
    cur.execute("SELECT filename FROM schema_migrations")
    return {r[0] for r in cur.fetchall()}


def apply_sql(cur, text: str) -> None:
    # mysql.connector: multi=True fuer Dateien mit mehreren Statements;
    # der Iterator MUSS konsumiert werden, damit alle Statements laufen.
    for _ in cur.execute(text, multi=True):
        pass


def main() -> int:
    ap = argparse.ArgumentParser(description="SQL migration runner")
    ap.add_argument("--baseline", action="store_true",
                    help="markiere vorhandene Migrationen als angewandt, ohne sie auszufuehren")
    ap.add_argument("--status", action="store_true", help="zeige Status und beende")
    args = ap.parse_args()

    load_env()
    conn = get_db(autocommit=False)
    cur = conn.cursor()
    cur.execute(DDL_TABLE)
    conn.commit()

    files = migration_files()
    applied = applied_set(cur)

    if args.status:
        for p in files:
            print(("APPLIED " if p.name in applied else "PENDING ") + p.name)
        cur.close(); conn.close()
        return 0

    pending = [p for p in files if p.name not in applied]
    if not pending:
        print("Keine ausstehenden Migrationen.")
        cur.close(); conn.close()
        return 0

    for p in pending:
        if args.baseline:
            cur.execute("INSERT INTO schema_migrations (filename) VALUES (%s)", (p.name,))
            conn.commit()
            print("baseline:", p.name)
            continue
        print("apply:", p.name)
        try:
            apply_sql(cur, p.read_text(encoding="utf-8"))
            cur.execute("INSERT INTO schema_migrations (filename) VALUES (%s)", (p.name,))
            conn.commit()
            print("  ok")
        except Exception as e:
            conn.rollback()
            print(f"  FEHLER in {p.name}: {e}", file=sys.stderr)
            cur.close(); conn.close()
            return 1

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
