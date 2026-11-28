#!/usr/bin/env python3
"""Einfacher, idempotenter SQL-Migrations-Runner (M-015).

Wendet ``migrations/*.sql`` in sortierter Reihenfolge an und protokolliert jede
Datei in der Tabelle ``schema_migrations``. Bereits angewandte Dateien werden
uebersprungen — der Runner ist damit idempotent.

  python scripts/migrate.py              # wendet ausstehende Migrationen an
  python scripts/migrate.py --status     # zeigt angewandt / ausstehend
  python scripts/migrate.py --nur 022 023 # nur Dateien mit diesen Namensanfaengen
  python scripts/migrate.py --baseline   # markiert ALLE vorhandenen Dateien als
                                          # angewandt, OHNE sie auszufuehren
                                          # (einmalig zur Adoption des Ist-Stands)

Dateien, deren Kopf "NICHT automatisch" enthaelt, laufen nur, wenn sie mit
--nur ausdruecklich genannt werden. Der normale Lauf ueberspringt sie mit
einem Hinweis, statt an ihnen haengen zu bleiben.

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


def split_statements(text: str) -> list[str]:
    """Zerlegt eine SQL-Datei an Semikolons ausserhalb von Strings und Kommentaren.

    mysql-connector-python 9 kennt ``execute(..., multi=True)`` nicht mehr,
    deshalb laeuft jedes Statement einzeln. DELIMITER wird nicht unterstuetzt.
    """
    out: list[str] = []
    buf: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        two = text[i:i + 2]
        if two == "--" and (i + 2 >= n or text[i + 2] in " \t\n\r") or ch == "#":
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if two == "/*":
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
            buf.append(" ")
            continue
        if ch in "'\"`":
            j = i + 1
            while j < n:
                if text[j] == "\\" and ch != "`":
                    j += 2
                    continue
                if text[j] == ch:
                    if j + 1 < n and text[j + 1] == ch:
                        j += 2
                        continue
                    break
                j += 1
            buf.append(text[i:j + 1])
            i = j + 1
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    rest = "".join(buf).strip()
    if rest:
        out.append(rest)
    return out


def is_manual(text: str) -> bool:
    """Kopf der Datei verlangt ausdruecklichen Aufruf (z. B. 011)."""
    return "NICHT automatisch" in "\n".join(text.splitlines()[:12])


def apply_sql(cur, text: str) -> None:
    for stmt in split_statements(text):
        cur.execute(stmt)
        if cur.with_rows:
            cur.fetchall()


def main() -> int:
    ap = argparse.ArgumentParser(description="SQL migration runner")
    ap.add_argument("--baseline", action="store_true",
                    help="markiere vorhandene Migrationen als angewandt, ohne sie auszufuehren")
    ap.add_argument("--status", action="store_true", help="zeige Status und beende")
    ap.add_argument("--nur", nargs="+", metavar="ANFANG",
                    help="nur Dateien, deren Name so beginnt (z. B. 022 023 024)")
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
            if p.name in applied:
                print("APPLIED " + p.name)
            elif is_manual(p.read_text(encoding="utf-8")):
                print("PENDING " + p.name + "  (nur von Hand: --nur)")
            else:
                print("PENDING " + p.name)
        cur.close(); conn.close()
        return 0

    pending = [p for p in files if p.name not in applied]
    if args.nur:
        pending = [p for p in pending if p.name.startswith(tuple(args.nur))]
    elif not args.baseline:
        hand = [p for p in pending if is_manual(p.read_text(encoding="utf-8"))]
        for p in hand:
            print(f"uebersprungen (nur von Hand, --nur {p.name[:3]}): {p.name}")
        pending = [p for p in pending if p not in hand]
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
