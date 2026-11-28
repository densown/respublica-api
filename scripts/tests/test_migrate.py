"""Zerlegung von Migrationsdateien in einzelne Statements."""
from pathlib import Path

import migrate


def test_split_einfach():
    assert migrate.split_statements("SELECT 1; SELECT 2;\n") == ["SELECT 1", "SELECT 2"]


def test_split_semikolon_in_string_und_kommentar():
    sql = (
        "-- Kommentar; mit Semikolon\n"
        "INSERT INTO t (a) VALUES ('x;y', \"p;q\"); # noch einer;\n"
        "/* block; */ UPDATE t SET a = 'it''s; ok' WHERE `we;ird` = 1;\n"
        "SELECT 5 -- ohne Semikolon am Ende"
    )
    teile = migrate.split_statements(sql)
    assert len(teile) == 3
    assert teile[0] == "INSERT INTO t (a) VALUES ('x;y', \"p;q\")"
    assert "it''s; ok" in teile[1] and "`we;ird`" in teile[1]
    assert teile[2] == "SELECT 5"


def test_split_minus_ohne_leerzeichen_ist_kein_kommentar():
    assert migrate.split_statements("SELECT 5--3;") == ["SELECT 5--3"]


def test_alle_migrationen_zerlegbar():
    for p in sorted(Path(migrate.MIGRATIONS_DIR).glob("*.sql")):
        teile = migrate.split_statements(p.read_text(encoding="utf-8"))
        assert teile, p.name
        assert all(t.strip() and not t.strip().startswith("--") for t in teile), p.name


def test_011_nur_von_hand():
    text = (Path(migrate.MIGRATIONS_DIR) / "011_fk_indexes_collation.sql").read_text(encoding="utf-8")
    assert migrate.is_manual(text)
    assert not migrate.is_manual((Path(migrate.MIGRATIONS_DIR) / "022_quellenregister.sql").read_text(encoding="utf-8"))
