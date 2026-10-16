"""Zuordnung von Umfragen zu Wahlterminen (import_dawum.pick_wahltermin)."""

from datetime import date

from import_dawum import pick_wahltermin

# Wie load_lookups sie liefert: nach Datum sortiert, ohne Datum am Ende.
MV = [
    {"id": 1, "datum": date(2021, 9, 26), "status": "abgeschlossen"},
    {"id": 2, "datum": date(2026, 9, 20), "status": "kommend"},
]
BUND = [
    {"id": 10, "datum": date(2025, 2, 23), "status": "abgeschlossen"},
    {"id": 11, "datum": None, "status": "kommend"},
]


def test_vor_der_wahl_naechster_termin():
    assert pick_wahltermin(MV, date(2026, 9, 1)) == 2


def test_am_wahltag_noch_dieser_termin():
    assert pick_wahltermin(MV, date(2026, 9, 20)) == 2


def test_nach_der_wahl_kein_termin_auch_wenn_status_kommend():
    assert pick_wahltermin(MV, date(2026, 10, 1)) is None


def test_ohne_datum_faellt_auf_kommenden_termin():
    assert pick_wahltermin(BUND, date(2026, 10, 1)) == 11
