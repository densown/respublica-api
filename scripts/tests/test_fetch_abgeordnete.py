"""Mandate aus abgeordnetenwatch: Mandatsdauer und Fraktion nach Wechsel."""

from datetime import date

import fetch_abgeordnete as fab


def mandat(**felder):
    item = {
        "id": 70129,
        "type": "mandate",
        "parliament_period": {"id": 161},
        "politician": {"id": 1, "label": "David Mandrella", "first_name": "David", "last_name": "Mandrella"},
        "start_date": None,
        "end_date": None,
        "electoral_data": {"list_position": 12, "constituency": None},
        "fraction_membership": [
            {"fraction": {"label": "SPD (Bundestag 2025 - 2029)"}, "valid_from": None, "valid_until": None}
        ],
    }
    item.update(felder)
    return item


def spalten(item):
    # Reihenfolge wie in UPSERT_SQL
    namen = [
        "aw_id", "politiker_id", "parliament_period", "typ", "mandat_von", "mandat_bis",
        "vorname", "nachname", "name", "fraktion", "partei", "wahlkreis", "wahlkreis_nr",
        "listenplatz", "profil_url",
    ]
    row = fab.extract_row(item)
    assert len(row) == len(namen) == fab.UPSERT_SQL.count("%s")
    return dict(zip(namen, row))


def test_nachruecker_mit_beginn():
    z = spalten(mandat(start_date="2026-09-03"))
    assert z["mandat_von"] == date(2026, 9, 3)
    assert z["mandat_bis"] is None
    assert z["fraktion"] == "SPD"


def test_ausgeschieden_mit_ende():
    z = spalten(mandat(end_date="2026-08-31"))
    assert z["mandat_bis"] == date(2026, 8, 31)


def test_unlesbares_datum_bleibt_leer():
    z = spalten(mandat(start_date="unbekannt", end_date=""))
    assert z["mandat_von"] is None and z["mandat_bis"] is None


def test_fraktionswechsel_gilt_die_offene_mitgliedschaft():
    z = spalten(
        mandat(
            fraction_membership=[
                {"fraction": {"label": "AfD (Bundestag 2025 - 2029)"}, "valid_from": None, "valid_until": "2026-02-01"},
                {"fraction": {"label": "fraktionslos (Bundestag 2025 - 2029)"}, "valid_from": "2026-02-02", "valid_until": None},
            ]
        )
    )
    assert z["fraktion"] == "fraktionslos"


def test_ohne_offene_mitgliedschaft_gilt_die_letzte():
    z = spalten(
        mandat(
            fraction_membership=[
                {"fraction": {"label": "B (Bundestag 2025 - 2029)"}, "valid_until": "2026-05-01"},
                {"fraction": {"label": "A (Bundestag 2025 - 2029)"}, "valid_until": "2026-01-01"},
            ]
        )
    )
    assert z["fraktion"] == "B"


def test_ohne_fraktion():
    assert spalten(mandat(fraction_membership=[]))["fraktion"] is None
