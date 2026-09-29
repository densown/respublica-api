"""Tests fuer die Versammlungs-Pipeline (nur reine Funktionen, keine DB/Netz)."""

from datetime import date, time

import pytest

from classify_versammlungen import parse_antwort
from fetch_versammlungen_berlin import find_records, parse_payload, parse_record
from lib.versammlungen import (
    clean_thema,
    is_ganztaegig,
    make_quelle_id,
    normalize_key,
    parse_datum,
    parse_zeit,
    split_serie,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Straße/Nr.", "strassenr"),
        ("Uhrzeit von", "uhrzeitvon"),
        ("PLZ", "plz"),
        ("Aufzugsstrecke", "aufzugsstrecke"),
    ],
)
def test_normalize_key(raw, expected):
    assert normalize_key(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("29.09.2026", date(2026, 9, 29)),
        ("1.2.26", date(2026, 2, 1)),
        ("2026-09-29", date(2026, 9, 29)),
        ("Di, 29.09.2026", date(2026, 9, 29)),
        ("31.02.2026", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_datum(raw, expected):
    assert parse_datum(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("10:00", time(10, 0)),
        ("9.30 Uhr", time(9, 30)),
        ("18 Uhr", time(18, 0)),
        ("24:00", time(23, 59)),
        ("25:00", None),
        ("offen", None),
        (None, None),
    ],
)
def test_parse_zeit(raw, expected):
    assert parse_zeit(raw) == expected


def test_find_records_varianten():
    rec = {"datum": "01.10.2026"}
    assert find_records({"messages": {}, "index": [rec]}) == [rec]
    assert find_records([rec, "kaputt"]) == [rec]
    assert find_records({"results": {"count": 1}, "items": [rec]}) == [rec]
    assert find_records({"foo": "bar"}) == []


def test_parse_record_kundgebung_und_aufzug():
    kundgebung = parse_record({
        "Datum": "01.10.2026", "Von": "10:00", "Bis": "12:00",
        "Thema": "Mietenstopp jetzt", "PLZ": "10178",
        "Straße/Nr.": "Alexanderplatz", "Aufzugsstrecke": "-",
    })
    assert kundgebung["datum"] == date(2026, 10, 1)
    assert kundgebung["typ"] == "kundgebung"
    assert kundgebung["aufzugsstrecke"] is None
    assert kundgebung["ort"] == "Alexanderplatz"

    aufzug = parse_record({
        "datum": "01.10.2026", "von": "14:00", "thema": "Klimastreik",
        "aufzugsstrecke": "Invalidenpark - Unter den Linden - Brandenburger Tor",
    })
    assert aufzug["typ"] == "aufzug"


def test_parse_record_ohne_datum():
    assert parse_record({"thema": "ohne Datum"}) is None


def test_quelle_id_stabil_bei_formatierung():
    a = make_quelle_id(date(2026, 10, 1), time(10), "10178", "Mietenstopp jetzt!")
    b = make_quelle_id(date(2026, 10, 1), time(10), "10178", "  mietenstopp  JETZT ")
    c = make_quelle_id(date(2026, 10, 1), time(11), "10178", "Mietenstopp jetzt!")
    assert a == b
    assert a != c


def test_parse_payload_dedupliziert():
    rec = {"datum": "01.10.2026", "von": "10:00", "thema": "X", "plz": "10115"}
    diag = {}
    assert len(parse_payload({"index": [rec, dict(rec), {"thema": "kein Datum"}]}, diag)) == 1
    assert len(diag["duplikate"]) == 1
    assert len(diag["ohne_datum"]) == 1


def test_parse_antwort():
    text = 'Hier:\n```json\n{"1": "klima_umwelt", "2": "erfunden", "99": "nahost", "x": "nahost"}\n```'
    assert parse_antwort(text, {1, 2}) == {1: "klima_umwelt"}
    assert parse_antwort("kein json", {1}) == {}
    assert parse_antwort(None, {1}) == {}


# ---------------------------------------------------------------------------
# Parser v2 (Migration 017), Beispiele aus dem Berliner Abruf vom 29.09.2026
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw,expected",
    [
        (",,Protest gegen die fortlaufenden  Hinrichtu- ngen unschuldiger Menschen im Iran",
         "„Protest gegen die fortlaufenden Hinrichtungen unschuldiger Menschen im Iran"),
        ("Verfolgung der Falun Gong Praktizieren- den beenden",
         "Verfolgung der Falun Gong Praktizierenden beenden"),
        ("Für Reise- und Versammlungs- freiheit", "Für Reise- und Versammlungsfreiheit"),
        ("Diplomatie- oder Kriegspolitik", "Diplomatie- oder Kriegspolitik"),
        ("Palliative Geriatrie - besser für alle", "Palliative Geriatrie - besser für alle"),
        ("Gegen den Recht- sruck ''jetzt''", "Gegen den Rechtsruck “jetzt“"),
        ("FRE&#304;HE&#304;T FÜR &quot;alle&quot;", "FREİHEİT FÜR \"alle\""),
        ("  ", None),
        (None, None),
    ],
)
def test_clean_thema(raw, expected):
    assert clean_thema(raw) == expected


@pytest.mark.parametrize(
    "raw,thema,von,bis,rhythmus",
    [
        ("Mahnwache für Menschenrechte im Iran. -> Verlängerung der Dauermahnwache "
         "(vom 01.08. bis 01.10.2026 - täglich)",
         "Mahnwache für Menschenrechte im Iran. -> Verlängerung der Dauermahnwache",
         date(2026, 8, 1), date(2026, 10, 1), "täglich"),
        ("Mahnwache (vom 07.09.2026 bis 27.09.2027 - jeweils Mo.)",
         "Mahnwache", date(2026, 9, 7), date(2027, 9, 27), "jeweils Mo."),
        ("Gewaltfreie Psychiatrie jetzt! (vom 25.11. bis 26.11.2026,Mi.,Do.)",
         "Gewaltfreie Psychiatrie jetzt!", date(2026, 11, 25), date(2026, 11, 26),
         "jeweils Mi., Do."),
        ("Kundgebung (vom 03.10. bis 02.12.2026 - jeweils Mi.,Sa.,So.)",
         "Kundgebung", date(2026, 10, 3), date(2026, 12, 2), "jeweils Mi., Sa., So."),
        ("Kundgebung (vom 05.01. bis 28.12.2026 - jeweils )",
         "Kundgebung", date(2026, 1, 5), date(2026, 12, 28), None),
        # Beginn ohne Jahr, der nach dem Ende laege -> Vorjahr
        ("Mahnwache (vom 01.12. bis 31.01.2027 - täglich)",
         "Mahnwache", date(2026, 12, 1), date(2027, 1, 31), "täglich"),
        # Altdaten-Varianten
        ("Mahnwache (vom 01.10. bis 30.12.2019 ? täglich)",
         "Mahnwache", date(2019, 10, 1), date(2019, 12, 30), "täglich"),
        ("Mahnwache (vom 07.01.19 bis 30.12.2019 - jeweils Mo.)",
         "Mahnwache", date(2019, 1, 7), date(2019, 12, 30), "jeweils Mo."),
        ("Mahnwache (vom 07.01.2019 - 30.12.2019 - jeweils Mo.)",
         "Mahnwache", date(2019, 1, 7), date(2019, 12, 30), "jeweils Mo."),
        ("Mahnwache (vom 07.01. 2019 bis 30.12.2019 - täglich)",
         "Mahnwache", date(2019, 1, 7), date(2019, 12, 30), "täglich"),
        # Suffix doppelt angehaengt
        ("Pandemie- Diktat! (vom 19.01. bis 28.12.2026 - jeweils Mo.) "
         "(vom 19.01. bis 28.12.2026 - jeweils Mo.)",
         "Pandemie- Diktat!", date(2026, 1, 19), date(2026, 12, 28), "jeweils Mo."),
        # Tippfehler der Quelle: bleibt Einzeltermin, Thema unveraendert
        ("Ohne JESUS kein Frieden (vom 16.01. bis 31.1202027,Sa.)",
         "Ohne JESUS kein Frieden (vom 16.01. bis 31.1202027,Sa.)", None, None, None),
        ("Mietenstopp jetzt", "Mietenstopp jetzt", None, None, None),
    ],
)
def test_split_serie(raw, thema, von, bis, rhythmus):
    assert split_serie(raw) == (thema, von, bis, rhythmus)


def _berlin(datum, thema, von="18:00", bis="19:00"):
    return {"id": 1, "datum": datum, "von": von, "bis": bis, "thema": thema,
            "plz": "10557", "strasse_nr": "Platz der Republik 1", "aufzugsstrecke": ""}


def test_serie_ueberlebt_verlaengerung():
    """Neues Serienende darf weder Termin- noch Serien-ID aendern."""
    alt = parse_record(_berlin("29.09.2026", "Dauermahnwache (vom 01.08. bis 01.10.2026 - täglich)"))
    neu = parse_record(_berlin("29.09.2026", "Dauermahnwache (vom 01.08. bis 01.11.2026 - täglich)"))
    anderer_tag = parse_record(_berlin("30.09.2026", "Dauermahnwache (vom 01.08. bis 01.11.2026 - täglich)"))
    assert alt["quelle_id"] == neu["quelle_id"]
    assert alt["serie_id"] == neu["serie_id"] == anderer_tag["serie_id"]
    assert alt["quelle_id"] != anderer_tag["quelle_id"]
    assert neu["serie_bis"] == date(2026, 11, 1)
    assert alt["thema_hash"] == anderer_tag["thema_hash"]


def test_einzeltermin_ohne_serie():
    row = parse_record(_berlin("01.10.2026", ",,Mietenstopp jetzt''"))
    assert row["serie_id"] is None
    assert row["thema"] == "„Mietenstopp jetzt“"
    assert row["erfassung"] == "liste"


@pytest.mark.parametrize(
    "von,bis,expected",
    [
        (time(0), time(23, 59), True),
        (time(0), time(0), True),
        (time(0), None, True),
        (time(0), time(14), False),
        (time(12), time(15), False),
        (None, None, False),
    ],
)
def test_ganztaegig(von, bis, expected):
    assert is_ganztaegig(von, bis) is expected
