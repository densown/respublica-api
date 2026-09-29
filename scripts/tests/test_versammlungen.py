"""Tests fuer die Versammlungs-Pipeline (nur reine Funktionen, keine DB/Netz)."""

from datetime import date, time

import pytest

from classify_versammlungen import parse_antwort
from fetch_versammlungen_berlin import (
    find_records,
    make_quelle_id,
    normalize_key,
    parse_datum,
    parse_payload,
    parse_record,
    parse_zeit,
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
    assert len(parse_payload({"index": [rec, dict(rec), {"thema": "kein Datum"}]})) == 1


def test_parse_antwort():
    text = 'Hier:\n```json\n{"1": "klima_umwelt", "2": "erfunden", "99": "nahost", "x": "nahost"}\n```'
    assert parse_antwort(text, {1, 2}) == {1: "klima_umwelt"}
    assert parse_antwort("kein json", {1}) == {}
    assert parse_antwort(None, {1}) == {}
