"""Strompreise: Leser fuer energy-charts und Rechenregeln.

Die Beispielantworten sind nach der Dokumentation der API gebaut, nicht
aufgezeichnet (der Entwicklungsrechner erreicht api.energy-charts.info nicht).
Vor dem ersten echten Lauf deshalb: fetch_strom.py ohne --schreiben.
"""

from datetime import date, datetime, timezone

import pytest

import fetch_strom
from lib import strom

LIZENZ = "CC BY 4.0 (creativecommons.org/licenses/by/4.0) from Bundesnetzagentur | SMARD.de"


def _antwort(start_utc: datetime, preise, schritt=900, **mehr):
    t0 = int(start_utc.replace(tzinfo=timezone.utc).timestamp())
    return {"license_info": LIZENZ, "unix_seconds": [t0 + i * schritt for i in range(len(preise))],
            "price": list(preise), "unit": "EUR / MWh", "deprecated": False, **mehr}


def test_viertelstunden_in_marktzeit():
    # 27.11.2026 23:00 UTC = 28.11. 00:00 in Berlin (Winterzeit)
    zeilen = strom.preise_lesen(_antwort(datetime(2026, 11, 27, 23, 0), [80.0, 82.5, 79.25, 81.0, 90.0]))
    assert len(zeilen) == 5
    assert zeilen[0] == {"zeit_utc": datetime(2026, 11, 27, 23, 0), "minuten": 15,
                         "liefertag": date(2026, 11, 28), "stunde": 0, "eur_mwh": 80.0}
    assert zeilen[4]["stunde"] == 1 and zeilen[4]["minuten"] == 15


def test_stundenwerte_und_luecken():
    zeilen = strom.preise_lesen(_antwort(datetime(2026, 6, 30, 22, 0), [50, None, -12.345], schritt=3600))
    assert [z["minuten"] for z in zeilen] == [60, 60]
    assert zeilen[0]["liefertag"] == date(2026, 7, 1) and zeilen[0]["stunde"] == 0  # Sommerzeit
    assert zeilen[1]["eur_mwh"] == -12.35 or zeilen[1]["eur_mwh"] == -12.34
    assert zeilen[1]["stunde"] == 2


@pytest.mark.parametrize(
    "aendern, grund",
    [
        (lambda a: a.pop("unit"), "unit"),
        (lambda a: a.update(unit="GBP / MWh"), "Einheit"),
        (lambda a: a.update(license_info="private use only"), "Lizenz"),
        (lambda a: a.update(deprecated=True), "veraltet"),
        (lambda a: a["price"].pop(), "passen nicht"),
        (lambda a: a["price"].__setitem__(0, 99999), "Boersengrenzen"),
        (lambda a: a["price"].__setitem__(0, "84"), "Boersengrenzen"),
        (lambda a: a["unix_seconds"].reverse(), "aufsteigend"),
    ],
)
def test_leser_bricht_bei_unerwartetem_format_ab(aendern, grund):
    a = _antwort(datetime(2026, 11, 27, 23, 0), [80.0, 82.5, 79.0])
    aendern(a)
    with pytest.raises(strom.FormatFehler, match=grund):
        strom.preise_lesen(a)


def test_unerwarteter_abstand_bricht_ab():
    with pytest.raises(strom.FormatFehler, match="Abstand"):
        strom.preise_lesen(_antwort(datetime(2026, 11, 27, 23, 0), [1, 2, 3], schritt=7200))


@pytest.mark.parametrize("tag, minuten", [(date(2026, 11, 28), 1440), (date(2026, 3, 29), 1380), (date(2026, 10, 25), 1500)])
def test_tageslaenge_mit_zeitumstellung(tag, minuten):
    assert strom.erwartete_minuten(tag) == minuten


def test_tag_vollstaendig_auch_am_tag_mit_25_stunden():
    # 24.10.2026 22:00 UTC = 25.10. 00:00 Sommerzeit; der Tag hat 100 Viertelstunden
    zeilen = strom.preise_lesen(_antwort(datetime(2026, 10, 24, 22, 0), [50.0] * 101))
    assert strom.tag_vollstaendig(zeilen[:100], date(2026, 10, 25))
    assert not strom.tag_vollstaendig(zeilen[:96], date(2026, 10, 25))
    # Stunde 2 kommt doppelt vor: 8 Viertelstunden
    assert sum(1 for z in zeilen[:100] if z["stunde"] == 2) == 8


def test_stundenmittel_gewichtet_nach_dauer():
    zeilen = [
        {"stunde": 12, "minuten": 15, "eur_mwh": 10}, {"stunde": 12, "minuten": 15, "eur_mwh": 20},
        {"stunde": 12, "minuten": 30, "eur_mwh": 40}, {"stunde": 13, "minuten": 60, "eur_mwh": -5},
    ]
    assert strom.stundenmittel(zeilen) == {12: 27.5, 13: -5}


def test_guenstigstes_fenster_zusammenhaengend():
    stunden = {h: 100.0 for h in range(24)}
    stunden.update({11: 30, 12: 20, 13: 25, 3: 10})
    assert strom.guenstigstes_fenster(stunden) == {"von": 11, "bis": 14, "eur_mwh": 25.0}
    # eine fehlende Stunde trennt das Fenster
    del stunden[12]
    f = strom.guenstigstes_fenster(stunden)
    assert not (f["von"] <= 12 < f["bis"])
    assert strom.guenstigstes_fenster({1: 5.0, 2: 6.0}) is None


def test_tagesbericht():
    je_zone = {
        "DE-LU": {h: (-10.0 if h == 13 else 80.0 + h) for h in range(24)},
        "SE4": {h: 20.0 for h in range(24)},
        "HU": {h: 150.0 for h in range(24)},
        "LEER": {},
    }
    b = strom.tagesbericht(je_zone)
    assert b["zonen"] == 3
    assert b["billigste"] == {"zone": "SE4", "eur_mwh": 20.0}
    assert b["teuerste"] == {"zone": "HU", "eur_mwh": 150.0}
    assert b["negativ"] == ["DE-LU"]
    assert b["heimat"]["billigste_stunde"] == {"stunde": 13, "eur_mwh": -10.0}
    assert b["heimat"]["teuerste_stunde"] == {"stunde": 23, "eur_mwh": 103.0}
    assert b["heimat"]["negative_stunden"] == 1
    assert strom.tagesbericht({}) is None


def test_stuecke_teilt_lange_zeitraeume():
    teile = list(fetch_strom.stuecke(date(2026, 1, 1), date(2026, 3, 5)))
    assert teile[0] == (date(2026, 1, 1), date(2026, 1, 31))
    assert teile[-1][1] == date(2026, 3, 5)
    assert all((b - a).days <= 30 for a, b in teile)
    assert list(fetch_strom.stuecke(date(2026, 5, 2), date(2026, 5, 2))) == [(date(2026, 5, 2), date(2026, 5, 2))]
