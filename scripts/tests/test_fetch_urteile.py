"""Titel der Urteile-Feeds und XML der Entscheidungen lesen."""

from datetime import date

import pytest

from backfill_urteile_metadaten import parse_dokument
from fetch_urteile import parse_title


@pytest.mark.parametrize(
    ("title", "gericht", "erwartet"),
    [
        # Format seit Ende Maerz 2026: Titel beginnt mit dem Gerichtskuerzel
        (
            "BGH 10. Zivilsenat, Urteil vom 08.09.2026, X ZR 76/24",
            "BGH",
            ("10. Zivilsenat", "Urteil", date(2026, 9, 8), "X ZR 76/24"),
        ),
        # Frueher stand etwas davor, das muss weiter gehen
        (
            "\n  BGH 6a. Zivilsenat, Urteil vom 17.03.2026, VIa ZR 110/23",
            "BGH",
            ("6a. Zivilsenat", "Urteil", date(2026, 3, 17), "VIa ZR 110/23"),
        ),
        # Typen ausser Urteil, Beschluss, Gerichtsbescheid
        (
            "BVerfG 2. Senat 1. Kammer, Ablehnung einstweilige Anordnung vom 01.10.2026, 2 BvR 1234/26",
            "BVerfG",
            ("2. Senat 1. Kammer", "Ablehnung einstweilige Anordnung", date(2026, 10, 1), "2 BvR 1234/26"),
        ),
        (
            "BVerfG 1. Senat, Einstweilige Anordnung vom 30.09.2026, 1 BvQ 50/26",
            "BVerfG",
            ("1. Senat", "Einstweilige Anordnung", date(2026, 9, 30), "1 BvQ 50/26"),
        ),
        (
            "BPatG 35. Senat, Beschluss vom 12.08.2026, 35 W (pat) 4/24",
            "BPatG",
            ("35. Senat", "Beschluss", date(2026, 8, 12), "35 W (pat) 4/24"),
        ),
        # Ort vor dem Senat, wie im BPatG-Feed vom Oktober 2026
        (
            "BPatG München 6. Senat, Urteil vom 09.07.2026, 6 Ni 10/25",
            "BPatG",
            ("6. Senat", "Urteil", date(2026, 7, 9), "6 Ni 10/25"),
        ),
        # Senatsnamen ohne Nummer bleiben, wie sie sind
        (
            "BGH Großer Senat für Zivilsachen, Beschluss vom 01.09.2026, GSZ 1/25",
            "BGH",
            ("Großer Senat für Zivilsachen", "Beschluss", date(2026, 9, 1), "GSZ 1/25"),
        ),
        (
            "BVerwG 2. Wehrdienstsenat, Beschluss vom 13.08.2026, 2 WDB 3.26",
            "BVerwG",
            ("2. Wehrdienstsenat", "Beschluss", date(2026, 8, 13), "2 WDB 3.26"),
        ),
    ],
)
def test_parse_title(title, gericht, erwartet):
    assert parse_title(title, gericht) == erwartet


@pytest.mark.parametrize(
    "title",
    [
        "Pressemitteilung zum Urteil",
        "BGH 1. Strafsenat, Beschluss vom 31.02.2026, 1 StR 1/26",  # kein Kalenderdatum
        "BVerwG 3. Senat, Urteil vom 01.09.2026, 3 C 1/25",  # anderes Gericht als der Feed
    ],
)
def test_parse_title_ohne_treffer(title):
    assert parse_title(title, "BGH") == (None, None, None, None)


XML = """<?xml version="1.0" encoding="utf-8"?>
<dokument>
  <doknr>KORE712052026</doknr>
  <ecli>ECLI:DE:BGH:2026:080926UXZR76.24.0</ecli>
  <gertyp>BGH</gertyp>
  <gerort></gerort>
  <spruchkoerper>10. Zivilsenat</spruchkoerper>
  <entsch-datum>20260908</entsch-datum>
  <aktenzeichen>X ZR 76/24</aktenzeichen>
  <doktyp>Urteil</doktyp>
  <tenor><p>Die Revision wird zurueckgewiesen.</p></tenor>
</dokument>
"""


def test_parse_dokument():
    assert parse_dokument(XML.encode("utf-8")) == (
        "10. Zivilsenat",
        "Urteil",
        date(2026, 9, 8),
        "X ZR 76/24",
    )


def test_parse_dokument_ohne_datum():
    xml = XML.replace("<entsch-datum>20260908</entsch-datum>", "<entsch-datum></entsch-datum>")
    senat, typ, datum, az = parse_dokument(xml.encode("utf-8"))
    assert datum is None
    assert (senat, typ, az) == ("10. Zivilsenat", "Urteil", "X ZR 76/24")
