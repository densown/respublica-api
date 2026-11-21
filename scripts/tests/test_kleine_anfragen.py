"""Tracker Kleine Anfragen (Migration 021): Parsing, Zahlenpruefung, Upsert.

Der Beispieltext ist dem Aufbau echter DIP-Volltexte nachgebaut (Drucksache
21/8937 vom 04.11.2026, gekuerzt): Kopf, gesperrte Vorbemerkung der
Fragesteller, Frage, Antwort.
"""

import json

import pytest

import fetch_kleine_anfragen as fka
import summarize_kleine_anfragen as ska
from lib import kleine_anfragen as ka

TEXT = """Deutscher Bundestag Drucksache 21/8937
21. Wahlperiode 04.11.2026
Antwort
der Bundesregierung
auf die Kleine Anfrage der Abgeordneten Erika Muster, Max Beispiel, weiterer Abgeordneter
und der Fraktion Die Linke
– Drucksache 21/8600 –
Regionale Verteilung der Kriegsdienstverweigerung
V o r b e m e r k u n g  d e r  F r a g e s t e l l e r
Nach Angaben der
Bundesregierung wurden im Jahr 2024 insgesamt 2 998 Anträge gestellt, im Jahr 2025 waren es 7 691.
Die Fragestellenden schätzen 50 000 Verweigerer bis 2027.
V o r b e m e r k u n g  d e r  B u n d e s r e g i e r u n g
Alle Angaben in dieser Antwort beziehen sich auf den Stichtag zum 30. September 2026.
1. Wie viele Anträge wurden in den Jahren 2011 bis 2025 gestellt, und wie
viele davon 2019?
Im Jahr 2011 wurden 3 526 Anträge auf Kriegsdienstverweigerung gestellt, im
Jahr 2012 490 und im Jahr 2013 350.
2. Wie viele wurden anerkannt?
Im Jahr 2026 wurden bislang 7 025 Anträge anerkannt.
Angaben nach Bundesländern werden statistisch nicht erfasst.
Die Antwort wurde namens der Bundesregierung mit Schreiben des Bundesministeriums der Verteidigung übermittelt.
"""


def test_fraktion_kuerzel():
    assert ka.fraktion_kuerzel(["Fraktion der AfD"]) == "afd"
    assert ka.fraktion_kuerzel(["Fraktion DIE LINKE"]) == "linke"
    assert ka.fraktion_kuerzel(["Fraktion BÜNDNIS 90/DIE GRÜNEN"]) == "gruene"
    assert ka.fraktion_kuerzel(["Fraktion der CDU/CSU", "Fraktion der SPD"]) == "mehrere"
    assert ka.fraktion_kuerzel(["Gruppe BSW"]) == "bsw"
    assert ka.fraktion_kuerzel(["Bundesregierung"]) is None
    assert ka.fraktion_kuerzel([]) is None


def test_titel_und_frage_drucksache():
    roh = "auf die Kleine Anfrage\r\n- Drucksache 21/8797 -\r\nPolitisch motivierte Kriminalität-rechts im dritten Quartal 2026"
    assert ka.titel_bereinigen(roh) == "Politisch motivierte Kriminalität-rechts im dritten Quartal 2026"
    assert ka.frage_drucksache(roh) == "21/8797"
    assert ka.einzelfragen("(insgesamt 8 Einzelfragen)") == 8
    assert ka.einzelfragen(None) is None


def test_fragesteller_aus_dem_kopf():
    assert ka.fragesteller(TEXT) == ["Erika Muster", "Max Beispiel"]
    assert ka.fragesteller("kein Kopf") == []


def test_bereich_fragesteller_endet_vor_regierung():
    a, b = ka.bereich_fragesteller(TEXT)
    teil = TEXT[a:b]
    assert "2 998" in teil and "50 000" in teil
    assert "Stichtag" not in teil


@pytest.mark.parametrize(
    "zitat, wert, erwartet",
    [
        ("Im Jahr 2026 wurden bislang 7 025 Anträge anerkannt.", "7 025", True),
        ("Im Jahr 2026 wurden bislang 7 025 Anträge anerkannt.", "7.025", True),
        # Zeilenumbruch im Original, im Zitat Leerzeichen
        ("3 526 Anträge auf Kriegsdienstverweigerung gestellt, im Jahr 2012 490", "3 526", True),
        # Zahl stimmt, Zitat erfunden
        ("Im Jahr 2026 wurden 7 025 Anträge abgelehnt.", "7 025", False),
        # Zahl aus der Vorbemerkung der Fragesteller
        ("im Jahr 2025 waren es 7 691", "7 691", False),
        ("Die Fragestellenden schätzen 50 000 Verweigerer bis 2027.", "50 000", False),
        # Zitat ohne die Zahl
        ("Im Jahr 2026 wurden bislang 7 025 Anträge anerkannt.", "9 999", False),
        # Frage statt Antwort
        ("viele davon 2019?", "2019", False),
        ("zu kurz", "7", False),
    ],
)
def test_zahl_belegt(zitat, wert, erwartet):
    assert ka.zahl_belegt(TEXT, zitat, wert) is erwartet


def test_merkmale():
    m = ka.merkmale(TEXT)
    assert m["nicht_erhoben"] == 1
    assert m["hat_anlage"] == 0
    assert m["eingestuft"] == 0
    m2 = ka.merkmale("Die Anlage 1 ist als VS-Nur für den Dienstgebrauch eingestuft. Liegen keine Erkenntnisse vor.")
    assert m2 == {"nicht_erhoben": 1, "hat_anlage": 1, "eingestuft": 1}


def test_fuer_prompt_markiert_und_kuerzt():
    p = ka.fuer_prompt(TEXT, 10_000)
    assert "[BEGINN VORBEMERKUNG DER FRAGESTELLER" in p
    assert p.index("[ENDE VORBEMERKUNG DER FRAGESTELLER]") < p.index("Stichtag")
    assert ka.fuer_prompt("x" * 500, 100).endswith("[... TEXT GEKUERZT ...]")


GUT = {
    "zusammenfassung": "Bis zum 30. September 2026 wurden 7 025 Anträge auf Kriegsdienstverweigerung anerkannt. "
    "Angaben nach Bundesländern erhebt die Bundesregierung nicht.",
    "zusammenfassung_en": "By 30 September 2026, 7,025 applications were approved.",
    "schlagzeile": "7 025 anerkannte Kriegsdienstverweigerer bis September 2026",
    "kernzahlen": [
        {"wert": "7 025", "einheit": "Anträge", "bedeutung": "anerkannt 2026",
         "zitat": "Im Jahr 2026 wurden bislang 7 025 Anträge anerkannt."},
        {"wert": "7 691", "einheit": "Anträge", "bedeutung": "2025", "zitat": "im Jahr 2025 waren es 7 691"},
    ],
    "story_wert": 9,
    "story_grund": "Starker Anstieg.",
}


def test_pruefen_behaelt_nur_belegte_kernzahlen():
    felder, grund = ska.pruefen(GUT, TEXT)
    assert grund is None
    assert [k["wert"] for k in felder["kernzahlen"]] == ["7 025"]
    assert felder["story_wert"] == 9


@pytest.mark.parametrize(
    "aenderung, teil_des_grunds",
    [
        ({"zusammenfassung": "Es wurden 7 691 Anträge gestellt."}, "7691"),  # Zahl der Fragesteller
        ({"schlagzeile": "Verweigerer verdoppelt: plus 134 Prozent"}, "134"),  # selbst gerechnet
        ({"story_wert": "viel"}, "story_wert"),
        ({"schlagzeile": ""}, "schlagzeile"),
        ({"zusammenfassung": "Ein Satz — mit Gedankenstrich."}, "Gedankenstrich"),
    ],
)
def test_pruefen_lehnt_ab(aenderung, teil_des_grunds):
    felder, grund = ska.pruefen({**GUT, **aenderung}, TEXT)
    assert felder is None
    assert teil_des_grunds in grund


def test_antwort_lesen_mit_zaun():
    roh = "Hier:\n```json\n" + json.dumps(GUT, ensure_ascii=False) + "\n```"
    assert ska.antwort_lesen(roh)["story_wert"] == 9
    assert ska.antwort_lesen("kein json") is None


def test_zeile_aus_dip_dokument():
    doc = {
        "id": "292574",
        "dokumentnummer": "21/8937",
        "wahlperiode": 21,
        "datum": "2026-11-04",
        "aktualisiert": "2026-11-18T14:45:19+01:00",
        "titel": "auf die Kleine Anfrage\r\n- Drucksache 21/8600 -\r\nRegionale Verteilung der Kriegsdienstverweigerung",
        "vorgangsbezug": [{"id": "340858", "vorgangstyp": "Kleine Anfrage"}],
        "ressort": [{"federfuehrend": True, "titel": "Bundesministerium der Verteidigung"}],
        "fundstelle": {"pdf_url": "https://dserver.bundestag.de/btd/21/089/2108937.pdf"},
        "text": TEXT,
    }
    vorgang = {
        "id": "340858",
        "abstract": "(insgesamt 6 Einzelfragen)",
        "initiative": ["Fraktion DIE LINKE"],
        "sachgebiet": ["Verteidigung"],
        "deskriptor": [{"name": "Kriegsdienstverweigerung"}],
    }
    z = fka.zeile(doc, vorgang)
    assert z["id"] == 292574
    assert z["titel"] == "Regionale Verteilung der Kriegsdienstverweigerung"
    assert z["frage_dokumentnummer"] == "21/8600"
    assert z["fraktion"] == "linke"
    assert z["einzelfragen"] == 6
    assert json.loads(z["fragesteller"]) == ["Erika Muster", "Max Beispiel"]
    assert json.loads(z["sachgebiete"]) == ["Verteidigung"]
    assert z["dip_aktualisiert"] == "2026-11-18 14:45:19"
    assert z["nicht_erhoben"] == 1
    assert set(fka.SPALTEN) <= set(z)

    ohne = fka.zeile({**doc, "text": ""}, None)
    assert ohne["volltext"] is None and ohne["fragesteller"] is None and ohne["fraktion"] is None


def test_upsert_ueberschreibt_volltext_nicht_mit_null():
    sql = fka.upsert_sql()
    assert "volltext = COALESCE(VALUES(volltext), volltext)" in sql
    assert "nicht_erhoben = IF(VALUES(volltext) IS NULL" in sql
    assert sql.count("%s") == len(fka.SPALTEN)


def test_kleine_anfrage_bezug():
    assert fka.kleine_anfrage_bezug({"vorgangsbezug": [{"id": "1", "vorgangstyp": "Große Anfrage"}]}) is None
    assert fka.kleine_anfrage_bezug({"vorgangsbezug": [{"id": "2", "vorgangstyp": "Kleine Anfrage"}]})["id"] == "2"
