"""EU-Parlament: Leser fuer den Export von HowTheyVote.eu und Pruefung der Titel-Uebersetzung.

Beispieldateien unter daten/ep/ sind ein Ausschnitt des echten Exports vom
28.11.2026: drei Hauptabstimmungen vom 26.11.2026, eine alte von 2019 und ein
Aenderungsantrag, dazu die Stimmen aus Deutschland, Luxemburg und Malta.
"""

import csv
from collections import Counter
from pathlib import Path

import pytest

import uebersetze_ep_titel as ue
from lib import ep

DATEN = Path(__file__).parent / "daten" / "ep"


def _abst():
    return ep.hauptabstimmungen(ep.csv_zeilen(DATEN / "votes.csv"))


def _stimmen(ids):
    with open(DATEN / "member_votes.csv", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        ep.kopf_pruefen(next(r), ep.STIMMEN_KOPF, "member_votes.csv")
        return list(ep.stimmen(r, ids))


def test_nur_hauptabstimmungen_der_laufenden_periode():
    abst = _abst()
    assert sorted(a["id"] for a in abst) == [201321, 201496, 201499]


def test_felder_einer_abstimmung():
    a = next(x for x in _abst() if x["id"] == 201321)
    assert a["titel_en"].startswith("Reduction of methane emissions")
    assert (a["ja"], a["nein"], a["enthaltung"], a["nicht_abgestimmt"]) == (341, 224, 31, 124)
    assert a["ergebnis"] == "angenommen"
    assert a["art"] == "einigung"
    assert a["verfahren"] == "2026/0325(COD)" and a["verfahren_typ"] == "COD" and a["lesung"] == 1
    assert a["zeit"].isoformat() == "2026-11-26T12:08:29"


def test_ablehnungsantrag_wird_erkannt():
    # "Rejet - Am 33", abgelehnt: der Antrag, den Text zu verwerfen, ist gescheitert
    a = next(x for x in _abst() if x["id"] == 201499)
    assert a["art"] == "ablehnungsantrag" and a["ergebnis"] == "abgelehnt"


@pytest.mark.parametrize(
    "beschreibung, subject, erwartet",
    [
        ("Proposition de résolution (ensemble du texte)", "", "text"),
        ("Proposition de la Commission", "", "text"),
        ("Accord provisoire - Am 2", "Provisional agreement", "einigung"),
        ("Proposition de rejet - Am 90= 104=", "Proposal for rejection", "ablehnungsantrag"),
        ("Proposition de rejet de la position du Conseil - Am 1= 2=", "", "ablehnungsantrag"),
        ("Décision d'engager des négociations interinstitutionnelles", "", "verhandlungsmandat"),
        ("Demande de décision d'urgence", "", "dringlichkeit"),
        ("", "", "text"),
    ],
)
def test_art(beschreibung, subject, erwartet):
    assert ep.art(beschreibung, subject) == erwartet


def test_einzelstimmen_nur_gewuenschte_abstimmungen_und_deutsche_summen():
    zeilen = _stimmen({201321, 201499, 201496})
    assert {z[0] for z in zeilen} == {201321, 201499, 201496}
    assert {z[2] for z in zeilen} <= {"ja", "nein", "enthaltung", "nicht"}
    de = ep.deutsche_stimmen(zeilen)
    assert de[201321] == {"ja": 43, "nein": 34, "enthaltung": 2, "nicht": 17}
    assert sum(de[201499].values()) == 96


def test_deutsche_parteinamen_in_ueblicher_schreibweise():
    parteien = {z[5] for z in _stimmen({201321}) if z[3] == "DEU"}
    assert {"Grüne", "Linke", "Volt"} <= parteien
    assert "GRÜNE." not in parteien and "V" not in parteien


def test_geaenderte_spalten_brechen_ab():
    with pytest.raises(ValueError):
        ep.kopf_pruefen(["vote_id", "position", "member_id"], ep.STIMMEN_KOPF, "member_votes.csv")


def test_abgeordnete_mit_aktueller_fraktion_und_partei():
    abg = ep.abgeordnete(
        ep.csv_zeilen(DATEN / "members.csv"),
        ep.csv_zeilen(DATEN / "group_memberships.csv"),
        ep.csv_zeilen(DATEN / "national_party_memberships.csv"),
        ep.csv_zeilen(DATEN / "national_parties.csv"),
    )
    deutsche = [a for a in abg if a["land"] == "DEU" and a["im_amt"]]
    assert len(deutsche) == 96
    namen = {a["nachname"] for a in deutsche}
    assert {"McAllister", "von der Schulenburg", "Strack-Zimmermann", "De Masi", "Boßdorf"} <= namen
    assert Counter(a["fraktion"] for a in deutsche)["EPP"] == 31


@pytest.mark.parametrize(
    "roh, erwartet",
    [
        ("MCALLISTER", "McAllister"),
        ("VON DER SCHULENBURG", "von der Schulenburg"),
        ("VAN DER LEYEN", "van der Leyen"),
        ("DI RUPO", "Di Rupo"),
        ("GARCÍA PÉREZ", "García Pérez"),
        ("D'ANGELO", "D'Angelo"),
        ("Körner", "Körner"),
    ],
)
def test_nachname(roh, erwartet):
    assert ep.nachname_schreiben(roh) == erwartet


def _gute_daten():
    abst = _abst()
    zahl = Counter({a["id"]: a["ja"] + a["nein"] + a["enthaltung"] + a["nicht_abgestimmt"] for a in abst})
    abg = [{"land": "DEU", "im_amt": 1}] * 96 + [{"land": "FRA", "im_amt": 1}] * 624
    return abst * 300, zahl, abg


def test_pruefung_besteht_mit_stimmigen_daten():
    abst, zahl, abg = _gute_daten()
    assert ep.pruefen(abst, zahl, 766, abg, 9) == []


def test_pruefung_faengt_schrumpfenden_export():
    abst, zahl, abg = _gute_daten()
    fehler = ep.pruefen(abst[:750], zahl, 766, abg, 9)
    assert any("766" in f for f in fehler)


def test_pruefung_faengt_fehlende_einzelstimmen():
    abst, zahl, abg = _gute_daten()
    zahl[201321] -= 5
    assert any("Einzelstimmen" in f for f in ep.pruefen(abst, zahl, 0, abg, 9))


def test_pruefung_faengt_fehlende_deutsche():
    abst, zahl, abg = _gute_daten()
    assert any("deutsche" in f for f in ep.pruefen(abst, zahl, 0, abg[50:], 9))


# --- Titel-Uebersetzung -----------------------------------------------------

def test_antwort_lesen_nimmt_json_aus_umgebendem_text():
    roh = 'Hier:\n```json\n{"1": "Lage in  Venezuela", "2": "Haushalt 2027", "9": "zu viel"}\n```'
    assert ue.antwort_lesen(roh, 2) == {0: "Lage in Venezuela", 1: "Haushalt 2027"}
    assert ue.antwort_lesen("kein json", 2) == {}
    assert ue.antwort_lesen(None, 2) == {}


@pytest.mark.parametrize(
    "original, deutsch, ok",
    [
        ("The situation in Venezuela", "Die Lage in Venezuela", True),
        ("General budget of the European Union for the financial year 2027",
         "Gesamthaushaltsplan der Europäischen Union für das Haushaltsjahr 2027", True),
        ("General budget of the European Union for the financial year 2027",
         "Gesamthaushaltsplan der Europäischen Union für das Haushaltsjahr 2026", False),
        ("The situation in Venezuela", "", False),
        ("The situation in Venezuela", "Die Lage in Venezuela – eine Einordnung der Krise", False),
        ("The situation in Venezuela", "The situation in Venezuela", False),
        ("Reduction of methane emissions in the energy sector: postponement of certain obligations",
         "Methan", False),
        ("Erasmus+", "Erasmus+", True),
    ],
)
def test_uebersetzung_pruefen(original, deutsch, ok):
    assert (ue.pruefen(original, deutsch) is None) is ok


def test_prompt_nummeriert_die_titel():
    p = ue.prompt_bauen(["Alpha", "Beta"])
    assert "1. Alpha" in p and "2. Beta" in p and "JSON" in p
