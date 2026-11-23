"""Leser und Pruefungen fuer amtliche Wahldateien (ohne Netz, ohne DB)."""
import pytest

from lib import wahl_amtlich as wa

KERG2_EW = """# Europawahl 2024
# Endgültiges Ergebnis
# Stand: 03.07.2024
Wahlart;Wahltag;Gebietsart;Gebietsnummer;Gebietsname;UegGebietsart;UegGebietsnummer;Gruppenart;Gruppenname;Gruppenreihenfolge;Stimme;Anzahl;Prozent;VorpAnzahl;VorpProzent;DiffProzent;DiffProzentPkt;Bemerkung
EP;09.06.2024;Bund;99;Bundesgebiet;;;System-Gruppe;Wahlberechtigte;-4;;300;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;System-Gruppe;Wählende;-3;;200;66,6;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;System-Gruppe;Ungültige;-2;1;10;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;System-Gruppe;Gültige;-1;1;190;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;Partei;CDU;1;1;50;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;Partei;CSU;2;1;20;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;Partei;GRÜNE;3;1;60;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;Partei;Die Linke;4;1;30;;;;;;
EP;09.06.2024;Bund;99;Bundesgebiet;;;Partei;Volt;5;1;30;;;;;;
EP;09.06.2024;Land;1;Schleswig-Holstein;BUND;99;Partei;CDU;1;1;999;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;System-Gruppe;Wahlberechtigte;-4;;100;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;System-Gruppe;Wählende;-3;;80;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;System-Gruppe;Ungültige;-2;1;4;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;System-Gruppe;Gültige;-1;1;76;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;Partei;CDU;1;1;20;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;Partei;GRÜNE;3;1;40;;;;;;
EP;09.06.2024;Kreis;01001;Flensburg, Stadt;LAND;01;Partei;Volt;5;1;16;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;System-Gruppe;Wahlberechtigte;-4;;200;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;System-Gruppe;Wählende;-3;;120;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;System-Gruppe;Ungültige;-2;1;6;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;System-Gruppe;Gültige;-1;1;114;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;Partei;CDU;1;1;30;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;Partei;CSU;2;1;20;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;Partei;GRÜNE;3;1;20;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;Partei;Die Linke;4;1;30;;;;;;
EP;09.06.2024;Kreis;09162;München;LAND;09;Partei;Volt;5;1;14;;;;;;
"""

KREIS_BTW = """# Bundestagswahl 2025
# Endgültige Ergebnisse nach kreisfreien Städten und Landkreisen
#
#
Land;Statistische Kennziffer;Kreisfreie Stadt bzw. Stadtkreis, Landkreis bzw. Kreis;Wahlberechtigte (A);Wählende (B);Ungültige - Erststimmen;Gültige - Erststimmen;SPD - Erststimmen;CDU - Erststimmen;Ungültige - Zweitstimmen;Gültige - Zweitstimmen;SPD - Zweitstimmen;CDU - Zweitstimmen;GRÜNE - Zweitstimmen;AfD - Zweitstimmen;CSU - Zweitstimmen;Die Linke - Zweitstimmen;Tierschutzpartei - Zweitstimmen
Berlin;11100;Berlin-Ost;1.000;800;9;791;400;391;10;790;200;150;100;200;;100;40
Berlin;11200;Berlin-West;2 000;1500;9;1491;700;791;20;1480;400;500;300;100;;100;80
Bayern;9162;München, Landeshauptstadt;500;400;1;399;199;200;5;395;80;;120;40;130;20;5
"""


def test_kerg2_europawahl_liest_kreise_und_bundeszeile():
    w = wa.lies_kerg2(KERG2_EW)
    assert w["status"] == "endgueltig"
    assert str(w["datum"]) == "2024-06-09"
    assert set(w["kreise"]) == {"01001", "09162"}
    m = w["kreise"]["09162"]
    assert (m["eligible"], m["voters"], m["invalid"], m["valid"]) == (200, 120, 6, 114)
    assert m["stimmen"] == {"cdu_csu": 50, "gruene": 20, "linke_pds": 30}
    assert w["soll"]["stimmen"]["cdu_csu"] == 70
    assert wa.pruefe(w["kreise"], soll=w["soll"], erwartete_kreise=2) == []


def test_pruefung_schlaegt_an_wenn_ein_kreis_fehlt_oder_summe_abweicht():
    w = wa.lies_kerg2(KERG2_EW)
    del w["kreise"]["01001"]
    fehler = wa.pruefe(w["kreise"], soll=w["soll"], erwartete_kreise=2)
    assert any("erwartet 2" in f for f in fehler)
    assert any("Wahlberechtigte" in f and "amtliche Gesamtzeile" in f for f in fehler)


def test_pruefung_findet_unplausible_kreise():
    kreise = {"01001": {"eligible": 100, "voters": 120, "valid": 110, "invalid": 5, "stimmen": {"spd": 200}}}
    fehler = " ".join(wa.pruefe(kreise))
    assert "mehr Wählende" in fehler and "gültig + ungültig" in fehler and "Parteistimmen" in fehler


def test_kreisdatei_bundestagswahl_zaehlt_zweitstimmen_und_legt_berlin_zusammen():
    w = wa.lies_btw_kreis(KREIS_BTW)
    assert w["status"] == "endgueltig"
    kreise = wa.fasse_zusammen(w["kreise"])
    assert set(kreise) == {"11000", "09162"}
    b = kreise["11000"]
    assert (b["eligible"], b["voters"], b["invalid"], b["valid"]) == (3000, 2300, 30, 2270)
    assert b["stimmen"] == {"spd": 600, "cdu_csu": 650, "gruene": 400, "afd": 300, "linke_pds": 200}
    assert kreise["09162"]["stimmen"]["cdu_csu"] == 130


def test_zeilen_rechnen_anteile_rest_und_staerkste_partei():
    kreise = wa.fasse_zusammen(wa.lies_btw_kreis(KREIS_BTW)["kreise"])
    zeilen = {z["ags"]: z for z in wa.zu_zeilen(kreise, typ="federal", jahr=2025, datum=None)}
    b = zeilen["11000"]
    assert b["cdu_csu"] == round(650 / 2270, 6)
    assert b["other"] == round(120 / 2270, 6)
    assert b["winning_party"] == "cdu_csu"
    assert b["turnout"] == round(2300 / 3000, 6)
    assert b["fdp"] is None and b["state"] == "11" and b["election_type"] == "" and b["round"] == 0


def test_vergleich_meldet_neu_gleich_geaendert():
    kreise = wa.fasse_zusammen(wa.lies_btw_kreis(KREIS_BTW)["kreise"])
    zeilen = wa.zu_zeilen(kreise, typ="federal", jahr=2025, datum=None)
    alt = {"11000": {**zeilen[1], "number_voters": 2200}, "99999": {}}
    b = wa.vergleiche(zeilen, alt)
    assert (b["neu"], b["geaendert"], b["gleich"]) == (1, 1, 0)
    assert b["nur_in_db"] == ["99999"]


def test_unbekannter_aufbau_ist_ein_formatfehler():
    with pytest.raises(wa.FormatFehler):
        wa.lies_kerg2("irgendwas;anderes\n1;2\n")
    with pytest.raises(wa.FormatFehler):
        wa.zahl("12,5")
