"""Leser der Landtagswahl-Dateien, mit woertlichen Zeilen aus den amtlichen Dateien von 2026."""
import io

import pytest

from lib import wahl_amtlich as wa

BW_KOPF = "Wahlkreisnummer;Wahlkreisname;AGS;Gemeindename;Gebietsnummer;Gebietsart;Gebietsname;Bezirksnummer;gemeldete Wahlbezirke;Anzahl Wahlbezirke;Ausgezaehlt abweichend Gebiet;Ausgezaehlt abweichend AGS;Ausbleibend;Wahlberechtigte gesamt (A);Wahlberechtigte ohne Wahlschein (A1);Wahlberechtigte mit Wahlschein (A2);Wahlberechtigte nicht im WVZ (A3);Waehler gesamt (B);Waehler ohne Wahlschein;Waehler mit Wahlschein;Erststimmen ungueltige (C);Erststimmen gueltige (D);D1;D2;D3;D4;D5;D6;D7;D8;D9;D11;D12;D13;D16;D17;D20;D21;D22;Zweitstimmen ungueltige (E);Zweitstimmen gueltige (F);F1;F2;F3;F4;F5;F6;F7;F8;F9;F10;F11;F12;F13;F14;F15;F16;F17;F18;F19;F20;F21;"
BW = "\n".join([
    BW_KOPF,
    ";;;;BW;LAND;Land Baden-Württemberg;;11570;11570;;;N;7764858;5681189;2083516;153;5406737;3452849;1953888;47550;5359187;1368015;1837689;450169;256137;1009984;265198;54559;8529;3091;5705;52207;4068;31293;644;2132;4659;;31628;5375109;1623208;1595929;298297;235598;1010574;236975;102716;22765;15126;2764;9339;45842;12533;2347;3526;76439;7315;9017;3649;49827;11323;",
    "1;01 - Stuttgart I;08111000;Stuttgart, Landeshauptstadt;1;WAHLKREIS;01 - Stuttgart I;;146;146;;;N;97613;59251;38339;23;73506;37079;36427;337;73169;34350;17400;3823;3336;4354;7023;;798;;;1703;;;;382;;;142;73364;36700;14377;2995;3981;4512;7060;454;354;98;58;88;1070;66;45;41;907;48;46;38;345;81;",
    "5;05 - Böblingen;08115002;Altdorf;08115002;GEMEINDE;Altdorf;;4;4;;;N;3449;2458;991;0;2697;1749;948;17;2680;765;906;305;132;428;78;;;;;21;;38;;;;7;8;2689;921;916;94;114;406;69;53;7;5;1;7;11;23;2;4;29;2;3;2;14;6;",
    "5;05 - Böblingen;08115002;Altdorf;001;URNENWAHLBEZIRK;Altdorf 1;001;1;1;;;N;900;700;200;0;500;500;0;3;497;1;1;1;1;1;1;;;;;1;;1;;;;1;2;498;100;100;10;10;50;5;5;1;1;1;1;1;1;1;1;5;1;1;1;1;1;",
    "65;65 - Ehingen;BW;Land Baden-Württemberg;001;BRIEFWAHLBEZIRK;ADK I;001;1;1;;;N;0;0;0;0;439;0;439;1;438;1;1;1;1;1;1;;;;;1;;1;;;;1;2;437;100;150;20;20;90;10;5;1;1;1;1;1;1;1;1;9;1;1;1;1;1;",
    "58;58 - Lörrach;BW;Land Baden-Württemberg;5-0833600005;BRIEFWAHLBEZIRK;Briefwahlbezirk Schönau u.a.;5-0833600005;1;1;;;N;0;0;0;0;558;0;558;1;557;1;1;1;1;1;1;;;;;1;;1;;;;1;3;555;200;150;20;20;90;10;5;1;1;1;1;1;1;1;1;9;1;1;1;1;1;",
])


def test_bw_kreise_aus_gemeinden_stadtwahlkreisen_und_sammelbriefwahl():
    w = wa.lies_bw(BW)
    assert set(w["kreise"]) == {"08111", "08115", "08425", "08336"}
    s = w["kreise"]["08111"]
    assert (s["eligible"], s["voters"], s["invalid"], s["valid"]) == (97613, 73506, 142, 73364)
    assert s["stimmen"]["gruene"] == 36700 and s["stimmen"]["cdu_csu"] == 14377 and s["stimmen"]["bsw"] == 907
    assert w["kreise"]["08115"]["valid"] == 2689          # Urnenbezirk zaehlt nicht doppelt
    assert w["kreise"]["08425"]["voters"] == 439 and w["kreise"]["08336"]["voters"] == 558
    assert w["soll"]["valid"] == 5375109 and w["soll"]["stimmen"]["afd"] == 1010574


def test_bw_unbekannter_sammelbezirk_ist_ein_fehler():
    kaputt = BW.replace("ADK I;", "Irgendwo;")
    with pytest.raises(wa.FormatFehler):
        wa.lies_bw(kaputt)


ST = '''"Ergebnisart";"Datum";"Satzart";"Schlüsselnummer";"Name";"Wahllokal";"A.Wahlberechtigte";"B.Wähler";"E.Ungültige.Zweitstimmen";"F.Gültige.Zweitstimmen";"F01.CDU";"F02.AfD";"F03.Die Linke";"F04.SPD";"F05.FDP";"F06.GRÜNE";"F07.FREIE WÄHLER";"F08.dieBasis";"F09.Tierschutzpartei";"F10.Gartenpartei";"F11.Die PARTEI";"F12.TIERSCHUTZALLIANZ";"F15.BSW";"F19.PdF";"F20.Volt";"C.Ungültige.Erststimmen";"D.Gültige.Erststimmen";"D01.CDU";"D02.AfD"
"E";"18.09.2026";"LAN";"15";"Sachsen-Anhalt";"";1706852;1327991;12709;1315282;226605;575971;112558;122296;33986;117494;15391;1897;14354;7479;3487;7296;69355;2773;4340;15278;1312713;314500;581899
"E";"17.09.2026";"KRS";"15001";"Dessau-Roßlau, Stadt";"";61152;47072;451;46621;8555;18137;3952;4709;1453;4937;456;52;566;230;120;260;2964;63;167;648;46424;13409;19128
"E";"17.09.2026";"KRS";"15001";"Dessau-Roßlau, Stadt";"B";0;9000;51;8949;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1
"E";"17.09.2026";"WKR";"26";"Dessau";"";61152;47072;451;46621;8555;18137;3952;4709;1453;4937;456;52;566;230;120;260;2964;63;167;648;46424;13409;19128
'''


def test_st_nimmt_nur_kreissummen():
    w = wa.lies_st(ST)
    assert w["status"] == "endgueltig"
    assert list(w["kreise"]) == ["15001"]
    k = w["kreise"]["15001"]
    assert (k["eligible"], k["voters"], k["invalid"], k["valid"]) == (61152, 47072, 451, 46621)
    assert k["stimmen"] == {"cdu_csu": 8555, "afd": 18137, "linke_pds": 3952, "spd": 4709, "fdp": 1453,
                            "gruene": 4937, "freie_waehler": 456, "die_partei": 120, "bsw": 2964}
    assert w["soll"]["stimmen"]["afd"] == 575971


MV_KOPF = "Berechnungsdatum;Ausgabe;Kreis;Kreisname;Amt;Amtsname;Gemeinde;Gemeindename;Wahlbezirke insg.;Erf. Wahlbezirke;Wahlberechtigte;Wähler;Wahlbeteiligung;Erst-/Zweitstimme;Ungültige Stimmen;Gültige Stimmen;SPD;AfD;CDU;Die Linke;GRÜNE;FDP;Tierschutzpartei;FREIE WÄHLER;Die PARTEI;PIRATEN;ÖDP;Bündnis C;BSW;Handwerker Partei Deutschland;KPD;PdF;Team Freiheit;Volt;WLD;LfK;Einzelbewerber"
MV = "\r\n".join([
    "Wahl zum Landtag von Mecklenburg-Vorpommern am 20. September 2026",
    "Endgültiges Ergebnis der Gemeinden - Stimmenanzahl der Parteien",
    'Beispiel: 13071751 / Die Partei "Handwerker Partei Deutschland" führt keine Kurzbezeichnung.',
    "(c) Der Landeswahlleiter Mecklenburg-Vorpommern", "", MV_KOPF,
    "29.09.2026 12:11:18;A;3;Rostock, Hanse- und Universitätsstadt;1;Rostock, Hanse- und Universitätsstadt;13003000;Rostock, Hanse- und Universitätsstadt;186;186;167351;130415;77,9;1;1399;129016;51700;33998;7063;21160;6764;2108;x;x;x;x;x;x;5880;x;x;x;x;343;x;x;x",
    "29.09.2026 12:11:18;A;3;Rostock, Hanse- und Universitätsstadt;1;Rostock, Hanse- und Universitätsstadt;13003000;Rostock, Hanse- und Universitätsstadt;186;186;167351;130415;77,9;2;850;129565;47642;33020;5718;16332;14643;1433;1488;369;469;338;167;103;6088;478;165;190;297;502;123;x;x",
    "29.09.2026 12:11:18;P;3;Rostock, Hanse- und Universitätsstadt;1;Rostock, Hanse- und Universitätsstadt;13003000;Rostock, Hanse- und Universitätsstadt;186;186;167351;130415;77,9;2;0,7;99,3;36,8;25,5;4,4;12,6;11,3;1,1;1,1;0,3;0,4;0,3;0,1;0,1;4,7;0,4;0,1;0,1;0,2;0,4;0,1;x;x",
    "29.09.2026 12:11:18;A;71;Mecklenburgische Seenplatte;5151;Demmin-Land;13071008;Beggerow;1;1;425;271;63,8;2;4;267;47;157;10;8;10;1;4;1;1;1;0;0;26;0;0;0;0;1;0;x;x",
    "29.09.2026 12:11:18;A;71;Mecklenburgische Seenplatte;5151;Demmin-Land;13071751;Briefwahl Demmin-Land;1;1;0;852;x;2;5;847;302;319;64;52;30;12;4;1;2;3;0;1;45;4;1;1;3;1;2;x;x",
])
MV_WK = "\r\n".join([
    "Wahl zum Landtag", "", "", "", "",
    MV_KOPF.replace("Kreis;Kreisname;Amt;Amtsname;Gemeinde;Gemeindename", "Wahlkreis;Wahlkreisname/Land"),
    "29.09.2026 12:11:18;A;99;Mecklenburg-Vorpommern;1974;1974;1312565;1024118;78,0;2;8795;1015323;360396;388050;49628;66381;57577;10419;11121;3827;2768;1983;967;821;49129;4797;815;936;2551;1906;1251;x;x",
])


def test_mv_summiert_gemeinden_und_briefwahl_zu_kreisen():
    w = wa.lies_mv(MV, MV_WK)
    assert w["status"] == "endgueltig"
    assert set(w["kreise"]) == {"13003", "13071"}
    assert w["kreise"]["13003"]["valid"] == 129565 and w["kreise"]["13003"]["stimmen"]["spd"] == 47642
    k = w["kreise"]["13071"]
    assert (k["eligible"], k["voters"], k["invalid"], k["valid"]) == (425, 1123, 9, 1114)
    assert k["stimmen"]["afd"] == 476 and k["stimmen"]["bsw"] == 71
    assert w["soll"]["eligible"] == 1312565 and w["soll"]["stimmen"]["afd"] == 388050


SL = "\n".join([
    "﻿Landtagswahl 2022;", "Amtliches Endergebnis;",
    "Nr;Gebiet;gehört zu;Christlich Demokratische Union Deutschlands;;Sozialdemokratische Partei Deutschlands;;DIE LINKE;;Alternative für Deutschland;;BÜNDNIS 90/DIE GRÜNEN;;Freie Demokratische Partei;;Wahlberechtigte;;Wähler;;Ungültige Stimmen;;Gültige Stimmen;;Übrige;;",
    ";;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;Stimmen;;",
    ";;;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;Endgültig;Vorperiode;",
    "10;Saarland;;129154;217263;196801;158057;11689;68566;25719;32971;22598;21392;21618;17419;746307;774951;458113;540290;5702;6507;452411;533783;;7555;",
    ";;",
    "1;Saarbrücken;10;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;1;;1;",
    "41100;Saarbrücken;1;15629;27200;28898;22919;2539;13953;3412;5019;6413;5975;4258;3751;121685;127531;69177;82753;699;916;68478;81837;;1398;",
    "46111;Freisen;3;1431;2311;1917;1614;94;386;221;248;90;88;99;68;6387;6640;4363;4967;67;66;4296;4901;;65;",
])


def test_sl_gemeinden_zu_kreisen_und_nur_aktuelle_spalten():
    w = wa.lies_sl(SL)
    assert set(w["kreise"]) == {"10041", "10046"}
    k = w["kreise"]["10041"]
    assert (k["eligible"], k["voters"], k["invalid"], k["valid"]) == (121685, 69177, 699, 68478)
    assert k["stimmen"] == {"cdu_csu": 15629, "spd": 28898, "linke_pds": 2539, "afd": 3412, "gruene": 6413, "fdp": 4258}
    assert w["soll"]["stimmen"]["spd"] == 196801 and w["status"] == "endgueltig"


def _xlsx(blaetter: dict) -> bytes:
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, zeilen in blaetter.items():
        ws = wb.create_sheet(name)
        for z in zeilen:
            ws.append(z)
    puffer = io.BytesIO()
    wb.save(puffer)
    return puffer.getvalue()


def test_rp_liest_landesstimmen_aus_xlsx():
    k5 = [None, None, None, "Status", "Grund", "A1", "A2", "A3", "A", "B", "%", "B1", "C", "%", "D", "%", "D1", "%",
          "E", "%", "F", "%", "F1", "%", "F2", "%", "F3", "%"]
    k3 = [None] * 16 + ["SPD", None, "ungültige", None, "gültige", None, "SPD", None, "CDU", None, "GRÜNE", None]
    def zeile(schl, name, a, b, e, f, spd, cdu, gr):
        return [schl, "G", name, None, None, 1, 1, 0, a, b, 0.6, 1, 5, 0.01, 9, 0.9, 4, 0.2, e, 0.01, f, 0.99,
                spd, 0.25, cdu, 0.3, gr, 0.1]
    roh = _xlsx({"LW_2026_LK_KS": [["Gebietsschlüssel", "KZ", "Gebietsbezeichnung"], [None], k3, [None], k5,
                                   zeile("111 ", "Koblenz, KS", 79627, 50005, 326, 49679, 12641, 15366, 6305),
                                   zeile("131 ", "Ahrweiler, LK", 98407, 66316, 595, 65721, 14225, 25965, 6067),
                                   zeile(None, "RLP", 178034, 116321, 921, 115400, 26866, 41331, 12372)]})
    w = wa.lies_rp(wa.lies_xlsx(roh, "LW_2026_LK_KS"))
    assert set(w["kreise"]) == {"07111", "07131"}
    k = w["kreise"]["07111"]
    assert (k["eligible"], k["voters"], k["invalid"], k["valid"]) == (79627, 50005, 326, 49679)
    assert k["stimmen"] == {"spd": 12641, "cdu_csu": 15366, "gruene": 6305}
    assert wa.pruefe(w["kreise"], soll=w["soll"], erwartete_kreise=2) == []


def test_be_summiert_wahlbezirke_zu_einem_kreis():
    kopf = ["Stimmart", "Adresse", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart", "Briefwahlbezirk",
            "Abgeordneten-\nhauswahlkreis", "Bundestags-\nwahlkreis", "OstWest", "Wahlberechtigte insgesamt",
            "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3", "Wählende", "Wählende B1",
            "Gültige Stimmen", "Ungültige Stimmen", "CDU", "SPD", "GRÜNE", "Die Linke", "AfD", "FDP", "Tierschutzpartei",
            "Die PARTEI", "Volt", "HEIMAT", "BSW"]
    urne = ["Zweitstimme", "01W101", "01", "Mitte", "101", "W", "1A", 1, 74, "O", 974, 595, 379, 0, 437, 6, 435, 2,
            76, 61, 102, 115, 29, 21, 0, 5, 12, 0, 12]
    brief = ["Zweitstimme", "01B1A", "01", "Mitte", "1A", "B", "1A", 1, 74, "O", 0, 0, 0, 0, 722, 722, 720, 2,
             142, 76, 199, 186, 30, 31, 5, 6, 36, 0, 8]
    roh = _xlsx({"Impressum": [["x"]], "AGH_W2": [kopf] + [urne, brief] * 600})
    w = wa.lies_be(wa.lies_xlsx(roh, "AGH_W2"))
    b = w["kreise"]["11000"]
    assert (b["eligible"], b["voters"], b["invalid"], b["valid"]) == (974 * 600, 1159 * 600, 4 * 600, 1155 * 600)
    assert b["stimmen"]["linke_pds"] == 301 * 600 and b["stimmen"]["die_partei"] == 11 * 600
    with pytest.raises(wa.FormatFehler):
        wa.lies_xlsx(roh, "Gibt es nicht")
