"""Amtliche Wahldateien lesen, pruefen und in Zeilen fuer `wahlen` umrechnen.

Reine Funktionen ohne Netz und ohne Datenbank, damit sie sich mit kleinen
Beispieldateien testen lassen. Das Laden und Schreiben macht
scripts/import_wahl_amtlich.py.

Grundsatz: Aus den Dateien werden nur absolute Stimmen uebernommen. Anteile,
Wahlbeteiligung und staerkste Partei werden hier gerechnet. So kann eine
Rundung der Quelle nichts verfaelschen, und die Summen lassen sich gegen die
amtliche Gesamtzeile pruefen.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime

# Spalten der Tabelle `wahlen`, die einen Stimmenanteil halten (wie import_wahlen.py).
PARTY_COLS = [
    "cdu_csu", "spd", "gruene", "fdp", "linke_pds", "afd", "bsw", "npd",
    "freie_waehler", "piraten", "die_partei",
]

# Namen aus den amtlichen Dateien -> Spalte. Verglichen wird in Kleinschrift
# ohne Leerzeichen und Punkte. Alles, was hier fehlt, landet in `other`.
_PARTEI = {
    "cdu": "cdu_csu", "csu": "cdu_csu", "cdu/csu": "cdu_csu",
    "spd": "spd",
    "grüne": "gruene", "gruene": "gruene", "grüne/b90": "gruene",
    "bündnis90/diegrünen": "gruene",
    "fdp": "fdp",
    "dielinke": "linke_pds", "linke": "linke_pds",
    "afd": "afd",
    "bsw": "bsw",
    "npd": "npd", "heimat": "npd", "dieheimat": "npd",
    "freiewähler": "freie_waehler", "freiewaehler": "freie_waehler",
    "piraten": "piraten",
    "diepartei": "die_partei",
    # Langformen (Saarland)
    "christlichdemokratischeuniondeutschlands": "cdu_csu",
    "sozialdemokratischeparteideutschlands": "spd",
    "alternativefürdeutschland": "afd",
    "bündnis90/diegrünen": "gruene",
    "freiedemokratischepartei": "fdp",
    "piratenparteideutschland": "piraten",
    "bündnissahrawagenknecht": "bsw",
}

# Kreise, die die Quelle teilt oder noch unter altem Schluessel fuehrt, wir aber als einen fuehren.
# Berlin Ost und West; Eisenach ging 2021 im Wartburgkreis auf, Goettingen und Osterode 2016 in 03159.
# Die Tabelle `wahlen` fuehrt auch alte Wahlen auf dem heutigen Gebietsstand.
AGS_ZUSAMMEN = {"11100": "11000", "11200": "11000",
                "16056": "16063", "03152": "03159", "03156": "03159"}


class FormatFehler(ValueError):
    """Die Datei sieht anders aus als erwartet. Dann wird nichts geschrieben."""


def partei_spalte(name: str) -> str | None:
    schluessel = re.sub(r"[\s.]", "", (name or "").lower())
    return _PARTEI.get(schluessel)


def zahl(wert) -> int:
    """'1.234', '1 234', '' oder '-' -> int. Unlesbares ist ein Formatfehler."""
    s = str(wert if wert is not None else "").strip().replace("\xa0", "").replace(" ", "")
    if s in ("", "-", "x", "X"):
        return 0
    s = s.replace(".", "")
    if not re.fullmatch(r"-?\d+", s):
        raise FormatFehler(f"keine ganze Zahl: {wert!r}")
    return int(s)


def text_aus_bytes(roh: bytes) -> str:
    """UTF-8 (mit oder ohne BOM), sonst Windows-1252."""
    try:
        return roh.decode("utf-8-sig")
    except UnicodeDecodeError:
        return roh.decode("cp1252")


def _leerer_kreis() -> dict:
    return {"eligible": 0, "voters": 0, "valid": 0, "invalid": 0, "stimmen": {}}


def _status(kopfzeilen: list[str]) -> str | None:
    kopf = " ".join(kopfzeilen).lower()
    if "endgültig" in kopf or "endgueltig" in kopf:
        return "endgueltig"
    if "vorläufig" in kopf or "vorlaeufig" in kopf:
        return "vorlaeufig"
    return None


def _zeilen_ab_kopf(text: str, kopf_beginnt_mit: str) -> tuple[list[str], list[str]]:
    zeilen = text.splitlines()
    for i, z in enumerate(zeilen):
        if z.lstrip("﻿").startswith(kopf_beginnt_mit):
            return zeilen[:i], zeilen[i:]
    raise FormatFehler(f"Kopfzeile '{kopf_beginnt_mit}…' nicht gefunden")


# ---------------------------------------------------------------------------
# Bundeswahlleiterin: kerg2.csv (lang, eine Zeile je Gebiet und Gruppe)
# ---------------------------------------------------------------------------

_SYSTEM = (("wahlberechtigte", "eligible"), ("wähl", "voters"),
           ("ungültige", "invalid"), ("gültige", "valid"))


def lies_kerg2(text: str, *, gebietsart: str = "Kreis", stimme: str = "1") -> dict:
    """Liest kerg2.csv. Liefert Kreise (oder Wahlkreise) und die Bundeszeile.

    Europawahl: gebietsart='Kreis', stimme='1'. Bundestagswahl: die Datei hat
    keine Kreise, dort dient sie nur als amtliche Gesamtsumme (stimme='2').
    """
    vorspann, rest = _zeilen_ab_kopf(text, "Wahlart;")
    leser = csv.DictReader(io.StringIO("\n".join(rest)), delimiter=";")
    noetig = {"Gebietsart", "Gebietsnummer", "Gruppenart", "Gruppenname", "Stimme", "Anzahl", "Wahltag"}
    fehlend = noetig - set(leser.fieldnames or [])
    if fehlend:
        raise FormatFehler(f"kerg2: Spalten fehlen: {sorted(fehlend)}")

    kreise: dict[str, dict] = {}
    bund = _leerer_kreis()
    wahltag = None
    for z in leser:
        art = (z["Gebietsart"] or "").strip()
        if art == "Bund":
            ziel = bund
        elif art == gebietsart:
            ags = (z["Gebietsnummer"] or "").strip().zfill(5)
            ziel = kreise.setdefault(ags, _leerer_kreis())
        else:
            continue
        if (z["Stimme"] or "").strip() not in ("", stimme):
            continue
        if wahltag is None and z["Wahltag"]:
            wahltag = datetime.strptime(z["Wahltag"].strip(), "%d.%m.%Y").date()
        gruppe = (z["Gruppenart"] or "").strip()
        name = (z["Gruppenname"] or "").strip()
        anzahl = zahl(z["Anzahl"])
        if gruppe.startswith("System"):
            klein = name.lower()
            for anfang, feld in _SYSTEM:
                if klein.startswith(anfang):
                    ziel[feld] = anzahl
                    break
        elif gruppe == "Partei":
            spalte = partei_spalte(name)
            if spalte:
                ziel["stimmen"][spalte] = ziel["stimmen"].get(spalte, 0) + anzahl
    if wahltag is None:
        raise FormatFehler("kerg2: kein Wahltag gefunden")
    return {"datum": wahltag, "status": _status(vorspann), "kreise": kreise,
            "soll": bund if bund["valid"] else None}


# ---------------------------------------------------------------------------
# Bundeswahlleiterin: kerg.csv (breit, aeltere Wahlen): je Merkmal zwei Spalten,
# links das Ergebnis, rechts die Vorperiode. Kreise haben 4- bis 5-stellige
# Nummern, Laender 901 bis 916, der Bund 999.
# ---------------------------------------------------------------------------

def lies_kerg_breit(text: str) -> dict:
    vorspann, rest = _zeilen_ab_kopf(text, "Nr;Gebiet;")
    zeilen = list(csv.reader(io.StringIO("\n".join(rest)), delimiter=";"))
    kopf = [k.strip() for k in zeilen[0]]
    art = [k.strip() for k in zeilen[1]] if len(zeilen) > 1 else []
    erste: dict[str, int] = {}
    for i, name in enumerate(kopf):
        if i >= 3 and name and name not in erste:
            erste[name] = i
    for name, i in erste.items():
        if i >= len(art) or art[i] not in ("Endgültig", "Vorläufig"):
            raise FormatFehler(f"kerg: Spalte {name!r} ist nicht das aktuelle Ergebnis")
    i_ber = _index(kopf, "Wahlberechtigte")
    i_wae = next((erste[n] for n in ("Wähler", "Wählende", "Wähler/-innen") if n in erste), None)
    if i_wae is None:
        raise FormatFehler("kerg: Spalte für die Wählenden fehlt")
    i_ung, i_gue = erste.get("Ungültige"), erste.get("Gültige")
    if i_ung is None or i_gue is None:
        raise FormatFehler("kerg: Spalten Ungültige/Gültige fehlen")
    fest = {i_ber, i_wae, i_ung, i_gue}
    kreise: dict[str, dict] = {}
    bund = _leerer_kreis()
    for z in zeilen[2:]:
        if len(z) <= i_gue:
            continue
        nr = z[0].strip()
        if nr == "999":
            ziel = bund
        elif re.fullmatch(r"\d{4,5}", nr):
            ziel = kreise.setdefault(nr.zfill(5), _leerer_kreis())
        else:
            continue
        _nimm(ziel, zahl(z[i_ber]), zahl(z[i_wae]), zahl(z[i_ung]), zahl(z[i_gue]),
              {name: zahl(z[i]) if i < len(z) else 0 for name, i in erste.items() if i not in fest})
    datum = None
    m = re.search(r"\((\d{1,2})\. (\w+) (\d{4})\)", " ".join(vorspann))
    monate = ["januar", "februar", "märz", "april", "mai", "juni", "juli", "august", "september",
              "oktober", "november", "dezember"]
    if m and m.group(2).lower() in monate:
        datum = date(int(m.group(3)), monate.index(m.group(2).lower()) + 1, int(m.group(1)))
    return {"datum": datum, "status": _status(vorspann), "kreise": kreise,
            "soll": bund if bund["valid"] else None}


# ---------------------------------------------------------------------------
# Bundeswahlleiterin: btw{JJJJ}kreis.csv (breit, eine Zeile je Kreis)
# ---------------------------------------------------------------------------

def lies_btw_kreis(text: str) -> dict:
    """Liest die Kreisdatei der Bundestagswahl, gezaehlt werden Zweitstimmen."""
    vorspann, rest = _zeilen_ab_kopf(text, "Land;")
    zeilen = list(csv.reader(io.StringIO("\n".join(rest)), delimiter=";"))
    kopf = [k.strip() for k in zeilen[0]]

    def spalte(*anfaenge: str) -> int:
        for i, k in enumerate(kopf):
            if any(k.lower().startswith(a) for a in anfaenge):
                return i
        raise FormatFehler(f"Kreisdatei: Spalte {anfaenge[0]!r} fehlt")

    i_ags = spalte("statistische kennziffer")
    i_ber = spalte("wahlberechtigte")
    i_wae = spalte("wählende", "wähler")
    i_ung = next((i for i, k in enumerate(kopf) if k.lower().startswith("ungültige") and "zweit" in k.lower()), None)
    i_gue = next((i for i, k in enumerate(kopf) if k.lower().startswith("gültige") and "zweit" in k.lower()), None)
    if i_ung is None or i_gue is None:
        raise FormatFehler("Kreisdatei: Spalten für Zweitstimmen fehlen")

    parteien = []  # (index, spalte in `wahlen`)
    for i in range(i_gue + 1, len(kopf)):
        name = re.sub(r"\s*-\s*Zweitstimmen\s*$", "", kopf[i], flags=re.I)
        if re.search(r"erststimmen", name, flags=re.I):
            raise FormatFehler("Kreisdatei: Erststimmen hinter den Zweitstimmen, Aufbau unbekannt")
        ziel = partei_spalte(name)
        if ziel:
            parteien.append((i, ziel))
    if len({z for _, z in parteien}) < 5:
        raise FormatFehler("Kreisdatei: zu wenige bekannte Parteien im Kopf")

    kreise: dict[str, dict] = {}
    for z in zeilen[1:]:
        if len(z) <= i_gue:
            continue
        kennziffer = z[i_ags].strip()
        if not re.fullmatch(r"\d{4,5}", kennziffer):
            continue
        ags = kennziffer.zfill(5)
        k = kreise.setdefault(ags, _leerer_kreis())
        k["eligible"] += zahl(z[i_ber])
        k["voters"] += zahl(z[i_wae])
        k["invalid"] += zahl(z[i_ung])
        k["valid"] += zahl(z[i_gue])
        for i, ziel in parteien:
            if i < len(z):
                k["stimmen"][ziel] = k["stimmen"].get(ziel, 0) + zahl(z[i])
    return {"datum": None, "status": _status(vorspann), "kreise": kreise, "soll": None}


# ---------------------------------------------------------------------------
# Landtagswahlen: je Land ein Leser. Gezaehlt wird die Stimme, nach der die
# Sitze verteilt werden (Zweit- bzw. Landesstimme).
# ---------------------------------------------------------------------------

def _nimm(ziel: dict, ber, wae, ung, gue, parteien: dict[str, int]) -> None:
    ziel["eligible"] += ber
    ziel["voters"] += wae
    ziel["invalid"] += ung
    ziel["valid"] += gue
    for name, n in parteien.items():
        spalte = partei_spalte(name)
        if spalte:
            ziel["stimmen"][spalte] = ziel["stimmen"].get(spalte, 0) + n


def _index(kopf: list[str], name: str) -> int:
    try:
        return kopf.index(name)
    except ValueError:
        raise FormatFehler(f"Spalte {name!r} fehlt") from None


def lies_xlsx(roh: bytes, blatt: str | None = None) -> list[list]:
    """Liest ein Blatt einer XLSX-Datei ohne openpyxl (auf dem Server nicht installiert)."""
    import zipfile
    import xml.etree.ElementTree as ET

    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
          "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
          "p": "http://schemas.openxmlformats.org/package/2006/relationships"}
    with zipfile.ZipFile(io.BytesIO(roh)) as z:
        texte: list[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", ns):
                texte.append("".join(t.text or "" for t in si.iter(f"{{{ns['m']}}}t")))
        buch = ET.fromstring(z.read("xl/workbook.xml"))
        bez = {r.get("Id"): r.get("Target") for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")).findall("p:Relationship", ns)}
        ziel = None
        for sh in buch.find("m:sheets", ns):
            if blatt is None or sh.get("name") == blatt:
                ziel = bez[sh.get(f"{{{ns['r']}}}id")]
                break
        if ziel is None:
            raise FormatFehler(f"Blatt {blatt!r} fehlt")
        pfad = ziel.lstrip("/") if ziel.startswith("/") else "xl/" + ziel
        zeilen: list[list] = []
        for row in ET.fromstring(z.read(pfad)).iter(f"{{{ns['m']}}}row"):
            werte: list = []
            for c in row.findall("m:c", ns):
                buchstaben = re.match(r"[A-Z]+", c.get("r", "A")).group(0)
                spalte = 0
                for b in buchstaben:
                    spalte = spalte * 26 + ord(b) - 64
                while len(werte) < spalte:
                    werte.append(None)
                v = c.find("m:v", ns)
                art = c.get("t")
                if art == "s" and v is not None:
                    wert = texte[int(v.text)]
                elif art == "inlineStr":
                    wert = "".join(t.text or "" for t in c.iter(f"{{{ns['m']}}}t"))
                elif v is None or v.text is None:
                    wert = None
                elif art in ("str", "b", "e"):
                    wert = v.text
                else:
                    f = float(v.text)
                    wert = int(f) if f == int(f) else f
                werte[spalte - 1] = wert
            zeilen.append(werte)
        return zeilen


def _ganz(wert) -> int:
    if wert is None or wert == "":
        return 0
    if isinstance(wert, (int, float)):
        if wert != int(wert):
            raise FormatFehler(f"keine ganze Zahl: {wert!r}")
        return int(wert)
    return zahl(wert)


# Baden-Wuerttemberg 2026: die Datei nennt die Parteien nur als F1 bis F21.
# Zuordnung ueber die Landessummen mit der Ergebnisseite abgeglichen.
BW_2026_F = {"F1": "GRÜNE", "F2": "CDU", "F3": "SPD", "F4": "FDP", "F5": "AfD", "F6": "Die Linke",
             "F7": "FREIE WÄHLER", "F8": "Die PARTEI", "F16": "BSW"}
# Gemeinsame Briefwahlbezirke ohne Gemeinde, die die Gebietsnummer nicht verraet.
BW_BRIEF_NACH_NAME = {"ADK": "08425"}


def lies_bw(text: str, f_spalten: dict[str, str] = BW_2026_F) -> dict:
    zeilen = list(csv.reader(io.StringIO(text.lstrip("\ufeff")), delimiter=";"))
    kopf = [k.strip() for k in zeilen[0]]
    i_ags, i_art, i_nr, i_name = (_index(kopf, n) for n in ("AGS", "Gebietsart", "Gebietsnummer", "Gebietsname"))
    i_ber, i_wae = _index(kopf, "Wahlberechtigte gesamt (A)"), _index(kopf, "Waehler gesamt (B)")
    i_ung, i_gue = _index(kopf, "Zweitstimmen ungueltige (E)"), _index(kopf, "Zweitstimmen gueltige (F)")
    i_f = {name: _index(kopf, f) for f, name in f_spalten.items()}
    kreise: dict[str, dict] = {}
    soll = _leerer_kreis()
    for z in zeilen[1:]:
        if len(z) <= i_gue:
            continue
        art, ags = z[i_art].strip(), z[i_ags].strip()
        if art == "LAND":
            ziel = soll
        elif art == "GEMEINDE" or (art == "WAHLKREIS" and ags):
            if not re.fullmatch(r"08\d{6}", ags):
                raise FormatFehler(f"BW: unerwarteter AGS {ags!r}")
            ziel = kreise.setdefault(ags[:5], _leerer_kreis())
        elif art == "BRIEFWAHLBEZIRK" and not re.fullmatch(r"\d{8}", ags):
            m = re.search(r"08[1-4]\d{2}", z[i_nr])
            kreis = m.group(0) if m else next(
                (k for anfang, k in BW_BRIEF_NACH_NAME.items() if z[i_name].strip().startswith(anfang)), None)
            if not kreis:
                raise FormatFehler(f"BW: Briefwahlbezirk {z[i_name]!r} lässt sich keinem Kreis zuordnen")
            ziel = kreise.setdefault(kreis, _leerer_kreis())
        else:
            continue
        _nimm(ziel, zahl(z[i_ber]), zahl(z[i_wae]), zahl(z[i_ung]), zahl(z[i_gue]),
              {name: zahl(z[i]) for name, i in i_f.items()})
    return {"datum": None, "status": None, "kreise": kreise, "soll": soll if soll["valid"] else None}


def lies_rp(zeilen: list[list]) -> dict:
    """Rheinland-Pfalz: XLSX, Kuerzel in Zeile 5, Parteinamen in Zeile 3, gezaehlt werden Landesstimmen."""
    kuerzel = next((z for z in zeilen[:8] if z and "A1" in z and "F1" in z), None)
    if kuerzel is None:
        raise FormatFehler("RP: Zeile mit den Kürzeln (A1 … F1) fehlt")
    namen = next((z for z in zeilen[:8] if z and "SPD" in z), None)
    if namen is None:
        raise FormatFehler("RP: Zeile mit den Parteinamen fehlt")
    pos = {str(k).strip(): i for i, k in enumerate(kuerzel) if k and str(k).strip() != "%"}
    for k in ("A", "B", "E", "F"):
        if k not in pos:
            raise FormatFehler(f"RP: Spalte {k} fehlt")
    parteien = {str(namen[i]).strip(): i for k, i in pos.items()
                if re.fullmatch(r"F\d+", k) and i < len(namen) and namen[i]}
    kreise: dict[str, dict] = {}
    soll = _leerer_kreis()
    start = zeilen.index(kuerzel) + 1
    for z in zeilen[start:]:
        if len(z) <= pos["F"] or z[pos["F"]] is None:
            continue
        schluessel = str(z[0] or "").strip()
        if re.fullmatch(r"\d{3}", schluessel):
            ziel = kreise.setdefault("07" + schluessel, _leerer_kreis())
        elif not schluessel and str(z[2] or "").strip().upper() in ("RLP", "RHEINLAND-PFALZ"):
            ziel = soll
        else:
            continue
        _nimm(ziel, _ganz(z[pos["A"]]), _ganz(z[pos["B"]]), _ganz(z[pos["E"]]), _ganz(z[pos["F"]]),
              {name: _ganz(z[i]) if i < len(z) else 0 for name, i in parteien.items()})
    return {"datum": None, "status": None, "kreise": kreise, "soll": soll if soll["valid"] else None}


def lies_st(text: str) -> dict:
    """Sachsen-Anhalt: Spalten nach Namen, Zeilen KRS mit Wahllokal '' sind die Kreissummen."""
    leser = csv.DictReader(io.StringIO(text.lstrip("\ufeff")), delimiter=";")
    kopf = leser.fieldnames or []
    for n in ("Satzart", "Schlüsselnummer", "Wahllokal", "A.Wahlberechtigte", "B.Wähler",
              "E.Ungültige.Zweitstimmen", "F.Gültige.Zweitstimmen"):
        _index(kopf, n)
    f_spalten = {k: k.split(".", 1)[1] for k in kopf if re.match(r"F\d+\.", k)}
    kreise: dict[str, dict] = {}
    soll = _leerer_kreis()
    status = None
    for z in leser:
        if (z["Wahllokal"] or "") != "":
            continue
        status = status or {"E": "endgueltig", "V": "vorlaeufig"}.get((z.get("Ergebnisart") or "").strip())
        if z["Satzart"] == "KRS":
            ziel = kreise.setdefault(z["Schlüsselnummer"].strip().zfill(5), _leerer_kreis())
        elif z["Satzart"] == "LAN":
            ziel = soll
        else:
            continue
        _nimm(ziel, zahl(z["A.Wahlberechtigte"]), zahl(z["B.Wähler"]), zahl(z["E.Ungültige.Zweitstimmen"]),
              zahl(z["F.Gültige.Zweitstimmen"]), {name: zahl(z[k]) for k, name in f_spalten.items()})
    return {"datum": None, "status": status, "kreise": kreise, "soll": soll if soll["valid"] else None}


def _mv_zeilen(text: str) -> tuple[list[str], list[list[str]], str | None]:
    vorspann, rest = _zeilen_ab_kopf(text, "Berechnungsdatum;")
    kopf = [k.strip() for k in rest[0].split(";")]
    return kopf, [z.split(";") for z in rest[1:] if z.strip()], _status(vorspann)


def lies_mv(text: str, wahlkreise_text: str | None = None) -> dict:
    """Mecklenburg-Vorpommern: Gemeinden zu Kreisen summieren, Briefwahl der Aemter zaehlt mit.

    Die Landessumme steht nicht in der Gemeindedatei, sondern in l_wahlkreise.csv (Wahlkreis 99).
    """
    def lies(t: str, schluessel: str):
        kopf, zeilen, status = _mv_zeilen(t)
        i_k, i_aus, i_st = _index(kopf, schluessel), _index(kopf, "Ausgabe"), _index(kopf, "Erst-/Zweitstimme")
        i_ber, i_wae = _index(kopf, "Wahlberechtigte"), _index(kopf, "Wähler")
        i_ung, i_gue = _index(kopf, "Ungültige Stimmen"), _index(kopf, "Gültige Stimmen")
        for z in zeilen:
            if len(z) < len(kopf) or z[i_aus].strip() != "A" or z[i_st].strip() != "2":
                continue
            yield z[i_k].strip(), (zahl(z[i_ber]), zahl(z[i_wae]), zahl(z[i_ung]), zahl(z[i_gue]),
                                   {kopf[i]: zahl(z[i]) for i in range(i_gue + 1, len(kopf))}), status

    kreise: dict[str, dict] = {}
    status = None
    for kreis, werte, status in lies(text, "Kreis"):
        if not re.fullmatch(r"\d{1,2}", kreis):
            raise FormatFehler(f"MV: unerwartete Kreisnummer {kreis!r}")
        _nimm(kreise.setdefault("130" + kreis.zfill(2), _leerer_kreis()), *werte)
    soll = None
    if wahlkreise_text:
        for nummer, werte, _ in lies(wahlkreise_text, "Wahlkreis"):
            if nummer == "99":
                soll = _leerer_kreis()
                _nimm(soll, *werte)
    return {"datum": None, "status": status, "kreise": kreise, "soll": soll}


def lies_be(zeilen: list[list]) -> dict:
    """Berlin: Blatt AGH_W2 (Zweitstimmen je Wahlbezirk), alles zu 11000 summiert.

    Die Datei hat keine Landeszeile. Berlin ist ein einziger Kreis, die Summe
    aller Wahlbezirke ist also selbst das Landesergebnis.
    """
    kopf = [re.sub(r"\s+", " ", str(k or "")).strip() for k in zeilen[0]]
    i_ber, i_wae = _index(kopf, "Wahlberechtigte insgesamt"), _index(kopf, "Wählende")
    i_gue, i_ung = _index(kopf, "Gültige Stimmen"), _index(kopf, "Ungültige Stimmen")
    erste_partei = max(i_gue, i_ung) + 1
    berlin = _leerer_kreis()
    n = 0
    for z in zeilen[1:]:
        if len(z) <= i_ung or z[i_wae] is None:
            continue
        if str(z[0] or "").strip() != "Zweitstimme":
            raise FormatFehler(f"Berlin: unerwartete Stimmart {z[0]!r}")
        n += 1
        _nimm(berlin, _ganz(z[i_ber]), _ganz(z[i_wae]), _ganz(z[i_ung]), _ganz(z[i_gue]),
              {kopf[i]: _ganz(z[i]) if i < len(z) else 0 for i in range(erste_partei, len(kopf)) if kopf[i]})
    if n < 1000:
        raise FormatFehler(f"Berlin: nur {n} Wahlbezirke, erwartet über 1000")
    soll = {**berlin, "stimmen": dict(berlin["stimmen"])}
    return {"datum": None, "status": None, "kreise": {"11000": berlin}, "soll": soll}


def lies_sl(text: str) -> dict:
    """Saarland: breite Datei, je Merkmal 'Endgültig' und rechts daneben 'Vorperiode'. Eine Stimme."""
    vorspann, rest = _zeilen_ab_kopf(text, "Nr;Gebiet;")
    zeilen = [z.split(";") for z in rest]
    kopf = [k.strip() for k in zeilen[0]]
    if len(zeilen) < 4 or "Vorperiode" not in zeilen[2]:
        raise FormatFehler("Saarland: Kopfzeilen sehen anders aus als erwartet")
    art = [k.strip() for k in zeilen[2]]
    spalten = {name: i for i, name in enumerate(kopf) if name and i >= 3}
    for i in spalten.values():
        if i < len(art) and art[i] not in ("Endgültig", "Vorläufig"):
            raise FormatFehler(f"Saarland: Spalte {i} ist nicht das aktuelle Ergebnis ({art[i]!r})")
    i_ber, i_wae = _index(kopf, "Wahlberechtigte"), _index(kopf, "Wähler")
    i_ung, i_gue = _index(kopf, "Ungültige Stimmen"), _index(kopf, "Gültige Stimmen")
    fest = {i_ber, i_wae, i_ung, i_gue}
    kreise: dict[str, dict] = {}
    soll = _leerer_kreis()
    for z in zeilen[3:]:
        if len(z) <= i_gue:
            continue
        nr = z[0].strip()
        if re.fullmatch(r"4\d{4}", nr):
            ziel = kreise.setdefault("100" + nr[:2], _leerer_kreis())
        elif nr == "10":
            ziel = soll
        else:
            continue
        _nimm(ziel, zahl(z[i_ber]), zahl(z[i_wae]), zahl(z[i_ung]), zahl(z[i_gue]),
              {name: zahl(z[i]) for name, i in spalten.items() if i not in fest})
    status = "vorlaeufig" if "Vorläufig" in art else _status(vorspann) or "endgueltig"
    return {"datum": None, "status": status, "kreise": kreise, "soll": soll if soll["valid"] else None}


# ---------------------------------------------------------------------------
# Zusammenfassen, pruefen, umrechnen
# ---------------------------------------------------------------------------

def fasse_zusammen(kreise: dict[str, dict], zuordnung: dict[str, str] = AGS_ZUSAMMEN) -> dict[str, dict]:
    """Legt geteilte Kreise zusammen (Berlin Ost und West -> 11000)."""
    aus: dict[str, dict] = {}
    for ags, k in kreise.items():
        ziel = aus.setdefault(zuordnung.get(ags, ags), _leerer_kreis())
        for feld in ("eligible", "voters", "valid", "invalid"):
            ziel[feld] += k[feld]
        for p, n in k["stimmen"].items():
            ziel["stimmen"][p] = ziel["stimmen"].get(p, 0) + n
    return aus


def pruefe(kreise: dict[str, dict], *, soll: dict | None = None,
           erwartete_kreise: int | None = None) -> list[str]:
    """Liefert Fehler. Bei einem einzigen Fehler wird nichts geschrieben."""
    fehler: list[str] = []
    if not kreise:
        return ["Datei enthält keine Kreise"]
    if erwartete_kreise is not None and len(kreise) != erwartete_kreise:
        fehler.append(f"{len(kreise)} Kreise in der Datei, erwartet {erwartete_kreise}")
    for ags, k in sorted(kreise.items()):
        if not re.fullmatch(r"\d{5}", ags):
            fehler.append(f"{ags}: kein fünfstelliger Kreisschlüssel")
        if k["valid"] <= 0 or k["eligible"] <= 0:
            fehler.append(f"{ags}: keine gültigen Stimmen oder keine Wahlberechtigten")
            continue
        if k["voters"] > k["eligible"]:
            fehler.append(f"{ags}: mehr Wählende ({k['voters']}) als Wahlberechtigte ({k['eligible']})")
        if k["valid"] + k["invalid"] != k["voters"]:
            fehler.append(f"{ags}: gültig + ungültig ({k['valid'] + k['invalid']}) ≠ Wählende ({k['voters']})")
        if sum(k["stimmen"].values()) > k["valid"]:
            fehler.append(f"{ags}: Parteistimmen über den gültigen Stimmen")
    summe = {f: sum(k[f] for k in kreise.values()) for f in ("eligible", "voters", "valid")}
    if soll:
        for feld, name in (("eligible", "Wahlberechtigte"), ("voters", "Wählende"), ("valid", "gültige Stimmen")):
            if summe[feld] != soll[feld]:
                fehler.append(f"Summe {name} {summe[feld]} ≠ amtliche Gesamtzeile {soll[feld]}")
        for p, n in soll.get("stimmen", {}).items():
            ist = sum(k["stimmen"].get(p, 0) for k in kreise.values())
            if ist != n:
                fehler.append(f"Summe {p} {ist} ≠ amtliche Gesamtzeile {n}")
    if summe["eligible"]:
        beteiligung = summe["voters"] / summe["eligible"]
        if not 0.25 <= beteiligung <= 0.95:
            fehler.append(f"Wahlbeteiligung {beteiligung:.1%} ist unplausibel")
    return fehler[:25]


def zu_zeilen(kreise: dict[str, dict], *, typ: str, jahr: int, datum: date | None,
              land: str | None = None) -> list[dict]:
    """Rechnet absolute Stimmen in Zeilen der Tabelle `wahlen` um."""
    angetreten = {p for k in kreise.values() for p in k["stimmen"]}
    zeilen = []
    for ags, k in sorted(kreise.items()):
        z = {
            "typ": typ, "ags": ags, "election_year": jahr, "election_date": datum,
            "state": land or ags[:2], "county": ags,
            "eligible_voters": k["eligible"], "number_voters": k["voters"],
            "valid_votes": k["valid"], "invalid_votes": k["invalid"],
            "turnout": round(k["voters"] / k["eligible"], 6),
            "election_type": "", "round": 0,
        }
        summe = 0.0
        beste, bester_wert = None, -1.0
        for p in PARTY_COLS:
            if p not in angetreten:
                z[p] = None
                continue
            anteil = k["stimmen"].get(p, 0) / k["valid"]
            z[p] = round(anteil, 6)
            summe += anteil
            if anteil > bester_wert:
                beste, bester_wert = p, anteil
        z["other"] = round(max(0.0, 1.0 - summe), 6)
        z["winning_party"] = beste
        zeilen.append(z)
    return zeilen


WERT_SPALTEN = ["eligible_voters", "number_voters", "valid_votes", "invalid_votes", "turnout",
                *PARTY_COLS, "other", "winning_party"]


def vergleiche(neu: list[dict], alt: dict[str, dict]) -> dict:
    """Vergleicht neue Zeilen mit dem Bestand (ags -> Zeile aus der DB)."""
    bericht = {"neu": 0, "gleich": 0, "geaendert": 0, "nur_in_db": sorted(set(alt) - {z["ags"] for z in neu}),
               "groesste_abweichung": 0.0, "beispiele": []}
    for z in neu:
        a = alt.get(z["ags"])
        if a is None:
            bericht["neu"] += 1
            continue
        anders = []
        for s in WERT_SPALTEN:
            n, v = z.get(s), a.get(s)
            if isinstance(n, float) or s == "turnout" or s in PARTY_COLS or s == "other":
                nf, vf = float(n or 0), float(v or 0)
                if abs(nf - vf) > 0.00005:
                    anders.append(f"{s} {vf:.4f}→{nf:.4f}")
                    bericht["groesste_abweichung"] = max(bericht["groesste_abweichung"], abs(nf - vf))
            elif (n or None) != (v or None):
                anders.append(f"{s} {v}→{n}")
        if anders:
            bericht["geaendert"] += 1
            if len(bericht["beispiele"]) < 8:
                bericht["beispiele"].append(f"{z['ags']}: " + ", ".join(anders[:4]))
        else:
            bericht["gleich"] += 1
    return bericht
