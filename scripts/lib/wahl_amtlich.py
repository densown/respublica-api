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
}

# Kreise, die die Quelle teilt, wir aber als einen fuehren.
AGS_ZUSAMMEN = {"11100": "11000", "11200": "11000"}


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
