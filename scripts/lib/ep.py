"""Leser fuer den CSV-Export von HowTheyVote.eu (Abstimmungen im EU-Parlament).

Reine Funktionen ohne Netz und Datenbank, damit sie mit kleinen Beispieldateien
pruefbar sind. scripts/fetch_ep.py ruft ab, prueft und schreibt.

Aufbau des Exports: README im Release
https://github.com/HowTheyVote/data/releases/latest
"""
from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any, Iterable, Iterator

# Erste Sitzung der 10. Wahlperiode. Aeltere Abstimmungen bleiben draussen.
PERIODE_START = "2024-07-16"
PERIODE = "10"
DEUTSCHLAND = "DEU"

POSITION = {"FOR": "ja", "AGAINST": "nein", "ABSTENTION": "enthaltung", "DID_NOT_VOTE": "nicht"}
ERGEBNIS = {"ADOPTED": "angenommen", "REJECTED": "abgelehnt", "LAPSED": "hinfaellig"}
LESUNG = {"OLP_FIRST_READING": 1, "OLP_SECOND_READING": 2, "OLP_THIRD_READING": 3}


def art(beschreibung: str, amendment_subject: str = "") -> str:
    """Worueber abgestimmt wurde, aus der (franzoesischen) Beschreibung im Protokoll.

    Reihenfolge zaehlt: "Proposition de rejet" ist ein Ablehnungsantrag, auch
    wenn weiter hinten "Proposition de la Commission" steht.
    """
    b = (beschreibung or "").lower()
    s = (amendment_subject or "").lower()
    if "rejet" in b or "reject" in s:
        return "ablehnungsantrag"
    if b.startswith("accord provisoire") or s.startswith("provisional agreement"):
        return "einigung"
    if "négociations interinstitutionnelles" in b or "negociations interinstitutionnelles" in b:
        return "verhandlungsmandat"
    if "décision d'urgence" in b or "decision d'urgence" in b:
        return "dringlichkeit"
    return "text"


def _zahl(v: str) -> int:
    return int(v) if v not in (None, "") else 0


def _oder_none(v: str | None, laenge: int) -> str | None:
    v = (v or "").strip()
    return v[:laenge] if v else None


def hauptabstimmungen(zeilen: Iterable[dict[str, str]], ab: str = PERIODE_START) -> list[dict[str, Any]]:
    """Zeilen aus votes.csv -> Hauptabstimmungen ab ``ab`` in der Form der Tabelle ep_abstimmungen."""
    out = []
    for z in zeilen:
        if z.get("is_main") != "True" or z["timestamp"] < ab:
            continue
        titel = (z.get("display_title") or z.get("procedure_title") or "").strip()
        out.append({
            "id": int(z["id"]),
            "zeit": datetime.strptime(z["timestamp"][:19], "%Y-%m-%d %H:%M:%S"),
            "titel_en": titel[:700],
            "referenz": _oder_none(z.get("reference"), 60),
            "beschreibung": _oder_none(z.get("description"), 255),
            "art": art(z.get("description", ""), z.get("amendment_subject", "")),
            "verfahren": _oder_none(z.get("procedure_reference"), 30),
            "verfahren_typ": _oder_none(z.get("procedure_type"), 8),
            "lesung": LESUNG.get(z.get("procedure_stage", "")),
            "ja": _zahl(z["count_for"]),
            "nein": _zahl(z["count_against"]),
            "enthaltung": _zahl(z["count_abstention"]),
            "nicht_abgestimmt": _zahl(z["count_did_not_vote"]),
            "ergebnis": ERGEBNIS.get(z.get("result", "")),
            "text_referenz": _oder_none(z.get("texts_adopted_reference"), 40),
        })
    return out


def stimmen(reader: Iterable[list[str]], ids: set[int]) -> Iterator[tuple[int, int, str, str, str | None, str | None]]:
    """Zeilen aus member_votes.csv (ohne Kopfzeile) -> Einzelstimmen der gewuenschten Abstimmungen.

    Spalten: vote_id, member_id, position, country_code, group_code,
    group_short_label, national_party_id, national_party_short_label.
    Arbeitet zeilenweise: die Datei hat ueber 18 Mio. Zeilen.
    """
    for r in reader:
        vid = int(r[0])
        if vid not in ids:
            continue
        yield (vid, int(r[1]), POSITION[r[2]], r[3], r[4] or None, partei_anzeige(r[7], r[3]))


def kopf_pruefen(kopf: list[str], erwartet: list[str], datei: str) -> None:
    """Der Export ist als experimentell markiert: aendert sich die Spaltenfolge, lieber abbrechen."""
    if kopf[: len(erwartet)] != erwartet:
        raise ValueError(f"{datei}: unerwartete Spalten {kopf}")


STIMMEN_KOPF = ["vote_id", "member_id", "position", "country_code", "group_code",
                "group_short_label", "national_party_id", "national_party_short_label"]


def abgeordnete(
    members: Iterable[dict[str, str]],
    group_memberships: Iterable[dict[str, str]],
    party_memberships: Iterable[dict[str, str]],
    parties: Iterable[dict[str, str]],
) -> list[dict[str, Any]]:
    """Alle Abgeordneten, die in der laufenden Periode ein Mandat hatten, mit aktueller
    Fraktion und Partei. im_amt = Fraktionsmitgliedschaft ohne Enddatum."""
    partei_name = {p["id"]: p["short_label"] or p["label"] for p in parties}

    # je Mitglied die juengste Mitgliedschaft der Periode
    fraktion: dict[str, dict[str, str]] = {}
    for g in group_memberships:
        if g["term"] != PERIODE:
            continue
        alt = fraktion.get(g["member_id"])
        if alt is None or g["start_date"] > alt["start_date"]:
            fraktion[g["member_id"]] = g
    partei: dict[str, dict[str, str]] = {}
    for p in party_memberships:
        if p["start_date"] < PERIODE_START and p.get("end_date") and p["end_date"] < PERIODE_START:
            continue
        alt = partei.get(p["member_id"])
        if alt is None or p["start_date"] > alt["start_date"]:
            partei[p["member_id"]] = p

    out = []
    for m in members:
        g = fraktion.get(m["id"])
        if g is None:
            continue
        geb = m.get("date_of_birth") or ""
        out.append({
            "id": int(m["id"]),
            "vorname": m["first_name"].strip()[:100],
            "nachname": nachname_schreiben(m["last_name"])[:100],
            "land": m["country_code"],
            "geburtsdatum": date.fromisoformat(geb) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", geb) else None,
            "fraktion": g["group_code"] or None,
            "partei": partei_anzeige(
                partei_name.get(partei[m["id"]]["national_party_id"]) if m["id"] in partei else None,
                m["country_code"],
            ),
            "im_amt": 0 if g.get("end_date") else 1,
        })
    return out


# Kurznamen deutscher Parteien, wie das Parlament sie fuehrt -> uebliche Schreibweise.
PARTEI_DE = {
    "GRÜNE.": "Grüne",
    "DIE LINKE.": "Linke",
    "V": "Volt",
    "Independent": "parteilos",
    "PdF": "Partei des Fortschritts",
    "Familien-Partei DE": "Familien-Partei",
    "Die Tierschutzpartei": "Tierschutzpartei",
}


def partei_anzeige(kurz: str | None, land: str) -> str | None:
    if not kurz:
        return None
    return (PARTEI_DE.get(kurz, kurz) if land == DEUTSCHLAND else kurz)[:80]


_KLEIN = {"von", "van", "de", "der", "den", "del", "della", "di", "da", "dos", "du", "le", "la", "zu", "und", "y", "e"}


def nachname_schreiben(name: str) -> str:
    """Das Parlament schreibt Nachnamen in Versalien ("VON DER SCHULENBURG"). Fuer die
    Anzeige: Wortanfaenge gross, Adelspartikel klein, Bindestriche und Apostrophe beachtet."""
    name = name.strip()
    if not name or any(ch.islower() and ch != "ß" for ch in name):
        return name
    woerter = []
    for i, w in enumerate(name.split(" ")):
        klein = w.lower()
        if (i > 0 and klein in _KLEIN) or (i == 0 and klein in ("von", "van") and " " in name):
            woerter.append(klein)
        else:
            wort = re.sub(r"(^|[-'’])(\w)", lambda m: m.group(1) + m.group(2).upper(), klein)
            woerter.append(re.sub(r"^Mc(\w)", lambda m: "Mc" + m.group(1).upper(), wort))
    return " ".join(woerter)


def pruefen(abst: list[dict[str, Any]], zahl_stimmen: Counter, bisher: int, abg: list[dict[str, Any]],
            fraktionen: int) -> list[str]:
    """Plausibilitaet vor dem Schreiben. Leere Liste = alles in Ordnung."""
    fehler = []
    if len(abst) < max(700, bisher):
        fehler.append(f"nur {len(abst)} Hauptabstimmungen, in der Datenbank stehen {bisher}")
    abweichend = [a["id"] for a in abst
                  if zahl_stimmen[a["id"]] != a["ja"] + a["nein"] + a["enthaltung"] + a["nicht_abgestimmt"]]
    if abweichend:
        fehler.append(f"{len(abweichend)} Abstimmungen, bei denen Einzelstimmen und Summen nicht passen, "
                      f"z. B. {abweichend[:5]}")
    deutsche = sum(1 for a in abg if a["land"] == DEUTSCHLAND and a["im_amt"])
    if not 90 <= deutsche <= 96:
        fehler.append(f"{deutsche} deutsche Abgeordnete im Amt, erwartet 90 bis 96")
    im_amt = sum(1 for a in abg if a["im_amt"])
    if not 690 <= im_amt <= 720:
        fehler.append(f"{im_amt} Abgeordnete im Amt, erwartet 690 bis 720")
    if fraktionen < 8:
        fehler.append(f"nur {fraktionen} Fraktionen")
    return fehler


def deutsche_stimmen(zeilen: Iterable[tuple]) -> dict[int, dict[str, int]]:
    """Summen der deutschen Abgeordneten je Abstimmung (fuer die Spalten de_*)."""
    out: dict[int, dict[str, int]] = defaultdict(lambda: {"ja": 0, "nein": 0, "enthaltung": 0, "nicht": 0})
    for vid, _mid, pos, land, _f, _p in zeilen:
        if land == DEUTSCHLAND:
            out[vid][pos] += 1
    return out


def csv_zeilen(pfad) -> list[dict[str, str]]:
    import gzip

    oeffnen = gzip.open if str(pfad).endswith(".gz") else open
    with oeffnen(pfad, "rt", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))
