"""Hilfsfunktionen fuer den Tracker Kleine Anfragen (Migration 021).

Reine Textverarbeitung ohne Netz und DB, damit die Tests sie direkt pruefen
koennen. Genutzt von fetch_kleine_anfragen.py, summarize_kleine_anfragen.py
und wochenliste_kleine_anfragen.py.

Wichtig fuer alles, was Zahlen betrifft: Eine Antwort-Drucksache enthaelt
drei Sorten Text.
  1. Vorbemerkung der Fragesteller: Behauptungen und Zahlen der Fraktion,
     keine Angaben der Bundesregierung.
  2. Die Fragen selbst (im PDF kleiner gedruckt, im Volltext nur an "?" zu
     erkennen).
  3. Vorbemerkung und Antworten der Bundesregierung.
Kernzahlen fuer den Tracker duerfen nur aus 3. stammen. bereich_fragesteller()
liefert die Spanne von 1., zahl_belegt() prueft, ob ein Zitat ausserhalb davon
woertlich im Text steht und die Zahl enthaelt.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

# Fraktionskuerzel fuer Filter und Anzeige. Schluessel: Teilstring der
# DIP-Initiative in Kleinbuchstaben, Reihenfolge zaehlt (spezifisch zuerst).
FRAKTIONEN: tuple[tuple[str, str], ...] = (
    ("afd", "afd"),
    ("cdu/csu", "cdu_csu"),
    ("spd", "spd"),
    ("bündnis 90/die grünen", "gruene"),
    ("grünen", "gruene"),
    ("die linke", "linke"),
    ("bsw", "bsw"),
    ("fdp", "fdp"),
)

FRAKTION_NAMEN = {
    "afd": "AfD",
    "cdu_csu": "CDU/CSU",
    "spd": "SPD",
    "gruene": "Grüne",
    "linke": "Die Linke",
    "bsw": "BSW",
    "fdp": "FDP",
    "mehrere": "mehrere",
    "sonstige": "sonstige",
}

# Formulierungen, mit denen die Bundesregierung sagt, dass sie etwas nicht
# weiss oder nicht sagt. Eine hohe Zahl ist selbst ein Thema ("Regierung
# weiss nicht, wie viele ...").
NICHT_ERHOBEN_RE = re.compile(
    r"(statistisch nicht erfasst|nicht statistisch erfasst|werden nicht erhoben|"
    r"nicht erhoben oder vorgehalten|liegen (?:der Bundesregierung )?keine (?:eigenen )?"
    r"(?:Erkenntnisse|Daten|Informationen|Angaben)|keine Erkenntnisse im Sinne|"
    r"mit (?:vertretbarem|zumutbarem) Aufwand nicht|nicht mit vertretbarem Aufwand|"
    r"liegen (?:hierzu )?keine (?:statistischen )?Daten vor)",
    re.IGNORECASE,
)

# Teile als Verschlusssache (Geheimschutzstelle) oder nicht oeffentlich
EINGESTUFT_RE = re.compile(
    r"(VS\s?-\s?(?:Nur für den Dienstgebrauch|NfD|Vertraulich|Geheim)|Geheimschutzstelle)",
    re.IGNORECASE,
)

# Tabellen als Anlage, die nur im PDF stehen
ANLAGE_RE = re.compile(
    r"(Anlage(?:n)?\s+\d|ist auf Bundestagsdrucksache \d+/\d+ auf der Internetseite)",
    re.IGNORECASE,
)

FRAGE_DS_RE = re.compile(r"Drucksache\s+(\d{1,2}/\d{1,6})")
EINZELFRAGEN_RE = re.compile(r"insgesamt\s+(\d+)\s+Einzelfragen", re.IGNORECASE)
FRAGESTELLER_RE = re.compile(
    r"auf die Kleine Anfrage der Abgeordneten\s+(.+?)\s+(?:und der Fraktion|der Fraktion|und der Gruppe)",
    re.IGNORECASE | re.DOTALL,
)

# Gesperrte Ueberschrift im Volltext: "V o r b e m e r k u n g  d e r  F r a g e s t e l l e r"
_SPERR = lambda wort: r"\s*".join(wort)  # noqa: E731
VORBEM_FRAGESTELLER_RE = re.compile(
    _SPERR("Vorbemerkung") + r"\s+" + _SPERR("der") + r"\s+" + _SPERR("Fragesteller"), re.IGNORECASE
)
VORBEM_REGIERUNG_RE = re.compile(
    _SPERR("Vorbemerkung") + r"\s+" + _SPERR("der") + r"\s+" + _SPERR("Bundesregierung"), re.IGNORECASE
)
ERSTE_FRAGE_RE = re.compile(r"\n\s*1\.\s+\S")


def fraktion_kuerzel(initiativen: list[str] | None) -> str | None:
    """DIP-Initiativen ("Fraktion der AfD") -> Kuerzel; mehrere Fraktionen -> 'mehrere'."""
    gefunden: list[str] = []
    for ini in initiativen or []:
        s = (ini or "").lower()
        if "bundesregierung" in s or "bundesrat" in s:
            continue
        kuerzel = next((k for teil, k in FRAKTIONEN if teil in s), "sonstige")
        if kuerzel not in gefunden:
            gefunden.append(kuerzel)
    if not gefunden:
        return None
    return gefunden[0] if len(gefunden) == 1 else "mehrere"


def titel_bereinigen(titel: str | None) -> str:
    """Antwort-Titel "auf die Kleine Anfrage\\r\\n- Drucksache 21/8797 -\\r\\nThema" -> "Thema"."""
    t = (titel or "").replace("\r", "\n")
    teile = [z.strip() for z in t.split("\n") if z.strip()]
    rest = [z for z in teile if not z.lower().startswith("auf die") and not FRAGE_DS_RE.search(z)]
    return re.sub(r"\s+", " ", " ".join(rest or teile)).strip()


def frage_drucksache(titel: str | None) -> str | None:
    m = FRAGE_DS_RE.search(titel or "")
    return m.group(1) if m else None


def einzelfragen(abstract: str | None) -> int | None:
    m = EINZELFRAGEN_RE.search(abstract or "")
    return int(m.group(1)) if m else None


def fragesteller(text: str | None) -> list[str]:
    """Namen aus dem Kopf der Drucksache, ohne "weiterer Abgeordneter"."""
    m = FRAGESTELLER_RE.search((text or "")[:3000])
    if not m:
        return []
    roh = re.sub(r"\s+", " ", m.group(1))
    namen = []
    for teil in re.split(r",\s*", roh):
        teil = re.sub(r"\s+weiterer Abgeordneter.*$", "", teil).strip(" ,")
        if teil and not teil.lower().startswith("weiterer"):
            namen.append(teil)
    return namen


def bereich_fragesteller(text: str) -> tuple[int, int] | None:
    """(start, ende) der Vorbemerkung der Fragesteller im Volltext, sonst None.

    Ende ist die Vorbemerkung der Bundesregierung oder die erste nummerierte
    Frage, je nachdem was zuerst kommt.
    """
    m = VORBEM_FRAGESTELLER_RE.search(text)
    if not m:
        return None
    enden = [e.start() for e in (VORBEM_REGIERUNG_RE.search(text, m.end()), ERSTE_FRAGE_RE.search(text, m.end())) if e]
    return (m.start(), min(enden) if enden else len(text))


def _norm(s: str) -> str:
    """Fuer den Vergleich: Unicode normalisieren, Trennstriche und Leerraum vereinheitlichen."""
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("­", "")  # weiches Trennzeichen
    s = re.sub(r"-\s*\n\s*", "-", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()


def _ziffern(s: str) -> str:
    """Nur Ziffern: "2 998" und "2.998" werden "2998", "3,27 Mrd." wird "327"."""
    return re.sub(r"\D", "", s)


def zahl_belegt(text: str, zitat: str, wert: str) -> bool:
    """True, wenn `zitat` woertlich (bis auf Leerraum) in den Regierungsteilen steht
    und die Ziffern von `wert` enthaelt.

    So fallen erfundene Zahlen, Zahlen aus der Vorbemerkung der Fragesteller und
    Zahlen aus den Fragen heraus.
    """
    if not zitat or not wert or len(zitat) < 8:
        return False
    z = _norm(zitat)
    if z.endswith("?"):
        return False
    w = _ziffern(wert)
    if not w or w not in _ziffern(z):
        return False
    bereich = bereich_fragesteller(text)
    if bereich:
        text = text[: bereich[0]] + "\n" + text[bereich[1]:]
    return z in _norm(text)


def merkmale(text: str | None) -> dict[str, int]:
    """Zaehlbare Signale ohne KI: Nicht-Wissen, Anlagen, Verschlusssachen."""
    t = text or ""
    return {
        "nicht_erhoben": len(NICHT_ERHOBEN_RE.findall(t)),
        "hat_anlage": 1 if ANLAGE_RE.search(t) else 0,
        "eingestuft": 1 if EINGESTUFT_RE.search(t) else 0,
    }


def text_hash(text: str | None) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


def fuer_prompt(text: str, max_zeichen: int) -> str:
    """Volltext fuer das Modell: Vorbemerkung der Fragesteller markiert, Laenge begrenzt.

    Der Anfang (Kopf, Vorbemerkungen, erste Antworten) traegt fast immer die
    Kernaussagen. Sehr lange Drucksachen sind meist lange Tabellen; deren Ende
    wird abgeschnitten und das im Prompt gesagt.
    """
    b = bereich_fragesteller(text)
    if b:
        text = (
            text[: b[0]]
            + "\n[BEGINN VORBEMERKUNG DER FRAGESTELLER: Angaben der Fraktion, keine Angaben der Bundesregierung]\n"
            + text[b[0]: b[1]]
            + "\n[ENDE VORBEMERKUNG DER FRAGESTELLER]\n"
            + text[b[1]:]
        )
    if len(text) > max_zeichen:
        text = text[:max_zeichen] + "\n[... TEXT GEKUERZT ...]"
    return text
