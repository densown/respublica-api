#!/usr/bin/env python3
"""Kurztext je namentlicher Abstimmung: zwei Saetze, Deutsch und Englisch.

Der Text steht im Dashboard in der Abstimmungsliste und auf dem Bild zum
Teilen direkt unter dem Titel. Luca hat am 19.10.2026 entschieden, dass er
ohne Freigabe erscheint; die Seite kennzeichnet ihn als automatisch
zusammengefasst. Grundlage sind nur Titel und Beschreibung von
abgeordnetenwatch (abstimmung_details.intro). Saetze mit dem Ergebnis
nimmt ohne_ergebnis() vorher heraus, das Modell bekommt es nicht zu sehen.

Die Texte schreibt Claude ueber die CLI im Max-Plan (lib/claude.py), bis zu
10 Abstimmungen je Aufruf und hoechstens 4 Aufrufe je Lauf. Groq
(openai/gpt-oss-120b) schrieb im Probelauf vom 19.10.2026 bei 4 von 7
Texten fehlerhaftes Deutsch oder falsche Fachbegriffe.

kurztext_hash ist SHA1 der Beschreibung, aus der der Text entstand. Aendert
sich die Beschreibung, schreibt der naechste Lauf den Text neu, und die API
zeigt den alten bis dahin nicht mehr an. Faellt ein Text zweimal durch die
Pruefung, bleibt kurztext leer und der Hash gesetzt, damit der Lauf nicht
jeden Tag dieselbe Abstimmung neu versucht; --fehlgeschlagene holt das nach.

Cron taeglich 06:40 UTC nach fetch_abstimmungen.py (06:10) und
fetch_votes.py (06:12) und vor dem Brain-Briefing (07:00), das die CLI
ebenfalls nutzt, siehe config/kurztexte-abstimmungen.cron.fragment.

  python scripts/kurztexte_abstimmungen.py                    # bis zu 20 neue
  python scripts/kurztexte_abstimmungen.py --probelauf --limit 5
  python scripts/kurztexte_abstimmungen.py --poll 6733        # eine neu schreiben
  python scripts/kurztexte_abstimmungen.py --fehlgeschlagene  # leere nochmal
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.claude import call_claude  # noqa: E402
from lib.log import (  # noqa: E402
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)

LOCK_NAME = "kurztexte_abstimmungen"
MODELL = "claude-cli"
# Je Stapel ein Aufruf und einer fuer die Durchgefallenen. Mehr als 5
# CLI-Aufrufe je Lauf sind im Projekt nicht erlaubt, also hoechstens 2 Stapel;
# der erste Durchlauf ueber alle Abstimmungen verteilt sich auf mehrere Tage.
# Der Prompt geht als ein Argument an die CLI (Linux: hoechstens 128 KB),
# 10 Beschreibungen bleiben deutlich darunter.
STAPEL = 10
MAX_STAPEL = 2
LIMIT = STAPEL * MAX_STAPEL
PAUSE_SEC = 5
TIMEOUT_SEC = 300
MAX_ZEICHEN = 320

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


class ClaudeFehler(RuntimeError):
    """CLI fehlt, Zeitueberschreitung oder Kontingent weg: kein Fehler des Textes."""


PROMPT = """Du schreibst für Res.Publica, eine unabhängige politische Datenplattform, zu jeder der folgenden namentlichen Abstimmungen im Bundestag einen Kurztext. Er steht in der Abstimmungsliste und auf einem Bild für Instagram direkt unter dem Titel.

Regeln für jeden Kurztext:
- Genau zwei vollständige Sätze, zusammen höchstens 260 Zeichen. Lies jeden Satz noch einmal: Er muss grammatisch vollständig sein.
- Satz 1 sagt, wer was wollte. Den Antragsteller nimmst du nur aus der Beschreibung: die Bundesregierung, eine oder mehrere Fraktionen oder eine Gruppe von Abgeordneten. Nennt die Beschreibung keinen, beginne mit dem Vorhaben selbst, etwa „Der Gesetzentwurf sollte …“. Nie mit „Der Bundestag“ beginnen.
- Bei einem Gesetzentwurf heißt es „legte einen Gesetzentwurf vor“ oder „wollte“, nicht „beantragte“.
- Satz 2: ein konkretes Detail oder die Begründung, als Sicht der Antragsteller gekennzeichnet. Eine Zahl nur in dem Zusammenhang, in dem sie in der Beschreibung steht.
- Nur wenn Titel oder Beschreibung ausdrücklich eine Beschlussempfehlung nennen, darf Satz 2 sagen, was der Ausschuss empfahl, etwa den Antrag abzulehnen.
- Nenne nicht, wie die Abstimmung ausging, und keine Stimmenzahlen. Die Wörter beschloss, beschlossen, verabschiedet, angenommen, abgelehnt, lehnte und stimmte kommen nicht vor.
- Sachlich und neutral, keine Wertung, keine Wörter wie umstritten oder wichtig.
- Präteritum. Parteien so: die Union, die SPD, die AfD, die Grünen, die Linke. Also „Die Grünen wollten …“, nie „Die Fraktion Grüne“ oder „BÜNDNIS 90/DIE GRÜNEN“. Einzahl und Mehrzahl passen in beiden Sätzen zusammen.
- Steht in der Beschreibung ein Satz, der nicht zum Titel passt, lass ihn weg.
- Nur Fakten aus Titel und Beschreibung, nichts dazuerfinden.

Dazu dieselben zwei Sätze auf Englisch für internationale Leser: inhaltlich genau dasselbe wie auf Deutsch, nichts weglassen und nichts ergänzen. Parteien so: CDU/CSU, SPD, AfD, the Greens, the Left. Auch dort nicht mit „The Bundestag“ beginnen und kein approved, adopted, passed, rejected oder voted.

{abstimmungen}

Antworte nur mit JSON, ohne Markdown, ein Eintrag je Abstimmungsnummer: {{"6733": {{"de": "...", "en": "..."}}}}"""

ABSTIMMUNG = """### Abstimmung {poll_id}

Titel: {titel}

Beschreibung von abgeordnetenwatch.de:
{intro}
{hinweis}{nochmal}"""

# Titel wie "Beschlussempfehlung zu drittem Antrag: Organisierte
# Steuerhinterziehung wie bisher bekaempfen" beschreiben, wofuer ein Ja stand.
# Im Probelauf vom 19.10. wurde daraus "Die Grünen wollten ... wie bisher
# bekämpfen", also das Gegenteil des Antrags.
HINWEIS_ABLEHNUNG = (
    "\nAchtung: Der Ausschuss empfahl, den Antrag abzulehnen. Der Titel beschreibt deshalb, "
    "wofür ein Ja stand, nicht, was der Antragsteller wollte. Was er wollte, steht in der "
    "Beschreibung, meist das Gegenteil des Titels.\n"
)

NOCHMAL = "\nDein letzter Vorschlag zu dieser Abstimmung passte nicht: {grund}. Schreib die beiden Sätze neu und halte dich an alle Regeln.\n"

# Was auf ein Ergebnis oder eine Wertung hindeutet. Die Empfehlung des
# Ausschusses ("empfahl, den Antrag abzulehnen") bleibt erlaubt.
VERBOTEN_DE = re.compile(
    r"\b(beschloss\w*|beschlossen|verabschiede\w*|angenommen|abgelehnt|lehnte|stimmte\w*|"
    r"Stimmen|Mehrheit|umstritten\w*|wichtig\w*|historisch\w*)\b",
    re.IGNORECASE,
)
VERBOTEN_EN = re.compile(
    r"\b(approved|adopted|passed|rejected|voted|votes|majority|controversial|landmark)\b",
    re.IGNORECASE,
)
START_DE = re.compile(r"^\s*(Der Bundestag|Das Parlament)\b")
START_EN = re.compile(r"^\s*(The Bundestag|Parliament|The German Bundestag)\b")

# Saetze der Beschreibung, die das Ergebnis nennen. Bei 6733 haengt
# ausserdem ein Satz aus einer anderen Abstimmung an ("Damit wurde die
# weitere Planung der Bahnstrecke beschlossen."), auch der faellt so weg.
ERGEBNIS_SATZ = re.compile(
    r"Ja-Stimmen|Nein-Stimmen|Enthaltung|\bmit den Stimmen\b|\bstimmten?\b|\benthielten?\b|^Damit wurden?\b",
    re.IGNORECASE,
)
SATZGRENZE = re.compile(r"(?<=[.!?])\s+(?=[A-ZÄÖÜ„])")
ZAHL = re.compile(r"\d+(?:[.,]\d+)*")
BUNDESREGIERUNG_EN = re.compile(r"\b(federal|German) government\b", re.IGNORECASE)
FRAKTION_STATT_PARTEI = re.compile(
    r"\bFraktion(en)? (der |die |Die )?(Grüne|Linke|SPD|AfD|Union|CDU)|BÜNDNIS|Bündnis 90"
)
TITEL_UEBERNOMMEN_DE = re.compile(r"wie bisher|beibehalten|weiterhin|\berhalten\b", re.IGNORECASE)
TITEL_UEBERNOMMEN_EN = re.compile(r"as before|\b(retain|keep|maintain|preserve)", re.IGNORECASE)


def intro_hash(intro: str) -> str:
    """Gleich wie SHA1(intro) in MariaDB (utf8mb4 hasht die UTF-8-Bytes)."""
    return hashlib.sha1(intro.encode("utf-8")).hexdigest()


def ohne_ergebnis(intro: str) -> str:
    """Beschreibung ohne die Saetze mit Stimmenzahlen und Ergebnis."""
    absaetze = []
    for absatz in re.split(r"\n\s*\n", intro.strip()):
        saetze = [s for s in SATZGRENZE.split(absatz.strip()) if s and not ERGEBNIS_SATZ.search(s)]
        if saetze:
            absaetze.append(" ".join(saetze))
    return "\n\n".join(absaetze) or intro.strip()


def ablehnung_empfohlen(titel: str, intro: str) -> bool:
    return titel.strip().startswith("Beschlussempfehlung") and bool(re.search(r"Ablehnung|abzulehnen", intro))


def _zahlen(text: str) -> set[str]:
    return {z.replace(".", "").replace(",", "") for z in ZAHL.findall(text)}


def prompt_bauen(stapel: list[tuple[int, str, str, str | None]]) -> str:
    """Ein Prompt fuer mehrere Abstimmungen: (poll_id, titel, intro ohne Ergebnis, Grund oder None)."""
    bloecke = [
        ABSTIMMUNG.format(
            poll_id=poll_id,
            titel=titel.strip(),
            intro=intro.strip(),
            hinweis=HINWEIS_ABLEHNUNG if ablehnung_empfohlen(titel, intro) else "",
            nochmal=NOCHMAL.format(grund=grund) if grund else "",
        )
        for poll_id, titel, intro, grund in stapel
    ]
    return PROMPT.format(abstimmungen="\n".join(bloecke).strip())


def antwort_lesen(roh: str) -> dict[int, dict[str, str]]:
    """Texte je poll_id aus der Antwort, auch mit Markdown drumherum.

    Eintraege ohne de und en fehlen im Ergebnis; erzeugen() behandelt sie
    wie einen durchgefallenen Text.
    """
    text = roh.replace("```json", "").replace("```", "").strip()
    start, ende = text.find("{"), text.rfind("}")
    if start < 0 or ende <= start:
        return {}
    try:
        daten = json.loads(text[start : ende + 1])
    except json.JSONDecodeError:
        return {}
    if not isinstance(daten, dict):
        return {}
    texte = {}
    for schluessel, eintrag in daten.items():
        if not str(schluessel).isdigit() or not isinstance(eintrag, dict):
            continue
        de, en = eintrag.get("de"), eintrag.get("en")
        if isinstance(de, str) and isinstance(en, str):
            texte[int(schluessel)] = {"de": " ".join(de.split()), "en": " ".join(en.split())}
    return texte


def pruefen(texte: dict[str, str], titel: str = "", intro: str = "") -> str | None:
    """Grund, warum der Vorschlag nicht taugt, oder None.

    titel und intro (ohne Ergebnis) braucht nur der inhaltliche Teil: wer
    der Antragsteller ist, welche Zahlen vorkommen duerfen.
    """
    for sprache, text in texte.items():
        if not text:
            return f"{sprache} ist leer"
        if len(text) > MAX_ZEICHEN:
            return f"{sprache} ist {len(text)} Zeichen lang"
        if text[-1] not in ".!?":
            return f"{sprache} endet nicht mit einem Satzende"
        if "*" in text or "#" in text:
            return f"{sprache} enthält Markdown"
    if START_DE.search(texte["de"]) or START_EN.search(texte["en"]):
        return "ein Satz beginnt mit dem Bundestag statt mit dem Antragsteller"
    treffer = VERBOTEN_DE.search(texte["de"]) or VERBOTEN_EN.search(texte["en"])
    if treffer:
        return f"„{treffer.group(0)}“ nennt das Ergebnis oder wertet"
    if not intro:
        return None
    de, en = texte["de"], texte["en"]
    if "Bundesregierung" not in intro and ("Bundesregierung" in de or BUNDESREGIERUNG_EN.search(en)):
        return "die Bundesregierung steht in der Beschreibung nicht als Antragstellerin"
    if "Gesetzentwurf" in titel and re.search(r"\bbeantragte", de):
        return "bei einem Gesetzentwurf heißt es nicht „beantragte“"
    treffer = FRAKTION_STATT_PARTEI.search(de)
    if treffer:
        return f"„{treffer.group(0)}“ statt der vorgegebenen Parteinamen"
    if ablehnung_empfohlen(titel, intro):
        treffer = TITEL_UEBERNOMMEN_DE.search(SATZGRENZE.split(de)[0]) or TITEL_UEBERNOMMEN_EN.search(
            SATZGRENZE.split(en)[0]
        )
        if treffer:
            return f"Satz 1 übernimmt mit „{treffer.group(0)}“ den Titel statt des Antrags"
    fremd = sorted((_zahlen(de) | _zahlen(en)) - _zahlen(intro))
    if fremd:
        return f"die Zahl {fremd[0]} steht so nicht in der Beschreibung"
    return None


def erzeugen(stapel: list[tuple[int, str, str]]) -> dict[int, tuple[dict[str, str] | None, str | None]]:
    """Texte fuer (poll_id, titel, intro), je poll_id (Texte, None) oder (None, letzter Grund).

    Hoechstens zwei Aufrufe: alle zusammen, dann die Durchgefallenen mit
    ihrem Grund. ClaudeFehler geht durch, die Abstimmungen bleiben offen.
    """
    offen = {poll_id: (titel, ohne_ergebnis(intro), None) for poll_id, titel, intro in stapel}
    ergebnis: dict[int, tuple[dict[str, str] | None, str | None]] = {}
    for versuch in range(2):
        if not offen:
            break
        if versuch:
            time.sleep(PAUSE_SEC)
        roh = call_claude(
            prompt_bauen([(poll_id, titel, quelle, grund) for poll_id, (titel, quelle, grund) in offen.items()]),
            timeout=TIMEOUT_SEC,
            log=log.warning,
        )
        if roh is None:
            raise ClaudeFehler("Claude-CLI ohne Antwort")
        antworten = antwort_lesen(roh)
        naechste = {}
        for poll_id, (titel, quelle, _) in offen.items():
            texte = antworten.get(poll_id)
            grund = "keine Antwort mit de und en" if texte is None else pruefen(texte, titel, quelle)
            if grund is None:
                ergebnis[poll_id] = (texte, None)
            else:
                naechste[poll_id] = (titel, quelle, grund)
        offen = naechste
    for poll_id, (_, _, grund) in offen.items():
        ergebnis[poll_id] = (None, grund)
    return ergebnis


def offene(cur, limit: int, poll: int | None, fehlgeschlagene: bool) -> list[tuple]:
    sql = """
        SELECT d.poll_id,
               (SELECT MAX(a.poll_titel) FROM abstimmungen a WHERE a.poll_id = d.poll_id),
               d.intro
          FROM abstimmung_details d
         WHERE d.intro IS NOT NULL AND d.intro <> ''
    """
    params: list = []
    if poll is not None:
        sql += " AND d.poll_id = %s"
        params.append(poll)
    elif fehlgeschlagene:
        sql += " AND d.kurztext IS NULL AND d.kurztext_hash = SHA1(d.intro)"
    else:
        sql += " AND (d.kurztext_hash IS NULL OR d.kurztext_hash <> SHA1(d.intro))"
    sql += " ORDER BY d.poll_id DESC LIMIT %s"
    params.append(limit)
    cur.execute(sql, params)
    return cur.fetchall()


def speichern(cur, poll_id: int, intro: str, texte: dict[str, str] | None) -> None:
    cur.execute(
        """UPDATE abstimmung_details
              SET kurztext = %s, kurztext_en = %s, kurztext_hash = %s,
                  kurztext_modell = %s, kurztext_am = NOW()
            WHERE poll_id = %s""",
        (
            texte["de"] if texte else None,
            texte["en"] if texte else None,
            intro_hash(intro),
            MODELL,
            poll_id,
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Kurztexte je Abstimmung schreiben")
    ap.add_argument("--limit", type=int, default=LIMIT, help=f"hoechstens so viele, je Lauf nie mehr als {LIMIT}")
    ap.add_argument("--poll", type=int, help="nur diese Abstimmung, auch wenn sie schon einen Text hat")
    ap.add_argument("--fehlgeschlagene", action="store_true", help="Abstimmungen ohne brauchbaren Text nochmal")
    ap.add_argument("--probelauf", action="store_true", help="nur anzeigen, nichts schreiben")
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)
    load_env()

    db = None
    try:
        db = get_db(autocommit=False)
        cur = db.cursor()
        zeilen = offene(cur, max(args.limit, 1), args.poll, args.fehlgeschlagene)
        log.info("%d Abstimmungen ohne aktuellen Kurztext%s", len(zeilen), " (Probelauf)" if args.probelauf else "")

        if len(zeilen) > LIMIT:
            log.info("Nur die ersten %d in diesem Lauf, Rest beim naechsten", LIMIT)
            zeilen = zeilen[:LIMIT]

        geschrieben = leer = 0
        for start in range(0, len(zeilen), STAPEL):
            if not _running:
                log.warning("Abbruch angefordert, beende nach %d Abstimmungen", start)
                break
            if start:
                time.sleep(PAUSE_SEC)
            stapel = zeilen[start : start + STAPEL]
            try:
                ergebnisse = erzeugen([(poll_id, titel or "", intro) for poll_id, titel, intro in stapel])
            except ClaudeFehler as exc:
                log.error("%s, Rest beim naechsten Lauf", exc)
                break
            for poll_id, _titel, intro in stapel:
                texte, grund = ergebnisse[poll_id]
                if texte:
                    log.info("%s: %s | %s", poll_id, texte["de"], texte["en"])
                    geschrieben += 1
                else:
                    log.warning("%s: kein brauchbarer Text (%s)", poll_id, grund)
                    leer += 1
                if not args.probelauf:
                    speichern(cur, poll_id, intro, texte)
            if not args.probelauf:
                db.commit()

        verb = "wuerden geschrieben" if args.probelauf else "geschrieben"
        log.info("Fertig: %d %s, %d ohne brauchbaren Text", geschrieben, verb, leer)
        return 0
    finally:
        if db is not None:
            db.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
