#!/usr/bin/env python3
"""Kurztext je namentlicher Abstimmung: zwei Saetze, Deutsch und Englisch.

Der Text steht im Dashboard in der Abstimmungsliste und auf dem Bild zum
Teilen direkt unter dem Titel. Luca hat am 19.10.2026 entschieden, dass er
ohne Freigabe erscheint; die Seite kennzeichnet ihn als automatisch
zusammengefasst. Grundlage sind nur Titel und Beschreibung von
abgeordnetenwatch (abstimmung_details.intro), das Ergebnis bekommt das
Modell nicht zu sehen.

kurztext_hash ist SHA1 der Beschreibung, aus der der Text entstand. Aendert
sich die Beschreibung, schreibt der naechste Lauf den Text neu, und die API
zeigt den alten bis dahin nicht mehr an. Faellt ein Text zweimal durch die
Pruefung, bleibt kurztext leer und der Hash gesetzt, damit der Lauf nicht
jeden Tag dieselbe Abstimmung neu versucht; --fehlgeschlagene holt das nach.

Cron taeglich 06:40 UTC nach fetch_abstimmungen.py (06:10) und
fetch_votes.py (06:12), siehe config/kurztexte-abstimmungen.cron.fragment.

  python scripts/kurztexte_abstimmungen.py                    # bis zu 30 neue
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
from lib.groq import GroqError, chat_completion  # noqa: E402
from lib.log import (  # noqa: E402
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)

LOCK_NAME = "kurztexte_abstimmungen"
MODELL = "openai/gpt-oss-120b"
# Tageskontingent bei Groq teilen sich alle Skripte; der erste Durchlauf
# ueber alle Abstimmungen verteilt sich so auf mehrere Tage.
LIMIT = 30
PAUSE_SEC = 8
MAX_ZEICHEN = 320

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


PROMPT = """Du schreibst für Res.Publica, eine unabhängige politische Datenplattform, einen Kurztext zu einer namentlichen Abstimmung im Bundestag. Er steht in der Abstimmungsliste und auf einem Bild für Instagram direkt unter dem Titel.

Titel der Abstimmung: {titel}

Beschreibung von abgeordnetenwatch.de:
{intro}

Regeln:
- Genau zwei vollständige Sätze, zusammen höchstens 260 Zeichen. Lies jeden Satz noch einmal: Er muss grammatisch vollständig sein.
- Satz 1 beginnt mit dem Antragsteller (Bundesregierung, eine Fraktion oder mehrere) und sagt, was er wollte. Nie mit „Der Bundestag“ beginnen.
- Satz 2: das wichtigste Detail oder die Begründung, als Sicht der Antragsteller gekennzeichnet.
- Nur wenn Titel oder Beschreibung ausdrücklich eine Beschlussempfehlung nennen, sag im zweiten Satz, was der Ausschuss empfahl, etwa den Antrag abzulehnen.
- Nenne nicht, wie die Abstimmung ausging, und keine Stimmenzahlen. Die Wörter beschloss, beschlossen, verabschiedet, angenommen, abgelehnt, lehnte und stimmte kommen nicht vor.
- Sachlich und neutral, keine Wertung, keine Wörter wie umstritten oder wichtig.
- Präteritum. Parteien so: Union, SPD, AfD, Grüne, Linke.
- Steht in der Beschreibung ein Satz, der nicht zum Titel passt, lass ihn weg.
- Nur Fakten aus Titel und Beschreibung, nichts dazuerfinden.

Dazu dieselben zwei Sätze auf Englisch für internationale Leser, Parteien so: CDU/CSU, SPD, AfD, the Greens, the Left. Auch dort nicht mit „The Bundestag“ beginnen und kein approved, adopted, passed, rejected oder voted.

Antworte nur mit JSON, ohne Markdown: {{"de": "...", "en": "..."}}"""

NOCHMAL = "\n\nDein letzter Vorschlag passte nicht: {grund}. Schreib die beiden Sätze neu und halte dich an alle Regeln."

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


def intro_hash(intro: str) -> str:
    """Gleich wie SHA1(intro) in MariaDB (utf8mb4 hasht die UTF-8-Bytes)."""
    return hashlib.sha1(intro.encode("utf-8")).hexdigest()


def prompt_bauen(titel: str, intro: str, grund: str | None = None) -> str:
    text = PROMPT.format(titel=titel.strip(), intro=intro.strip())
    if grund:
        text += NOCHMAL.format(grund=grund)
    return text


def antwort_lesen(roh: str) -> dict[str, str] | None:
    """JSON aus der Antwort, auch wenn das Modell Markdown drumherum schreibt."""
    text = roh.replace("```json", "").replace("```", "").strip()
    start, ende = text.find("{"), text.rfind("}")
    if start < 0 or ende <= start:
        return None
    try:
        daten = json.loads(text[start : ende + 1])
    except json.JSONDecodeError:
        return None
    if not isinstance(daten, dict):
        return None
    de, en = daten.get("de"), daten.get("en")
    if not isinstance(de, str) or not isinstance(en, str):
        return None
    return {"de": " ".join(de.split()), "en": " ".join(en.split())}


def pruefen(texte: dict[str, str]) -> str | None:
    """Grund, warum der Vorschlag nicht taugt, oder None."""
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
    return None


def erzeugen(titel: str, intro: str) -> tuple[dict[str, str] | None, str | None]:
    """Bis zu zwei Versuche. (Texte, None) oder (None, letzter Grund).

    GroqError geht durch: Kontingent weg oder Groq nicht erreichbar ist
    kein Fehler des Textes, die Abstimmung bleibt offen.
    """
    grund: str | None = None
    for versuch in range(2):
        if versuch:
            time.sleep(PAUSE_SEC)
        roh = chat_completion(
            [{"role": "user", "content": prompt_bauen(titel, intro, grund)}],
            model=MODELL,
            max_tokens=500,
            temperature=0.2,
        )
        texte = antwort_lesen(roh)
        grund = "die Antwort war kein JSON mit de und en" if texte is None else pruefen(texte)
        if grund is None:
            return texte, None
    return None, grund


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
    ap.add_argument("--limit", type=int, default=LIMIT, help=f"hoechstens so viele (Standard {LIMIT})")
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

        geschrieben = leer = 0
        for i, (poll_id, titel, intro) in enumerate(zeilen):
            if not _running:
                log.warning("Abbruch angefordert, beende nach %d Abstimmungen", i)
                break
            if i:
                time.sleep(PAUSE_SEC)
            try:
                texte, grund = erzeugen(titel or "", intro)
            except GroqError as exc:
                log.error("%s: Groq-Fehler (%s)", poll_id, exc)
                if exc.quota_exhausted:
                    log.warning("Kontingent erschoepft, Rest morgen")
                    break
                continue
            if texte:
                log.info("%s: %s | %s", poll_id, texte["de"], texte["en"])
                geschrieben += 1
            else:
                log.warning("%s: kein brauchbarer Text (%s)", poll_id, grund)
                leer += 1
            if not args.probelauf:
                speichern(cur, poll_id, intro, texte)
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
