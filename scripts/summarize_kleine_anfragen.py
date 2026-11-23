#!/usr/bin/env python3
"""Zusammenfassung, Kernzahlen und Themen-Einschaetzung je Antwort auf eine Kleine Anfrage.

Schreibt in kleine_anfragen (Migration 021): zusammenfassung (DE), zusammenfassung_en,
schlagzeile, kernzahlen (JSON), story_wert (0 bis 10), story_grund.

Modell: Claude ueber die CLI im Max-Plan (lib/claude.py), eine Antwort je
Aufruf, hoechstens 4 Aufrufe je Lauf (Projektregel: hoechstens 5). Der Prompt
geht als ein Argument an die CLI (Linux: hoechstens 128 KB), deshalb wird der
Volltext auf MAX_ZEICHEN gekuerzt; sehr lange Antworten sind fast immer lange
Tabellen, die Kernaussagen stehen vorne.

Pruefungen, bevor etwas gespeichert wird:
  - Jede Kernzahl braucht ein Zitat, das woertlich in einem Teil der
    Bundesregierung steht (nicht Vorbemerkung der Fragesteller, keine Frage)
    und die Zahl enthaelt. Andere Kernzahlen werden verworfen.
  - Jede Zahl in Zusammenfassung und Schlagzeile muss im Regierungsteil des
    Volltexts vorkommen. Sonst gilt der Text als durchgefallen.
Faellt ein Text zweimal durch, bleibt er leer und ki_fehlversuche zaehlt hoch;
nach 2 Fehlversuchen versucht der Lauf es nicht mehr (--fehlgeschlagene holt
das nach).

ki_text_hash = text_hash zum Zeitpunkt des Schreibens. Aendert das DIP den
Volltext, schreibt der naechste Lauf neu.

  python scripts/summarize_kleine_anfragen.py                     # bis zu 4 neue
  python scripts/summarize_kleine_anfragen.py --dokument 21/8937 --probelauf
  python scripts/summarize_kleine_anfragen.py --fehlgeschlagene
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import kleine_anfragen as ka  # noqa: E402
from lib.claude import call_claude  # noqa: E402
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock  # noqa: E402

LOCK_NAME = "summarize_kleine_anfragen"
MODELL = "claude-cli"
MAX_AUFRUFE = 4
MAX_ZEICHEN = 55_000
TIMEOUT_SEC = 300
PAUSE_SEC = 5
MAX_KERNZAHLEN = 5
MAX_FEHLVERSUCHE = 2

log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


class ClaudeFehler(RuntimeError):
    """CLI fehlt, Zeitueberschreitung oder Kontingent weg: kein Fehler des Textes."""


PROMPT = """Du wertest eine Antwort der Bundesregierung auf eine Kleine Anfrage des Bundestags aus.
Leser sind Journalisten, die schnell sehen wollen, welche neuen, belegten Zahlen darin stehen.

Regeln:
- Nur Angaben der Bundesregierung zählen. Der Abschnitt zwischen [BEGINN VORBEMERKUNG DER FRAGESTELLER]
  und [ENDE ...] enthält Behauptungen der fragenden Fraktion; übernimm daraus keine Zahl und keine Wertung.
  Sätze, die mit "?" enden, sind Fragen, keine Antworten.
- Übernimm nicht die Sprache oder Deutung der Fragesteller. Sachlich, ohne Wertung, ohne Gedankenstriche.
- Wenn die Bundesregierung etwas nicht weiß, nicht erhebt oder nicht beantwortet, ist das eine Aussage: nenne es.
- Zahlen exakt wie im Text, mit Bezugszeitraum und Stichtag. Rechne nicht selbst (keine eigenen Summen,
  Differenzen oder Prozentwerte), jede Zahl in deinem Text muss so in der Antwort stehen.

Gib NUR ein JSON-Objekt zurück, ohne Erklärung davor oder danach:
{{
  "zusammenfassung": "2 bis 3 Sätze Deutsch: was die Bundesregierung mitteilt, die wichtigsten Zahlen mit Zeitraum",
  "zusammenfassung_en": "dieselben Aussagen auf Englisch",
  "schlagzeile": "sachliche deutsche Schlagzeile mit der wichtigsten belegten Aussage, höchstens 110 Zeichen",
  "kernzahlen": [
    {{"wert": "7 025", "einheit": "Anträge", "bedeutung": "anerkannte Anträge auf Kriegsdienstverweigerung 2026 bis 30. September",
      "zitat": "ein zusammenhängender Ausschnitt von 8 bis 200 Zeichen, Zeichen für Zeichen aus dem Text kopiert, der die Zahl enthält"}}
  ],
  "story_wert": 0,
  "story_grund": "ein Satz: warum die Antwort ein Thema hergibt oder nicht"
}}

kernzahlen: höchstens {max_kernzahlen}, die aussagekräftigsten, nur aus Antworten der Bundesregierung. Leere Liste, wenn es keine gibt.
story_wert (0 bis 10), unabhängig davon, welche Fraktion fragt:
  8 bis 10: neue, bisher unveröffentlichte Zahl mit breiter Relevanz, starker Anstieg oder Rückgang,
            Widerspruch zu früheren Aussagen, oder die Regierung weiß Wichtiges nicht
  5 bis 7: solide neue Zahl für ein Fachpublikum oder eine laufende Debatte
  2 bis 4: wenig Neues, Verweise auf frühere Antworten, allgemeine Positionen
  0 bis 1: keine Substanz
{nachbesserung}
Titel: {titel}
Fraktion: {fraktion}
Federführendes Ressort: {ressort}
Datum der Antwort: {datum}

Volltext der Drucksache:
{text}
"""


def prompt_bauen(z: dict, grund: str | None) -> str:
    nach = ""
    if grund:
        nach = f"\nDein letzter Versuch wurde verworfen: {grund}. Halte dich genau an die Regeln.\n"
    return PROMPT.format(
        max_kernzahlen=MAX_KERNZAHLEN,
        nachbesserung=nach,
        titel=z["titel"],
        fraktion=ka.FRAKTION_NAMEN.get(z["fraktion"] or "", z["initiative"] or "unbekannt"),
        ressort=z["ressort"] or "unbekannt",
        datum=z["datum"],
        text=ka.fuer_prompt(z["volltext"], MAX_ZEICHEN),
    )


def antwort_lesen(roh: str) -> dict | None:
    """JSON aus der Modellantwort, auch mit ```json-Zaun oder Text drumherum."""
    m = re.search(r"\{.*\}", roh or "", re.DOTALL)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return d if isinstance(d, dict) else None


_ZAHL_RE = re.compile(r"\d[\d.   ]*\d|\d")


def _zahlen(s: str) -> set[str]:
    """Zahlen als Ziffernfolge ohne Tausendertrenner; Komma trennt (3,27 -> 3 und 27)."""
    return {re.sub(r"\D", "", z) for z in _ZAHL_RE.findall(s or "")} - {""}


def regierungstext(text: str) -> str:
    b = ka.bereich_fragesteller(text)
    if b:
        text = text[: b[0]] + "\n" + text[b[1]:]
    # Fragen raus: Saetze, die mit ? enden
    return re.sub(r"[^.?!]*\?", " ", text)


def pruefen(d: dict, text: str) -> tuple[dict | None, str | None]:
    """(bereinigte Felder, None) oder (None, Grund)."""
    felder = {}
    for k in ("zusammenfassung", "zusammenfassung_en", "schlagzeile", "story_grund"):
        v = d.get(k)
        if not isinstance(v, str) or not v.strip():
            return None, f"Feld {k} fehlt"
        felder[k] = re.sub(r"\s+", " ", v).strip()
    if len(felder["zusammenfassung"]) > 700 or len(felder["zusammenfassung_en"]) > 800:
        return None, "Zusammenfassung zu lang (höchstens 3 Sätze)"
    if len(felder["schlagzeile"]) > 140:
        return None, "Schlagzeile zu lang"
    if "—" in felder["zusammenfassung"] + felder["schlagzeile"]:
        return None, "Gedankenstrich verwendet"
    try:
        wert = int(d.get("story_wert"))
    except (TypeError, ValueError):
        return None, "story_wert fehlt"
    felder["story_wert"] = max(0, min(10, wert))

    gt = regierungstext(text)
    im_text = _zahlen(gt)
    for k in ("zusammenfassung", "schlagzeile"):
        fremd = sorted(z for z in _zahlen(felder[k]) if len(z) >= 2 and z not in im_text)
        if fremd:
            return None, f"Zahlen in {k} stehen nicht in der Antwort der Bundesregierung: {', '.join(fremd[:5])}"

    kern = []
    for kz in d.get("kernzahlen") or []:
        if not isinstance(kz, dict):
            continue
        wert_s, zitat = str(kz.get("wert") or ""), str(kz.get("zitat") or "")
        if ka.zahl_belegt(text, zitat, wert_s):
            kern.append({
                "wert": wert_s.strip(),
                "einheit": str(kz.get("einheit") or "").strip()[:60],
                "bedeutung": str(kz.get("bedeutung") or "").strip()[:200],
                "zitat": re.sub(r"\s+", " ", zitat).strip()[:300],
            })
        else:
            log.info("  Kernzahl verworfen (kein Beleg): %s", wert_s)
        if len(kern) >= MAX_KERNZAHLEN:
            break
    felder["kernzahlen"] = kern
    return felder, None


def erzeugen(z: dict, max_versuche: int = 2) -> tuple[dict | None, str | None, int]:
    """Bis zu max_versuche Aufrufe. Liefert (Felder, Grund, verbrauchte Aufrufe)."""
    grund = None
    for versuch in range(max_versuche):
        if versuch:
            time.sleep(PAUSE_SEC)
        roh = call_claude(prompt_bauen(z, grund), timeout=TIMEOUT_SEC, log=log.warning)
        if roh is None:
            raise ClaudeFehler("Claude-CLI ohne Antwort")
        d = antwort_lesen(roh)
        if d is None:
            grund = "kein gültiges JSON"
            continue
        felder, grund = pruefen(d, z["volltext"])
        if felder:
            return felder, None, versuch + 1
    return None, grund, max_versuche


def offene(cur, limit: int, dokument: str | None, fehlgeschlagene: bool) -> list[dict]:
    sql = """SELECT id, dokumentnummer, datum, titel, fraktion, initiative, ressort, volltext, text_hash
               FROM kleine_anfragen WHERE volltext IS NOT NULL"""
    params: list = []
    if dokument:
        sql += " AND dokumentnummer = %s"
        params.append(dokument)
    elif fehlgeschlagene:
        sql += " AND zusammenfassung IS NULL AND ki_fehlversuche >= %s"
        params.append(MAX_FEHLVERSUCHE)
    else:
        sql += " AND (ki_text_hash IS NULL OR ki_text_hash <> text_hash) AND ki_fehlversuche < %s"
        params.append(MAX_FEHLVERSUCHE)
    sql += " ORDER BY datum DESC, id DESC LIMIT %s"
    params.append(limit)
    cur.execute(sql, params)
    namen = [c[0] for c in cur.description]
    return [dict(zip(namen, r)) for r in cur.fetchall()]


def speichern(cur, z: dict, felder: dict | None) -> None:
    if felder is None:
        cur.execute(
            "UPDATE kleine_anfragen SET ki_fehlversuche = ki_fehlversuche + 1 WHERE id = %s", (z["id"],)
        )
        return
    cur.execute(
        """UPDATE kleine_anfragen
              SET zusammenfassung = %s, zusammenfassung_en = %s, schlagzeile = %s,
                  kernzahlen = %s, story_wert = %s, story_grund = %s,
                  ki_text_hash = %s, ki_modell = %s, ki_am = NOW(), ki_fehlversuche = 0
            WHERE id = %s""",
        (
            felder["zusammenfassung"], felder["zusammenfassung_en"], felder["schlagzeile"],
            json.dumps(felder["kernzahlen"], ensure_ascii=False), felder["story_wert"],
            felder["story_grund"][:600], z["text_hash"], MODELL, z["id"],
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dokument", help="eine Drucksache, z. B. 21/8937 (schreibt neu)")
    ap.add_argument("--fehlgeschlagene", action="store_true", help="zweimal durchgefallene nochmal")
    ap.add_argument("--probelauf", action="store_true", help="ausgeben statt speichern")
    args = ap.parse_args()

    load_env()
    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)
    conn = get_db(autocommit=True)
    cur = conn.cursor()
    try:
        liste = offene(cur, MAX_AUFRUFE, args.dokument, args.fehlgeschlagene)
        log.info("%d Antworten offen in diesem Lauf", len(liste))
        aufrufe = ok = 0
        for z in liste:
            if not _running or aufrufe >= MAX_AUFRUFE:
                break
            # Zweiter Versuch nur, wenn noch ein Aufruf frei ist
            try:
                felder, grund, n = erzeugen(z, min(2, MAX_AUFRUFE - aufrufe))
            except ClaudeFehler as e:
                log.error("%s: %s, Lauf endet", z["dokumentnummer"], e)
                break
            aufrufe += n
            if args.probelauf:
                print(json.dumps({"dokument": z["dokumentnummer"], "felder": felder, "grund": grund},
                                 ensure_ascii=False, indent=2))
                continue
            speichern(cur, z, felder)
            if felder:
                ok += 1
                log.info("%s: Wert %s, %d Kernzahlen", z["dokumentnummer"], felder["story_wert"], len(felder["kernzahlen"]))
            else:
                log.warning("%s: durchgefallen (%s)", z["dokumentnummer"], grund)
        log.info("Fertig: %d geschrieben, %d CLI-Aufrufe", ok, aufrufe)
        return 0
    finally:
        cur.close()
        conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
