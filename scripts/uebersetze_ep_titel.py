#!/usr/bin/env python3
"""Deutsche Titel fuer Abstimmungen im EU-Parlament (ep_abstimmungen.titel_de).

Der Export von HowTheyVote.eu fuehrt nur englische Titel. Dieses Skript laesst
sie von der Claude-CLI (Max-Plan, keine API-Kosten) ins Deutsche uebertragen.
Viele Abstimmungen teilen sich einen Titel (Ablehnungsantrag und Einigung zum
selben Gesetz), uebersetzt wird deshalb jeder Titel nur einmal.

Je Lauf hoechstens 4 Aufrufe mit je 40 Titeln. Jede Uebersetzung wird geprueft,
bevor sie gespeichert wird: gleiche Zahlen wie im Original, plausible Laenge,
kein Gedankenstrich. Was durchfaellt, bleibt englisch und kommt im naechsten
Lauf wieder dran. Die App kennzeichnet uebersetzte Titel und zeigt das Original.

  uebersetze_ep_titel.py              uebersetzt und speichert
  uebersetze_ep_titel.py --probelauf  zeigt, was gespeichert wuerde
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.claude import call_claude  # noqa: E402
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock  # noqa: E402

LOCK_NAME = "uebersetze_ep_titel"
STAPEL = 40
MAX_AUFRUFE = 4

log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


def prompt_bauen(titel: list[str]) -> str:
    zeilen = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(titel))
    return (
        "Übertrage diese Titel von Abstimmungen im Europäischen Parlament ins Deutsche.\n"
        "Regeln:\n"
        "- Sachlich und knapp, so wie der Titel im deutschen Amtsblatt oder in einer Zeitung stünde.\n"
        "- Nichts hinzufügen, nichts weglassen, nichts erklären. Zahlen, Jahreszahlen und Aktenzeichen unverändert.\n"
        "- Eigennamen von Personen und Organisationen bleiben, Ländernamen deutsch.\n"
        "- Feste EU-Begriffe in der amtlichen deutschen Form (regulation = Verordnung, directive = Richtlinie, "
        "discharge = Entlastung, objection = Einwand, own-initiative report = Initiativbericht).\n"
        "- Keine Gedankenstriche, keine Anführungszeichen um den ganzen Titel.\n"
        "Antworte nur mit einem JSON-Objekt, Schlüssel ist die Nummer als Text, Wert der deutsche Titel.\n\n"
        f"{zeilen}\n"
    )


def antwort_lesen(roh: str | None, anzahl: int) -> dict[int, str]:
    """Liest das JSON-Objekt aus der Antwort. Gibt Index (ab 0) -> Titel zurueck, nur gueltige Eintraege."""
    if not roh:
        return {}
    start, ende = roh.find("{"), roh.rfind("}")
    if start < 0 or ende <= start:
        return {}
    try:
        daten = json.loads(roh[start : ende + 1])
    except ValueError:
        return {}
    out = {}
    for k, v in daten.items():
        if str(k).strip().isdigit() and isinstance(v, str) and 1 <= int(k) <= anzahl:
            out[int(k) - 1] = " ".join(v.split())
    return out


def _zahlen(text: str) -> list[str]:
    return sorted(re.findall(r"\d+", text))


def pruefen(original: str, deutsch: str) -> str | None:
    """None = in Ordnung, sonst der Grund der Ablehnung."""
    if not deutsch:
        return "leer"
    if len(deutsch) > 700:
        return "zu lang"
    if len(original) >= 20 and not 0.4 <= len(deutsch) / len(original) <= 2.5:
        return "Laenge passt nicht zum Original"
    if _zahlen(original) != _zahlen(deutsch):
        return "Zahlen weichen ab"
    if "—" in deutsch or "–" in deutsch:
        return "Gedankenstrich"
    if deutsch.strip() == original.strip() and len(original.split()) > 3 and not re.search(r"[äöüß]", deutsch.lower()):
        # kurze Eigennamen duerfen gleich bleiben, ein ganzer englischer Satz nicht
        if re.search(r"\b(the|of|and|on|for)\b", original.lower()):
            return "nicht uebersetzt"
    return None


def offene_titel(cur, limit: int) -> list[str]:
    cur.execute(
        """SELECT titel_en FROM ep_abstimmungen
           WHERE titel_de IS NULL AND titel_en <> ''
           GROUP BY titel_en ORDER BY MAX(zeit) DESC LIMIT %s""",
        (limit,),
    )
    return [r[0] for r in cur.fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--probelauf", action="store_true", help="nichts speichern")
    ap.add_argument("--aufrufe", type=int, default=MAX_AUFRUFE, help=f"Aufrufe der CLI, hoechstens {MAX_AUFRUFE}")
    args = ap.parse_args()
    aufrufe = max(1, min(MAX_AUFRUFE, args.aufrufe))

    load_env()
    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)
    conn = get_db(autocommit=False)
    cur = conn.cursor()
    try:
        titel = offene_titel(cur, STAPEL * aufrufe)
        if not titel:
            log.info("Keine Titel ohne Uebersetzung")
            return 0
        gespeichert = abgelehnt = 0
        for start in range(0, len(titel), STAPEL):
            if not _running:
                break
            stapel = titel[start : start + STAPEL]
            antwort = antwort_lesen(call_claude(prompt_bauen(stapel), timeout=240, log=log.info), len(stapel))
            for i, original in enumerate(stapel):
                deutsch = antwort.get(i, "")
                grund = pruefen(original, deutsch)
                if grund:
                    abgelehnt += 1
                    log.info("abgelehnt (%s): %s -> %s", grund, original[:80], deutsch[:80])
                    continue
                gespeichert += 1
                if args.probelauf:
                    log.info("%s -> %s", original[:80], deutsch[:80])
                else:
                    cur.execute(
                        "UPDATE ep_abstimmungen SET titel_de = %s WHERE titel_en = %s AND titel_de IS NULL",
                        (deutsch, original),
                    )
            if not args.probelauf:
                conn.commit()
        log.info("Fertig: %d Titel uebersetzt, %d abgelehnt%s", gespeichert, abgelehnt,
                 " [Probelauf]" if args.probelauf else "")
        return 0
    except Exception:
        conn.rollback()
        log.exception("Abbruch")
        return 1
    finally:
        cur.close()
        conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
