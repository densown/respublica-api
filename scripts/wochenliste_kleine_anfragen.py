#!/usr/bin/env python3
"""Themenliste der Woche aus kleine_anfragen: die Antworten mit dem hoechsten story_wert.

Interne Liste fuer die Redaktion (nichts davon ist automatisch oeffentlich).
Drei Bloecke:
  1. Top-Antworten nach story_wert, mit Schlagzeile, Kernzahlen und Link aufs PDF
  2. "Regierung weiss es nicht": viele Formulierungen wie "statistisch nicht erfasst"
  3. Noch ohne Auswertung (Volltext fehlt im DIP noch oder KI-Lauf steht aus)

Schreibt Markdown nach --ausgabe (Standard data/kleine-anfragen/woche-JJJJ-KWnn.md)
und auf stdout. Liest nur, schreibt nicht in die Datenbank.

  python scripts/wochenliste_kleine_anfragen.py                 # letzte 7 Tage
  python scripts/wochenliste_kleine_anfragen.py --tage 14 --top 15
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import kleine_anfragen as ka  # noqa: E402
from lib.db import get_db  # noqa: E402
from lib.env import ROOT, load_env  # noqa: E402

# DIP braucht einen Titel-Teil im Pfad, "-" genuegt (ohne: "Seite nicht gefunden")
DIP_VORGANG = "https://dip.bundestag.de/vorgang/-/{}"


def _fraktion(z: dict) -> str:
    return ka.FRAKTION_NAMEN.get(z.get("fraktion") or "", z.get("initiative") or "?")


def _kopf(z: dict) -> str:
    return f"{z['dokumentnummer']}, {z['datum']:%d.%m.}, {_fraktion(z)}, {z['ressort'] or 'Ressort unbekannt'}"


def markdown(top: list[dict], nicht: list[dict], offen: list[dict], von: date, bis: date) -> str:
    kw = bis.isocalendar()
    zeilen = [
        f"# Kleine Anfragen: Themen der Woche {kw.week}/{kw.year}",
        "",
        f"Antworten der Bundesregierung vom {von:%d.%m.} bis {bis:%d.%m.%Y}. Automatisch ausgewertet, "
        "Kernzahlen gegen den Volltext geprüft. Vor Veröffentlichung im PDF nachlesen.",
        "",
        "## Die interessantesten Antworten",
        "",
    ]
    if not top:
        zeilen.append("Keine ausgewerteten Antworten im Zeitraum.")
    for i, z in enumerate(top, 1):
        zeilen.append(f"### {i}. {z['schlagzeile'] or z['titel']}")
        zeilen.append("")
        zeilen.append(f"Wert {z['story_wert']}/10. {z['story_grund'] or ''}")
        zeilen.append("")
        zeilen.append(f"Anfrage: {z['titel']} ({_kopf(z)})")
        zeilen.append("")
        if z["zusammenfassung"]:
            zeilen.append(z["zusammenfassung"])
            zeilen.append("")
        for kz in json.loads(z["kernzahlen"] or "[]"):
            zeilen.append(f"- **{kz['wert']} {kz['einheit']}**: {kz['bedeutung']}")
        zeilen.append("")
        zeilen.append("Quellen:")
        if z["pdf_url"] and z["text_zeichen"]:
            zeilen.append(f"- PDF: {z['pdf_url']}")
        if z["vorgang_id"]:
            zeilen.append(f"- DIP: {DIP_VORGANG.format(z['vorgang_id'])}")
        zeilen.append("")
    zeilen += ["## Regierung weiß es nicht", ""]
    if not nicht:
        zeilen.append("Keine Antwort mit auffällig vielen Nicht-Angaben.")
    for z in nicht:
        zeilen.append(f"- {z['titel']} ({_kopf(z)}): {z['nicht_erhoben']}x nicht erhoben oder nicht beantwortet. {z['pdf_url'] or ''}")
    zeilen += ["", "## Noch ohne Auswertung", ""]
    if not offen:
        zeilen.append("Keine.")
    for z in offen:
        grund = "Volltext fehlt im DIP noch" if not z["text_zeichen"] else "Auswertung steht aus"
        # Ohne Volltext liefert dserver fuer das PDF noch 404, der DIP-Vorgang geht sofort
        link = DIP_VORGANG.format(z["vorgang_id"]) if z["vorgang_id"] else ""
        zeilen.append(f"- {z['titel']} ({_kopf(z)}), {grund}. {link}")
    return "\n".join(zeilen) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tage", type=int, default=7)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--ausgabe", type=Path, help="Markdown-Datei (Standard data/kleine-anfragen/)")
    args = ap.parse_args()

    load_env()
    bis = date.today()
    von = bis - timedelta(days=args.tage)
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    felder = """dokumentnummer, datum, titel, fraktion, initiative, ressort, pdf_url, vorgang_id,
                schlagzeile, zusammenfassung, kernzahlen, story_wert, story_grund, nicht_erhoben, text_zeichen"""
    cur.execute(
        f"""SELECT {felder} FROM kleine_anfragen
             WHERE datum >= %s AND story_wert IS NOT NULL
             ORDER BY story_wert DESC, datum DESC LIMIT %s""",
        (von, args.top),
    )
    top = cur.fetchall()
    cur.execute(
        f"""SELECT {felder} FROM kleine_anfragen
             WHERE datum >= %s AND nicht_erhoben >= 3
             ORDER BY nicht_erhoben DESC LIMIT 5""",
        (von,),
    )
    nicht = cur.fetchall()
    cur.execute(
        f"""SELECT {felder} FROM kleine_anfragen
             WHERE datum >= %s AND zusammenfassung IS NULL ORDER BY datum DESC""",
        (von,),
    )
    offen = cur.fetchall()
    cur.close()
    conn.close()

    md = markdown(top, nicht, offen, von, bis)
    kw = bis.isocalendar()
    ziel = args.ausgabe or ROOT / "data" / "kleine-anfragen" / f"woche-{kw.year}-KW{kw.week:02d}.md"
    ziel.parent.mkdir(parents=True, exist_ok=True)
    ziel.write_text(md, encoding="utf-8")
    print(md)
    print(f"[geschrieben: {ziel}]", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
