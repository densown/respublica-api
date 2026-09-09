#!/usr/bin/env python3
"""Amtliche Wahlergebnisse in `wahl_ergebnisse` schreiben.

Anders als die Umfragen (import_dawum.py) gibt es fuer Wahlergebnisse keine
bundesweite Sammelquelle: jede Landeswahlleitung veroeffentlicht in eigenem
Format, meist als JavaScript-gerenderte Seite ohne Open-Data-Endpunkt. Ein
generischer Scraper waere pro Land eine eigene Baustelle und wuerde bei jedem
Relaunch brechen.

Deshalb liegen die Zahlen hier als geprueftes Literal — je Wahl einmal aus der
amtlichen Quelle uebernommen, mit Quellenangabe und Stand. Das ist nachlesbar,
im Diff reviewbar und bricht nicht unbemerkt.

Die Prozentwerte werden bewusst NICHT mit abgeschrieben, sondern aus den
absoluten Stimmen berechnet. Damit kann sich kein Tippfehler einschleichen,
der erst auf der fertigen Seite auffaellt, und die Summe stimmt per
Konstruktion.

  python scripts/import_wahlergebnis.py                    # alle definierten
  python scripts/import_wahlergebnis.py ltw-sachsen-anhalt-2026
"""
from __future__ import annotations

import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402


# ---------------------------------------------------------------------------
# Ergebnisse
# ---------------------------------------------------------------------------
# `stimmen` sind gueltige Landesstimmen (Zweitstimmen-Aequivalent).
# `sitze` = (gesamt, direkt, liste). None = nicht im Parlament.
#
# Kleinparteien ohne Eintrag in `parteien` werden unter `other` summiert. Die
# Sammelzeile wird nicht getippt, sondern als Rest zu `gueltige_stimmen`
# berechnet — so bleibt sie korrekt, auch wenn eine Partei ergaenzt wird.
ERGEBNISSE = {
    "ltw-sachsen-anhalt-2026": {
        "wahlberechtigte": 1_706_851,
        "waehler": 1_328_211,
        "gueltige_stimmen": 1_315_315,
        "sitze_gesamt": 83,
        "status": "vorlaeufig",
        # Alle 2.661 Wahlbezirke ausgezaehlt. Das endgueltige amtliche
        # Ergebnis stellt der Landeswahlausschuss spaeter fest.
        "stand": "2026-09-08 15:06:00",
        "quelle": "Landeswahlleiterin Sachsen-Anhalt",
        "quelle_url": "https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/",
        "parteien": {
            #                stimmen,  (sitze, direkt, liste)
            "afd":          (576_037, (39, 38, 1)),
            "cdu_csu":      (226_622, (15, 0, 15)),
            "spd":          (122_303, (8, 0, 8)),
            "gruene":       (117_498, (8, 0, 8)),
            "linke_pds":    (112_541, (8, 3, 5)),
            "bsw":          (69_291, (5, 0, 5)),
            "fdp":          (33_969, None),
            "freie_waehler": (15_430, None),
        },
    },
}


def pct(stimmen: int, gueltig: int) -> Decimal:
    """Prozent auf zwei Nachkommastellen, kaufmaennisch gerundet."""
    return (Decimal(stimmen) * 100 / Decimal(gueltig)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def importiere(db, slug: str, daten: dict) -> None:
    cur = db.cursor()

    cur.execute("SELECT id FROM wahltermine WHERE slug = %s", (slug,))
    row = cur.fetchone()
    if not row:
        print(f"  ÜBERSPRUNGEN {slug}: kein Wahltermin mit diesem Slug")
        return
    wahltermin_id = row[0]

    cur.execute("SELECT kuerzel, id FROM parteien")
    partei_id = {k: i for k, i in cur.fetchall()}

    gueltig = daten["gueltige_stimmen"]
    zeilen = []
    summe = 0

    for kuerzel, (stimmen, sitze) in daten["parteien"].items():
        if kuerzel not in partei_id:
            print(f"  ABBRUCH: Partei '{kuerzel}' fehlt in `parteien`")
            return
        summe += stimmen
        s, sd, sl = sitze if sitze else (None, None, None)
        zeilen.append((partei_id[kuerzel], pct(stimmen, gueltig), stimmen, s, sd, sl))

    # Sammelzeile als Rest — nie getippt, immer gerechnet.
    rest = gueltig - summe
    if rest < 0:
        print(f"  ABBRUCH: Einzelstimmen ({summe}) über gültigen ({gueltig})")
        return
    if rest:
        zeilen.append((partei_id["other"], pct(rest, gueltig), rest, None, None, None))

    # Ersetzen statt ergaenzen: bei Nachkorrektur der Landeswahlleitung soll
    # keine alte Partei-Zeile zurueckbleiben.
    cur.execute("DELETE FROM wahl_ergebnisse WHERE wahltermin_id = %s", (wahltermin_id,))
    cur.executemany(
        """INSERT INTO wahl_ergebnisse
             (wahltermin_id, partei_id, prozent, stimmen, sitze, sitze_direkt, sitze_liste)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        [(wahltermin_id, *z) for z in zeilen],
    )

    beteiligung = (Decimal(daten["waehler"]) * 100 / Decimal(daten["wahlberechtigte"])).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Der Statuswechsel gehoert hierher: ein Wahltermin mit Ergebnis ist
    # abgeschlossen. Sonst bleibt er in der Frontend-Sortierung "kommend" und
    # die Seite zeigt eine gelaufene Wahl als anstehend.
    cur.execute(
        """UPDATE wahltermine
              SET status = 'abgeschlossen',
                  wahlbeteiligung = %s,
                  sitze_gesamt = %s,
                  ergebnis_status = %s,
                  ergebnis_stand = %s,
                  ergebnis_quelle = %s,
                  ergebnis_quelle_url = %s
            WHERE id = %s""",
        (
            beteiligung,
            daten["sitze_gesamt"],
            daten["status"],
            daten["stand"],
            daten["quelle"],
            daten["quelle_url"],
            wahltermin_id,
        ),
    )
    db.commit()

    print(f"  {slug}: {len(zeilen)} Parteien, Wahlbeteiligung {beteiligung} %, "
          f"{daten['sitze_gesamt']} Sitze ({daten['status']})")
    for kuerzel, (stimmen, sitze) in daten["parteien"].items():
        s = f" · {sitze[0]} Sitze ({sitze[1]} direkt)" if sitze else ""
        print(f"      {kuerzel:15} {pct(stimmen, gueltig):>6} %  {stimmen:>9,}".replace(",", " ") + s)
    if rest:
        print(f"      {'other':15} {pct(rest, gueltig):>6} %  {rest:>9,}".replace(",", " "))


def main() -> int:
    load_env()
    slugs = sys.argv[1:] or list(ERGEBNISSE)
    db = get_db()
    print(f"Importiere {len(slugs)} Wahlergebnis(se)")
    for slug in slugs:
        if slug not in ERGEBNISSE:
            print(f"  ÜBERSPRUNGEN {slug}: keine Daten hinterlegt")
            continue
        importiere(db, slug, ERGEBNISSE[slug])
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
