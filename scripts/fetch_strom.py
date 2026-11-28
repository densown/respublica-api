#!/usr/bin/env python3
"""Day-Ahead-Strompreise je Gebotszone von energy-charts.info nach strom_preise.

Geladen werden die 15 Zonen, deren Preise dort unter CC BY 4.0 stehen
(strom_zonen). Die Preise fuer morgen stehen ab etwa 13 Uhr fest.

  fetch_strom.py                  Trockenlauf: gestern bis morgen abrufen, pruefen, zaehlen
  fetch_strom.py --schreiben      schreibt (Cron)
  fetch_strom.py --tage 7         die letzten 7 Tage bis morgen (naechtlicher Korrekturlauf)
  fetch_strom.py --von 2025-01-01 --bis 2025-12-31 --schreiben   Altbestand, in Monatsstuecken
  fetch_strom.py --morgen-noetig  endet mit Fehler, wenn morgen noch fehlt (Lauf um 13:30:
                                  dann zeigt /api/stand die Quelle nicht faelschlich als frisch)

energy-charts drosselt stark (429). Zwischen zwei Zonen wartet das Skript
deshalb 3 Sekunden, bei 429 nach Vorgabe der Quelle. Geprueft wird vor dem
Schreiben: Format, Einheit, Lizenzvermerk, Preise innerhalb der Boersengrenzen,
mindestens 12 von 15 Zonen mit Daten. Faellt eine Pruefung durch, bleibt der
Bestand unveraendert. Nach dem Schreiben rechnet das Skript die Kennzahlen neu
(schnappschuesse: strom_kennzahlen).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import strom  # noqa: E402
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock  # noqa: E402
from lib.quelle import Lauf  # noqa: E402

SLUG = "energy-charts"
LOCK_NAME = "fetch_strom"
BASIS = os.environ.get("ENERGY_CHARTS_URL", "https://api.energy-charts.info/price")
PAUSE = float(os.environ.get("ENERGY_CHARTS_PAUSE", "3"))
MIN_ZONEN = 12
KERN = ("DE-LU", "FR", "NL", "BE", "AT")

log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


def heute() -> date:
    return datetime.now(strom.MARKTZEIT).date()


def stuecke(von: date, bis: date, tage: int = 31):
    """Zeitraum in Stuecke von hoechstens ``tage`` Tagen."""
    a = von
    while a <= bis:
        b = min(bis, a + timedelta(days=tage - 1))
        yield a, b
        a = b + timedelta(days=1)


def kennzahlen(cur, jahr: int, heimat: str = "DE-LU") -> dict:
    """Zaehler fuer das laufende Jahr und Berichte fuer die juengsten Liefertage, aus der Datenbank."""
    cur.execute(
        """SELECT zone, ROUND(SUM(minuten) / 60, 1), COUNT(DISTINCT liefertag)
           FROM strom_preise WHERE liefertag >= %s AND liefertag <= %s AND eur_mwh < 0 GROUP BY zone""",
        (date(jahr, 1, 1), date(jahr, 12, 31)),
    )
    negativ = {z: {"stunden": float(s), "tage": int(t)} for z, s, t in cur.fetchall()}

    # Stunden, in denen die fuenf eng gekoppelten Zonen (KERN) hoechstens 1 EUR/MWh auseinanderlagen.
    # Dieselben Laender wie im Artikel "Ein Markt, 23 Preise" (dort 48 % der Stunden 2019, 14 % 2025).
    platz = ", ".join(["%s"] * len(KERN))
    cur.execute(
        f"""SELECT COUNT(*), SUM(spanne <= 1), ROUND(AVG(spanne), 2) FROM (
             SELECT MAX(m) - MIN(m) AS spanne FROM (
               SELECT liefertag, stunde, zone, SUM(eur_mwh * minuten) / SUM(minuten) AS m
               FROM strom_preise WHERE liefertag >= %s AND liefertag <= %s AND zone IN ({platz})
               GROUP BY liefertag, stunde, zone
             ) s GROUP BY liefertag, stunde HAVING COUNT(*) = %s
           ) t""",
        (date(jahr, 1, 1), date(jahr, 12, 31), *KERN, len(KERN)),
    )
    stunden, gleich, spanne = cur.fetchone()
    stunden = int(stunden or 0)

    cur.execute("SELECT MIN(liefertag), MAX(liefertag) FROM strom_preise WHERE liefertag >= %s", (date(jahr, 1, 1),))
    erster, letzter = cur.fetchone()
    berichte = {}
    if letzter:
        cur.execute(
            """SELECT liefertag, zone, stunde, SUM(eur_mwh * minuten) / SUM(minuten)
               FROM strom_preise WHERE liefertag >= %s GROUP BY liefertag, zone, stunde""",
            (letzter - timedelta(days=2),),
        )
        je_tag: dict[date, dict[str, dict[int, float]]] = {}
        for tag, zone, stunde, m in cur.fetchall():
            je_tag.setdefault(tag, {}).setdefault(zone, {})[int(stunde)] = round(float(m), 2)
        for tag, je_zone in je_tag.items():
            # nur Tage, an denen genug Zonen den ganzen Tag haben
            voll = {z: h for z, h in je_zone.items() if len(h) >= 23}
            if len(voll) >= MIN_ZONEN:
                berichte[tag.isoformat()] = strom.tagesbericht(voll, heimat)
    return {
        "jahr": jahr,
        "von": erster.isoformat() if erster else None,
        "bis": letzter.isoformat() if letzter else None,
        "negativ": negativ,
        "einheitspreis": {
            "stunden": stunden,
            "gleich": int(gleich or 0),
            "anteil": round(int(gleich or 0) / stunden * 100, 1) if stunden else None,
            "mittlere_spanne_eur_mwh": float(spanne) if spanne is not None else None,
            "schwelle_eur_mwh": 1,
            "zonen": list(KERN),
        },
        "tage": berichte,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--schreiben", action="store_true", help="in die Datenbank schreiben (sonst Trockenlauf)")
    ap.add_argument("--tage", type=int, default=1, help="so viele Tage zurueck bis morgen (Vorgabe 1)")
    ap.add_argument("--von", type=date.fromisoformat, help="Altbestand ab diesem Tag")
    ap.add_argument("--bis", type=date.fromisoformat, help="bis zu diesem Tag (nur mit --von)")
    ap.add_argument("--morgen-noetig", action="store_true", help="Fehler, wenn die Preise fuer morgen fehlen")
    args = ap.parse_args()

    load_env()
    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    von = args.von or heute() - timedelta(days=max(1, args.tage))
    bis = (args.bis if args.von and args.bis else heute() + timedelta(days=1))
    conn = get_db(autocommit=False)
    try:
        with Lauf(SLUG, conn, schreiben=args.schreiben, logger=log) as lauf:
            conn.commit()
            cur = conn.cursor()
            cur.execute("SELECT code, quelle_code FROM strom_zonen WHERE aktiv = 1 ORDER BY code")
            zonen = cur.fetchall()
            lauf.pruefen(len(zonen) >= MIN_ZONEN, f"nur {len(zonen)} Zonen in strom_zonen")
            lauf.merken(von=von.isoformat(), bis=bis.isoformat())

            gelesen: dict[str, list[dict]] = {}
            erster_abruf = True
            for a, b in stuecke(von, bis):
                for code, quelle_code in zonen:
                    if not _running:
                        raise KeyboardInterrupt("abgebrochen")
                    if not erster_abruf:
                        time.sleep(PAUSE)
                    erster_abruf = False
                    roh = lauf.abrufen(f"{BASIS}?bzn={quelle_code}&start={a}&end={b}")
                    zeilen = strom.preise_lesen(json.loads(roh.decode("utf-8")))
                    gelesen.setdefault(code, []).extend(zeilen)

            mit_daten = [z for z, zeilen in gelesen.items() if zeilen]
            lauf.pruefen(len(mit_daten) >= MIN_ZONEN, f"nur {len(mit_daten)} von {len(zonen)} Zonen lieferten Preise")
            morgen = heute() + timedelta(days=1)
            morgen_da = [z for z in mit_daten if strom.tag_vollstaendig(gelesen[z], morgen)]
            lauf.merken(zonen=len(mit_daten), morgen_vollstaendig=len(morgen_da),
                        werte=sum(len(v) for v in gelesen.values()))
            log.info("Geprueft: %d Zonen, %d Werte, morgen (%s) vollstaendig in %d Zonen",
                     len(mit_daten), sum(len(v) for v in gelesen.values()), morgen, len(morgen_da))

            if not args.von:
                # der laufende Tag muss da sein, sonst gilt der Lauf nicht als Erfolg
                heute_da = [z for z in mit_daten if strom.tag_vollstaendig(gelesen[z], heute())]
                lauf.pruefen(len(heute_da) >= MIN_ZONEN, f"heute nur in {len(heute_da)} Zonen vollstaendig")

            if lauf.schreiben:
                cur.execute("SELECT COUNT(*) FROM strom_preise WHERE liefertag BETWEEN %s AND %s", (von, bis))
                vorher = int(cur.fetchone()[0])
                sql = """INSERT INTO strom_preise (zone, zeit_utc, minuten, liefertag, stunde, eur_mwh, quelle_id, abgerufen_am)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())
                         ON DUPLICATE KEY UPDATE minuten = VALUES(minuten), liefertag = VALUES(liefertag),
                           stunde = VALUES(stunde), eur_mwh = VALUES(eur_mwh), quelle_id = VALUES(quelle_id),
                           abgerufen_am = VALUES(abgerufen_am)"""
                gesamt = 0
                for code, zeilen in gelesen.items():
                    for i in range(0, len(zeilen), 2000):
                        cur.executemany(sql, [(code, z["zeit_utc"], z["minuten"], z["liefertag"], z["stunde"],
                                               z["eur_mwh"], lauf.source_id) for z in zeilen[i:i + 2000]])
                    gesamt += len(zeilen)
                cur.execute("SELECT COUNT(*) FROM strom_preise WHERE liefertag BETWEEN %s AND %s", (von, bis))
                nachher = int(cur.fetchone()[0])
                lauf.zaehlen(neu=nachher - vorher, geaendert=gesamt - (nachher - vorher))
                kz = kennzahlen(cur, heute().year)
                cur.execute(
                    """INSERT INTO schnappschuesse (schluessel, inhalt, quelle_id, erzeugt_am)
                       VALUES ('strom_kennzahlen', %s, %s, UTC_TIMESTAMP())
                       ON DUPLICATE KEY UPDATE inhalt = VALUES(inhalt), quelle_id = VALUES(quelle_id),
                         erzeugt_am = VALUES(erzeugt_am)""",
                    (json.dumps(kz, ensure_ascii=False), lauf.source_id),
                )
                conn.commit()
            cur.close()
            if args.morgen_noetig:
                lauf.pruefen(len(morgen_da) >= MIN_ZONEN,
                             f"Preise fuer morgen erst in {len(morgen_da)} Zonen, spaeterer Lauf holt sie")
        return 0
    except Exception:
        conn.rollback()
        log.exception("Abbruch")
        return 1
    finally:
        try:
            conn.commit()
            conn.close()
        except Exception:
            pass
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
