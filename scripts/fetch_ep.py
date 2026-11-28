#!/usr/bin/env python3
"""Namentliche Abstimmungen im EU-Parlament von HowTheyVote.eu nach ep_*.

Quelle ist der taegliche CSV-Export (ODbL 1.0):
https://github.com/HowTheyVote/data/releases/latest

Uebernommen werden die Hauptabstimmungen der 10. Wahlperiode (ab 16.07.2024)
mit allen Einzelstimmen, dazu Abgeordnete und Fraktionen. Der Lauf prueft vor
dem Schreiben: Zahl der Abstimmungen nicht kleiner als im Bestand, Einzelstimmen
passen zu den Summen jeder Abstimmung, 90 bis 96 deutsche Abgeordnete im Amt.
Faellt eine Pruefung durch, bleibt der Bestand unveraendert.

  fetch_ep.py                 Trockenlauf: abrufen, pruefen, zaehlen
  fetch_ep.py --schreiben     schreibt (Cron)
  fetch_ep.py --alles         auch wenn der Export seit dem letzten Lauf gleich ist
  fetch_ep.py --ordner PFAD   liest einen entpackten Export statt zu laden (Test)

Hat sich der Export seit dem letzten erfolgreichen Lauf nicht geaendert
(last_updated.txt), endet der Lauf sofort als Erfolg: die Quelle wurde geprueft
und hat nichts Neues. Die grosse Datei member_votes.csv.gz (rund 100 MB) wird
dann nicht geladen.

Deutsche Titel schreibt uebersetze_ep_titel.py, dieser Import fasst titel_de
nicht an.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import ep  # noqa: E402
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock  # noqa: E402
from lib.quelle import Lauf  # noqa: E402

SLUG = "howtheyvote"
LOCK_NAME = "fetch_ep"
BASIS = "https://github.com/HowTheyVote/data/releases/latest/download/"
DATEIEN = ["votes", "members", "groups", "group_memberships", "national_parties",
           "national_party_memberships", "member_votes"]
STAPEL = 2000

log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


def letzter_export_stand(conn, source_id: int) -> str | None:
    cur = conn.cursor()
    cur.execute(
        """SELECT context FROM data_update_log
           WHERE source_id = %s AND status = 'success' ORDER BY id DESC LIMIT 1""",
        (source_id,),
    )
    row = cur.fetchone()
    cur.close()
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0]).get("export_stand")
    except (ValueError, AttributeError):
        return None


def stimmen_lesen(pfad: Path, ids: set[int]):
    """Einzelstimmen der gewuenschten Abstimmungen, zeilenweise aus member_votes.csv(.gz)."""
    oeffnen = gzip.open if pfad.suffix == ".gz" else open
    with oeffnen(pfad, "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        ep.kopf_pruefen(next(reader), ep.STIMMEN_KOPF, pfad.name)
        yield from ep.stimmen(reader, ids)


def datei(ordner: Path, name: str) -> Path:
    for endung in (".csv.gz", ".csv"):
        p = ordner / f"{name}{endung}"
        if p.exists():
            return p
    raise FileNotFoundError(f"{name}.csv(.gz) fehlt in {ordner}")


ABST_SPALTEN = ["id", "zeit", "titel_en", "referenz", "beschreibung", "art", "verfahren", "verfahren_typ",
                "lesung", "ja", "nein", "enthaltung", "nicht_abgestimmt", "ergebnis", "text_referenz",
                "de_ja", "de_nein", "de_enthaltung", "de_nicht"]


def schreiben(conn, source_id: int, abst, abg, fraktionen, ordner: Path, de) -> tuple[int, int, int]:
    """Schreibt alles in einer Transaktion. Gibt (neue, geaenderte Abstimmungen, Einzelstimmen) zurueck."""
    cur = conn.cursor()
    cur.execute("SELECT id, ja, nein, enthaltung, nicht_abgestimmt FROM ep_abstimmungen")
    bestand = {r[0]: tuple(r[1:]) for r in cur.fetchall()}

    cur.executemany(
        "INSERT INTO ep_fraktionen (code, name, kurz) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE name = VALUES(name), kurz = VALUES(kurz)",
        [(f["code"], f["label"][:200], f["short_label"][:40]) for f in fraktionen],
    )
    cur.executemany(
        """INSERT INTO ep_abgeordnete (id, vorname, nachname, land, geburtsdatum, fraktion, partei, im_amt, abgerufen_am)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())
           ON DUPLICATE KEY UPDATE vorname = VALUES(vorname), nachname = VALUES(nachname), land = VALUES(land),
             geburtsdatum = VALUES(geburtsdatum), fraktion = VALUES(fraktion), partei = VALUES(partei),
             im_amt = VALUES(im_amt), abgerufen_am = VALUES(abgerufen_am)""",
        [(a["id"], a["vorname"], a["nachname"], a["land"], a["geburtsdatum"], a["fraktion"], a["partei"], a["im_amt"])
         for a in abg],
    )
    # wer nicht mehr im Export der Periode steht, ist nicht mehr im Amt
    cur.execute("UPDATE ep_abgeordnete SET im_amt = 0 WHERE abgerufen_am < UTC_TIMESTAMP() - INTERVAL 1 HOUR")

    neu = geaendert = 0
    stimmen_neu: set[int] = set()
    platzhalter = ", ".join(["%s"] * len(ABST_SPALTEN))
    updates = ", ".join(f"{s} = VALUES({s})" for s in ABST_SPALTEN if s != "id")
    for a in abst:
        d = de.get(a["id"], {"ja": 0, "nein": 0, "enthaltung": 0, "nicht": 0})
        zeile = {**a, "de_ja": d["ja"], "de_nein": d["nein"], "de_enthaltung": d["enthaltung"], "de_nicht": d["nicht"]}
        summen = (a["ja"], a["nein"], a["enthaltung"], a["nicht_abgestimmt"])
        if a["id"] not in bestand:
            neu += 1
            stimmen_neu.add(a["id"])
        elif bestand[a["id"]] != summen:
            geaendert += 1
            stimmen_neu.add(a["id"])
        cur.execute(
            f"""INSERT INTO ep_abstimmungen ({", ".join(ABST_SPALTEN)}, quelle_id, abgerufen_am)
                VALUES ({platzhalter}, %s, UTC_TIMESTAMP())
                ON DUPLICATE KEY UPDATE {updates}, quelle_id = VALUES(quelle_id), abgerufen_am = VALUES(abgerufen_am)""",
            [zeile[s] for s in ABST_SPALTEN] + [source_id],
        )

    geschrieben = 0
    if stimmen_neu:
        ids = sorted(stimmen_neu)
        for i in range(0, len(ids), 500):
            teil = ids[i:i + 500]
            cur.execute(f"DELETE FROM ep_stimmen WHERE abstimmung_id IN ({', '.join(['%s'] * len(teil))})", teil)
        stapel = []
        sql = ("INSERT INTO ep_stimmen (abstimmung_id, mitglied_id, position, land, fraktion, partei) "
               "VALUES (%s, %s, %s, %s, %s, %s)")
        for zeile in stimmen_lesen(datei(ordner, "member_votes"), stimmen_neu):
            stapel.append(zeile)
            if len(stapel) >= STAPEL:
                cur.executemany(sql, stapel)
                geschrieben += len(stapel)
                stapel.clear()
        if stapel:
            cur.executemany(sql, stapel)
            geschrieben += len(stapel)
    cur.close()
    return neu, geaendert, geschrieben


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--schreiben", action="store_true", help="in die Datenbank schreiben (sonst Trockenlauf)")
    ap.add_argument("--alles", action="store_true", help="auch bei unveraendertem Export alles neu lesen")
    ap.add_argument("--ordner", type=Path, help="entpackter Export statt Download")
    args = ap.parse_args()

    load_env()
    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = get_db(autocommit=False)
    tmp = None
    try:
        with Lauf(SLUG, conn, schreiben=args.schreiben, logger=log) as lauf:
            conn.commit()  # Protokollzeile "started" sichtbar machen
            if args.ordner:
                ordner = args.ordner
                stand = (ordner / "last_updated.txt").read_text().strip() if (ordner / "last_updated.txt").exists() else None
            else:
                stand = lauf.abrufen(BASIS + "last_updated.txt").decode("utf-8").strip()
                if not args.alles and stand and stand == letzter_export_stand(conn, lauf.source_id):
                    log.info("Export unveraendert seit %s, nichts zu tun", stand)
                    lauf.merken(export_stand=stand, unveraendert=True)
                    return 0
                tmp = Path(tempfile.mkdtemp(prefix="ep_export_"))
                ordner = tmp
                for name in DATEIEN:
                    if not _running:
                        raise KeyboardInterrupt("abgebrochen")
                    lauf.laden(BASIS + f"{name}.csv.gz", ordner / f"{name}.csv.gz")
            lauf.merken(export_stand=stand)

            abst = ep.hauptabstimmungen(ep.csv_zeilen(datei(ordner, "votes")))
            abg = ep.abgeordnete(
                ep.csv_zeilen(datei(ordner, "members")),
                ep.csv_zeilen(datei(ordner, "group_memberships")),
                ep.csv_zeilen(datei(ordner, "national_party_memberships")),
                ep.csv_zeilen(datei(ordner, "national_parties")),
            )
            fraktionen = ep.csv_zeilen(datei(ordner, "groups"))
            ids = {a["id"] for a in abst}

            # erster Durchgang durch die grosse Datei: zaehlen und pruefen, nichts halten
            zahl: Counter = Counter()
            de: dict[int, dict[str, int]] = {}
            for vid, _mid, pos, land, _f, _p in stimmen_lesen(datei(ordner, "member_votes"), ids):
                zahl[vid] += 1
                if land == ep.DEUTSCHLAND:
                    de.setdefault(vid, {"ja": 0, "nein": 0, "enthaltung": 0, "nicht": 0})[pos] += 1

            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM ep_abstimmungen")
            bisher = int(cur.fetchone()[0])
            cur.close()
            fehler = ep.pruefen(abst, zahl, bisher, abg, len(fraktionen))
            lauf.pruefen(not fehler, "; ".join(fehler))
            lauf.merken(abstimmungen=len(abst), einzelstimmen=sum(zahl.values()),
                        letzte=max(a["zeit"] for a in abst).isoformat())
            log.info("Geprueft: %d Hauptabstimmungen, %d Einzelstimmen, %d Abgeordnete, Export vom %s",
                     len(abst), sum(zahl.values()), len(abg), stand)

            if lauf.schreiben:
                neu, geaendert, geschrieben = schreiben(conn, lauf.source_id, abst, abg, fraktionen, ordner, de)
                lauf.zaehlen(neu=neu, geaendert=geaendert, uebersprungen=len(abst) - neu - geaendert)
                lauf.merken(einzelstimmen_geschrieben=geschrieben)
                conn.commit()
        return 0
    except Exception:
        conn.rollback()
        log.exception("Abbruch")
        return 1
    finally:
        # Protokoll des Laufs (auch im Fehlerfall) festschreiben
        try:
            conn.commit()
            conn.close()
        except Exception:
            pass
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
