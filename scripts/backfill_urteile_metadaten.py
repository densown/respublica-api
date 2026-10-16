#!/usr/bin/env python3
"""Urteile ohne Datum, Senat, Typ und Aktenzeichen nachtragen.

Von Ende Maerz bis Oktober 2026 erkannte fetch_urteile.py die Titel der
Feeds nicht und legte die Urteile ohne diese vier Felder an. Was noch im
Feed steht, traegt fetch_urteile.py selbst nach. Fuer aeltere Zeilen holt
dieses Skript die Angaben aus dem XML des Dokuments, das
rechtsprechung-im-internet.de je Entscheidung als ZIP anbietet.

Fuellt nur leere Felder und ueberschreibt nichts. Ohne --schreiben wird
nur angezeigt, was sich aendern wuerde.

  python scripts/backfill_urteile_metadaten.py                 # Probelauf, alle
  python scripts/backfill_urteile_metadaten.py --limit 5       # Probelauf, 5 Zeilen
  python scripts/backfill_urteile_metadaten.py --schreiben     # in die DB
"""
from __future__ import annotations

import argparse
import io
import sys
import time
import zipfile
from datetime import date, datetime
from pathlib import Path
from xml.etree import ElementTree

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import (  # noqa: E402
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)

LOCK_NAME = "backfill_urteile_metadaten"
ZIP_URL = "https://www.rechtsprechung-im-internet.de/jportal/docs/bsjrs/jb-{doc_id}.zip"
TIMEOUT = 30

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


def _text(root: ElementTree.Element, tag: str) -> str | None:
    el = root.find(f".//{tag}")
    if el is None:
        return None
    text = " ".join("".join(el.itertext()).split())
    return text or None


def parse_dokument(xml_bytes: bytes) -> tuple[str | None, str | None, date | None, str | None]:
    """(senat, typ, datum, aktenzeichen) aus dem XML einer Entscheidung.

    Elemente laut rii-dok.dtd: spruchkoerper, doktyp, entsch-datum (JJJJMMTT),
    aktenzeichen.
    """
    root = ElementTree.fromstring(xml_bytes)
    datum = None
    roh = _text(root, "entsch-datum")
    if roh:
        try:
            datum = datetime.strptime(roh, "%Y%m%d").date()
        except ValueError:
            datum = None

    def kurz(tag: str) -> str | None:
        wert = _text(root, tag)
        return wert[:100] if wert else None

    return kurz("spruchkoerper"), kurz("doktyp"), datum, kurz("aktenzeichen")


def lade_dokument(doc_id: str) -> bytes | None:
    """XML der Entscheidung aus dem ZIP, oder None."""
    try:
        resp = requests.get(ZIP_URL.format(doc_id=doc_id), timeout=TIMEOUT)
    except requests.RequestException as exc:
        log.warning("%s: Abruf fehlgeschlagen (%s)", doc_id, exc)
        return None
    if resp.status_code != 200:
        log.warning("%s: HTTP %s", doc_id, resp.status_code)
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            namen = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not namen:
                log.warning("%s: kein XML im ZIP", doc_id)
                return None
            return zf.read(namen[0])
    except zipfile.BadZipFile:
        log.warning("%s: kein gueltiges ZIP", doc_id)
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Urteile ohne Datum nachtragen")
    ap.add_argument("--schreiben", action="store_true", help="Aenderungen in die DB schreiben")
    ap.add_argument("--limit", type=int, default=0, help="hoechstens so viele Zeilen")
    ap.add_argument("--gericht", help="nur dieses Gericht, z. B. BGH")
    ap.add_argument("--pause", type=float, default=1.0, help="Sekunden zwischen zwei Abrufen")
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)
    load_env()

    db = None
    try:
        db = get_db(autocommit=False)
        cur = db.cursor()
        sql = "SELECT id, doc_id, gericht FROM urteile WHERE datum IS NULL"
        params: list = []
        if args.gericht:
            sql += " AND gericht = %s"
            params.append(args.gericht)
        sql += " ORDER BY id DESC"
        if args.limit > 0:
            sql += " LIMIT %s"
            params.append(args.limit)
        cur.execute(sql, params)
        zeilen = cur.fetchall()
        log.info("%d Urteile ohne Datum%s", len(zeilen), "" if args.schreiben else " (Probelauf)")

        gefuellt = fehlend = 0
        for i, (uid, doc_id, gericht) in enumerate(zeilen):
            if not _running:
                log.warning("Abbruch angefordert, beende nach %d Zeilen", i)
                break
            if i:
                time.sleep(args.pause)
            xml_bytes = lade_dokument(doc_id)
            if xml_bytes is None:
                fehlend += 1
                continue
            try:
                senat, typ, datum, az = parse_dokument(xml_bytes)
            except ElementTree.ParseError as exc:
                log.warning("%s: XML nicht lesbar (%s)", doc_id, exc)
                fehlend += 1
                continue
            if not datum:
                log.warning("%s: kein Entscheidungsdatum im XML", doc_id)
                fehlend += 1
                continue
            log.info("%s %s: %s, %s vom %s, %s", gericht, doc_id, senat, typ, datum.isoformat(), az)
            if args.schreiben:
                cur.execute(
                    """UPDATE urteile
                          SET senat = COALESCE(senat, %s),
                              typ = COALESCE(typ, %s),
                              datum = COALESCE(datum, %s),
                              aktenzeichen = COALESCE(aktenzeichen, %s)
                        WHERE id = %s""",
                    (senat, typ, datum, az, uid),
                )
                db.commit()
            gefuellt += 1

        verb = "nachgetragen" if args.schreiben else "wuerden nachgetragen"
        log.info("Fertig: %d %s, %d ohne Angaben", gefuellt, verb, fehlend)
        return 0
    finally:
        if db is not None:
            db.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
