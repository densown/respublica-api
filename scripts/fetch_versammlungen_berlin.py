#!/usr/bin/env python3
"""Archiviert und importiert die angezeigten Versammlungen der Polizei Berlin.

Quelle: Versammlungsbehoerde der Polizei Berlin, veroeffentlicht nach
§ 12 VersFG BE, zweimal taeglich aktualisiert. Die Liste zeigt nur kommende
Versammlungen und wird von niemandem archiviert. Genau das macht dieses Skript.

Ablauf pro Lauf:
  1. Liste abrufen (JSON)
  2. Rohdaten gzip-komprimiert nach `versammlungen_rohdaten`, falls sich der
     Inhalt seit dem letzten Abruf geaendert hat (Phase 0, das Archiv)
  3. Eintraege normalisieren und nach `versammlungen` upserten
  4. Status nachfuehren: vergangen / vor Termin entfernt

Schritt 2 laeuft auch dann, wenn das Parsen scheitert. Das Archiv ist
wichtiger als der Import; aus den Rohdaten laesst sich mit --rebuild alles
neu aufbauen.

Personenbezogene Daten liefert die Quelle nicht, und das Skript speichert
auch keine (siehe migrations/016_versammlungen.sql).

Modi:
  --dry-run   Nichts schreiben, nur zeigen was passieren wuerde
  --rebuild   `versammlungen` (quelle=polizei_berlin) aus allen archivierten
              Rohdaten in zeitlicher Reihenfolge neu aufbauen, kein Abruf
  --quiet     Nur die Zusammenfassung loggen

Cron-tauglich: Exit 0 bei Erfolg, Exit 1 bei Fehler.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import unicodedata
from datetime import date, datetime, time, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env
from lib.log import (
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)

LOCK_NAME = "fetch_versammlungen_berlin"
QUELLE = "polizei_berlin"
LAND = "BE"
STADT = "Berlin"

# Ueber VERSAMMLUNGEN_BERLIN_URL ueberschreibbar, falls berlin.de den Pfad aendert
DEFAULT_URL = (
    "https://www.berlin.de/polizei/service/versammlungsbehoerde/"
    "versammlungen-aufzuege/index.php/index/all.json?q="
)
TIMEOUT = 60

# Faellt die Liste gegenueber dem Bestand um mehr als diesen Anteil,
# wird nichts als "vor Termin entfernt" markiert. Schuetzt vor
# Teilantworten und Wartungsseiten, die sonst hunderte Absagen erzeugen.
MIN_ANTEIL_FUER_ENTFERNT = 0.5

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


# ---------------------------------------------------------------------------
# Reine Parsing-Helfer (ohne DB/Netz, siehe scripts/tests)
# ---------------------------------------------------------------------------

# Feldnamen der Quelle, normalisiert (klein, ohne Umlaute/Sonderzeichen).
# Mehrere Varianten, weil berlin.de das Format nicht dokumentiert und
# CSV-/JSON-Export unterschiedlich benennen.
FELDER = {
    "datum": ("datum", "date", "tag"),
    "von": ("von", "beginn", "uhrzeitvon", "zeitvon", "start"),
    "bis": ("bis", "ende", "uhrzeitbis", "zeitbis", "end"),
    "thema": ("thema", "titel", "motto", "anlass"),
    "plz": ("plz", "postleitzahl"),
    "ort": ("strassenr", "strasse", "versammlungsort", "ort", "adresse", "strassehausnummer"),
    "aufzugsstrecke": ("aufzugsstrecke", "strecke", "route", "aufzugsweg"),
}

# Platzhalter, die "kein Aufzug" bedeuten
LEERE_STRECKE = {"", "-", "--", "keine", "entfaellt", "k.a.", "ka", "n/a"}


def normalize_key(key: str) -> str:
    """'Straße/Nr.' -> 'strassenr', 'Uhrzeit von' -> 'uhrzeitvon'."""
    s = str(key).strip().lower().replace("ß", "ss")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", s)


def clean_text(value) -> str | None:
    if value is None:
        return None
    s = re.sub(r"\s+", " ", str(value)).strip()
    return s or None


def find_records(payload) -> list[dict]:
    """Findet die Liste der Eintraege im JSON.

    Berlin liefert ein Objekt mit einer Liste unter 'index'. Zur Sicherheit
    wird auch eine nackte Liste oder die erste Liste von Objekten akzeptiert.
    """
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("index"), list):
            return [r for r in payload["index"] if isinstance(r, dict)]
        for value in payload.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return [r for r in value if isinstance(r, dict)]
    return []


def pick(record: dict, feld: str) -> str | None:
    norm = {normalize_key(k): v for k, v in record.items()}
    for alias in FELDER[feld]:
        if alias in norm:
            wert = clean_text(norm[alias])
            if wert is not None:
                return wert
    return None


def parse_datum(value: str | None) -> date | None:
    """'29.09.2026', '29.9.26' oder '2026-09-29'."""
    if not value:
        return None
    s = value.strip()
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
    else:
        m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", s)
        if not m:
            return None
        d, mo, y = int(m[1]), int(m[2]), int(m[3])
        if y < 100:
            y += 2000
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def parse_zeit(value: str | None) -> time | None:
    """'10:00', '10.00 Uhr', '9 Uhr' -> time; alles andere -> None."""
    if not value:
        return None
    m = re.search(r"(\d{1,2})(?:[:.](\d{2}))?", value)
    if not m:
        return None
    h, mi = int(m[1]), int(m[2] or 0)
    if h == 24 and mi == 0:
        return time(23, 59)
    if 0 <= h <= 23 and 0 <= mi <= 59:
        return time(h, mi)
    return None


def normalize_thema(thema: str | None) -> str:
    """Fuer den Hash: Gross-/Kleinschreibung und Satzzeichen egal."""
    return re.sub(r"[^a-z0-9]", "", normalize_key(thema or ""))


def make_quelle_id(datum: date, von: time | None, plz: str | None, thema: str | None) -> str:
    """Stabile ID aus dem Inhalt.

    Bewusst nicht die 'id' der Quelle: bei berlin.de-Listen ist nicht
    gesichert, dass sie ueber Aktualisierungen hinweg gleich bleibt.
    """
    basis = "|".join([
        datum.isoformat(),
        von.strftime("%H:%M") if von else "",
        (plz or "").strip(),
        normalize_thema(thema),
    ])
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


def parse_record(record: dict) -> dict | None:
    """Ein Quelleintrag -> normalisierte Zeile, oder None ohne gueltiges Datum."""
    datum = parse_datum(pick(record, "datum"))
    if datum is None:
        return None
    von = parse_zeit(pick(record, "von"))
    plz = pick(record, "plz")
    thema = pick(record, "thema")
    ort = pick(record, "ort")
    strecke = pick(record, "aufzugsstrecke")
    if strecke is not None and normalize_key(strecke) in {normalize_key(x) for x in LEERE_STRECKE}:
        strecke = None
    return {
        "quelle_id": make_quelle_id(datum, von, plz, thema),
        "datum": datum,
        "von": von,
        "bis": parse_zeit(pick(record, "bis")),
        "thema": thema,
        "plz": plz[:10] if plz else None,
        "ort": ort[:500] if ort else None,
        "aufzugsstrecke": strecke,
        "typ": "aufzug" if strecke else "kundgebung",
    }


def parse_payload(payload) -> list[dict]:
    """Alle gueltigen Eintraege, Duplikate (gleiche quelle_id) zusammengefasst."""
    rows: dict[str, dict] = {}
    for record in find_records(payload):
        row = parse_record(record)
        if row is not None:
            rows[row["quelle_id"]] = row
    return list(rows.values())


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def archive_raw(cur, raw: bytes, anzahl: int | None, jetzt: datetime) -> bool:
    """Rohdaten ablegen. True = neu archiviert, False = identisch mit Bestand."""
    sha = hashlib.sha256(raw).hexdigest()
    cur.execute(
        """
        INSERT IGNORE INTO versammlungen_rohdaten
          (quelle, abgerufen, sha256, anzahl, bytes, inhalt_gz)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (QUELLE, jetzt, sha, anzahl, len(raw), gzip.compress(raw, compresslevel=9)),
    )
    return cur.rowcount == 1


def apply_snapshot(cur, rows: list[dict], jetzt: datetime) -> dict:
    """Upsert einer Liste und Status-Fortschreibung relativ zu `jetzt`."""
    stats = {"neu": 0, "aktualisiert": 0, "vergangen": 0, "entfernt": 0}
    heute = jetzt.date()

    for r in rows:
        cur.execute(
            """
            INSERT INTO versammlungen
              (quelle, quelle_id, land, stadt, datum, von, bis, thema, plz, ort,
               aufzugsstrecke, typ, status, erstmals_gesehen, zuletzt_gesehen)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'angezeigt',%s,%s)
            ON DUPLICATE KEY UPDATE
              bis            = VALUES(bis),
              ort            = VALUES(ort),
              aufzugsstrecke = VALUES(aufzugsstrecke),
              typ            = VALUES(typ),
              status         = 'angezeigt',
              zuletzt_gesehen= VALUES(zuletzt_gesehen)
            """,
            (
                QUELLE, r["quelle_id"], LAND, STADT, r["datum"], r["von"], r["bis"],
                r["thema"], r["plz"], r["ort"], r["aufzugsstrecke"], r["typ"],
                jetzt, jetzt,
            ),
        )
        if cur.rowcount == 1:
            stats["neu"] += 1
        elif cur.rowcount == 2:
            stats["aktualisiert"] += 1

    # Termin liegt zurueck -> vergangen. Heute bleibt 'angezeigt', weil die
    # Liste tagsueber aktualisiert wird und laufende Termine herausfallen.
    cur.execute(
        "UPDATE versammlungen SET status = 'vergangen' "
        "WHERE quelle = %s AND status = 'angezeigt' AND datum < %s",
        (QUELLE, heute),
    )
    stats["vergangen"] = cur.rowcount

    # Zukuenftige Termine, die nicht mehr gelistet sind
    cur.execute(
        "SELECT COUNT(*) FROM versammlungen "
        "WHERE quelle = %s AND status = 'angezeigt' AND datum > %s",
        (QUELLE, heute),
    )
    bestand = cur.fetchone()[0]
    aktuell = sum(1 for r in rows if r["datum"] > heute)
    if bestand and aktuell < bestand * MIN_ANTEIL_FUER_ENTFERNT:
        log.warning(
            "Nur %d von %d kommenden Terminen gelistet, Absage-Markierung ausgesetzt",
            aktuell, bestand,
        )
    else:
        cur.execute(
            "UPDATE versammlungen SET status = 'vor_termin_entfernt' "
            "WHERE quelle = %s AND status = 'angezeigt' AND datum > %s "
            "AND zuletzt_gesehen < %s",
            (QUELLE, heute, jetzt),
        )
        stats["entfernt"] = cur.rowcount
    return stats


def rebuild(cur, quiet: bool) -> None:
    """`versammlungen` aus dem Rohdaten-Archiv neu aufbauen."""
    cur.execute("DELETE FROM versammlungen WHERE quelle = %s", (QUELLE,))
    log.info("%d Zeilen geloescht, baue aus Archiv neu auf", cur.rowcount)
    cur.execute(
        "SELECT id FROM versammlungen_rohdaten WHERE quelle = %s ORDER BY abgerufen",
        (QUELLE,),
    )
    ids = [r[0] for r in cur.fetchall()]
    for rid in ids:
        if not _running:
            raise RuntimeError("Abbruch angefordert, Rebuild unvollstaendig")
        cur.execute(
            "SELECT abgerufen, inhalt_gz FROM versammlungen_rohdaten WHERE id = %s",
            (rid,),
        )
        abgerufen, blob = cur.fetchone()
        rows = parse_payload(json.loads(gzip.decompress(blob)))
        stats = apply_snapshot(cur, rows, abgerufen)
        if not quiet:
            log.info("Snapshot %s (%s): %d Eintraege, %s", rid, abgerufen, len(rows), stats)
    log.info("Rebuild fertig: %d Snapshots", len(ids))


def main() -> int:
    ap = argparse.ArgumentParser(description="Versammlungen Berlin archivieren und importieren")
    ap.add_argument("--dry-run", action="store_true", help="nichts schreiben")
    ap.add_argument("--rebuild", action="store_true", help="aus Archiv neu aufbauen")
    ap.add_argument("--quiet", action="store_true", help="nur Zusammenfassung")
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = None
    try:
        load_env()

        if args.rebuild:
            conn = get_db(autocommit=False)
            rebuild(conn.cursor(), args.quiet)
            conn.commit()
            return 0

        url = os.environ.get("VERSAMMLUNGEN_BERLIN_URL", DEFAULT_URL)
        resp = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "respublica.media/1.0"})
        resp.raise_for_status()
        raw = resp.content
        jetzt = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        log.info("Liste geladen (%d Bytes)", len(raw))

        # Parsen darf scheitern, Archivieren nicht
        rows: list[dict] | None
        try:
            payload = json.loads(raw)
            gesamt = len(find_records(payload))
            rows = parse_payload(payload)
            log.info("%d Eintraege, davon %d mit gueltigem Datum", gesamt, len(rows))
            if gesamt and not rows:
                log.error("Kein Eintrag lesbar, Feldnamen der Quelle pruefen: %s",
                          sorted(find_records(payload)[0].keys()))
        except ValueError as exc:
            log.error("Antwort ist kein JSON (%s), archiviere trotzdem", exc)
            gesamt, rows = None, None

        if args.dry_run:
            for r in (rows or [])[:10]:
                log.info("[dry-run] %s %s %s | %s", r["datum"], r["von"], r["typ"], r["thema"])
            log.info("[dry-run] nichts geschrieben")
            return 0

        conn = get_db(autocommit=False)
        cur = conn.cursor()
        neu_archiviert = archive_raw(cur, raw, gesamt, jetzt)
        conn.commit()
        log.info("Rohdaten %s", "archiviert" if neu_archiviert else "unveraendert")

        if not rows:
            return 1 if rows is None or gesamt else 0

        stats = apply_snapshot(cur, rows, jetzt)
        conn.commit()
        log.info(
            "Fertig: %d neu, %d aktualisiert, %d vergangen, %d vor Termin entfernt",
            stats["neu"], stats["aktualisiert"], stats["vergangen"], stats["entfernt"],
        )
        return 0

    except Exception as exc:  # noqa: BLE001 — Cron soll den Fehler im Log sehen
        log.exception("Abbruch: %s", exc)
        if conn is not None:
            conn.rollback()
        return 1
    finally:
        if conn is not None:
            conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
