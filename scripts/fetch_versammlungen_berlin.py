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

Gemeinsame Logik (Themenbereinigung, Serien, Upsert, Status, Rebuild) liegt
in lib/versammlungen.py; hier stehen nur Abruf und Feldzuordnung.

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
import json
import os
import sys
from datetime import datetime, timezone
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
from lib.versammlungen import (
    apply_snapshot,
    archive_raw,
    build_row,
    clean_text,
    dedupe,
    normalize_key,
    parse_datum,
    parse_zeit,
    rebuild,
    sync_kategorien,
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

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


# ---------------------------------------------------------------------------
# Feldzuordnung Berlin (reine Funktionen, siehe scripts/tests)
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
LEERE_STRECKE = {normalize_key(x) for x in ("", "-", "--", "keine", "entfaellt", "k.a.", "ka", "n/a")}


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


def parse_record(record: dict) -> dict | None:
    """Ein Quelleintrag -> normalisierte Zeile, oder None ohne gueltiges Datum."""
    datum = parse_datum(pick(record, "datum"))
    if datum is None:
        return None
    strecke = pick(record, "aufzugsstrecke")
    if strecke is not None and normalize_key(strecke) in LEERE_STRECKE:
        strecke = None
    return build_row(
        datum=datum,
        von=parse_zeit(pick(record, "von")),
        bis=parse_zeit(pick(record, "bis")),
        thema_roh=pick(record, "thema"),
        plz=pick(record, "plz"),
        ort=pick(record, "ort"),
        aufzugsstrecke=strecke,
    )


def parse_payload(payload, diag: dict | None = None) -> list[dict]:
    """Alle gueltigen Eintraege, Duplikate (gleiche quelle_id) zusammengefasst.

    `diag` sammelt verworfene Eintraege ('ohne_datum', 'duplikate') fuers Log.
    """
    rows = []
    for record in find_records(payload):
        row = parse_record(record)
        if row is None:
            if diag is not None:
                diag.setdefault("ohne_datum", []).append(record)
            continue
        rows.append(row)
    return dedupe(rows, diag)


def log_diag(diag: dict) -> None:
    for record in diag.get("ohne_datum", [])[:5]:
        log.warning("Ohne gueltiges Datum verworfen: %s", record)
    for row in diag.get("duplikate", [])[:5]:
        log.info("Doppelt gelistet: %s %s %s | %s", row["datum"], row["von"], row["plz"], row["thema"])
    if diag:
        log.info(
            "Verworfen: %d ohne Datum, %d doppelt gelistet",
            len(diag.get("ohne_datum", [])), len(diag.get("duplikate", [])),
        )


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
            rebuild(
                conn.cursor(), QUELLE, LAND, STADT,
                lambda raw: parse_payload(json.loads(raw)),
                log, running=lambda: _running, quiet=args.quiet,
            )
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
            diag: dict = {}
            rows = parse_payload(payload, diag)
            log.info("%d Eintraege, %d Versammlungstage, davon %d in Serien",
                     gesamt, len(rows), sum(1 for r in rows if r["serie_id"]))
            log_diag(diag)
            if gesamt and not rows:
                log.error("Kein Eintrag lesbar, Feldnamen der Quelle pruefen: %s",
                          sorted(find_records(payload)[0].keys()))
        except ValueError as exc:
            log.error("Antwort ist kein JSON (%s), archiviere trotzdem", exc)
            gesamt, rows = None, None

        if args.dry_run:
            for r in (rows or [])[:10]:
                log.info("[dry-run] %s %s %s %s | %s", r["datum"], r["von"], r["typ"],
                         r["serie_rhythmus"] or "einzeln", r["thema"])
            log.info("[dry-run] nichts geschrieben")
            return 0

        conn = get_db(autocommit=False)
        cur = conn.cursor()
        neu_archiviert = archive_raw(cur, QUELLE, raw, gesamt, jetzt)
        conn.commit()
        log.info("Rohdaten %s", "archiviert" if neu_archiviert else "unveraendert")

        if not rows:
            return 1 if rows is None or gesamt else 0

        stats = apply_snapshot(cur, QUELLE, LAND, STADT, rows, jetzt, log)
        kategorien = sync_kategorien(cur)
        conn.commit()
        log.info(
            "Fertig: %d neu, %d aktualisiert, %d vergangen, %d vor Termin entfernt, "
            "%d Kategorien aus Cache",
            stats["neu"], stats["aktualisiert"], stats["vergangen"], stats["entfernt"],
            kategorien,
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
