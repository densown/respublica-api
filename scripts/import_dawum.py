#!/usr/bin/env python3
"""Spiegelt Wahlumfragen von dawum.de nach `umfragen` / `umfrage_werte`.

Quelle: https://api.dawum.de/ (ODC-ODbL, Publisher dawum.de,
Autor Dipl.-Jur. Philipp Guttmann). Die Attribution ist Lizenzpflicht und
gehoert sichtbar ins Frontend, nicht ins Impressum.

Verarbeitet werden ausschliesslich Umfragen zu Parlamenten, fuer die in
`wahltermine.dawum_parliament_id` ein Eintrag existiert. Jede Umfrage wird dem
naechsten Wahltermin desselben Parlaments am oder nach ihrem Veroeffentlichungs-
datum zugeordnet; gibt es keinen, faellt sie auf den Termin mit status='kommend'.

Modi:
  --dry-run   Nichts schreiben, nur zeigen was passieren wuerde
  --force     last_update-Check uebergehen und in jedem Fall importieren
  --quiet     Nur die Zusammenfassung loggen

Cron-tauglich: Exit 0 bei Erfolg, Exit 1 bei Fehler.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import ROOT, load_env
from lib.log import (
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)

LOCK_NAME = "import_dawum"
STATE_FILE = ROOT / "logs" / "import_dawum.state"

API_URL = "https://api.dawum.de/"
LAST_UPDATE_URL = "https://api.dawum.de/last_update.txt"
TIMEOUT = 60

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_id(value) -> int | None:
    """IDs duerfen 0 sein: Bundestag ist Parlament 0, 'Sonstige' ist Partei 0."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_count(value) -> int | None:
    """Fallzahlen: 0 oder negativ ist keine gueltige Stichprobengroesse."""
    n = parse_id(value)
    return n if n is not None and n > 0 else None


def read_state() -> str | None:
    try:
        return STATE_FILE.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def write_state(value: str) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(value, encoding="utf-8")
    except OSError as exc:
        log.warning("State-Datei nicht schreibbar: %s", exc)


def load_lookups(cur):
    """Wahltermine je Parlament und dawum-Party-ID -> parteien.id."""
    cur.execute(
        "SELECT id, dawum_parliament_id, datum, status FROM wahltermine "
        "WHERE dawum_parliament_id IS NOT NULL"
    )
    termine: dict[int, list[dict]] = defaultdict(list)
    for tid, parl, datum, status in cur.fetchall():
        termine[int(parl)].append({"id": tid, "datum": datum, "status": status})
    for rows in termine.values():
        # datum NULL ans Ende — das ist der noch unbestimmte kommende Termin
        rows.sort(key=lambda r: (r["datum"] is None, r["datum"]))

    cur.execute("SELECT dawum_id, partei_id FROM parteien_dawum")
    parteien = {int(d): int(p) for d, p in cur.fetchall()}

    cur.execute("SELECT id FROM parteien WHERE kuerzel = 'other'")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Partei 'other' fehlt — scripts/seed_wahlen_stammdaten.sql ausfuehren")
    return termine, parteien, int(row[0])


def pick_wahltermin(kandidaten: list[dict], veroeffentlicht: date) -> int | None:
    """Naechster Termin am/nach dem Datum, sonst der kommende, sonst keiner."""
    for row in kandidaten:
        if row["datum"] is not None and row["datum"] >= veroeffentlicht:
            return row["id"]
    for row in kandidaten:
        if row["status"] == "kommend":
            return row["id"]
    return None


def fetch_json(url: str, what: str):
    resp = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "respublica.media/1.0"})
    resp.raise_for_status()
    log.info("%s geladen (%d Bytes)", what, len(resp.content))
    return resp.json()


def main() -> int:
    ap = argparse.ArgumentParser(description="dawum-Umfragen importieren")
    ap.add_argument("--dry-run", action="store_true", help="nichts schreiben")
    ap.add_argument("--force", action="store_true", help="last_update-Check uebergehen")
    ap.add_argument("--quiet", action="store_true", help="nur Zusammenfassung")
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = None
    try:
        # --- last_update prüfen: spart 1 MB Transfer pro Lauf ------------------
        try:
            resp = requests.get(LAST_UPDATE_URL, timeout=20)
            resp.raise_for_status()
            remote_update = resp.text.strip()
        except requests.RequestException as exc:
            log.warning("last_update nicht abrufbar (%s) — importiere trotzdem", exc)
            remote_update = None

        if remote_update and not args.force and remote_update == read_state():
            log.info("Keine Aenderung seit %s — nichts zu tun.", remote_update)
            return 0

        data = fetch_json(API_URL, "dawum-Vollbestand")

        surveys = data.get("Surveys") or {}
        institutes = data.get("Institutes") or {}
        taskers = data.get("Taskers") or {}
        methods = data.get("Methods") or {}
        lizenz = (data.get("Database") or {}).get("License", {}).get("Shortcut", "?")
        log.info("%d Umfragen im Bestand, Lizenz %s", len(surveys), lizenz)

        load_env()
        conn = get_db(autocommit=True)
        cur = conn.cursor()
        termine, partei_map, other_id = load_lookups(cur)
        log.info(
            "Zuordnung: %d Parlamente, %d Party-Mappings",
            len(termine), len(partei_map),
        )

        neu = aktualisiert = uebersprungen = ohne_termin = 0
        unbekannte_parteien: set[str] = set()

        # Aeltere zuerst — macht die Logs bei Erstlaeufen lesbar
        for sid, s in sorted(surveys.items(), key=lambda kv: kv[1].get("Date", "")):
            if not _running:
                log.warning("Abbruch angefordert — beende nach %d Umfragen", neu + aktualisiert)
                break

            parl = parse_id(s.get("Parliament_ID"))
            if parl is None or parl not in termine:
                uebersprungen += 1
                continue

            veroeffentlicht = parse_date(s.get("Date"))
            if veroeffentlicht is None:
                log.warning("Umfrage %s ohne gueltiges Datum — uebersprungen", sid)
                uebersprungen += 1
                continue

            wahltermin_id = pick_wahltermin(termine[parl], veroeffentlicht)
            if wahltermin_id is None:
                ohne_termin += 1
                continue

            periode = s.get("Survey_Period") or {}
            werte_roh = s.get("Results") or {}

            # Mehrere dawum-Parteien koennen auf dieselbe interne Partei zeigen
            # (CDU/CSU) oder auf 'other' — deshalb aufsummieren statt ueberschreiben.
            werte: dict[int, float] = defaultdict(float)
            for dawum_pid, prozent in werte_roh.items():
                pid = parse_id(dawum_pid)
                ziel = partei_map.get(pid) if pid is not None else None
                if ziel is None:
                    unbekannte_parteien.add(str(dawum_pid))
                    ziel = other_id
                try:
                    werte[ziel] += float(prozent)
                except (TypeError, ValueError):
                    log.warning("Umfrage %s: unlesbarer Wert %r", sid, prozent)

            if not werte:
                uebersprungen += 1
                continue

            if args.dry_run:
                if not args.quiet:
                    log.info(
                        "[dry-run] Umfrage %s -> wahltermin %s, %d Parteien",
                        sid, wahltermin_id, len(werte),
                    )
                neu += 1
                continue

            cur.execute(
                """
                INSERT INTO umfragen
                  (dawum_survey_id, wahltermin_id, institut, auftraggeber,
                   erhebung_start, erhebung_ende, veroeffentlicht, befragte, methode)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  wahltermin_id = VALUES(wahltermin_id),
                  institut      = VALUES(institut),
                  auftraggeber  = VALUES(auftraggeber),
                  erhebung_start= VALUES(erhebung_start),
                  erhebung_ende = VALUES(erhebung_ende),
                  veroeffentlicht = VALUES(veroeffentlicht),
                  befragte      = VALUES(befragte),
                  methode       = VALUES(methode)
                """,
                (
                    int(sid),
                    wahltermin_id,
                    (institutes.get(str(s.get("Institute_ID"))) or {}).get("Name") or "unbekannt",
                    (taskers.get(str(s.get("Tasker_ID"))) or {}).get("Name"),
                    parse_date(periode.get("Date_Start")),
                    parse_date(periode.get("Date_End")),
                    veroeffentlicht,
                    parse_count(s.get("Surveyed_Persons")),
                    (methods.get(str(s.get("Method_ID"))) or {}).get("Name"),
                ),
            )
            # rowcount: 1 = eingefuegt, 2 = aktualisiert, 0 = unveraendert
            if cur.rowcount == 1:
                neu += 1
            else:
                aktualisiert += 1

            cur.execute("SELECT id FROM umfragen WHERE dawum_survey_id = %s", (int(sid),))
            umfrage_id = cur.fetchone()[0]

            # dawum korrigiert Werte rueckwirkend -> immer komplett neu schreiben
            cur.execute("DELETE FROM umfrage_werte WHERE umfrage_id = %s", (umfrage_id,))
            cur.executemany(
                "INSERT INTO umfrage_werte (umfrage_id, partei_id, prozent) VALUES (%s,%s,%s)",
                [(umfrage_id, pid, round(v, 1)) for pid, v in werte.items()],
            )

        if unbekannte_parteien:
            log.warning(
                "Nicht gemappte dawum-Party-IDs (auf 'other' gebucht): %s",
                ", ".join(sorted(unbekannte_parteien)),
            )
        if ohne_termin:
            log.warning("%d Umfragen ohne passenden Wahltermin", ohne_termin)

        log.info(
            "Fertig%s: %d neu, %d aktualisiert, %d uebersprungen (anderes Parlament)",
            " [dry-run]" if args.dry_run else "", neu, aktualisiert, uebersprungen,
        )

        if remote_update and not args.dry_run and _running:
            write_state(remote_update)
        return 0

    except Exception as exc:  # noqa: BLE001 — Cron soll den Fehler im Log sehen
        log.exception("Abbruch: %s", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
