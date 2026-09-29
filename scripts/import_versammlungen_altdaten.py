#!/usr/bin/env python3
"""Importiert veroeffentlichte historische Versammlungsdaten (vor dem eigenen Archiv).

Jede Quelle wird als eigene `quelle` gefuehrt und nie mit dem laufenden
Archiv (polizei_berlin, fetch_versammlungen_berlin.py) vermischt. Die
Quelldatei wird unveraendert nach `versammlungen_rohdaten` archiviert (liegt
damit im DB-Backup), der abgedeckte Zeitraum nach `versammlungen_abdeckung`.

Quellen (Dateien unter data/versammlungen_altdaten/, Herkunft siehe QUELLEN):
  pomerenke_berlin  2018-2022, Datum, Thema, Teilnehmende angemeldet/tatsaechlich
  fds_berlin_2023   01/2023-04/2024, zusaetzlich Uhrzeit, Ort, Aufzugsstrecke

Teilnehmerzahlen gehen nach `versammlung_zahlen`: angemeldet = Erwartung bei
der Anzeige, polizei = von der Polizei festgestellte Zahl. Eine festgestellte
Zahl > 0 belegt die Durchfuehrung (Status 'stattgefunden'), sonst 'vergangen'.

Der Import ist idempotent: vorhandene Zeilen der Quelle werden ersetzt.
Bricht ab, wenn an diesen Zeilen Zahlen aus anderen Quellen haengen.

Modi:
  --quelle Q   nur diese Quelle (Standard: alle)
  --dry-run    nur parsen und zaehlen

Cron-tauglich (einmaliger Lauf): Exit 0 bei Erfolg, Exit 1 bei Fehler.
"""
from __future__ import annotations

import argparse
import html
import io
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock
from lib.versammlungen import (
    archive_raw,
    build_row,
    clean_text,
    parse_zeit,
    sync_kategorien,
)

LOCK_NAME = "import_versammlungen_altdaten"
DATEN = Path(__file__).resolve().parent.parent / "data" / "versammlungen_altdaten"

QUELLEN = {
    "pomerenke_berlin": {
        "datei": "zenodo_german_protest_registrations_16_cities_unfiltered.csv",
        "land": "BE",
        "stadt": "Berlin",
        "von": date(2018, 1, 1),
        "bis": date(2022, 12, 31),
        "name": "The German Protest Registrations Dataset (David Pomerenke, 2023), Berlin-Teil",
        "url": "https://doi.org/10.5281/zenodo.10094245",
        "lizenz": "CC BY-SA 4.0",
        "hinweis": (
            "Aus IFG-Anfragen bei der Polizei Berlin zusammengestellt. Ohne Ort, "
            "Uhrzeit und Typ (Kundgebung/Aufzug). Jahressummen weichen teils von "
            "Presseangaben ab (2021: 6.220 gegenüber 7.008 laut Tagesspiegel)."
        ),
    },
    "fds_berlin_2023": {
        "datei": "berlin_2023-2024_fds_auswertungzuifg64-24semsrott.xlsx",
        "land": "BE",
        "stadt": "Berlin",
        "von": date(2023, 1, 1),
        "bis": date(2024, 4, 30),
        "name": "Polizei Berlin, IFG-Auskunft 64/24 (FragDenStaat, Anfrage „Versammlungen 2023 und 2024“)",
        "url": "https://fragdenstaat.de/anfrage/versammlungen-2023-und-2024/",
        "lizenz": "amtliche Auskunft, Weiterverwendung ungeklärt",
        "hinweis": "Liste angezeigter Versammlungen mit Ort, Uhrzeit und Teilnehmenden.",
    },
}

log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


# ---------------------------------------------------------------------------
# Parser je Quelle: Rohbytes -> [(row, angemeldet, tatsaechlich)]
# ---------------------------------------------------------------------------

def _zahl(value) -> int | None:
    """'100', 100.0, '1.500' -> int > 0, sonst None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().replace(".", "").replace(" ", "")
    if s.endswith(",0"):
        s = s[:-2]
    try:
        n = int(float(s))
    except ValueError:
        return None
    return n if n > 0 else None


def _text(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return clean_text(html.unescape(str(value)))


def parse_pomerenke(raw: bytes, stadt: str) -> list[tuple[dict, int | None, int | None]]:
    df = pd.read_csv(io.BytesIO(raw), dtype=str)
    df = df[df["city"] == stadt]
    out = []
    for r in df.itertuples(index=False):
        try:
            datum = date.fromisoformat(r.date)
        except (TypeError, ValueError):
            continue
        row = build_row(
            datum=datum, von=None, bis=None, thema_roh=_text(r.topic),
            plz=None, ort=None, aufzugsstrecke=None, typ_bekannt=False,
        )
        out.append((row, _zahl(r.participants_registered), _zahl(r.participants_actual)))
    return out


def parse_fds_berlin_2023(raw: bytes, stadt: str) -> list[tuple[dict, int | None, int | None]]:
    # Blatt "2024 01-04" ist vollstaendig in "2023" enthalten (geprueft 29.09.2026)
    df = pd.read_excel(io.BytesIO(raw), sheet_name="2023", dtype=object)
    out = []
    for r in df.to_dict("records"):
        datum = pd.to_datetime(r.get("Datum"), errors="coerce")
        if pd.isna(datum):
            continue
        plz = _zahl(r.get("PLZ"))
        strasse = _text(r.get("Strasse"))
        nr = _text(r.get("Nr"))
        if nr and nr.endswith(".0"):
            nr = nr[:-2]
        ort = " ".join(x for x in (strasse, nr) if x) or None
        row = build_row(
            datum=datum.date(),
            von=parse_zeit(_text(r.get("Von"))),
            bis=parse_zeit(_text(r.get("Bis"))),
            thema_roh=_text(r.get("Thema")),
            plz=f"{plz:05d}" if plz else None,
            ort=ort,
            aufzugsstrecke=_text(r.get("Aufzugsstrecke")),
        )
        out.append((
            row,
            _zahl(r.get("Teilnehmende (angemeldet)")),
            _zahl(r.get("Teilnehmende (tatsächlich)")),
        ))
    return out


PARSER = {"pomerenke_berlin": parse_pomerenke, "fds_berlin_2023": parse_fds_berlin_2023}


def eindeutig(eintraege: list[tuple[dict, int | None, int | None]]) -> None:
    """quelle_id pro Quelle eindeutig machen.

    Ohne Ort und Uhrzeit (Pomerenke) sind zwei "Mahnwache"-Termine am selben
    Tag inhaltlich gleich, aber zwei Versammlungen. Anders als bei der
    laufenden Liste wird deshalb nicht dedupliziert, sondern durchnummeriert.
    """
    gesehen: dict[str, int] = {}
    for row, _, _ in eintraege:
        qid = row["quelle_id"]
        n = gesehen.get(qid, 0)
        gesehen[qid] = n + 1
        if n:
            row["quelle_id"] = f"{qid[:36]}#{n:03d}"


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def importiere(cur, quelle: str, cfg: dict, eintraege, jetzt: datetime) -> dict:
    cur.execute(
        "SELECT COUNT(*) FROM versammlung_zahlen z JOIN versammlungen v ON v.id = z.versammlung_id "
        "WHERE v.quelle = %s AND NOT (z.quelle_name <=> %s)",
        (quelle, cfg["name"]),
    )
    if cur.fetchone()[0]:
        raise RuntimeError(f"{quelle}: Zahlen aus anderen Quellen vorhanden, Import abgebrochen")
    cur.execute("DELETE FROM versammlungen WHERE quelle = %s", (quelle,))
    geloescht = cur.rowcount

    stats = {"geloescht": geloescht, "zeilen": 0, "angemeldet": 0, "polizei": 0, "stattgefunden": 0}
    for row, angemeldet, tatsaechlich in eintraege:
        if not _running:
            raise RuntimeError("Abbruch angefordert")
        status = "stattgefunden" if tatsaechlich else "vergangen"
        cur.execute(
            """
            INSERT INTO versammlungen
              (quelle, quelle_id, land, stadt, datum, von, bis, ganztaegig, thema,
               thema_hash, plz, ort, aufzugsstrecke, typ, serie_id, serie_von,
               serie_bis, serie_rhythmus, erfassung, status, erstmals_gesehen,
               zuletzt_gesehen)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                quelle, row["quelle_id"], cfg["land"], cfg["stadt"], row["datum"], row["von"],
                row["bis"], int(row["ganztaegig"]), row["thema"], row["thema_hash"], row["plz"],
                row["ort"], row["aufzugsstrecke"], row["typ"], row["serie_id"], row["serie_von"],
                row["serie_bis"], row["serie_rhythmus"], row["erfassung"], status, jetzt, jetzt,
            ),
        )
        vid = cur.lastrowid
        stats["zeilen"] += 1
        stats["stattgefunden"] += status == "stattgefunden"
        for typ, wert in (("angemeldet", angemeldet), ("polizei", tatsaechlich)):
            if wert:
                cur.execute(
                    "INSERT INTO versammlung_zahlen "
                    "(versammlung_id, quelle_typ, wert, quelle_name, quelle_url) VALUES (%s,%s,%s,%s,%s)",
                    (vid, typ, wert, cfg["name"], cfg["url"]),
                )
                stats[typ] += 1

    cur.execute(
        """
        INSERT INTO versammlungen_abdeckung (quelle, land, stadt, von, bis, name, url, lizenz, hinweis)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE von=VALUES(von), bis=VALUES(bis), name=VALUES(name),
          url=VALUES(url), lizenz=VALUES(lizenz), hinweis=VALUES(hinweis)
        """,
        (quelle, cfg["land"], cfg["stadt"], cfg["von"], cfg["bis"], cfg["name"], cfg["url"],
         cfg["lizenz"], cfg["hinweis"]),
    )
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Historische Versammlungsdaten importieren")
    ap.add_argument("--quelle", choices=sorted(QUELLEN), help="nur diese Quelle")
    ap.add_argument("--dry-run", action="store_true", help="nur parsen und zaehlen")
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = None
    try:
        load_env()
        jetzt = datetime.now().replace(microsecond=0)
        if not args.dry_run:
            conn = get_db(autocommit=False)
        for quelle in [args.quelle] if args.quelle else list(QUELLEN):
            cfg = QUELLEN[quelle]
            pfad = DATEN / cfg["datei"]
            raw = pfad.read_bytes()
            eintraege = [
                e for e in PARSER[quelle](raw, cfg["stadt"])
                if cfg["von"] <= e[0]["datum"] <= cfg["bis"]
            ]
            eindeutig(eintraege)
            jahre: dict[int, int] = {}
            for row, _, _ in eintraege:
                jahre[row["datum"].year] = jahre.get(row["datum"].year, 0) + 1
            log.info(
                "%s: %d Versammlungstage, davon %d in Serien, %d verschiedene Themen; pro Jahr %s",
                quelle, len(eintraege), sum(1 for r, _, _ in eintraege if r["serie_id"]),
                len({r["thema_hash"] for r, _, _ in eintraege}), jahre,
            )
            if args.dry_run:
                continue
            cur = conn.cursor()
            archiviert = archive_raw(cur, quelle, raw, len(eintraege), jetzt)
            stats = importiere(cur, quelle, cfg, eintraege, jetzt)
            conn.commit()
            log.info("%s: Rohdatei %s, %s", quelle, "archiviert" if archiviert else "unveraendert", stats)
        if conn is not None:
            n = sync_kategorien(conn.cursor())
            conn.commit()
            log.info("%d Kategorien aus Cache uebernommen", n)
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
