#!/usr/bin/env python3
"""
Taeglicher Sync: gesetze-im-internet.de -> respublica_gesetze.gesetze
Siehe PIPELINE-PLAN.md (Cron-Vorschlag 04:00).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import mysql.connector
import requests
from dotenv import load_dotenv

from gii_match import match_gesetz_id
from gii_parse import GII_TOC_URL, extract_metadata, fetch_law_tree, parse_toc_bytes, save_toc_cache

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
DATA_DIR = ROOT / "data"
TOC_CACHE = DATA_DIR / "gii_toc.xml"
XML_CACHE = DATA_DIR / "xml_cache"


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logfile = LOG_DIR / f"gii_sync_{datetime.now():%Y-%m-%d}.log"
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)


def get_db():
    load_dotenv(ROOT / ".env")
    kw: dict = dict(
        user=os.environ.get("DB_USER", "root"),
        password=os.environ.get("DB_PASSWORD", ""),
        database=os.environ.get("DB_NAME", "respublica_gesetze"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )
    sock = os.environ.get("DB_UNIX_SOCKET", "/var/run/mysqld/mysqld.sock")
    if Path(sock).exists():
        kw["unix_socket"] = sock
    else:
        kw["host"] = os.environ.get("DB_HOST", "localhost")
    return mysql.connector.connect(**kw)


def log_run_start(cur) -> int:
    cur.execute(
        "INSERT INTO gesetze_sync_log (run_started_at, status) VALUES (NOW(), 'running')"
    )
    return int(cur.lastrowid)


def row_is_current(cur, gesetz_id: int, doknr: str, builddate: str) -> bool:
    if not doknr or not builddate:
        return False
    cur.execute(
        """
        SELECT 1 FROM gesetze
        WHERE id = %s
          AND IFNULL(gii_doknr,'') = %s
          AND IFNULL(gii_builddate,'') = %s
        LIMIT 1
        """,
        (gesetz_id, doknr, builddate),
    )
    return cur.fetchone() is not None


def pick_insert_kuerzel(meta: dict, slug: str, doknr: str) -> str:
    for key in ("jurabk", "amtabk"):
        v = (meta.get(key) or "").strip()
        if v:
            cand = v[:50]
            return cand
    s = slug.strip()[:50]
    if s:
        return s
    return (doknr or slug)[:50]


def update_law(cur, gesetz_id: int, meta: dict, slug: str) -> None:
    amt = (meta.get("amtabk") or meta.get("jurabk") or "").strip() or None
    cur.execute(
        """
        UPDATE gesetze SET
            titel_offiziell = %s,
            amtliche_abkuerzung = %s,
            ausfertigung_datum = NULLIF(%s, ''),
            fundstelle_periodikum = %s,
            fundstelle_zitstelle = %s,
            letzter_stand = %s,
            gii_slug = %s,
            gii_doknr = %s,
            gii_builddate = %s,
            gii_last_synced = NOW(),
            status = 'aktiv'
        WHERE id = %s
        """,
        (
            meta.get("langue") or None,
            amt,
            meta.get("ausfertigung_datum") or "",
            meta.get("fundstelle_periodikum") or None,
            meta.get("fundstelle_zitstelle") or None,
            meta.get("letzter_stand"),
            slug,
            meta.get("doknr"),
            meta.get("builddate"),
            gesetz_id,
        ),
    )


def insert_law(cur, meta: dict, slug: str) -> None:
    doknr = (meta.get("doknr") or "").strip()
    ku = pick_insert_kuerzel(meta, slug, doknr)
    cur.execute("SELECT id FROM gesetze WHERE kuerzel = %s LIMIT 1", (ku,))
    if cur.fetchone() and doknr:
        ku = doknr[:50]
    pfad = f"gii/{slug}"
    titel = meta.get("langue") or ""
    amt = (meta.get("amtabk") or meta.get("jurabk") or "").strip() or None
    cur.execute(
        """
        INSERT INTO gesetze (
            kuerzel, name, pfad, titel_offiziell, amtliche_abkuerzung,
            ausfertigung_datum, fundstelle_periodikum, fundstelle_zitstelle,
            letzter_stand, gii_slug, gii_doknr, gii_builddate, gii_last_synced, status
        ) VALUES (
            %s, %s, %s, %s, %s,
            NULLIF(%s, ''), %s, %s,
            %s, %s, %s, %s, NOW(), 'aktiv'
        )
        """,
        (
            ku,
            "",
            pfad,
            titel or None,
            amt,
            meta.get("ausfertigung_datum") or "",
            meta.get("fundstelle_periodikum") or None,
            meta.get("fundstelle_zitstelle") or None,
            meta.get("letzter_stand"),
            slug,
            doknr or None,
            meta.get("builddate") or None,
        ),
    )


def run_sync(*, initial_import: bool = False, limit: int | None = None) -> int:
    setup_logging()
    log = logging.getLogger("gii_sync")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    XML_CACHE.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {"User-Agent": "ResPublica-GII-Sync/1.0 (+https://respublica.media)"}
    )

    try:
        r = session.get(GII_TOC_URL, timeout=90)
        r.raise_for_status()
        save_toc_cache(str(TOC_CACHE), r.content)
        toc = parse_toc_bytes(r.content)
    except Exception as e:
        log.error("TOC Download/Parse fehlgeschlagen: %s", e)
        return 1

    if limit is not None:
        toc = toc[: max(0, limit)]

    total_toc = len(toc)
    stats_neu = 0
    stats_geaendert = 0
    stats_fehler = 0
    stats_skip = 0

    conn = get_db()
    cur = conn.cursor()
    log_id = log_run_start(cur)
    conn.commit()

    try:
        for i, entry in enumerate(toc):
            slug = entry["slug"]
            if i % 500 == 0:
                log.info("Fortschritt %s / %s", i, total_toc)
            try:
                tree = fetch_law_tree(session, slug)
                if tree is None:
                    stats_fehler += 1
                    continue
                meta = extract_metadata(tree)
                if not meta or not meta.get("doknr"):
                    stats_fehler += 1
                    continue
                doknr = str(meta["doknr"])
                builddate = str(meta.get("builddate") or "")

                gesetz_id = match_gesetz_id(cur, doknr, slug)
                if gesetz_id is not None and not initial_import:
                    if row_is_current(cur, gesetz_id, doknr, builddate):
                        stats_skip += 1
                        continue

                if gesetz_id is not None:
                    update_law(cur, gesetz_id, meta, slug)
                    stats_geaendert += 1
                else:
                    insert_law(cur, meta, slug)
                    stats_neu += 1

                if (stats_neu + stats_geaendert) % 25 == 0:
                    conn.commit()
            except Exception as e:
                log.error("Fehler slug=%s: %s", slug, e)
                stats_fehler += 1
                continue

        conn.commit()
        cur.execute(
            """
            UPDATE gesetze_sync_log
            SET run_ended_at = NOW(), status = 'success',
                gesetze_total = %s, gesetze_neu = %s,
                gesetze_geaendert = %s, gesetze_fehler = %s
            WHERE id = %s
            """,
            (total_toc, stats_neu, stats_geaendert, stats_fehler, log_id),
        )
        conn.commit()
        log.info(
            "Fertig total=%s neu=%s geaendert=%s fehler=%s skip=%s initial=%s",
            total_toc,
            stats_neu,
            stats_geaendert,
            stats_fehler,
            stats_skip,
            initial_import,
        )
        return 0
    except Exception as e:
        log.exception("FATAL: %s", e)
        try:
            cur.execute(
                """
                UPDATE gesetze_sync_log
                SET run_ended_at = NOW(), status = 'failed', error_message = %s
                WHERE id = %s
                """,
                (str(e)[:2000], log_id),
            )
            conn.commit()
        except Exception:
            pass
        return 1
    finally:
        cur.close()
        conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description="GII Taeglicher Sync")
    p.add_argument(
        "--initial",
        action="store_true",
        help="Kein builddate-Skip (alle Eintraege neu verarbeiten)",
    )
    p.add_argument("--limit", type=int, default=None, help="Max. TOC-Eintraege")
    args = p.parse_args()
    return run_sync(initial_import=bool(args.initial), limit=args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
