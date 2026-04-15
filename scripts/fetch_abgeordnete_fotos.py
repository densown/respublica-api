#!/usr/bin/env python3
"""Ergänzt fehlende foto_url-Werte in Tabelle abgeordnete via Wikidata (P18)."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import json
import urllib.request
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = ROOT / "logs" / "fetch_abgeordnete_fotos.log"
AW_SLEEP_SEC = 0.5
WIKIDATA_SLEEP_SEC = 0.2
MANDATE_URL = "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/{aw_id}"
POLITICIAN_URL = "https://www.abgeordnetenwatch.de/api/v2/politicians/{politiker_id}"
WIKIDATA_CLAIMS_URL = (
    "https://www.wikidata.org/w/api.php"
    "?action=wbgetclaims&entity={qid}&property=P18&format=json"
)
USER_AGENT = "ResPublicaGesetze/1.0 (+https://respublica.media)"

SELECT_SQL = """
SELECT id, aw_id, politiker_id, name
FROM abgeordnete
WHERE foto_url IS NULL
  AND (politiker_id IS NOT NULL OR aw_id IS NOT NULL)
ORDER BY id ASC
"""

UPDATE_SQL = """
UPDATE abgeordnete
SET politiker_id = %s,
    foto_url = %s
WHERE id = %s
"""


def setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s fetch_abgeordnete_fotos: %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_env() -> None:
    load_dotenv(ROOT / ".env")


def connect_db() -> mysql.connector.MySQLConnection:
    host = os.environ.get("DB_HOST", "127.0.0.1")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "respublica_gesetze")
    if not user:
        raise RuntimeError("DB_USER fehlt in .env")
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lädt fehlende Foto-URLs für Abgeordnete von Abgeordnetenwatch."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Verarbeitet nur die ersten N Datensätze mit foto_url IS NULL.",
    )
    return parser.parse_args()


def fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    url = value.strip()
    if not url:
        return None
    if len(url) > 500:
        return url[:500]
    return url[:500] if len(url) > 500 else url


def fetch_politiker_id_from_mandate(
    aw_id: int,
) -> int | None:
    payload = fetch_json(MANDATE_URL.format(aw_id=aw_id))
    time.sleep(AW_SLEEP_SEC)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None
    politician = data.get("politician")
    if not isinstance(politician, dict):
        return None
    return to_int(politician.get("id"))


def fetch_qid_for_politiker(
    politiker_id: int,
) -> str | None:
    payload = fetch_json(POLITICIAN_URL.format(politiker_id=politiker_id))
    data = payload.get("data") if isinstance(payload, dict) else None
    time.sleep(AW_SLEEP_SEC)
    if not isinstance(data, dict):
        return None
    qid = data.get("qid_wikidata")
    return qid.strip() if isinstance(qid, str) and qid.strip() else None


def fetch_photo_filename_from_wikidata(qid: str) -> str | None:
    payload = fetch_json(WIKIDATA_CLAIMS_URL.format(qid=qid))
    time.sleep(WIKIDATA_SLEEP_SEC)
    claims = payload.get("claims") if isinstance(payload, dict) else None
    if not isinstance(claims, dict):
        return None
    p18_claims = claims.get("P18")
    if not isinstance(p18_claims, list) or not p18_claims:
        return None
    first_claim = p18_claims[0]
    if not isinstance(first_claim, dict):
        return None
    mainsnak = first_claim.get("mainsnak")
    if not isinstance(mainsnak, dict):
        return None
    datavalue = mainsnak.get("datavalue")
    if not isinstance(datavalue, dict):
        return None
    value = datavalue.get("value")
    if not isinstance(value, str):
        return None
    return value.strip() if value.strip() else None


def load_targets(
    conn: mysql.connector.MySQLConnection,
    limit: int | None,
) -> list[tuple[int, int, int | None, str | None]]:
    sql = SELECT_SQL
    params: tuple[Any, ...] = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()

    result: list[tuple[int, int, int | None, str | None]] = []
    for row in rows:
        db_id = to_int(row[0])
        aw_id = to_int(row[1])
        politiker_id = to_int(row[2])
        name = str(row[3]).strip() if row[3] else None
        if db_id is None or aw_id is None:
            continue
        result.append((db_id, aw_id, politiker_id, name))
    return result


def main() -> int:
    args = parse_args()
    setup_logging()
    load_env()

    if args.limit is not None and args.limit <= 0:
        logging.error("--limit muss > 0 sein.")
        return 1

    conn: mysql.connector.MySQLConnection | None = None
    try:
        conn = connect_db()
        targets = load_targets(conn, args.limit)
        total = len(targets)
        logging.info("Starte: %d Datensätze mit fehlender foto_url gefunden.", total)

        updated = 0
        filled_photo = 0
        filled_politiker_id = 0
        skipped_without_qid = 0
        skipped_without_p18 = 0
        errors = 0

        cur = conn.cursor()
        try:
            for index, (db_id, aw_id, existing_politiker_id, name) in enumerate(targets, start=1):
                display_name = name or f"id={db_id}"
                politiker_id = existing_politiker_id
                photo_url: str | None = None

                try:
                    if politiker_id is None:
                        politiker_id = fetch_politiker_id_from_mandate(aw_id)
                        if politiker_id is None:
                            logging.warning(
                                "[%d/%d] %s (aw_id=%s): keine politiker_id im mandate endpoint gefunden.",
                                index,
                                total,
                                display_name,
                                aw_id,
                            )
                            skipped_without_qid += 1
                            continue
                        filled_politiker_id += 1

                    qid = fetch_qid_for_politiker(politiker_id)
                    if qid is None:
                        logging.warning(
                            "[%d/%d] %s (aw_id=%s, politiker_id=%s): keine qid_wikidata gefunden.",
                            index,
                            total,
                            display_name,
                            aw_id,
                            politiker_id,
                        )
                        skipped_without_qid += 1
                        continue

                    filename = fetch_photo_filename_from_wikidata(qid)
                    if filename is None:
                        skipped_without_p18 += 1
                        logging.warning(
                            "[%d/%d] %s (qid=%s): kein Wikidata-P18-Foto gefunden.",
                            index,
                            total,
                            display_name,
                            qid,
                        )
                        continue

                    filename_encoded = filename.replace(" ", "_")
                    photo_url = normalize_url(
                        f"https://commons.wikimedia.org/wiki/Special:Redirect/file/{filename_encoded}"
                    )
                    if photo_url is None:
                        skipped_without_p18 += 1
                        logging.warning(
                            "[%d/%d] %s (qid=%s): erzeugte Foto-URL ungültig.",
                            index,
                            total,
                            display_name,
                            qid,
                        )
                        continue

                    cur.execute(UPDATE_SQL, (politiker_id, photo_url, db_id))
                    conn.commit()
                    updated += 1
                    filled_photo += 1
                    logging.info(
                        "[%d/%d] %s aktualisiert (aw_id=%s, politiker_id=%s, qid=%s).",
                        index,
                        total,
                        display_name,
                        aw_id,
                        politiker_id,
                        qid,
                    )
                except Exception as exc:
                    conn.rollback()
                    errors += 1
                    logging.error(
                        "[%d/%d] %s (aw_id=%s): Fehler: %s",
                        index,
                        total,
                        display_name,
                        aw_id,
                        exc,
                    )
        finally:
            cur.close()

        logging.info(
            (
                "Fertig: %d aktualisiert, %d Foto-URLs gesetzt, %d politiker_id ergänzt, "
                "%d ohne qid_wikidata, %d ohne P18, %d Fehler."
            ),
            updated,
            filled_photo,
            filled_politiker_id,
            skipped_without_qid,
            skipped_without_p18,
            errors,
        )
        return 0 if errors == 0 else 1
    except Exception as exc:  # pragma: no cover
        logging.exception("Abbruch: %s", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
