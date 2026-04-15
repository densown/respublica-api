#!/usr/bin/env python3
"""Lädt Einzelstimmen zu vorhandenen poll_id-Werten in Tabelle votes."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = ROOT / "logs" / "fetch_votes.log"
BASE_URL = "https://www.abgeordnetenwatch.de/api/v2/polls"
USER_AGENT = "ResPublicaGesetze/1.0 (+https://respublica.media)"
ALLOWED_VOTES = {"yes", "no", "abstain", "no_show"}


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS votes (
  id INT AUTO_INCREMENT PRIMARY KEY,
  vote_id INT UNIQUE,
  poll_id INT,
  mandate_id INT,
  abgeordneter_name VARCHAR(200),
  vote ENUM('yes', 'no', 'abstain', 'no_show'),
  fraction_id INT,
  fraction_label VARCHAR(200),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_poll_id (poll_id),
  INDEX idx_mandate_id (mandate_id)
);
"""


INSERT_SQL = """
INSERT IGNORE INTO votes (
  vote_id,
  poll_id,
  mandate_id,
  abgeordneter_name,
  vote,
  fraction_id,
  fraction_label
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s fetch_votes: %(message)s",
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
    if not user:
        raise RuntimeError("DB_USER fehlt in .env")
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database="respublica_gesetze",
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )


def fetch_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def get_poll_ids(
    conn: mysql.connector.MySQLConnection,
    limit: int | None,
) -> list[int]:
    cur = conn.cursor()
    try:
        sql = "SELECT DISTINCT poll_id FROM abstimmungen WHERE poll_id IS NOT NULL ORDER BY poll_id"
        if limit is not None:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def extract_name(label: Any) -> str | None:
    if label is None:
        return None
    text = str(label).strip()
    if not text:
        return None
    if " - " in text:
        text = text.split(" - ", 1)[0].strip()
    return text[:200] if text else None


def normalize_vote(raw_vote: Any) -> str | None:
    if raw_vote is None:
        return None
    vote = str(raw_vote).strip()
    if vote in ALLOWED_VOTES:
        return vote
    return None


def process_poll(
    conn: mysql.connector.MySQLConnection,
    poll_id: int,
) -> tuple[int, int]:
    url = f"{BASE_URL}/{poll_id}?related_data=votes"
    payload = fetch_json(url)
    related_votes = ((payload.get("data") or {}).get("related_data") or {}).get("votes") or []
    if not isinstance(related_votes, list):
        return 0, 0

    cur = conn.cursor()
    processed = 0
    inserted = 0
    try:
        for item in related_votes:
            if not isinstance(item, dict):
                continue
            vote_id = as_int(item.get("id"))
            src_poll_id = as_int((item.get("poll") or {}).get("id")) or poll_id
            mandate_id = as_int((item.get("mandate") or {}).get("id"))
            abgeordneter_name = extract_name(item.get("label"))
            vote = normalize_vote(item.get("vote"))
            fraction = item.get("fraction") or {}
            fraction_id = as_int(fraction.get("id")) if isinstance(fraction, dict) else None
            fraction_label = (
                str(fraction.get("label")).strip()[:200]
                if isinstance(fraction, dict) and fraction.get("label")
                else None
            )

            if vote_id is None or src_poll_id is None or mandate_id is None or vote is None:
                continue

            cur.execute(
                INSERT_SQL,
                (
                    vote_id,
                    src_poll_id,
                    mandate_id,
                    abgeordneter_name,
                    vote,
                    fraction_id,
                    fraction_label,
                ),
            )
            processed += 1
            if cur.rowcount == 1:
                inserted += 1
        conn.commit()
    finally:
        cur.close()
    return processed, inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lädt Stimmen in Tabelle votes.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nur die ersten N poll_ids aus abstimmungen verarbeiten.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging()
    load_env()

    if args.limit is not None and args.limit <= 0:
        logging.error("--limit muss > 0 sein.")
        return 1

    try:
        # Kurze Initialverbindung: Tabelle sicherstellen + poll_ids laden.
        init_conn = connect_db()
        try:
            cur = init_conn.cursor()
            try:
                cur.execute(CREATE_TABLE_SQL)
                init_conn.commit()
            finally:
                cur.close()

            poll_ids = get_poll_ids(init_conn, args.limit)
        finally:
            init_conn.close()

        logging.info("Starte: %d poll_ids gefunden.", len(poll_ids))

        total_processed = 0
        total_inserted = 0
        for index, poll_id in enumerate(poll_ids, start=1):
            conn = None
            try:
                # Neue DB-Verbindung pro poll_id, damit lange Läufe robust bleiben.
                conn = connect_db()
                processed, inserted = process_poll(conn, poll_id)
            except urllib.error.HTTPError as exc:
                logging.error("HTTP-Fehler für poll_id=%s: %s", poll_id, exc)
                continue
            except urllib.error.URLError as exc:
                logging.error("Netzwerkfehler für poll_id=%s: %s", poll_id, exc)
                continue
            except mysql.connector.Error as exc:
                if conn is not None:
                    conn.rollback()
                logging.error("DB-Fehler für poll_id=%s: %s", poll_id, exc)
                continue
            finally:
                if conn is not None:
                    conn.close()

            total_processed += processed
            total_inserted += inserted
            logging.info(
                "[%d/%d] poll_id=%s verarbeitet: %d gelesen, %d neu eingefügt.",
                index,
                len(poll_ids),
                poll_id,
                processed,
                inserted,
            )
            time.sleep(1)

        logging.info(
            "Fertig: %d Stimmen gelesen, %d neu eingefügt.",
            total_processed,
            total_inserted,
        )
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Abbruch: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
