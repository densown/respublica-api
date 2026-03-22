#!/usr/bin/env python3
"""
Verknüpft aenderungen (BGBl-Referenz) mit abstimmungen (poll_id) über die DIP-API
und unscharfen Abgleich von Datum ±30 Tage und Titel/Kürzel.

DIP: Volltext q=… wirkt zuverlässig zusammen mit f.datum.start / f.datum.end
(siehe DIP-OpenAPI-Parameter f.datum.*).
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

DIP_BASE = "https://search.dip.bundestag.de/api/v1/vorgang"
# Öffentlicher DIP-Schlüssel (bund.dev / DIP-Hilfe); gültig bis ca. 05/2026.
# Häufiger Schreibfehler: „…YKkhw“ statt „…YKtwKkhw“.
DIP_APIKEY = "OSOegLs.PR2lwJ1dwCeje9vTj7FPOt3hvpYKtwKkhw"
WINDOW_DAYS = 30
MIN_SCORE = 0.28
DIP_MAX_PAGES = 40

UA = "gesetze-match-abstimmungen/1.0"


def load_env() -> None:
    load_dotenv(ROOT / ".env")


def connect():
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "respublica_gesetze")
    if not user:
        print("Fehler: DB_USER fehlt in .env", file=sys.stderr)
        sys.exit(1)
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )


def norm_text(s: str | None) -> str:
    if not s:
        return ""
    t = s.replace("\u00ad", "").lower()
    t = re.sub(r"\s+", " ", t).strip()
    return t


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def parse_mysql_date(val: Any) -> date | None:
    if val is None:
        return None
    if hasattr(val, "year"):
        return date(val.year, val.month, val.day)
    s = str(val)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def dip_datum_window(change_date: date) -> tuple[str, str]:
    """DIP f.datum.*: großzügig um das Änderungsdatum (Verkündung/Abstimmung streuen)."""
    start = change_date - timedelta(days=800)
    end = change_date + timedelta(days=400)
    return start.isoformat(), end.isoformat()


def vorgang_text_blob(doc: dict[str, Any]) -> str:
    parts = [doc.get("titel"), doc.get("abstract"), doc.get("mitteilung")]
    return norm_text(" ".join(str(p or "") for p in parts))


def vorgang_matches(doc: dict[str, Any], bgbl: str, kuerzel: str) -> bool:
    blob = vorgang_text_blob(doc)
    bg = norm_text(bgbl)
    if len(bg) >= 6 and bg in blob:
        return True
    nums = re.findall(r"\d{4}|\d{3,5}", bgbl)
    seen: list[str] = []
    for n in nums:
        if n not in seen:
            seen.append(n)
    if len(seen) >= 2 and all(n in blob for n in seen[-2:]):
        return True
    ku = norm_text(kuerzel)
    tit = norm_text(doc.get("titel") or "")
    if len(ku) >= 3 and ku in tit:
        return True
    if len(ku) >= 3 and similarity(ku, tit) >= 0.42:
        return True
    return False


def dip_request(params: dict[str, str], apikey: str) -> dict[str, Any]:
    """GET /vorgang mit apikey-Parameter; bei 401 mit Authorization: ApiKey."""
    q = dict(params)
    q["apikey"] = apikey
    url = f"{DIP_BASE}?{urllib.parse.urlencode(q)}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": UA, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code != 401:
            raise
        q2 = {k: v for k, v in q.items() if k != "apikey"}
        url2 = f"{DIP_BASE}?{urllib.parse.urlencode(q2)}"
        req2 = urllib.request.Request(
            url2,
            headers={
                "User-Agent": UA,
                "Accept": "application/json",
                "Authorization": f"ApiKey {apikey}",
            },
        )
        with urllib.request.urlopen(req2, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8"))


def find_vorgang_for_bgbl(
    bgbl: str,
    kuerzel: str,
    change_date: date,
    apikey: str,
    max_pages: int = DIP_MAX_PAGES,
) -> tuple[date | None, str | None]:
    """
    Sucht per q=BGBl und f.datum.* paginiert den passenden Vorgang.
    """
    ds, de = dip_datum_window(change_date)
    cursor: str | None = None
    page = 0
    while page < max_pages:
        params: dict[str, str] = {
            "q": bgbl,
            "f.datum.start": ds,
            "f.datum.end": de,
        }
        if cursor:
            params["cursor"] = cursor
        data = dip_request(params, apikey)
        docs = data.get("documents") or []
        for d in docs:
            if vorgang_matches(d, bgbl, kuerzel):
                raw_d = d.get("datum")
                titel = d.get("titel")
                if raw_d:
                    try:
                        dd = date.fromisoformat(str(raw_d)[:10])
                    except ValueError:
                        dd = change_date
                else:
                    dd = change_date
                return dd, titel if isinstance(titel, str) else None
        cursor = data.get("cursor")
        if not cursor or not docs:
            break
        page += 1
    return None, None


def find_vorgang_by_kuerzel(
    kuerzel: str,
    change_date: date,
    apikey: str,
    max_pages: int = 20,
) -> tuple[date | None, str | None]:
    """Fallback: Suche mit q=Kürzel."""
    if not (kuerzel or "").strip():
        return None, None
    ds, de = dip_datum_window(change_date)
    cursor = None
    page = 0
    while page < max_pages:
        params: dict[str, str] = {
            "q": kuerzel.strip(),
            "f.datum.start": ds,
            "f.datum.end": de,
        }
        if cursor:
            params["cursor"] = cursor
        data = dip_request(params, apikey)
        docs = data.get("documents") or []
        for d in docs:
            tit = norm_text(d.get("titel") or "")
            ku = norm_text(kuerzel)
            if len(ku) >= 3 and (ku in tit or similarity(ku, tit) >= 0.5):
                raw_d = d.get("datum")
                titel = d.get("titel")
                if raw_d:
                    try:
                        dd = date.fromisoformat(str(raw_d)[:10])
                    except ValueError:
                        dd = change_date
                else:
                    dd = change_date
                return dd, titel if isinstance(titel, str) else None
        cursor = data.get("cursor")
        if not cursor or not docs:
            break
        page += 1
    return None, None


def score_match(
    dip_datum: date,
    dip_titel: str,
    kuerzel: str,
    poll_datum: date,
    poll_titel: str,
) -> float:
    dd = abs((poll_datum - dip_datum).days)
    date_score = max(0.0, 1.0 - min(dd, WINDOW_DAYS) / float(WINDOW_DAYS))
    d1 = norm_text(dip_titel)
    d2 = norm_text(poll_titel)
    ku = norm_text(kuerzel)
    sim_title = similarity(d1, d2)
    sim_k = max(
        similarity(ku, d2),
        1.0 if ku and (ku in d2 or d2 in ku) else 0.0,
    )
    return 0.35 * date_score + 0.4 * sim_title + 0.25 * sim_k


def fetch_poll_candidates(
    cur,
    d0: date,
) -> list[tuple[int, str, date]]:
    d_min = d0 - timedelta(days=WINDOW_DAYS)
    d_max = d0 + timedelta(days=WINDOW_DAYS)
    cur.execute(
        """
        SELECT DISTINCT poll_id, poll_titel, poll_datum
        FROM abstimmungen
        WHERE poll_datum IS NOT NULL
          AND poll_datum >= %s AND poll_datum <= %s
        """,
        (d_min, d_max),
    )
    out: list[tuple[int, str, date]] = []
    for row in cur.fetchall():
        pid, titel, pdat = row[0], row[1], parse_mysql_date(row[2])
        if pdat is None:
            continue
        out.append((int(pid), str(titel or ""), pdat))
    return out


def main() -> int:
    load_env()
    apikey = os.environ.get("DIP_API_KEY", DIP_APIKEY)

    conn = connect()
    matched = 0
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT a.id AS aenderung_id, a.bgbl_referenz, a.datum AS aenderung_datum,
                   g.kuerzel AS kuerzel
            FROM aenderungen a
            INNER JOIN gesetze g ON g.id = a.gesetz_id
            WHERE a.bgbl_referenz IS NOT NULL
              AND TRIM(a.bgbl_referenz) != ''
              AND a.poll_id IS NULL
            """
        )
        rows = cur.fetchall()

        for row in rows:
            aid = row["aenderung_id"]
            bgbl = (row["bgbl_referenz"] or "").strip()
            kuerzel = row["kuerzel"] or ""
            change_date = parse_mysql_date(row.get("aenderung_datum"))
            if not bgbl:
                continue
            if change_date is None:
                change_date = date.today()

            try:
                dip_datum, dip_titel = find_vorgang_for_bgbl(
                    bgbl, kuerzel, change_date, apikey
                )
                if dip_datum is None:
                    dip_datum, dip_titel = find_vorgang_by_kuerzel(
                        kuerzel, change_date, apikey
                    )
            except Exception as e:
                print(f"DIP-Fehler ({bgbl[:40]}…): {e}", file=sys.stderr)
                continue

            if dip_datum is None:
                continue

            candidates = fetch_poll_candidates(cur, dip_datum)
            if not candidates:
                continue

            best_score = -1.0
            best_poll_id: int | None = None
            for pid, poll_titel, pdat in candidates:
                sc = score_match(
                    dip_datum,
                    dip_titel or "",
                    kuerzel,
                    pdat,
                    poll_titel,
                )
                if sc > best_score:
                    best_score = sc
                    best_poll_id = pid

            if best_poll_id is None or best_score < MIN_SCORE:
                continue

            cur.execute(
                "UPDATE aenderungen SET poll_id = %s WHERE id = %s",
                (best_poll_id, aid),
            )
            if cur.rowcount:
                matched += 1

        conn.commit()
    finally:
        conn.close()

    print(f"Verknüpfungen hergestellt: {matched}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
