#!/usr/bin/env python3
"""
Ruft alle namentlichen Abstimmungen der Wahlperiode 161 von abgeordnetenwatch.de ab,
aggregiert Stimmen je Fraktion und schreibt sie nach MariaDB (Tabelle abstimmungen).

Voraussetzung: UNIQUE(poll_id, partei). Duplikate (zweiter Lauf) per IntegrityError 1062.
Nach jedem Poll: conn.commit(), am Ende ein weiteres commit().
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

BASE = "https://www.abgeordnetenwatch.de/api/v2"
LEGISLATURE = 161
POLL_PAGE = 100
VOTES_PAGE = 1000

UA = "gesetze-fetch/1.0 (+https://github.com/bundestag/gesetze)"


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
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )
    return conn


def fetch_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": UA, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_all_polls() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    start = 0
    total: int | None = None
    while True:
        q = urllib.parse.urlencode(
            {
                "field_legislature": str(LEGISLATURE),
                "range_start": start,
                "range_end": POLL_PAGE,
            }
        )
        url = f"{BASE}/polls?{q}"
        try:
            j = fetch_json(url)
        except urllib.error.HTTPError as e:
            print(f"HTTP-Fehler beim Abruf der Abstimmungen: {e}", file=sys.stderr)
            raise
        data = j.get("data") or []
        meta = (j.get("meta") or {}).get("result") or {}
        total = meta.get("total", total)
        out.extend(data)
        if not data:
            break
        if total is not None and len(out) >= total:
            break
        if len(data) < POLL_PAGE:
            break
        start += POLL_PAGE
    return out


def fetch_all_votes(poll_id: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    start = 0
    total: int | None = None
    while True:
        q = urllib.parse.urlencode(
            {"poll": str(poll_id), "range_start": start, "range_end": VOTES_PAGE}
        )
        url = f"{BASE}/votes?{q}"
        j = fetch_json(url)
        data = j.get("data") or []
        meta = (j.get("meta") or {}).get("result") or {}
        total = meta.get("total", total)
        out.extend(data)
        if not data:
            break
        if total is not None and len(out) >= total:
            break
        start += len(data)
    return out


def fraction_label(vote_row: dict[str, Any]) -> str:
    fr = vote_row.get("fraction")
    if isinstance(fr, dict) and fr.get("label"):
        return str(fr["label"])
    return "(keine Fraktion)"


def aggregate_by_fraction(votes: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """
    Pro Fraktion: Zähler ja, nein, enthalten, abwesend.
    API vote: yes | no | abstain | no_show
    """
    agg: dict[str, dict[str, int]] = defaultdict(
        lambda: {"ja": 0, "nein": 0, "enthalten": 0, "abwesend": 0}
    )
    for v in votes:
        label = fraction_label(v)
        key = v.get("vote")
        bucket = agg[label]
        if key == "yes":
            bucket["ja"] += 1
        elif key == "no":
            bucket["nein"] += 1
        elif key == "abstain":
            bucket["enthalten"] += 1
        elif key == "no_show":
            bucket["abwesend"] += 1
        else:
            bucket["abwesend"] += 1
    return dict(agg)


def parse_poll_datum(poll: dict[str, Any]) -> date | None:
    raw = poll.get("field_poll_date")
    if not raw or not isinstance(raw, str):
        return None
    try:
        y, m, d = raw.split("-")
        return date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def poll_id_as_int(raw: Any) -> int | None:
    """API liefert id oft als int, manchmal als str — ohne int() werden alle Polls übersprungen."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def main() -> int:
    load_env()

    try:
        polls = fetch_all_polls()
    except Exception as e:
        print(f"Abstimmungen konnten nicht geladen werden: {e}", file=sys.stderr)
        return 1

    print(f"[debug] Polls von der API: {len(polls)}")
    print(
        "[debug] DB-Verbindung: "
        f"host={os.environ.get('DB_HOST', 'localhost')!r}, "
        f"database={os.environ.get('DB_NAME', 'respublica_gesetze')!r}, "
        f"user={os.environ.get('DB_USER')!r}"
    )

    conn = connect()
    neue_abstimmungen_polls: set[int] = set()
    neue_votes_summe = 0

    try:
        cur = conn.cursor()
        for poll in polls:
            pid = poll_id_as_int(poll.get("id"))
            if pid is None:
                print(
                    f"[debug] Poll übersprungen (keine gültige id): id={poll.get('id')!r}",
                    file=sys.stderr,
                )
                continue
            titel = poll.get("label") or ""
            if not isinstance(titel, str):
                titel = str(titel)
            poll_datum = parse_poll_datum(poll)

            votes = fetch_all_votes(pid)
            print(f"[debug] Poll {pid}: {len(votes)} Votes")
            by_fr = aggregate_by_fraction(votes)

            for partei, z in by_fr.items():
                ja, nein, enthalten, abwesend = (
                    z["ja"],
                    z["nein"],
                    z["enthalten"],
                    z["abwesend"],
                )
                row = (
                    partei,
                    ja,
                    nein,
                    enthalten,
                    abwesend,
                    pid,
                    titel,
                    poll_datum,
                )
                print(
                    "[debug] INSERT-Zeile: "
                    f"aenderung_id=NULL, partei={partei!r}, "
                    f"ja={ja}, nein={nein}, enthalten={enthalten}, abwesend={abwesend}, "
                    f"poll_id={pid}, poll_titel={titel[:80]!r}…, poll_datum={poll_datum!r}"
                )
                inserted = False
                try:
                    cur.execute(
                        """
                        INSERT INTO abstimmungen
                        (aenderung_id, partei, ja, nein, enthalten, abwesend,
                         poll_id, poll_titel, poll_datum)
                        VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        row,
                    )
                except mysql.connector.IntegrityError as err:
                    # Duplikat (poll_id, partei) — zweiter Lauf
                    if getattr(err, "errno", None) == 1062:
                        print(
                            f"[debug] INSERT übersprungen (Duplikat): poll_id={pid} "
                            f"partei={partei!r}",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            f"[debug] INSERT-Fehler IntegrityError (poll_id={pid}, "
                            f"partei={partei!r}): {err} (errno={getattr(err, 'errno', None)})",
                            file=sys.stderr,
                        )
                        raise
                except mysql.connector.Error as err:
                    print(
                        f"[debug] INSERT-Fehler (poll_id={pid}, partei={partei!r}): "
                        f"{err} (errno={getattr(err, 'errno', None)})",
                        file=sys.stderr,
                    )
                    raise
                else:
                    inserted = True
                w = cur.fetchwarnings()
                if w:
                    print(f"[debug] INSERT-Warnungen: {w}", file=sys.stderr)
                rc = cur.rowcount
                lid = cur.lastrowid
                print(
                    f"[debug] INSERT rowcount={rc}, lastrowid={lid} "
                    f"(Duplikat-Handling: errno 1062 → Zeile existierte schon)"
                )
                if inserted:
                    neue_abstimmungen_polls.add(pid)
                    neue_votes_summe += ja + nein + enthalten + abwesend

            # Nach allen Fraktionen dieses Polls fest schreiben (sichtbar für andere Sessions)
            conn.commit()
            print(f"[debug] conn.commit() nach Poll {pid} ({len(by_fr)} Zeilen)")

        # Sicherheit: falls die Schleife leer war, nichts offen lassen
        conn.commit()
    finally:
        conn.close()

    print(
        f"Fertig: {len(neue_abstimmungen_polls)} Abstimmungen mit neuen Zeilen, "
        f"{neue_votes_summe} Stimmen (ja/nein/enthalten/abwesend) neu gespeichert."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
