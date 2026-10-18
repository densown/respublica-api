"""Abrufe bei abgeordnetenwatch.de: wann und wie oft bei Drosselung wiederholt wird.

abgeordnetenwatch antwortet auf zu dichte Anfragen mit 429, etwa wenn ein
Abruf direkt auf den vorigen folgt oder zwei Laeufe gleichzeitig abfragen
(fetch_abstimmungen.py ab 06:10 und fetch_votes.py ab 06:12). Ein spaeterer
Versuch klappt in der Regel. Gilt fuer fetch_abstimmungen.py, fetch_votes.py
und fetch_abgeordnete.py.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

VERSUCHE = 5
MAX_WARTEN = 60


def wiederholen_bei(status: int) -> bool:
    """429 und 5xx gehen voruebergehend vorbei, alles andere nicht."""
    return status == 429 or status >= 500


def wartezeit(retry_after: str | None, versuch: int) -> float:
    """Sekunden bis zum naechsten Versuch: Retry-After der Quelle, sonst 3, 6, 12, 24."""
    if retry_after and str(retry_after).strip().isdigit():
        return min(int(str(retry_after).strip()), MAX_WARTEN)
    return min(3 * 2**versuch, MAX_WARTEN)


def json_abrufen(url: str, user_agent: str, timeout: int = 120, versuche: int = VERSUCHE) -> dict[str, Any]:
    """GET per urllib mit Wiederholung bei 429 und 5xx; danach die letzte HTTPError."""
    for versuch in range(versuche):
        req = urllib.request.Request(
            url,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if versuch == versuche - 1 or not wiederholen_bei(e.code):
                raise
            time.sleep(wartezeit(e.headers.get("Retry-After") if e.headers else None, versuch))
    raise AssertionError("nicht erreichbar")
