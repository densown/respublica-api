"""Zuordnung GII-doknr / Slug zu gesetze.id (bestehende DB)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
OVERRIDES_FILE = SCRIPTS_DIR / "gesetze_mapping_overrides.json"

_BJNR_RE = re.compile(r"BJNR[0-9A-Za-z]+")


def extract_bjnr_from_kuerzel(kuerzel: str | None) -> str | None:
    if not kuerzel:
        return None
    m = _BJNR_RE.search(kuerzel)
    return m.group(0) if m else None


def load_overrides() -> dict[str, str]:
    try:
        raw = OVERRIDES_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return {str(k).strip(): str(v).strip() for k, v in data.items() if k and v}
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def match_gesetz_id(cur: Any, doknr: str, slug: str) -> int | None:
    """Liefert gesetze.id oder None."""
    if doknr:
        cur.execute(
            "SELECT id FROM gesetze WHERE gii_doknr = %s LIMIT 1",
            (doknr,),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        cur.execute(
            "SELECT id FROM gesetze WHERE kuerzel LIKE %s LIMIT 1",
            (f"%{doknr}%",),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        overrides = load_overrides()
        ku = overrides.get(doknr)
        if ku:
            cur.execute(
                "SELECT id FROM gesetze WHERE kuerzel = %s LIMIT 1",
                (ku,),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])

    if slug:
        cur.execute(
            """
            SELECT id FROM gesetze
            WHERE LOWER(kuerzel) = %s OR gii_slug = %s
            LIMIT 1
            """,
            (slug.lower(), slug),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

    return None
