#!/usr/bin/env python3
"""
Lädt den Aktualitätendienst gesetze-im-internet.de, parst BGBl.-Einträge,
ordnet per Kürzel-Mapping und Datum (±60 Tage) aenderungen zu und setzt bgbl_referenz.
"""

from __future__ import annotations

import html as html_module
import os
import re
import sys
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

AKTU_URL = "https://www.gesetze-im-internet.de/aktuDienst.html"
WINDOW_DAYS = 60

UA = "gesetze-fetch-bgbl/1.0 (+https://github.com/kmein/gesetze)"

# Häufige deutsche Gesetzesbezeichnungen im Verkündungstitel → Kürzel (längere Phrasen zuerst prüfen)
TITLE_PHRASE_TO_KUERZEL: tuple[tuple[str, str], ...] = (
    ("bürgerlichen gesetzbuchs", "BGB"),
    ("buergerlichen gesetzbuchs", "BGB"),
    ("strafgesetzbuchs", "StGB"),
    ("strafprozessordnung", "StPO"),
    ("handelsgesetzbuchs", "HGB"),
    ("mindestlohngesetzes", "MiLoG"),
    ("mindestlohngesetz", "MiLoG"),
    ("sozialgesetzbuchs drittes buch", "SGB III"),
    ("sozialgesetzbuchs zweites buch", "SGB II"),
    ("sozialgesetzbuchs fünftes buch", "SGB V"),
    ("sozialgesetzbuchs sechstes buch", "SGB VI"),
    ("sozialgesetzbuchs", "SGB"),
    ("grundgesetzes", "GG"),
    ("grundgesetz", "GG"),
    ("abgabenordnung", "AO"),
    ("einkommensteuergesetzes", "EStG"),
    ("einkommensteuergesetz", "EStG"),
    ("umsatzsteuergesetzes", "UStG"),
    ("umsatzsteuergesetz", "UStG"),
    ("luftsicherheitsgesetzes", "LuftSiG"),
    ("luftsicherheitsgesetz", "LuftSiG"),
    ("güterkraftverkehrsgesetzes", "GüKG"),
    ("güterkraftverkehrsgesetz", "GüKG"),
    ("personenbeförderungsgesetzes", "PBefG"),
    ("personenbefoerderungsgesetzes", "PBefG"),
    ("personenbeförderungsgesetz", "PBefG"),
    ("personenbefoerderungsgesetz", "PBefG"),
    ("gesetzes gegen den unlauteren wettbewerb", "UWG"),
    ("produktsicherheitsgesetzes", "ProdSG"),
    ("produktsicherheitsgesetz", "ProdSG"),
    ("tiergesundheitsgesetzes", "TierGesG"),
    ("tiergesundheitsgesetz", "TierGesG"),
    ("europol-gesetzes", "EuropolG"),
    ("europol-gesetz", "EuropolG"),
    ("eurojust-gesetzes", "EurojustG"),
    ("eurojust-gesetz", "EurojustG"),
    ("neue-psychoaktive-stoffe-gesetzes", "NpSG"),
    ("neue-psychoaktive-stoffe-gesetz", "NpSG"),
    ("bauproduktengesetzes", "BauPG"),
    ("bauproduktengesetz", "BauPG"),
    ("sicherheitsüberprüfungsgesetzes", "SÜG"),
    ("sicherheitsueberpruefungsgesetzes", "SÜG"),
    ("onlinezugangsgesetzes", "OZG"),
    ("onlinezugangsgesetz", "OZG"),
)

# Einträge mit Platzhalter "—" ignorieren
SKIP_KUERZEL = frozenset({"", "—", "-", "–"})


@dataclass
class BgblEintrag:
    jahr: int
    nr: int
    titel_roh: str
    veroeffentlicht: date
    referenz: str


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


MONTH_DE = {
    "januar": 1,
    "februar": 2,
    "märz": 3,
    "maerz": 3,
    "april": 4,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "august": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "dezember": 12,
}


def parse_de_datum_am_ende(text: str) -> tuple[str, date] | None:
    """Text nach <br />; Titel ohne abschließendes „vom …“; Datum parsen."""
    t = html_module.unescape(text)
    t = re.sub(r"\s+", " ", t).strip()
    m = re.search(
        r"^(.*)\s+vom\s+(\d{1,2})\.\s*([A-Za-zäöüÄÖÜß]+)\s+(\d{4})\s*$",
        t,
        re.DOTALL,
    )
    if not m:
        return None
    titel, tag, monat_w, jahr_s = m.group(1), m.group(2), m.group(3), m.group(4)
    monat_w = monat_w.lower()
    mi = MONTH_DE.get(monat_w)
    if mi is None:
        return None
    try:
        d = date(int(jahr_s), mi, int(tag))
    except ValueError:
        return None
    titel = titel.strip()
    return titel, d


def strip_html_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s)


def titel_zu_kuerzel(titel: str) -> str:
    s = html_module.unescape(strip_html_tags(titel))
    s = re.sub(r"\s+", " ", s).strip().lower()
    if not s:
        return ""
    for phrase, kuerzel in TITLE_PHRASE_TO_KUERZEL:
        if phrase in s:
            return kuerzel
    return ""


def fetch_aktu_html() -> str:
    req = urllib.request.Request(
        AKTU_URL,
        headers={"User-Agent": UA},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read()
    return raw.decode("latin-1", errors="replace")


def parse_bgbl_eintraege(html: str) -> list[BgblEintrag]:
    """Parst <p>-Blöcke mit BGBl.-Link (Teil I)."""
    text = html_module.unescape(html)
    out: list[BgblEintrag] = []
    block_pat = re.compile(
        r'<p>\s*<a[^>]+>\s*(BGBl\.\s*(\d+)\s+I\s+Nr\.\s*(\d+))\s*</a>\s*<br\s*/>\s*(.*?)\s*</p>',
        re.DOTALL | re.IGNORECASE,
    )
    for m in block_pat.finditer(text):
        jahr_s, nr_s, body = m.group(2), m.group(3), m.group(4)
        jahr, nr = int(jahr_s), int(nr_s)
        body_plain = strip_html_tags(body)
        parsed = parse_de_datum_am_ende(body_plain)
        if not parsed:
            continue
        titel_roh, vdatum = parsed
        ref = f"BGBl. {jahr} I Nr. {nr}"
        out.append(
            BgblEintrag(
                jahr=jahr,
                nr=nr,
                titel_roh=titel_roh,
                veroeffentlicht=vdatum,
                referenz=ref,
            )
        )
    return out


def best_bgbl_fuer_aenderung(
    a_datum: date,
    db_kuerzel: str,
    eintraege: list[BgblEintrag],
) -> BgblEintrag | None:
    """Passender BGBl-Eintrag: gleiches Kürzel, a.datum in [pub±60], minimaler Abstand."""
    ku_db = (db_kuerzel or "").strip()
    if not ku_db:
        return None
    best: BgblEintrag | None = None
    best_dist: int | None = None
    d_lo = timedelta(days=WINDOW_DAYS)
    for e in eintraege:
        k = titel_zu_kuerzel(e.titel_roh)
        if not k or k in SKIP_KUERZEL:
            continue
        if k.lower() != ku_db.lower():
            continue
        if not (e.veroeffentlicht - d_lo <= a_datum <= e.veroeffentlicht + d_lo):
            continue
        dist = abs((a_datum - e.veroeffentlicht).days)
        if best is None or dist < best_dist:
            best = e
            best_dist = dist
    return best


def main() -> int:
    load_env()
    html = fetch_aktu_html()
    eintraege = parse_bgbl_eintraege(html)
    if not eintraege:
        print("Keine BGBl-Einträge geparst.", file=sys.stderr)
        return 1

    conn = connect()
    neu = 0
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT a.id, a.datum, g.kuerzel AS kuerzel
            FROM aenderungen a
            INNER JOIN gesetze g ON g.id = a.gesetz_id
            WHERE a.bgbl_referenz IS NULL OR TRIM(a.bgbl_referenz) = ''
            """
        )
        rows = cur.fetchall()

        for row in rows:
            aid = row["id"]
            ku = row["kuerzel"] or ""
            raw_d = row["datum"]
            if hasattr(raw_d, "year"):
                a_datum = date(raw_d.year, raw_d.month, raw_d.day)
            else:
                try:
                    a_datum = date.fromisoformat(str(raw_d)[:10])
                except ValueError:
                    continue

            hit = best_bgbl_fuer_aenderung(a_datum, ku, eintraege)
            if hit is None:
                continue

            cur.execute(
                """
                UPDATE aenderungen
                SET bgbl_referenz = %s
                WHERE id = %s
                  AND (bgbl_referenz IS NULL OR TRIM(bgbl_referenz) = '')
                """,
                (hit.referenz, aid),
            )
            neu += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    print(f"bgbl_referenz neu eingetragen: {neu}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
