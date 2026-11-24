#!/usr/bin/env python3
"""Antworten der Bundesregierung auf Kleine Anfragen aus dem DIP -> kleine_anfragen.

Quelle: DIP-API des Bundestags (search.dip.bundestag.de/api/v1).
  drucksache-text  Antwort-Drucksachen mit Volltext (Volltext etwa 5 Tage nach
                   der Drucksache, vorher leer)
  vorgang          Fraktion (initiative), Sachgebiete, Deskriptoren, Zahl der
                   Einzelfragen; bis zu 20 Vorgaenge je Abruf ueber f.id

Antworten auf Grosse Anfragen und andere Vorgaenge werden uebersprungen.
Ohne --seit holt der Lauf alles, was das DIP seit dem letzten bekannten
Stand (MAX(dip_aktualisiert) minus 2 Tage) geaendert hat. So kommen neue
Antworten sofort und ihr Volltext beim naechsten Lauf nach dem DIP-Update.

Schluessel: DIP_API_KEY in .env. Der oeffentliche Schluessel steht auf
https://dip.bundestag.de/über-dip/hilfe/api und gilt laut Seite bis Ende
Mai 2027 (am 21.11.2026: R2BZaee...). Ein eigener Schluessel (10 Jahre)
gibt es per Mail an parlamentsdokumentation@bundestag.de.

  python scripts/fetch_kleine_anfragen.py                         # inkrementell
  python scripts/fetch_kleine_anfragen.py --seit 2025-03-25       # Backfill WP 21
  python scripts/fetch_kleine_anfragen.py --seit 2026-11-01 --probelauf
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import kleine_anfragen as ka  # noqa: E402
from lib.db import get_db  # noqa: E402
from lib.env import load_env  # noqa: E402
from lib.log import acquire_lock, get_logger, install_signal_handlers, release_lock  # noqa: E402

LOCK_NAME = "fetch_kleine_anfragen"
DIP = "https://search.dip.bundestag.de/api/v1"
UA = "respublica-kleine-anfragen/1.0 (+https://respublica.media)"
PAUSE_SEC = 0.5  # zwischen Abrufen, das DIP droht bei Missbrauch mit Sperre
MAX_SEITEN = 400  # 20 Dokumente je Seite, reicht fuer eine ganze Wahlperiode
VORLAUF_TAGE = 2

log = get_logger(LOCK_NAME)
_running = True


def _stop() -> None:
    global _running
    _running = False


def dip_get(pfad: str, params: list[tuple[str, str]], apikey: str) -> dict[str, Any]:
    """GET mit Wiederholung bei 429/5xx. Schluessel als Header, nicht in der URL (Logs)."""
    for versuch in range(5):
        r = requests.get(
            f"{DIP}/{pfad}",
            params=params,
            headers={"User-Agent": UA, "Accept": "application/json", "Authorization": f"ApiKey {apikey}"},
            timeout=90,
        )
        if r.status_code == 401:
            raise SystemExit("DIP 401: DIP_API_KEY ungueltig oder abgelaufen (siehe Kopf dieses Skripts)")
        if r.status_code in (429, 500, 502, 503, 504):
            warte = 10 * 2**versuch
            log.warning("DIP %s bei %s, warte %ss", r.status_code, pfad, warte)
            time.sleep(warte)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"DIP {pfad}: zu viele Fehlversuche")


def abruf_params(seit: date, bis: date | None, nach_aktualisierung: bool) -> list[tuple[str, str]]:
    """Filter fuer drucksache-text. f.datum nimmt ein Datum, f.aktualisiert will laut
    DIP-API einen Zeitstempel (YYYY-MM-DDTHH:mm:ss); mit blossem Datum antwortet das
    DIP mit 400, der erste Cron-Lauf am 24.11.2026 importierte deshalb nichts."""
    if nach_aktualisierung:
        params = [("f.drucksachetyp", "Antwort"), ("f.aktualisiert.start", f"{seit.isoformat()}T00:00:00")]
        if bis:
            params.append(("f.aktualisiert.end", f"{bis.isoformat()}T23:59:59"))
        return params
    params = [("f.drucksachetyp", "Antwort"), ("f.datum.start", seit.isoformat())]
    if bis:
        params.append(("f.datum.end", bis.isoformat()))
    return params


def antworten(seit: date, bis: date | None, nach_aktualisierung: bool, apikey: str) -> Iterator[dict]:
    """Alle Antwort-Drucksachen (mit Volltext, falls vorhanden), seitenweise per Cursor."""
    params = abruf_params(seit, bis, nach_aktualisierung)
    cursor = None
    for _ in range(MAX_SEITEN):
        if not _running:
            return
        p = params + ([("cursor", cursor)] if cursor else [])
        daten = dip_get("drucksache-text", p, apikey)
        docs = daten.get("documents") or []
        yield from docs
        neu = daten.get("cursor")
        if not docs or not neu or neu == cursor:
            return
        cursor = neu
        time.sleep(PAUSE_SEC)


def vorgaenge(ids: list[str], apikey: str) -> dict[str, dict]:
    if not ids:
        return {}
    daten = dip_get("vorgang", [("f.id", i) for i in ids], apikey)
    time.sleep(PAUSE_SEC)
    return {str(v["id"]): v for v in daten.get("documents") or []}


def kleine_anfrage_bezug(doc: dict) -> dict | None:
    for b in doc.get("vorgangsbezug") or []:
        if b.get("vorgangstyp") == "Kleine Anfrage":
            return b
    return None


def zeile(doc: dict, vorgang: dict | None) -> dict:
    """DIP-Dokument + Vorgang -> Spalten von kleine_anfragen."""
    text = doc.get("text") or None
    ressort = next((r.get("titel") for r in doc.get("ressort") or [] if r.get("federfuehrend")), None)
    if not ressort and doc.get("ressort"):
        ressort = doc["ressort"][0].get("titel")
    v = vorgang or {}
    initiative = v.get("initiative") or []
    deskr = [d.get("name") for d in v.get("deskriptor") or [] if d.get("name")]
    m = ka.merkmale(text)
    return {
        "id": int(doc["id"]),
        "dokumentnummer": doc["dokumentnummer"],
        "wahlperiode": int(doc.get("wahlperiode") or 0),
        "datum": doc["datum"],
        "titel": (ka.titel_bereinigen(doc.get("titel")) or v.get("titel") or doc["dokumentnummer"])[:700],
        "vorgang_id": int(v["id"]) if v.get("id") else None,
        "frage_dokumentnummer": ka.frage_drucksache(doc.get("titel")),
        "fraktion": ka.fraktion_kuerzel(initiative),
        "initiative": "; ".join(initiative)[:255] or None,
        "fragesteller": json.dumps(ka.fragesteller(text), ensure_ascii=False) if text else None,
        "ressort": (ressort or "")[:255] or None,
        "sachgebiete": json.dumps(v.get("sachgebiet") or [], ensure_ascii=False),
        "deskriptoren": json.dumps(deskr, ensure_ascii=False),
        "einzelfragen": ka.einzelfragen(v.get("abstract")),
        "pdf_url": ((doc.get("fundstelle") or {}).get("pdf_url") or "")[:255] or None,
        "volltext": text,
        "text_zeichen": len(text) if text else None,
        "text_hash": ka.text_hash(text) if text else None,
        "nicht_erhoben": m["nicht_erhoben"],
        "hat_anlage": m["hat_anlage"],
        "eingestuft": m["eingestuft"],
        "dip_aktualisiert": (doc.get("aktualisiert") or "")[:19].replace("T", " ") or None,
    }


SPALTEN = [
    "id", "dokumentnummer", "wahlperiode", "datum", "titel", "vorgang_id", "frage_dokumentnummer",
    "fraktion", "initiative", "fragesteller", "ressort", "sachgebiete", "deskriptoren", "einzelfragen",
    "pdf_url", "volltext", "text_zeichen", "text_hash", "nicht_erhoben", "hat_anlage", "eingestuft",
    "dip_aktualisiert",
]
# Volltext und daraus abgeleitete Felder nie mit NULL ueberschreiben (DIP liefert
# den Text bei manchen Aktualisierungen kurz leer).
NUR_WENN_GESETZT = {"volltext", "text_zeichen", "text_hash", "fragesteller"}


def upsert_sql() -> str:
    updates = []
    for s in SPALTEN:
        if s == "id":
            continue
        if s in NUR_WENN_GESETZT:
            updates.append(f"{s} = COALESCE(VALUES({s}), {s})")
        elif s in {"nicht_erhoben", "hat_anlage", "eingestuft"}:
            updates.append(f"{s} = IF(VALUES(volltext) IS NULL, {s}, VALUES({s}))")
        else:
            updates.append(f"{s} = VALUES({s})")
    platzhalter = ", ".join(["%s"] * len(SPALTEN))
    return (
        f"INSERT INTO kleine_anfragen ({', '.join(SPALTEN)}) VALUES ({platzhalter}) "
        f"ON DUPLICATE KEY UPDATE {', '.join(updates)}"
    )


def letzter_stand(cur) -> date | None:
    cur.execute("SELECT MAX(dip_aktualisiert) FROM kleine_anfragen")
    (wert,) = cur.fetchone()
    if not wert:
        return None
    return (wert if isinstance(wert, datetime) else datetime.fromisoformat(str(wert))).date() - timedelta(days=VORLAUF_TAGE)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--seit", type=date.fromisoformat, help="Drucksachen ab diesem Datum (Backfill)")
    ap.add_argument("--bis", type=date.fromisoformat, help="bis zu diesem Datum (nur mit --seit)")
    ap.add_argument("--probelauf", action="store_true", help="nichts schreiben, nur zaehlen")
    args = ap.parse_args()

    load_env()
    apikey = os.environ.get("DIP_API_KEY", "").strip()
    if not apikey:
        log.error("DIP_API_KEY fehlt in .env")
        return 1
    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = get_db(autocommit=False)
    cur = conn.cursor()
    try:
        if args.seit:
            seit, nach_akt = args.seit, False
        else:
            seit = letzter_stand(cur) or (date.today() - timedelta(days=30))
            nach_akt = True
        log.info("Abruf ab %s (%s)", seit, "aktualisiert" if nach_akt else "Datum")

        sql = upsert_sql()
        stapel: list[dict] = []
        gesamt = uebersprungen = mit_text = 0

        def schreiben() -> None:
            nonlocal gesamt, mit_text
            v = vorgaenge([kleine_anfrage_bezug(d)["id"] for d in stapel], apikey)
            for d in stapel:
                z = zeile(d, v.get(str(kleine_anfrage_bezug(d)["id"])))
                gesamt += 1
                mit_text += 1 if z["volltext"] else 0
                if not args.probelauf:
                    cur.execute(sql, [z[s] for s in SPALTEN])
            if not args.probelauf:
                conn.commit()
            stapel.clear()

        for doc in antworten(seit, args.bis if args.seit else None, nach_akt, apikey):
            if not kleine_anfrage_bezug(doc):
                uebersprungen += 1
                continue
            stapel.append(doc)
            if len(stapel) >= 20:
                schreiben()
        if stapel and _running:
            schreiben()
        log.info(
            "Fertig: %d Antworten auf Kleine Anfragen (%d mit Volltext), %d andere uebersprungen%s",
            gesamt, mit_text, uebersprungen, " [Probelauf]" if args.probelauf else "",
        )
        return 0
    except Exception:
        conn.rollback()
        log.exception("Abbruch")
        return 1
    finally:
        cur.close()
        conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
