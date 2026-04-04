#!/usr/bin/env python3
"""Import World Bank WDI-style indicators into respublica_gesetze.

- Country metadata: /v2/country?format=json&per_page=500
- Per indicator: /v2/country/all/indicator/{CODE}?format=json&per_page=10000&date=2000:2024
- 2 s pause between HTTP requests; INSERT IGNORE for duplicates.

Run: cd /root/apps/gesetze && .venv/bin/python scripts/import_world_indicators.py

Cron example (weekly):
  0 4 * * 0 cd /root/apps/gesetze && .venv/bin/python scripts/import_world_indicators.py >> logs/cron.log 2>&1
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

WB_BASE = "https://api.worldbank.org/v2"
DATE_RANGE = "2000:2024"
REQUEST_PAUSE_SEC = 2.0

INDICATOR_CODES = [
    "NY.GDP.PCAP.CD",
    "NY.GDP.MKTP.KD.ZG",
    "NY.GDP.MKTP.CD",
    "FP.CPI.TOTL.ZG",
    "SL.UEM.TOTL.ZS",
    "NE.EXP.GNFS.ZS",
    "BN.CAB.XOKA.GD.ZS",
    "GC.DOD.TOTL.GD.ZS",
    "GC.REV.XGRT.GD.ZS",
    "SP.POP.TOTL",
    "SP.DYN.LE00.IN",
    "SP.DYN.CBRT.IN",
    "SP.DYN.CDRT.IN",
    "SP.URB.TOTL.IN.ZS",
    "SP.DYN.TFRT.IN",
    "SP.POP.65UP.TO.ZS",
    "SM.POP.NETM",
    "SP.POP.DPND",
    "SE.ADT.LITR.ZS",
    "SE.XPD.TOTL.GD.ZS",
    "SE.PRM.ENRR",
    "SE.SEC.ENRR",
    "SH.XPD.CHEX.GD.ZS",
    "SH.DYN.MORT",
    "SH.MED.PHYS.ZS",
    "SH.STA.MMRT",
    "SH.H2O.BASW.ZS",
    "EN.ATM.CO2E.PC",
    "EG.FEC.RNEW.ZS",
    "AG.LND.FRST.ZS",
    "EG.USE.PCAP.KG.OE",
    "EG.ELC.ACCS.ZS",
    "CC.EST",
    "RL.EST",
    "GE.EST",
    "PV.EST",
    "VA.EST",
    "RQ.EST",
    "MS.MIL.XPND.GD.ZS",
    "MS.MIL.TOTL.P1",
    "SI.POV.GINI",
    "SI.POV.DDAY",
    "SG.GEN.PARL.ZS",
    "IT.NET.USER.ZS",
    "IT.CEL.SETS.P2",
    "GB.XPD.RSDV.GD.ZS",
    "IP.PAT.RESD",
    "BX.KLT.DINV.WD.GD.ZS",
    "DT.ODA.ODAT.GN.ZS",
    "TG.VAL.TOTL.GD.ZS",
    "VC.IHR.PSRC.P5",
]

INSERT_SQL = """
INSERT IGNORE INTO world_indicators (
  country_code, country_name, region, income_level,
  indicator_code, indicator_name, year, value
) VALUES (
  %(country_code)s, %(country_name)s, %(region)s, %(income_level)s,
  %(indicator_code)s, %(indicator_name)s, %(year)s, %(value)s
)
"""


def get_db():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD") or "",
        database=os.getenv("DB_NAME"),
    )


def http_get_json(url: str):
    time.sleep(REQUEST_PAUSE_SEC)
    req = urllib.request.Request(url, headers={"User-Agent": "ResPublicaWorldImport/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wb_fetch_paged(url_without_page: str) -> list:
    """World Bank returns [metadata_dict, list_of_rows]. Follow pages."""
    out: list = []
    page = 1
    while True:
        sep = "&" if "?" in url_without_page else "?"
        url = f"{url_without_page}{sep}page={page}"
        try:
            chunk = http_get_json(url)
        except urllib.error.HTTPError as e:
            print(f"HTTP error {e.code} for {url}", file=sys.stderr)
            raise
        if not isinstance(chunk, list) or len(chunk) < 2:
            break
        meta, rows = chunk[0], chunk[1]
        if not rows:
            break
        out.extend(rows)
        total_pages = int(meta.get("pages", 1) or 1)
        if page >= total_pages:
            break
        page += 1
    return out


def load_country_meta() -> dict[str, dict]:
    """id -> {name, region, income_level} for real countries (ISO3 id)."""
    url = f"{WB_BASE}/country?format=json&per_page=500"
    rows = wb_fetch_paged(url)
    by_id: dict[str, dict] = {}
    for row in rows:
        cid = (row.get("id") or "").strip().upper()
        if len(cid) != 3 or not cid.isalpha():
            continue
        region_obj = row.get("region") or {}
        income_obj = row.get("incomeLevel") or {}
        by_id[cid] = {
            "name": row.get("name") or cid,
            "region": region_obj.get("value") if isinstance(region_obj, dict) else None,
            "income_level": income_obj.get("value") if isinstance(income_obj, dict) else None,
        }
    return by_id


def load_indicator_names_from_db(cur) -> dict[str, str]:
    cur.execute("SELECT indicator_code, indicator_name FROM world_indicator_meta")
    return {r[0]: r[1] for r in cur.fetchall()}


def import_indicator(
    code: str,
    country_by_id: dict[str, dict],
    indicator_names: dict[str, str],
    cur,
    batch: list,
) -> int:
    name = indicator_names.get(code, code)
    url = (
        f"{WB_BASE}/country/all/indicator/{code}"
        f"?format=json&per_page=10000&date={DATE_RANGE}"
    )
    rows = wb_fetch_paged(url)
    n = 0
    for row in rows:
        iso = (row.get("countryiso3code") or "").strip().upper()
        if len(iso) != 3 or iso not in country_by_id:
            continue
        meta = country_by_id[iso]
        date_s = row.get("date")
        if not date_s:
            continue
        try:
            year = int(str(date_s)[:4])
        except ValueError:
            continue
        val = row.get("value")
        if val is None:
            continue
        try:
            fval = float(val)
        except (TypeError, ValueError):
            continue
        batch.append(
            {
                "country_code": iso,
                "country_name": meta["name"],
                "region": meta["region"],
                "income_level": meta["income_level"],
                "indicator_code": code,
                "indicator_name": name,
                "year": year,
                "value": fval,
            }
        )
        n += 1
        if len(batch) >= 2000:
            cur.executemany(INSERT_SQL, batch)
            batch.clear()
    return n


def main() -> int:
    print("Loading country list from World Bank…")
    country_by_id = load_country_meta()
    print(f"  {len(country_by_id)} countries/territories with ISO3 id")

    conn = get_db()
    cur = conn.cursor()
    indicator_names = load_indicator_names_from_db(cur)
    missing = [c for c in INDICATOR_CODES if c not in indicator_names]
    if missing:
        print("Missing indicator meta in DB (run migration 004):", ", ".join(missing), file=sys.stderr)
        return 1

    batch: list = []
    total_rows = 0
    for i, code in enumerate(INDICATOR_CODES, start=1):
        print(f"[{i}/{len(INDICATOR_CODES)}] {code} …")
        added = import_indicator(code, country_by_id, indicator_names, cur, batch)
        total_rows += added
        print(f"  queued rows (this indicator): {added}")
        conn.commit()

    if batch:
        cur.executemany(INSERT_SQL, batch)
        batch.clear()
        conn.commit()

    cur.close()
    conn.close()
    print(f"Done. Total data points queued (non-null): {total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
