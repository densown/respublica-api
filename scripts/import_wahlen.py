#!/usr/bin/env python3
"""Import GERDA CSVs from /mnt/data/wahlen/ into wahlen (respublica_gesetze).

- federal: Kreisebene aus county_code, keine Aggregation.
- state / municipal / european: nur aggregierte Kreiszeilen (ags = erste 5 Stellen der AGS),
  Gemeinde-Rohdaten werden nicht eingefuegt.
- mayoral: winner_party, winner_voteshare, election_type, round aus CSV.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv("/root/apps/gesetze/.env")

WAHlen_DIR = Path("/mnt/data/wahlen")

PARTY_COLS = [
    "cdu_csu",
    "spd",
    "gruene",
    "fdp",
    "linke_pds",
    "afd",
    "bsw",
    "npd",
    "freie_waehler",
    "piraten",
    "die_partei",
]

ROUND_ALIASES = {
    "hauptwahl": 1,
    "stichwahl": 2,
    "1": 1,
    "2": 2,
    "0": 0,
}

INSERT_SQL = """
INSERT IGNORE INTO wahlen (
  typ, ags, ags_name, election_year, election_date, state, state_name, county,
  eligible_voters, number_voters, valid_votes, invalid_votes, turnout,
  cdu_csu, spd, gruene, fdp, linke_pds, afd, bsw, npd, freie_waehler, piraten, die_partei,
  other, far_right, far_left, winning_party,
  winner_party, winner_voteshare, election_type, round
) VALUES (
  %(typ)s, %(ags)s, %(ags_name)s, %(election_year)s, %(election_date)s, %(state)s, %(state_name)s, %(county)s,
  %(eligible_voters)s, %(number_voters)s, %(valid_votes)s, %(invalid_votes)s, %(turnout)s,
  %(cdu_csu)s, %(spd)s, %(gruene)s, %(fdp)s, %(linke_pds)s, %(afd)s, %(bsw)s, %(npd)s, %(freie_waehler)s, %(piraten)s, %(die_partei)s,
  %(other)s, %(far_right)s, %(far_left)s, %(winning_party)s,
  %(winner_party)s, %(winner_voteshare)s, %(election_type)s, %(round)s
)
"""


def get_db():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD") or "",
        database=os.getenv("DB_NAME"),
    )


def to_float(x):
    if pd.isna(x):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def to_int(x):
    if pd.isna(x):
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def parse_date(val):
    if pd.isna(val) or val is None or val == "":
        return None
    try:
        d = pd.to_datetime(val, errors="coerce")
        if pd.isna(d):
            return None
        return d.date()
    except Exception:
        return None


def parse_round_mayoral(val):
    if pd.isna(val) or val is None or val == "":
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return int(val)
    s = str(val).strip().lower()
    return ROUND_ALIASES.get(s, None)


def norm_ags5(x) -> str:
    s = str(x).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    if len(s) <= 5:
        return s.zfill(5)[:5]
    return s.zfill(8)[:5]


def ensure_party_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "cdu_csu" not in df.columns and "cdu" in df.columns:
        cdu = pd.to_numeric(df["cdu"], errors="coerce").fillna(0)
        csu = pd.to_numeric(df["csu"], errors="coerce").fillna(0) if "csu" in df.columns else 0
        df["cdu_csu"] = cdu + csu
    if "linke_pds" not in df.columns and "die_linke" in df.columns:
        df["linke_pds"] = df["die_linke"]
    if "freie_waehler" not in df.columns and "freie_wahler" in df.columns:
        df["freie_waehler"] = df["freie_wahler"]
    return df


def weighted_avg(g: pd.DataFrame, col: str, wcol: str):
    w = pd.to_numeric(g[wcol], errors="coerce").fillna(0)
    v = pd.to_numeric(g[col], errors="coerce")
    mask = (w > 0) & v.notna()
    if not mask.any():
        return None
    tw = w[mask].sum()
    if tw <= 0:
        return None
    return float((v[mask] * w[mask]).sum() / tw)


def row_winning_party(parties: dict):
    best = None
    best_v = -1.0
    for k, v in parties.items():
        if v is None:
            continue
        if v > best_v:
            best_v = v
            best = k
    return best


def compute_other_and_winner(row: dict) -> None:
    s = 0.0
    parts = {}
    for c in PARTY_COLS:
        v = row.get(c)
        if v is not None:
            s += v
            parts[c] = v
        else:
            parts[c] = None
    if any(parts.get(c) is not None for c in PARTY_COLS):
        row["other"] = max(0.0, 1.0 - s)
    else:
        row["other"] = None
    row["winning_party"] = row_winning_party(parts)


def aggregate_to_kreis(df: pd.DataFrame, typ: str) -> pd.DataFrame:
    df = ensure_party_columns(df)
    if "ags" not in df.columns:
        raise ValueError("aggregation braucht Spalte ags")
    df = df.copy()
    df["ags_kreis"] = df["ags"].map(norm_ags5)
    idx = df.index
    if "valid_votes" in df.columns:
        df["valid_votes"] = pd.to_numeric(df["valid_votes"], errors="coerce").fillna(0)
    else:
        df["valid_votes"] = pd.Series(0, index=idx, dtype=float)
    if "eligible_voters" in df.columns:
        df["eligible_voters"] = pd.to_numeric(df["eligible_voters"], errors="coerce")
    else:
        df["eligible_voters"] = pd.Series(pd.NA, index=idx, dtype="Int64")
    if "number_voters" in df.columns:
        df["number_voters"] = pd.to_numeric(df["number_voters"], errors="coerce")
    else:
        df["number_voters"] = pd.Series(pd.NA, index=idx, dtype="Int64")
    if "invalid_votes" in df.columns:
        df["invalid_votes"] = pd.to_numeric(df["invalid_votes"], errors="coerce").fillna(0)
    else:
        df["invalid_votes"] = pd.Series(0, index=idx, dtype=float)

    if "election_type" in df.columns:
        et = df["election_type"].fillna("").astype(str)
    else:
        et = pd.Series([""] * len(df), index=df.index)

    if "round" in df.columns:
        rd = pd.to_numeric(df["round"], errors="coerce").fillna(0).astype(int)
    else:
        rd = pd.Series([0] * len(df), index=df.index, dtype=int)

    df["_election_type"] = et
    df["_round"] = rd

    gcols = ["ags_kreis", "election_year", "_election_type", "_round"]
    rows_out = []

    for key, g in df.groupby(gcols, sort=False):
        ags_kreis = key[0]
        wv = float(g["valid_votes"].sum())
        ev = g["eligible_voters"].sum()
        nv = g["number_voters"].sum()
        iv = g["invalid_votes"].sum()
        ev_i = int(ev) if pd.notna(ev) else None
        nv_i = int(nv) if pd.notna(nv) else None
        if ev_i and ev_i > 0 and nv_i is not None:
            turnout = float(nv_i / ev_i)
        else:
            turnout = weighted_avg(g, "turnout", "valid_votes")

        row = {
            "typ": typ,
            "ags": str(ags_kreis).zfill(5)[:5],
            "election_year": int(g["election_year"].iloc[0]),
            "election_type": str(g["_election_type"].iloc[0]) or "",
            "round": int(g["_round"].iloc[0]),
            "eligible_voters": ev_i,
            "number_voters": nv_i,
            "valid_votes": int(wv) if wv else None,
            "invalid_votes": int(iv) if iv else None,
            "turnout": turnout,
            "state": None,
            "state_name": None,
            "county": None,
            "election_date": None,
            "ags_name": None,
            "far_right": weighted_avg(g, "far_right", "valid_votes") if "far_right" in g.columns else None,
            "far_left": weighted_avg(g, "far_left", "valid_votes") if "far_left" in g.columns else None,
            "winner_party": None,
            "winner_voteshare": None,
        }

        if "state" in g.columns and pd.notna(g["state"].iloc[0]):
            row["state"] = str(g["state"].iloc[0]).zfill(2)[:2]
        if "state_name" in g.columns and g["state_name"].notna().any():
            row["state_name"] = g["state_name"].dropna().iloc[0]
        if "county" in g.columns:
            c0 = g["county"].dropna()
            if len(c0):
                row["county"] = str(c0.iloc[0])

        if "election_date" in g.columns:
            edates = g["election_date"].dropna()
            if len(edates):
                row["election_date"] = parse_date(edates.iloc[0])

        if "ags_name" in g.columns:
            idx_max = g["valid_votes"].idxmax()
            row["ags_name"] = g.loc[idx_max, "ags_name"]

        for c in PARTY_COLS:
            row[c] = weighted_avg(g, c, "valid_votes") if c in g.columns else None

        compute_other_and_winner(row)
        rows_out.append(row)

    return pd.DataFrame(rows_out)


def federal_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = ensure_party_columns(df)
    if "county_code" not in df.columns:
        raise ValueError("%s: erwartet Spalte county_code" % path)
    out = []
    for _, r in df.iterrows():
        cc = r.get("county_code")
        if pd.isna(cc):
            continue
        ags = str(int(float(cc))).zfill(5)[:5]
        row = {
            "typ": "federal",
            "ags": ags,
            "ags_name": None,
            "election_year": to_int(r.get("election_year")),
            "election_date": parse_date(r.get("election_date")),
            "state": str(r.get("state")).zfill(2)[:2] if pd.notna(r.get("state")) else None,
            "state_name": None,
            "county": ags,
            "eligible_voters": to_int(r.get("eligible_voters")),
            "number_voters": to_int(r.get("number_voters")),
            "valid_votes": to_int(r.get("valid_votes")),
            "invalid_votes": to_int(r.get("invalid_votes")),
            "turnout": to_float(r.get("turnout")),
            "election_type": "",
            "round": 0,
            "far_right": to_float(r.get("far_right")),
            "far_left": to_float(r.get("far_left")),
            "winner_party": None,
            "winner_voteshare": None,
        }
        for c in PARTY_COLS:
            row[c] = to_float(r.get(c))
        compute_other_and_winner(row)
        out.append(row)
    return pd.DataFrame(out)


def mayoral_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    out = []
    for _, r in df.iterrows():
        ags_raw = r.get("ags")
        if pd.isna(ags_raw):
            continue
        try:
            ags = str(int(float(ags_raw)))
        except (TypeError, ValueError):
            ags = str(ags_raw).strip()
        pr = parse_round_mayoral(r.get("round"))
        if pr is None:
            pr = 0
        row = {
            "typ": "mayoral",
            "ags": ags,
            "ags_name": r.get("ags_name") if pd.notna(r.get("ags_name")) else None,
            "election_year": to_int(r.get("election_year")),
            "election_date": parse_date(r.get("election_date")),
            "state": str(r.get("state")).zfill(2)[:2] if pd.notna(r.get("state")) else None,
            "state_name": r.get("state_name") if pd.notna(r.get("state_name")) else None,
            "county": None,
            "eligible_voters": to_int(r.get("eligible_voters")),
            "number_voters": to_int(r.get("number_voters")),
            "valid_votes": to_int(r.get("valid_votes")),
            "invalid_votes": to_int(r.get("invalid_votes")),
            "turnout": to_float(r.get("turnout")),
            "election_type": (str(r.get("election_type") or ""))[:50],
            "round": pr,
            "winner_party": (str(r.get("winner_party")))[:50] if pd.notna(r.get("winner_party")) else None,
            "winner_voteshare": to_float(r.get("winner_voteshare")),
            "winning_party": (str(r.get("winner_party")))[:50] if pd.notna(r.get("winner_party")) else None,
            "other": None,
            "far_right": None,
            "far_left": None,
        }
        for c in PARTY_COLS:
            row[c] = None
        out.append(row)
    return pd.DataFrame(out)


def df_to_rows(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        d = {}
        for k in list(r.index):
            v = r[k]
            if k == "election_date" and hasattr(v, "isoformat"):
                d[k] = v
            elif pd.isna(v):
                d[k] = None
            else:
                d[k] = v
        if d.get("round") is None:
            d["round"] = 0
        if d.get("election_type") is None:
            d["election_type"] = ""
        rows.append(d)
    return rows


def insert_rows(conn, rows):
    cur = conn.cursor()
    n = 0
    for d in rows:
        try:
            cur.execute(INSERT_SQL, d)
            n += cur.rowcount
        except Exception as e:
            print("INSERT error:", e, d.get("typ"), d.get("ags"), d.get("election_year"), file=sys.stderr)
    conn.commit()
    cur.close()
    return n


def classify_csv(path: Path):
    n = path.name.lower()
    if n.startswith("mayoral"):
        return "mayoral", False
    if n.startswith("european") or "european_muni" in n:
        return "european", True
    if n.startswith("municipal"):
        return "municipal", True
    if n.startswith("state"):
        return "state", True
    if "federal" in n and n.endswith(".csv"):
        return "federal", False
    return None, None


def federal_sort_key(p: Path):
    n = p.name.lower()
    # harm zuerst: bei gleichem UNIQUE gewinnt die erste Zeile
    pref = 0 if "harm" in n else 1
    return (pref, p.name)


def main():
    if not WAHlen_DIR.is_dir():
        print("Fehlt: %s" % WAHlen_DIR, file=sys.stderr)
        sys.exit(1)

    all_csv = sorted(WAHlen_DIR.glob("*.csv"))
    federal_files = sorted(
        [p for p in all_csv if classify_csv(p)[0] == "federal"],
        key=federal_sort_key,
    )

    conn = get_db()
    total_inserts = 0
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'wahlen'"
    )
    if cur.fetchone()[0] == 0:
        cur.close()
        conn.close()
        print("Abbruch: Tabelle wahlen fehlt. Bitte zuerst migrations/003_wahlen.sql ausführen.", file=sys.stderr)
        sys.exit(1)
    cur.close()

    try:
        for p in federal_files:
            header = pd.read_csv(p, nrows=0, low_memory=False)
            if "county_code" not in header.columns:
                print("skip federal", p.name, "(kein county_code)")
                continue
            print("federal", p.name)
            df = federal_frame(p)
            total_inserts += insert_rows(conn, df_to_rows(df))

        for p in all_csv:
            typ, _agg = classify_csv(p)
            if typ is None or typ == "federal":
                continue
            print(typ, p.name)
            if typ == "mayoral":
                df = mayoral_frame(p)
            else:
                raw = pd.read_csv(p, low_memory=False)
                df = aggregate_to_kreis(raw, typ)
            total_inserts += insert_rows(conn, df_to_rows(df))
    finally:
        conn.close()

    print("Rowcount sum from inserts:", total_inserts)


if __name__ == "__main__":
    main()
