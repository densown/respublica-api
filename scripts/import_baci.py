#!/usr/bin/env python3
"""
Import CEPII BACI HS17 trade flows into trade_flows_v2.

Usage:
  python3 import_baci.py --year 2024 --dry-run
  python3 import_baci.py --year 2024 --truncate-first
  python3 import_baci.py --all --truncate-first
"""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

import duckdb
import mysql.connector


BACI_DIR = Path("/root/data/baci")
COUNTRY_CODES_CSV = BACI_DIR / "country_codes_V202601.csv"
YEAR_FILE_PATTERN = "BACI_HS17_Y{year}_V202601.csv"
VALID_YEARS = list(range(2017, 2025))
SOURCE_SLUG = "cepii_baci_hs17"

DB_CONFIG = {
    "unix_socket": "/var/run/mysqld/mysqld.sock",
    "user": "root",
    "password": "",
    "database": "respublica_gesetze",
    "use_pure": True,
    "allow_local_infile": True,
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import BACI HS17 bilateral trade data into trade_flows_v2."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Import a single year (2017-2024).")
    group.add_argument("--all", action="store_true", help="Import all years (2017-2024).")
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="Truncate trade_flows_v2 before importing (only for non-dry runs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run full aggregation and validation without writing to MySQL.",
    )
    return parser.parse_args()


def get_years(args):
    if args.all:
        return VALID_YEARS
    if args.year not in VALID_YEARS:
        raise ValueError("Year must be between 2017 and 2024.")
    return [args.year]


def ensure_input_files(years):
    if not COUNTRY_CODES_CSV.exists():
        raise FileNotFoundError(f"Missing country code file: {COUNTRY_CODES_CSV}")
    missing = []
    for year in years:
        path = BACI_DIR / YEAR_FILE_PATTERN.format(year=year)
        if not path.exists():
            missing.append(str(path))
    if missing:
        raise FileNotFoundError(f"Missing BACI files:\n- " + "\n- ".join(missing))


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def fetch_source_id(cur):
    cur.execute("SELECT id FROM data_sources WHERE slug = %s LIMIT 1", (SOURCE_SLUG,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"Required data source '{SOURCE_SLUG}' not found. Run DB prep SQL first."
        )
    return int(row[0])


def fetch_valid_iso3(cur):
    cur.execute(
        """
        SELECT UPPER(TRIM(iso3))
        FROM data_countries
        WHERE iso3 IS NOT NULL AND TRIM(iso3) <> ''
        """
    )
    return sorted({row[0] for row in cur.fetchall() if row[0]})


def create_log_entry(cur, source_id):
    cur.execute(
        """
        INSERT INTO data_update_log
          (source_id, status, records_added, records_updated, records_skipped, started_at)
        VALUES (%s, 'started', 0, 0, 0, NOW())
        """,
        (source_id,),
    )
    return int(cur.lastrowid)


def finish_log_entry(cur, log_id, status, duration_seconds, counts, error_message, context):
    cur.execute(
        """
        UPDATE data_update_log
        SET status = %s,
            records_added = %s,
            records_updated = %s,
            records_skipped = %s,
            duration_seconds = %s,
            error_message = %s,
            context = %s,
            finished_at = NOW()
        WHERE id = %s
        """,
        (
            status,
            int(counts.get("added", 0)),
            int(counts.get("updated", 0)),
            int(counts.get("skipped", 0)),
            int(duration_seconds),
            error_message,
            json.dumps(context, ensure_ascii=True),
            log_id,
        ),
    )


def hs_case_sql(alias):
    return f"""
    CASE
      WHEN {alias}.hs4 BETWEEN 101 AND 511 THEN 'I'
      WHEN {alias}.hs4 BETWEEN 601 AND 1404 THEN 'II'
      WHEN {alias}.hs4 BETWEEN 1501 AND 1522 THEN 'III'
      WHEN {alias}.hs4 BETWEEN 1601 AND 2403 THEN 'IV'
      WHEN {alias}.hs4 BETWEEN 2501 AND 2716 THEN 'V'
      WHEN {alias}.hs4 BETWEEN 2801 AND 3826 THEN 'VI'
      WHEN {alias}.hs4 BETWEEN 3901 AND 4017 THEN 'VII'
      WHEN {alias}.hs4 BETWEEN 4101 AND 4304 THEN 'VIII'
      WHEN {alias}.hs4 BETWEEN 4401 AND 4602 THEN 'IX'
      WHEN {alias}.hs4 BETWEEN 4701 AND 4911 THEN 'X'
      WHEN {alias}.hs4 BETWEEN 5001 AND 6310 THEN 'XI'
      WHEN {alias}.hs4 BETWEEN 6401 AND 6704 THEN 'XII'
      WHEN {alias}.hs4 BETWEEN 6801 AND 7020 THEN 'XIII'
      WHEN {alias}.hs4 BETWEEN 7101 AND 7118 THEN 'XIV'
      WHEN {alias}.hs4 BETWEEN 7201 AND 8311 THEN 'XV'
      WHEN {alias}.hs4 BETWEEN 8401 AND 8548 THEN 'XVI'
      WHEN {alias}.hs4 BETWEEN 8601 AND 8908 THEN 'XVII'
      WHEN {alias}.hs4 BETWEEN 9001 AND 9033 THEN 'XVIII'
      WHEN {alias}.hs4 BETWEEN 9301 AND 9307 THEN 'XIX'
      WHEN {alias}.hs4 BETWEEN 9401 AND 9619 THEN 'XX'
      WHEN {alias}.hs4 BETWEEN 9701 AND 9706 THEN 'XXI'
      ELSE 'OTHER'
    END
    """


def setup_duckdb(con, baci_csv_path, valid_iso3):
    con.execute("PRAGMA threads=8;")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE baci_raw AS
        SELECT
          CAST(t AS INTEGER) AS year,
          CAST(i AS INTEGER) AS exporter_code,
          CAST(j AS INTEGER) AS importer_code,
          LPAD(CAST(k AS VARCHAR), 6, '0') AS hs6,
          CAST(SUBSTR(LPAD(CAST(k AS VARCHAR), 6, '0'), 1, 4) AS INTEGER) AS hs4,
          CAST(v AS DOUBLE) * 1000.0 AS value_usd
        FROM read_csv_auto('{baci_csv_path}', header = true)
        WHERE v IS NOT NULL AND CAST(v AS DOUBLE) > 0
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE country_map AS
        SELECT
          CAST(country_code AS INTEGER) AS country_code,
          UPPER(TRIM(country_iso3)) AS iso3
        FROM read_csv_auto('{COUNTRY_CODES_CSV}', header = true)
        WHERE country_code IS NOT NULL
          AND country_iso3 IS NOT NULL
          AND LENGTH(TRIM(country_iso3)) = 3
        """
    )

    con.execute("CREATE OR REPLACE TEMP TABLE valid_iso3 (iso3 VARCHAR)")
    con.executemany("INSERT INTO valid_iso3 VALUES (?)", [(x,) for x in valid_iso3])

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE mapped_base AS
        SELECT
          b.year,
          exp.iso3 AS exporter_iso3,
          imp.iso3 AS importer_iso3,
          b.hs6,
          {hs_case_sql('b')} AS hs_section,
          b.value_usd
        FROM baci_raw b
        JOIN country_map exp ON b.exporter_code = exp.country_code
        JOIN country_map imp ON b.importer_code = imp.country_code
        JOIN valid_iso3 vex ON exp.iso3 = vex.iso3
        JOIN valid_iso3 vim ON imp.iso3 = vim.iso3
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE directional AS
        SELECT
          exporter_iso3 AS reporter_iso3,
          importer_iso3 AS partner_iso3,
          'export' AS flow,
          year,
          hs6,
          hs_section,
          value_usd
        FROM mapped_base
        UNION ALL
        SELECT
          importer_iso3 AS reporter_iso3,
          exporter_iso3 AS partner_iso3,
          'import' AS flow,
          year,
          hs6,
          hs_section,
          value_usd
        FROM mapped_base
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_sections AS
        SELECT
          reporter_iso3,
          partner_iso3,
          flow,
          year,
          hs_section,
          CAST(ROUND(SUM(value_usd), 0) AS BIGINT) AS value_usd
        FROM directional
        GROUP BY reporter_iso3, partner_iso3, flow, year, hs_section
        HAVING SUM(value_usd) > 0
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_total AS
        SELECT
          reporter_iso3,
          partner_iso3,
          flow,
          year,
          'TOTAL' AS hs_section,
          CAST(ROUND(SUM(value_usd), 0) AS BIGINT) AS value_usd
        FROM directional
        GROUP BY reporter_iso3, partner_iso3, flow, year
        HAVING SUM(value_usd) > 0
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_all AS
        SELECT * FROM agg_sections
        UNION ALL
        SELECT * FROM agg_total
        """
    )


def collect_context(con):
    stats = con.execute(
        """
        WITH
          raw AS (
            SELECT COUNT(*) AS rows_read, COALESCE(SUM(value_usd), 0) AS raw_value_usd
            FROM baci_raw
          ),
          mapped AS (
            SELECT COUNT(*) AS rows_mapped, COALESCE(SUM(value_usd), 0) AS mapped_value_usd
            FROM mapped_base
          ),
          other AS (
            SELECT COALESCE(SUM(value_usd), 0) AS other_value_usd
            FROM mapped_base
            WHERE hs_section = 'OTHER'
          ),
          aggregated AS (
            SELECT COUNT(*) AS rows_aggregated
            FROM agg_all
          )
        SELECT
          raw.rows_read,
          raw.raw_value_usd,
          mapped.rows_mapped,
          mapped.mapped_value_usd,
          other.other_value_usd,
          aggregated.rows_aggregated
        FROM raw, mapped, other, aggregated
        """
    ).fetchone()

    rows_read = int(stats[0] or 0)
    raw_value_usd = int(round(stats[1] or 0))
    rows_mapped = int(stats[2] or 0)
    mapped_value_usd = int(round(stats[3] or 0))
    other_value_usd = int(round(stats[4] or 0))
    rows_aggregated = int(stats[5] or 0)
    other_share = (other_value_usd / mapped_value_usd) if mapped_value_usd else 0.0

    unmapped_top10 = con.execute(
        """
        SELECT hs6, CAST(ROUND(SUM(value_usd), 0) AS BIGINT) AS value_usd
        FROM mapped_base
        WHERE hs_section = 'OTHER'
        GROUP BY hs6
        ORDER BY value_usd DESC
        LIMIT 10
        """
    ).fetchall()

    country_unmapped = con.execute(
        """
        WITH joined AS (
          SELECT
            b.exporter_code,
            b.importer_code,
            b.value_usd,
            exp.iso3 AS exp_iso3,
            imp.iso3 AS imp_iso3,
            vex.iso3 AS exp_valid,
            vim.iso3 AS imp_valid
          FROM baci_raw b
          LEFT JOIN country_map exp ON b.exporter_code = exp.country_code
          LEFT JOIN country_map imp ON b.importer_code = imp.country_code
          LEFT JOIN valid_iso3 vex ON exp.iso3 = vex.iso3
          LEFT JOIN valid_iso3 vim ON imp.iso3 = vim.iso3
        )
        SELECT side, country_code, CAST(ROUND(SUM(value_usd), 0) AS BIGINT) AS value_usd
        FROM (
          SELECT 'exporter' AS side, exporter_code AS country_code, value_usd
          FROM joined
          WHERE exp_iso3 IS NULL OR exp_valid IS NULL
          UNION ALL
          SELECT 'importer' AS side, importer_code AS country_code, value_usd
          FROM joined
          WHERE imp_iso3 IS NULL OR imp_valid IS NULL
        ) x
        GROUP BY side, country_code
        ORDER BY value_usd DESC
        LIMIT 10
        """
    ).fetchall()

    return {
        "rows_read": rows_read,
        "rows_mapped": rows_mapped,
        "rows_aggregated": rows_aggregated,
        "raw_value_usd": raw_value_usd,
        "mapped_value_usd": mapped_value_usd,
        "other_value_usd": other_value_usd,
        "other_share_global": round(other_share, 6),
        "unmapped_top10": [
            {"hs6": str(row[0]), "value_usd": int(row[1] or 0)} for row in unmapped_top10
        ],
        "unmapped_country_top10": [
            {"side": str(row[0]), "country_code": int(row[1]), "value_usd": int(row[2] or 0)}
            for row in country_unmapped
        ],
    }


def export_aggregates_to_csv(con, year):
    temp_dir = Path(tempfile.mkdtemp(prefix=f"baci_{year}_"))
    out_csv = temp_dir / f"trade_flows_v2_agg_{year}.csv"
    con.execute(
        f"""
        COPY (
          SELECT reporter_iso3, partner_iso3, flow, year, value_usd, hs_section
          FROM agg_all
          ORDER BY reporter_iso3, partner_iso3, flow, year, hs_section
        ) TO '{out_csv}' WITH (HEADER, DELIMITER ',')
        """
    )
    return out_csv


def merge_into_mysql(conn, csv_path, source_id):
    cur = conn.cursor()
    cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_trade_flows_v2_import")
    cur.execute(
        """
        CREATE TEMPORARY TABLE tmp_trade_flows_v2_import (
          reporter_iso3 VARCHAR(3) NOT NULL,
          partner_iso3 VARCHAR(3) NOT NULL,
          flow ENUM('export','import') NOT NULL,
          year INT NOT NULL,
          value_usd BIGINT NOT NULL,
          hs_section VARCHAR(20) NOT NULL,
          source_id INT NOT NULL
        )
        """
    )

    load_sql = (
        "LOAD DATA LOCAL INFILE %s "
        "INTO TABLE tmp_trade_flows_v2_import "
        "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES "
        "(reporter_iso3, partner_iso3, flow, year, value_usd, hs_section) "
        "SET source_id = %s"
    )
    cur.execute(load_sql, (str(csv_path), int(source_id)))

    cur.execute("SELECT COUNT(*) FROM tmp_trade_flows_v2_import")
    staging_rows = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM tmp_trade_flows_v2_import t
        LEFT JOIN trade_flows_v2 d
          ON d.reporter_iso3 = t.reporter_iso3
         AND d.partner_iso3 = t.partner_iso3
         AND d.flow = t.flow
         AND d.year = t.year
         AND d.hs_section = t.hs_section
        WHERE d.id IS NULL
        """
    )
    added = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM tmp_trade_flows_v2_import t
        JOIN trade_flows_v2 d
          ON d.reporter_iso3 = t.reporter_iso3
         AND d.partner_iso3 = t.partner_iso3
         AND d.flow = t.flow
         AND d.year = t.year
         AND d.hs_section = t.hs_section
        WHERE d.value_usd <> t.value_usd OR d.source_id <> t.source_id
        """
    )
    updated = int(cur.fetchone()[0])

    cur.execute(
        """
        INSERT INTO trade_flows_v2
          (reporter_iso3, partner_iso3, flow, year, value_usd, hs_section, source_id)
        SELECT
          reporter_iso3, partner_iso3, flow, year, value_usd, hs_section, source_id
        FROM tmp_trade_flows_v2_import
        ON DUPLICATE KEY UPDATE
          value_usd = VALUES(value_usd),
          source_id = VALUES(source_id),
          fetched_at = CURRENT_TIMESTAMP
        """
    )

    conn.commit()
    cur.close()
    return {"staging_rows": staging_rows, "added": added, "updated": updated, "skipped": 0}


def run_year(conn, year, source_id, dry_run):
    year_start = time.time()
    baci_csv = BACI_DIR / YEAR_FILE_PATTERN.format(year=year)
    print(f"\n=== Year {year} ===")
    print(f"Input: {baci_csv}")

    db_cur = conn.cursor()
    valid_iso3 = fetch_valid_iso3(db_cur)
    if not valid_iso3:
        raise RuntimeError("No valid ISO3 codes found in data_countries.")

    log_id = None
    if not dry_run:
        log_id = create_log_entry(db_cur, source_id)
        conn.commit()
        print(f"Log entry started: id={log_id}")

    context = {"year": year}
    counts = {"added": 0, "updated": 0, "skipped": 0}
    try:
        duck_start = time.time()
        ddb = duckdb.connect(database=":memory:")
        setup_duckdb(ddb, baci_csv, valid_iso3)
        context.update(collect_context(ddb))
        context["duckdb_seconds"] = round(time.time() - duck_start, 2)

        agg_csv = export_aggregates_to_csv(ddb, year)
        context["agg_csv_path"] = str(agg_csv)
        ddb.close()

        print(
            "Rows read/mapped/aggregated:",
            context["rows_read"],
            context["rows_mapped"],
            context["rows_aggregated"],
        )
        print("OTHER share:", f"{context['other_share_global'] * 100:.3f}%")

        if dry_run:
            context["mode"] = "dry_run"
            duration = int(time.time() - year_start)
            print(f"Dry run completed in {duration}s.")
            return {"duration": duration, "counts": counts, "context": context}

        counts = merge_into_mysql(conn, agg_csv, source_id)
        duration = int(time.time() - year_start)
        context["mode"] = "write"
        context["load"] = {
            "staging_rows": counts["staging_rows"],
            "records_added": counts["added"],
            "records_updated": counts["updated"],
            "records_skipped": counts["skipped"],
        }

        finish_log_entry(
            db_cur,
            log_id=log_id,
            status="success",
            duration_seconds=duration,
            counts=counts,
            error_message=None,
            context=context,
        )
        conn.commit()
        print(
            f"Import completed in {duration}s "
            f"(added={counts['added']}, updated={counts['updated']})."
        )
        return {"duration": duration, "counts": counts, "context": context}
    except Exception as exc:
        duration = int(time.time() - year_start)
        context["mode"] = "dry_run" if dry_run else "write"
        context["error"] = str(exc)
        if (not dry_run) and log_id is not None:
            finish_log_entry(
                db_cur,
                log_id=log_id,
                status="failed",
                duration_seconds=duration,
                counts=counts,
                error_message=str(exc),
                context=context,
            )
            conn.commit()
        raise
    finally:
        db_cur.close()


def main():
    args = parse_args()
    years = get_years(args)
    ensure_input_files(years)

    conn = get_db_connection()
    cur = conn.cursor()
    source_id = fetch_source_id(cur)
    cur.close()
    print(f"Using source_id={source_id} ({SOURCE_SLUG})")

    if args.truncate_first and not args.dry_run:
        print("Truncating trade_flows_v2 before import...")
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE trade_flows_v2")
        conn.commit()
        cur.close()
        print("trade_flows_v2 truncated.")
    elif args.truncate_first and args.dry_run:
        print("--truncate-first ignored in --dry-run mode.")

    total_start = time.time()
    try:
        for year in years:
            run_year(conn, year=year, source_id=source_id, dry_run=args.dry_run)
    finally:
        conn.close()

    print(f"\nDone. Total runtime: {int(time.time() - total_start)}s")


if __name__ == "__main__":
    main()
