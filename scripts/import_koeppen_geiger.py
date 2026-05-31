#!/usr/bin/env python3
"""Import dominante Köppen-Geiger-Klimaklasse pro Land in respublica_gesetze (Hybrid).

Quelle: Beck et al. 2023, "High-resolution (1 km) Köppen-Geiger maps for
1901–2099 based on constrained CMIP6 projections", *Scientific Data* 10:724.
Daten: https://www.gloh2o.org/koppen/  (Figshare 10.6084/m9.figshare.21789074)
Archiv-Layout:
    1991_2020/koppen_geiger_<res>.tif                 (Gegenwart, kein SSP)
    2041_2070/<ssp>/koppen_geiger_<res>.tif           (Mid-Century)
    2071_2099/<ssp>/koppen_geiger_<res>.tif           (Ende des Jahrhunderts)
mit <res> ∈ {0p00833333, 0p1, 0p5, 1p0} und
     <ssp> ∈ {ssp126, ssp245, ssp370, ssp585}  (Tier-1, wie in Migration
     008 registriert; ssp119/434/460 sind im Beck-Archiv vorhanden, aber
     nicht vom Hybrid-Schema referenziert).

Pixel-Kodierung: uint8 1..30 nach Peel-et-al-2007-Reihenfolge
(Beck-Archiv `legend.txt`; siehe KOEPPEN_CLASSES unten). NoData = 0.

Schreibt ins Hybrid-Schema von Migration 008_climate_koeppen_geiger.sql:

  * data_values — pro Land eine Zeile mit der dominanten Klasse (1..30)
    als DECIMAL gegen den passenden Indikator je nach --scenario:
        historical → climate.koppen.dominant_historical (year=2020)
        ssp126     → climate.koppen.dominant_ssp126     (year=2099 bzw. 2070)
        ssp245     → climate.koppen.dominant_ssp245     (   -''-          )
        ssp370     → climate.koppen.dominant_ssp370     (   -''-          )
        ssp585     → climate.koppen.dominant_ssp585     (   -''-          )
    quality='measured' für historical, 'projected' für SSP-Läufe. note
    enthält Symbol + Anteil-Prozent für UI-Tooltips.

  * climate_koeppen_distribution — volle Verteilung im Long-Format
    (eine Zeile pro tatsächlich vorkommender Klasse). Pro
    (country, scenario, period) erst DELETE der alten Zeilen, dann
    INSERT der neuen — damit Klassen, die in einem früheren Lauf
    vorkamen aber jetzt nicht mehr, nicht als Geisterzeilen
    zurückbleiben.

  * data_update_log — ein Eintrag pro Lauf (status='started' →
    'success' bzw. 'failed'), Context-JSON mit Periode, Szenario,
    Raster-Metadaten und Top-Verteilung der dominanten Klassen.

Speicher-Disziplin (Server hat ~2 GB frei):

  * Das globale Raster wird NIE komplett gelesen. `rasterio.open()`
    öffnet nur die Header; jede Pixel-Decodierung passiert pro
    Polygon-Fenster.
  * Pro Land werden die einzelnen Polygone der (Multi-)Geometrie
    nacheinander abgearbeitet. Für jedes Polygon: bounds → Window →
    `read(1, window=…)` (rasterio decodiert nur diesen Ausschnitt),
    `geometry_mask` über das gleiche Fenster, Pixel-Histogramm
    aktualisieren, Arrays sofort verwerfen, `gc.collect()`, nächstes
    Polygon.
  * Spitzenverbrauch ist damit durch das größte Einzelpolygon-Fenster
    beschränkt (uint8 + bool-Maske). Für eine 1-km-Quelle bleibt das
    selbst bei Russland deutlich unter 500 MB RSS.
  * Multi-Polygone werden polygon-weise verarbeitet — das entschärft
    das Antimeridian-Problem (Alaska / Tschukotka kreuzen die Datums-
    grenze als separate Polygone) automatisch.

Ein Lauf importiert genau EINE (period, scenario)-Kombination. Default
ist Dry-Run (keine DB-Verbindung). DB-Schreiben nur mit --commit.

Aufruf:

  # Gegenwart 1991-2020 (Dry-Run, kein --scenario nötig):
  scripts/venv-koeppen/bin/python scripts/import_koeppen_geiger.py \\
      --raster    data/climate/koppen/1991_2020/koppen_geiger_0p00833333.tif \\
      --period    1991_2020 \\
      --countries data/geo/ne_10m_admin_0_countries.geojson

  # Zukunft 2071-2099 unter SSP5-8.5, mit DB-Schreiben:
  scripts/venv-koeppen/bin/python scripts/import_koeppen_geiger.py \\
      --raster    data/climate/koppen/2071_2099/ssp585/koppen_geiger_0p00833333.tif \\
      --period    2071_2099 \\
      --scenario  ssp585 \\
      --countries data/geo/ne_10m_admin_0_countries.geojson \\
      --commit
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import Window, from_bounds
import geopandas as gpd
import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

# Zulässige Period-Scenario-Kombinationen laut Beck-2023-Archivlayout.
HISTORICAL_PERIODS = {"1901_1930", "1931_1960", "1961_1990", "1991_2020"}
FUTURE_PERIODS     = {"2041_2070", "2071_2099"}

# Tier-1-SSPs, wie in Migration 008 als data_indicators registriert.
# Wenn ssp119/434/460 dazukommen sollen: neue Indikator-Zeilen in der DB
# anlegen und hier (sowie in INDICATOR_CODE_BY_SCENARIO) ergänzen.
SSP_SCENARIOS = {"ssp126", "ssp245", "ssp370", "ssp585"}

INDICATOR_CODE_BY_SCENARIO: dict[str, str] = {
    "historical": "climate.koppen.dominant_historical",
    "ssp126":     "climate.koppen.dominant_ssp126",
    "ssp245":     "climate.koppen.dominant_ssp245",
    "ssp370":     "climate.koppen.dominant_ssp370",
    "ssp585":     "climate.koppen.dominant_ssp585",
}

# data_values.year = Endjahr der Klimatologie-Periode
# (Konvention bei Beck et al. und im IPCC AR6 Atlas).
PERIOD_END_YEAR: dict[str, int] = {
    "1901_1930": 1930,
    "1931_1960": 1960,
    "1961_1990": 1990,
    "1991_2020": 2020,
    "2041_2070": 2070,
    "2071_2099": 2099,
}

# Beck et al. 2018/2023 – Klassen 1..30 (Peel-et-al-2007-Reihenfolge).
KOEPPEN_CLASSES: dict[int, tuple[str, str]] = {
    1:  ("Af",  "Tropisch, Regenwald"),
    2:  ("Am",  "Tropisch, Monsun"),
    3:  ("Aw",  "Tropisch, Savanne"),
    4:  ("BWh", "Arid, Wüste, heiß"),
    5:  ("BWk", "Arid, Wüste, kalt"),
    6:  ("BSh", "Arid, Steppe, heiß"),
    7:  ("BSk", "Arid, Steppe, kalt"),
    8:  ("Csa", "Gemäßigt, trockener heißer Sommer"),
    9:  ("Csb", "Gemäßigt, trockener warmer Sommer"),
    10: ("Csc", "Gemäßigt, trockener kühler Sommer"),
    11: ("Cwa", "Gemäßigt, trockener Winter, heißer Sommer"),
    12: ("Cwb", "Gemäßigt, trockener Winter, warmer Sommer"),
    13: ("Cwc", "Gemäßigt, trockener Winter, kühler Sommer"),
    14: ("Cfa", "Gemäßigt, feucht, heißer Sommer"),
    15: ("Cfb", "Gemäßigt, feucht, warmer Sommer"),
    16: ("Cfc", "Gemäßigt, feucht, kühler Sommer"),
    17: ("Dsa", "Kontinental, trockener heißer Sommer"),
    18: ("Dsb", "Kontinental, trockener warmer Sommer"),
    19: ("Dsc", "Kontinental, trockener kühler Sommer"),
    20: ("Dsd", "Kontinental, trockener Sommer, sehr kalter Winter"),
    21: ("Dwa", "Kontinental, trockener Winter, heißer Sommer"),
    22: ("Dwb", "Kontinental, trockener Winter, warmer Sommer"),
    23: ("Dwc", "Kontinental, trockener Winter, kühler Sommer"),
    24: ("Dwd", "Kontinental, trockener Winter, sehr kalter Winter"),
    25: ("Dfa", "Kontinental, feucht, heißer Sommer"),
    26: ("Dfb", "Kontinental, feucht, warmer Sommer"),
    27: ("Dfc", "Kontinental, feucht, kühler Sommer"),
    28: ("Dfd", "Kontinental, feucht, sehr kalter Winter"),
    29: ("ET",  "Polar, Tundra"),
    30: ("EF",  "Polar, Eis"),
}

# ---------------------------------------------------------------------------
# SQL — Hybrid-Schema, Migration 008
# ---------------------------------------------------------------------------
UPSERT_DATA_VALUE_SQL = """
INSERT INTO data_values
  (indicator_id, country_code, year, value, quality, note)
VALUES
  (%(indicator_id)s, %(country_code)s, %(year)s, %(value)s,
   %(quality)s, %(note)s)
ON DUPLICATE KEY UPDATE
  value      = VALUES(value),
  quality    = VALUES(quality),
  note       = VALUES(note),
  fetched_at = CURRENT_TIMESTAMP
"""

DELETE_DISTRIBUTION_SQL = """
DELETE FROM climate_koeppen_distribution
WHERE country_code = %s AND scenario = %s AND period = %s
"""

INSERT_DISTRIBUTION_SQL = """
INSERT INTO climate_koeppen_distribution
  (country_code, scenario, period, class_code, share, pixel_count, raster_file)
VALUES
  (%(country_code)s, %(scenario)s, %(period)s, %(class_code)s,
   %(share)s, %(pixel_count)s, %(raster_file)s)
"""

LOG_INSERT_SQL = """
INSERT INTO data_update_log
  (source_id, indicator_id, status, records_added, records_updated,
   records_skipped, started_at)
VALUES
  (%s, %s, 'started', 0, 0, 0, NOW())
"""

LOG_UPDATE_SQL = """
UPDATE data_update_log
SET status           = %s,
    records_added    = %s,
    records_updated  = %s,
    records_skipped  = %s,
    duration_seconds = %s,
    error_message    = %s,
    context          = %s,
    finished_at      = NOW()
WHERE id = %s
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import Köppen-Geiger climate per country "
                    "(hybrid schema, memory-bounded).",
    )
    p.add_argument("--raster", required=True,
                   help="Globaler Köppen-Geiger-GeoTIFF (uint8, Klassen 1..30, NoData=0).")
    p.add_argument("--countries", required=True,
                   help="Landespolygone (GeoPackage / GeoJSON / Shapefile).")
    p.add_argument("--period", required=True,
                   choices=sorted(HISTORICAL_PERIODS | FUTURE_PERIODS),
                   help="Zeitscheibe des Rasters, z.B. '1991_2020' oder '2071_2099'.")
    p.add_argument("--scenario", default="historical",
                   choices=["historical", *sorted(SSP_SCENARIOS)],
                   help="'historical' für 1901..2020-Perioden, sonst eines der "
                        "vier Tier-1-SSPs (Default 'historical').")
    p.add_argument("--iso3-field", default="ADM0_A3",
                   help="Attribut mit ISO-3166-1-alpha-3 (Default ADM0_A3 = Natural Earth).")
    p.add_argument("--name-field", default="NAME",
                   help="Attribut mit Klartext-Ländernamen (Default NAME).")
    p.add_argument("--raster-source",
                   default="Beck et al. 2023 (10.1038/s41597-023-02549-6)",
                   help="Frei-Text-Tag fürs data_update_log-Context-JSON.")
    p.add_argument("--only", default=None,
                   help="Komma-Liste ISO3 — nur diese Länder verarbeiten.")
    p.add_argument("--commit", action="store_true",
                   help="ERST mit diesem Flag wird in die DB geschrieben.")
    args = p.parse_args()

    if args.scenario == "historical" and args.period not in HISTORICAL_PERIODS:
        p.error(f"--scenario historical erlaubt nur Perioden "
                f"{sorted(HISTORICAL_PERIODS)} (du: {args.period}).")
    if args.scenario != "historical" and args.period not in FUTURE_PERIODS:
        p.error(f"--scenario {args.scenario} ist nur für Zukunfts-Perioden "
                f"{sorted(FUTURE_PERIODS)} definiert (du: {args.period}).")
    return args


def iter_single_polygons(geom):
    """Liefere die einzelnen Polygon-Teile einer (Multi)Polygon-Geometrie."""
    if geom is None or geom.is_empty:
        return
    gtype = geom.geom_type
    if gtype == "Polygon":
        yield geom
    elif gtype == "MultiPolygon":
        for part in geom.geoms:
            if not part.is_empty:
                yield part
    elif gtype == "GeometryCollection":
        for part in geom.geoms:
            yield from iter_single_polygons(part)


def clip_window(win: Window, raster_w: int, raster_h: int) -> Window | None:
    """Beschneide ein Window auf die Raster-Extents (oder None, falls leer)."""
    col_off = max(0, int(win.col_off))
    row_off = max(0, int(win.row_off))
    col_end = min(raster_w, int(win.col_off) + int(win.width))
    row_end = min(raster_h, int(win.row_off) + int(win.height))
    width = col_end - col_off
    height = row_end - row_off
    if width <= 0 or height <= 0:
        return None
    return Window(col_off, row_off, width, height)


def accumulate_country(rds, polygons) -> Counter:
    """Histogramm der Köppen-Klassen über alle Polygone eines Landes.

    Liest pro Polygon NUR das passende Fenster aus dem Raster und verwirft die
    Arrays sofort wieder. Spitzen­verbrauch ≈ größtes Einzelpolygon-Fenster.
    """
    counter: Counter = Counter()
    for poly in polygons:
        minx, miny, maxx, maxy = poly.bounds
        if maxx <= minx or maxy <= miny:
            continue

        try:
            win = from_bounds(minx, miny, maxx, maxy, transform=rds.transform)
        except Exception as exc:  # rasterio kann bei entarteten Bounds werfen
            print(f"  ! Window-Berechnung fehlgeschlagen ({exc}); Polygon übersprungen",
                  file=sys.stderr)
            continue
        # rasterio 1.5.0 hat round_offsets()/round_lengths() entkernt: kwds
        # werden ignoriert, round_lengths() rundet jetzt zur nächsten ganzen
        # Zahl statt aufzurunden — Fenster würden bei Bruchteilen < 0.5 zu
        # klein und der rechte/untere Polygon-Rand fehlt. Daher explizit
        # outward-rounden (floor für Offsets, ceil für die rechte/untere
        # Kante), damit das Fenster die Polygon-Bounds garantiert abdeckt.
        col_off = int(np.floor(win.col_off))
        row_off = int(np.floor(win.row_off))
        width   = int(np.ceil(win.col_off + win.width))  - col_off
        height  = int(np.ceil(win.row_off + win.height)) - row_off
        win = Window(col_off, row_off, width, height)
        win = clip_window(win, rds.width, rds.height)
        if win is None:
            continue

        win_transform = rds.window_transform(win)

        # *Windowed read* — rasterio dekomprimiert nur diesen Block aus dem GeoTIFF.
        data = rds.read(1, window=win)

        mask = geometry_mask(
            [poly.__geo_interface__],
            out_shape=(int(win.height), int(win.width)),
            transform=win_transform,
            invert=True,            # True = innerhalb des Polygons
            all_touched=False,
        )

        if mask.any():
            values = data[mask]
            valid = values[(values >= 1) & (values <= 30)]
            if valid.size:
                classes, counts = np.unique(valid, return_counts=True)
                for cls, cnt in zip(classes.tolist(), counts.tolist()):
                    counter[int(cls)] += int(cnt)
            del values, valid

        # Speicher unmittelbar freigeben — wichtig auf einem 2-GB-Host.
        del data, mask
        gc.collect()

    return counter


def summarize(counter: Counter) -> dict:
    total = sum(counter.values())
    if total == 0:
        return {
            "dominant_class": None,
            "dominant_class_code": None,
            "dominant_share": None,
            "class_distribution": {},
            "pixel_count": 0,
        }
    dom_code, dom_count = counter.most_common(1)[0]
    distribution = {
        KOEPPEN_CLASSES[code][0]: round(100.0 * cnt / total, 4)
        for code, cnt in counter.most_common()
    }
    return {
        "dominant_class": KOEPPEN_CLASSES[dom_code][0],
        "dominant_class_code": dom_code,
        "dominant_share": round(100.0 * dom_count / total, 2),
        "class_distribution": distribution,
        "pixel_count": total,
    }


def get_db():
    load_dotenv(ROOT / ".env")
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD") or "",
        database=os.getenv("DB_NAME"),
    )


# ---------------------------------------------------------------------------
# data_update_log + Indicator-Lookup — Muster aus import_baci.py übernommen
# ---------------------------------------------------------------------------
def lookup_indicator(cur, code: str) -> tuple[int, int]:
    """Hole (indicator_id, source_id) für den Code aus data_indicators."""
    cur.execute(
        "SELECT id, source_id FROM data_indicators WHERE code = %s",
        (code,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"Indikator '{code}' nicht in data_indicators registriert. "
            "Migration 008_climate_koeppen_geiger.sql appliziert?"
        )
    return int(row[0]), int(row[1])


def create_log_entry(cur, source_id: int, indicator_id: int) -> int:
    """Lege einen 'started'-Eintrag in data_update_log an. Gibt die id zurück."""
    cur.execute(LOG_INSERT_SQL, (source_id, indicator_id))
    return int(cur.lastrowid)


def finish_log_entry(cur, log_id: int, status: str, duration_s: int,
                     counts: dict, error_message: str | None,
                     context: dict) -> None:
    """Schließe den vorhandenen Log-Eintrag mit Endstatus + Counters ab."""
    cur.execute(LOG_UPDATE_SQL, (
        status,
        int(counts.get("added", 0)),
        int(counts.get("updated", 0)),
        int(counts.get("skipped", 0)),
        int(duration_s),
        error_message,
        json.dumps(context, ensure_ascii=True, default=str),
        log_id,
    ))


def main() -> int:
    args = parse_args()
    indicator_code = INDICATOR_CODE_BY_SCENARIO[args.scenario]
    year           = PERIOD_END_YEAR[args.period]
    quality        = "measured" if args.scenario == "historical" else "projected"

    raster_path = Path(args.raster)
    countries_path = Path(args.countries)
    if not raster_path.exists():
        print(f"Raster nicht gefunden: {raster_path}", file=sys.stderr)
        return 1
    if not countries_path.exists():
        print(f"Countries-Datei nicht gefunden: {countries_path}", file=sys.stderr)
        return 1

    only_set: set[str] | None = (
        {c.strip().upper() for c in args.only.split(",") if c.strip()}
        if args.only else None
    )

    print(f"Lade Landespolygone aus {countries_path} …")
    countries = gpd.read_file(countries_path)
    for fld in (args.iso3_field, args.name_field):
        if fld not in countries.columns:
            print(f"Feld '{fld}' fehlt. Verfügbar: {list(countries.columns)}",
                  file=sys.stderr)
            return 1

    conn = cur = None
    indicator_id = source_id = log_id = None
    if args.commit:
        conn = get_db()
        cur = conn.cursor()
        indicator_id, source_id = lookup_indicator(cur, indicator_code)
        log_id = create_log_entry(cur, source_id, indicator_id)
        conn.commit()
        print(f"data_update_log id={log_id}  source_id={source_id}  "
              f"indicator_id={indicator_id}  indicator={indicator_code}")
    else:
        print("Dry-run: keine DB-Verbindung (--commit nicht gesetzt). "
              f"Würde gegen Indikator '{indicator_code}' "
              f"(year={year}, quality={quality}) schreiben.")

    print(f"Period={args.period} (year={year})  Scenario={args.scenario}")

    counts = {"added": 0, "updated": 0, "skipped": 0}
    top_dominants: Counter = Counter()
    context: dict = {
        "period":         args.period,
        "scenario":       args.scenario,
        "year":           year,
        "quality":        quality,
        "indicator_code": indicator_code,
        "raster_path":    str(raster_path),
        "raster_file":    raster_path.name,
        "raster_source":  args.raster_source,
        "countries_path": str(countries_path),
        "only":           sorted(only_set) if only_set else None,
        "mode":           "write" if args.commit else "dry_run",
    }

    t0 = time.time()
    duration = 0
    total = 0

    try:
        with rasterio.open(raster_path) as rds:
            print(f"Raster CRS={rds.crs}  size={rds.width}x{rds.height}  "
                  f"dtype={rds.dtypes[0]}  nodata={rds.nodata}")

            if countries.crs is None:
                print("Warnung: Country-Layer ohne CRS — nehme EPSG:4326 an.",
                      file=sys.stderr)
                countries = countries.set_crs(4326)
            if rds.crs is not None and countries.crs != rds.crs:
                print(f"Reprojiziere Länder von {countries.crs} → {rds.crs}")
                countries = countries.to_crs(rds.crs)

            px = abs(rds.transform.a)
            if rds.crs and rds.crs.is_geographic:
                res_tag = f"{px:.6g} deg/pixel"
            else:
                unit = rds.crs.linear_units if rds.crs else "unit"
                res_tag = f"{px:.6g} {unit}/pixel"

            context["raster_crs"]        = str(rds.crs)
            context["raster_size"]       = [int(rds.width), int(rds.height)]
            context["raster_dtype"]      = str(rds.dtypes[0])
            context["raster_nodata"]     = rds.nodata
            context["raster_resolution"] = res_tag

            total = len(countries)
            for i, row in enumerate(countries.itertuples(index=False), start=1):
                iso3 = str(getattr(row, args.iso3_field) or "").strip().upper()
                name = str(getattr(row, args.name_field) or "").strip() or iso3
                geom = getattr(row, "geometry", None)

                # Natural Earth markiert umstrittene Gebiete mit '-99'.
                if not iso3 or iso3 == "-99" or len(iso3) != 3 or not iso3.isalpha():
                    counts["skipped"] += 1
                    continue
                if only_set is not None and iso3 not in only_set:
                    continue
                if geom is None or geom.is_empty:
                    counts["skipped"] += 1
                    continue

                polys = list(iter_single_polygons(geom))
                if not polys:
                    counts["skipped"] += 1
                    continue

                counter = accumulate_country(rds, polys)
                summary = summarize(counter)

                if summary["pixel_count"] == 0:
                    print(f"[{i}/{total}] {iso3} {name}: keine Köppen-Pixel "
                          "(z.B. Mikrostaat unterhalb der Pixelauflösung)")
                    counts["skipped"] += 1
                    del counter, summary
                    gc.collect()
                    continue

                top_dominants[summary["dominant_class"]] += 1

                print(f"[{i}/{total}] {iso3} {name}: "
                      f"dom={summary['dominant_class']} "
                      f"({summary['dominant_share']}%)  px={summary['pixel_count']:,}")

                if args.commit:
                    # (1) data_values — dominante Klasse als Integer-Code
                    cur.execute(UPSERT_DATA_VALUE_SQL, {
                        "indicator_id": indicator_id,
                        "country_code": iso3,
                        "year":         year,
                        "value":        summary["dominant_class_code"],
                        "quality":      quality,
                        "note":         f"{summary['dominant_class']} "
                                        f"({summary['dominant_share']:.2f}%)",
                    })
                    # mysql.connector: rowcount=1 → insert, 2 → update mit
                    # Änderung, 0 → bereits identisch (nicht gezählt).
                    if cur.rowcount == 1:
                        counts["added"] += 1
                    elif cur.rowcount >= 2:
                        counts["updated"] += 1

                    # (2) climate_koeppen_distribution — volle Verteilung
                    # via DELETE+INSERT, damit keine Geisterklassen aus
                    # einem früheren Lauf zurückbleiben.
                    cur.execute(DELETE_DISTRIBUTION_SQL,
                                (iso3, args.scenario, args.period))
                    total_px = summary["pixel_count"]
                    dist_rows = [
                        {
                            "country_code": iso3,
                            "scenario":     args.scenario,
                            "period":       args.period,
                            "class_code":   int(cls_code),
                            "share":        round(100.0 * px / total_px, 4),
                            "pixel_count":  int(px),
                            "raster_file":  raster_path.name,
                        }
                        for cls_code, px in counter.most_common()
                        if 1 <= cls_code <= 30
                    ]
                    if dist_rows:
                        cur.executemany(INSERT_DISTRIBUTION_SQL, dist_rows)

                    if i % 25 == 0:
                        conn.commit()

                del counter, summary
                gc.collect()

        # finaler commit + log-Abschluss
        duration = int(time.time() - t0)
        context["countries_total"] = total
        context["top_dominants"]   = top_dominants.most_common(10)

        if args.commit:
            conn.commit()
            finish_log_entry(cur, log_id, "success", duration,
                             counts, None, context)
            conn.commit()

    except Exception as exc:
        duration = int(time.time() - t0)
        context["error"]         = str(exc)
        context["top_dominants"] = top_dominants.most_common(10)
        if args.commit and log_id is not None and cur is not None and conn is not None:
            try:
                finish_log_entry(cur, log_id, "failed", duration,
                                 counts, str(exc), context)
                conn.commit()
            except Exception as log_exc:
                print(f"Konnte Log-Eintrag nicht abschließen: {log_exc}",
                      file=sys.stderr)
        raise

    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    print(f"Fertig in {duration}s — counts={counts} commit={args.commit}")
    if not args.commit and top_dominants:
        print("Top-dominante Klassen (Dry-Run, nicht geschrieben):",
              ", ".join(f"{sym}={n}"
                        for sym, n in top_dominants.most_common(10)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
