#!/usr/bin/env python3
"""
Phase D2.2 – Migration der bestehenden Daten ins neue Schema

Migriert:
1. world_indicator_meta → data_indicators (mit korrekter source_id Zuordnung)
2. world_indicators → data_values (mit indicator_id lookup)
3. trade_flows → trade_flows_v2 (mit ISO3 cleanup wo möglich)

Idempotent – kann mehrfach laufen ohne Schaden.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.db import get_db
from lib.env import load_env

# Indikator-Code → Quelle (slug)
def detect_source(code):
    """Heuristik: V-Dem Codes beginnen mit v2, alles andere ist Weltbank WDI."""
    if code.startswith("v2"):
        return "vdem"
    return "worldbank_wdi"


# Manuell gepflegte deutsche Übersetzungen für die wichtigsten Indikatoren
# Format: code -> (name_de, name_en, unit_de, unit_en, lower_is_better, value_type, decimals, priority)
KNOWN_INDICATORS = {
    # Wirtschaft (priority 90+)
    "NY.GDP.MKTP.CD":     ("Bruttoinlandsprodukt",         "GDP (current US$)",                  "$",        "$",        False, "currency", 0, 95),
    "NY.GDP.PCAP.CD":     ("BIP pro Kopf",                 "GDP per capita (current US$)",       "$",        "$",        False, "currency", 0, 98),
    "NY.GDP.MKTP.KD.ZG":  ("BIP-Wachstum",                 "GDP growth (annual %)",              "%",        "%",        False, "percent",  1, 90),
    "FP.CPI.TOTL.ZG":     ("Inflation",                    "Inflation, consumer prices",         "%",        "%",        True,  "percent",  1, 92),
    "SL.UEM.TOTL.ZS":     ("Arbeitslosenquote",            "Unemployment rate",                  "%",        "%",        True,  "percent",  1, 91),
    "GC.DOD.TOTL.GD.ZS":  ("Staatsverschuldung",           "Government debt (% of GDP)",         "% des BIP","% of GDP", True,  "percent",  1, 88),
    "BN.CAB.XOKA.GD.ZS":  ("Leistungsbilanzsaldo",         "Current account balance (% GDP)",    "% des BIP","% of GDP", False, "percent",  1, 80),

    # Bevölkerung (priority 70+)
    "SP.POP.TOTL":        ("Bevölkerung",                  "Population, total",                  "Personen", "people",   False, "count",    0, 95),
    "SP.POP.GROW":        ("Bevölkerungswachstum",         "Population growth (annual %)",       "%",        "%",        False, "percent",  2, 75),
    "SP.URB.TOTL.IN.ZS":  ("Anteil Stadtbevölkerung",      "Urban population (% of total)",      "%",        "%",        False, "percent",  1, 70),
    "SP.DYN.LE00.IN":     ("Lebenserwartung bei Geburt",   "Life expectancy at birth",           "Jahre",    "years",    False, "absolute", 1, 90),
    "SP.DYN.TFRT.IN":     ("Geburtenrate",                 "Fertility rate",                     "Kinder/Frau","children/woman", False, "absolute", 2, 75),
    "SM.POP.NETM":        ("Nettomigration",               "Net migration",                      "Personen", "people",   False, "count",    0, 70),

    # Bildung
    "SE.ADT.LITR.ZS":     ("Alphabetisierungsrate",        "Literacy rate, adult total",         "%",        "%",        False, "percent",  1, 80),
    "SE.XPD.TOTL.GD.ZS":  ("Bildungsausgaben",             "Government education expenditure",   "% des BIP","% of GDP", False, "percent",  1, 75),
    "SE.SEC.ENRR":        ("Schulbesuchsquote Sekundar",   "Secondary school enrollment",        "%",        "%",        False, "percent",  1, 65),
    "SE.TER.ENRR":        ("Hochschulquote",               "Tertiary school enrollment",         "%",        "%",        False, "percent",  1, 65),

    # Gesundheit
    "SH.XPD.CHEX.GD.ZS":  ("Gesundheitsausgaben",          "Current health expenditure",         "% des BIP","% of GDP", False, "percent",  1, 80),
    "SH.DYN.MORT":        ("Kindersterblichkeit unter 5",  "Mortality rate, under-5",            "je 1.000", "per 1,000",True,  "rate",     1, 75),
    "SH.STA.MMRT":        ("Müttersterblichkeit",          "Maternal mortality ratio",           "je 100.000","per 100k", True,  "rate",     0, 70),
    "SH.MED.PHYS.ZS":     ("Ärzte je 1.000",               "Physicians per 1,000",               "Ärzte/1.000","per 1k",  False, "ratio",    2, 65),

    # Umwelt
    "EN.GHG.ALL.PC.CE.AR5": ("Treibhausgas-Emissionen",    "GHG emissions per capita",           "t CO₂eq/Kopf","t CO2eq/cap", True, "ratio",  2, 85),
    "EN.ATM.CO2E.PC":     ("CO₂-Emissionen pro Kopf",      "CO2 emissions per capita",           "t/Kopf",   "t/capita", True,  "ratio",    2, 85),
    "AG.LND.FRST.ZS":     ("Waldfläche",                   "Forest area (% of land area)",       "%",        "%",        False, "percent",  1, 60),
    "EG.ELC.RNEW.ZS":     ("Anteil erneuerbare Energien",  "Renewable energy share",             "%",        "%",        False, "percent",  1, 80),

    # Governance
    "IQ.CPA.PUBS.XQ":     ("Öffentliche Verwaltung Index", "Public sector management",           "Index 1-6","Index 1-6",False, "index",    2, 50),

    # Militär
    "MS.MIL.XPND.GD.ZS":  ("Militärausgaben",              "Military expenditure (% of GDP)",    "% des BIP","% of GDP", False, "percent",  2, 85),

    # Ungleichheit
    "SI.POV.GINI":        ("Gini-Koeffizient",             "Gini index",                         "Index",    "index",    True,  "index",    1, 90),
    "SI.POV.DDAY":        ("Armutsquote $2.15/Tag",        "Poverty headcount $2.15/day",        "%",        "%",        True,  "percent",  1, 75),

    # Technologie
    "IT.NET.USER.ZS":     ("Internetnutzer",               "Individuals using the Internet",     "%",        "%",        False, "percent",  1, 80),
    "IT.CEL.SETS.P2":     ("Mobilfunkverträge",            "Mobile cellular subscriptions",      "je 100",   "per 100",  False, "rate",     1, 60),

    # Handel (Aggregate)
    "NE.EXP.GNFS.ZS":     ("Exportquote",                  "Exports of goods/services (% GDP)",  "% des BIP","% of GDP", False, "percent",  1, 80),
    "NE.IMP.GNFS.ZS":     ("Importquote",                  "Imports of goods/services (% GDP)",  "% des BIP","% of GDP", False, "percent",  1, 75),

    # Demokratie (V-Dem)
    "v2x_libdem":         ("Liberale Demokratie",          "Liberal Democracy Index",            "Index 0-1","Index 0-1",False, "index",    2, 98),
    "v2x_polyarchy":      ("Wahldemokratie",               "Electoral Democracy Index",          "Index 0-1","Index 0-1",False, "index",    2, 95),
    "v2x_civlib":         ("Bürgerrechte",                 "Civil Liberties Index",              "Index 0-1","Index 0-1",False, "index",    2, 90),
    "v2x_freexp":         ("Meinungsfreiheit",             "Freedom of Expression",              "Index 0-1","Index 0-1",False, "index",    2, 92),
    "v2x_frassoc_thick":  ("Vereinigungsfreiheit",         "Freedom of Association",             "Index 0-1","Index 0-1",False, "index",    2, 88),
    "v2xel_frefair":      ("Freie und faire Wahlen",       "Free and Fair Elections",            "Index 0-1","Index 0-1",False, "index",    2, 90),
    "v2x_rule":           ("Rechtsstaatlichkeit",          "Rule of Law",                        "Index 0-1","Index 0-1",False, "index",    2, 93),
    "v2x_corr":           ("Politische Korruption",        "Political Corruption",               "Index 0-1","Index 0-1",True,  "index",    2, 85),
}


def get_source_id(cur, slug):
    cur.execute("SELECT id FROM data_sources WHERE slug = %s", (slug,))
    row = cur.fetchone()
    return row[0] if row else None


def migrate_indicators(cur):
    """Migriert world_indicator_meta nach data_indicators."""
    print("\n=== Indikatoren-Metadaten migrieren ===")

    cur.execute("""
        SELECT indicator_code, indicator_name, category, unit,
               description_de, description_en, source, source_url
          FROM world_indicator_meta
    """)
    old_meta = {row[0]: row for row in cur.fetchall()}

    # Auch alle Codes aus world_indicators ohne Meta-Eintrag
    cur.execute("SELECT DISTINCT indicator_code FROM world_indicators WHERE indicator_code IS NOT NULL")
    all_codes = {row[0] for row in cur.fetchall()}
    print(f"  {len(all_codes)} eindeutige Indikator-Codes gefunden, {len(old_meta)} mit Metadaten")

    wb_id = get_source_id(cur, "worldbank_wdi")
    vdem_id = get_source_id(cur, "vdem")

    inserted = 0
    skipped = 0

    for code in sorted(all_codes):
        meta = old_meta.get(code)
        source_slug = detect_source(code)
        source_id = vdem_id if source_slug == "vdem" else wb_id

        # Bekannte Indikatoren mit gepflegten Übersetzungen
        if code in KNOWN_INDICATORS:
            name_de, name_en, unit_de, unit_en, low_better, vtype, decimals, prio = KNOWN_INDICATORS[code]
            category = meta[2] if meta else ("democracy" if source_slug == "vdem" else "economy")
            desc_de = meta[4] if meta else None
            desc_en = meta[5] if meta else None
        else:
            # Fallback: nutze alte Metadaten oder generiere
            if meta:
                name_en = meta[1] or code
                name_de = name_en  # keine deutsche Übersetzung – englisch als Fallback
                category = meta[2] or ("democracy" if source_slug == "vdem" else "economy")
                unit = meta[3] or ""
                unit_de = unit_en = unit
                desc_de = meta[4]
                desc_en = meta[5]
            else:
                name_en = name_de = code
                category = "democracy" if source_slug == "vdem" else "economy"
                unit_de = unit_en = ""
                desc_de = desc_en = None

            low_better = code in ("SI.POV.GINI", "FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS", "v2x_corr",
                                   "SH.DYN.MORT", "SH.STA.MMRT", "EN.ATM.CO2E.PC")
            vtype = "index" if "v2x" in code else "absolute"
            decimals = 2
            prio = 50

        cur.execute("""
            INSERT INTO data_indicators
              (code, source_id, category, name_de, name_en, description_de, description_en,
               unit_de, unit_en, value_type, lower_is_better, display_decimals, priority)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              source_id=VALUES(source_id),
              category=VALUES(category),
              name_de=VALUES(name_de),
              name_en=VALUES(name_en),
              description_de=VALUES(description_de),
              description_en=VALUES(description_en),
              unit_de=VALUES(unit_de),
              unit_en=VALUES(unit_en),
              value_type=VALUES(value_type),
              lower_is_better=VALUES(lower_is_better),
              display_decimals=VALUES(display_decimals),
              priority=VALUES(priority)
        """, (code, source_id, category, name_de, name_en, desc_de, desc_en,
              unit_de, unit_en, vtype, low_better, decimals, prio))

        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    print(f"  Eingefügt: {inserted}, Aktualisiert: {skipped}")


def migrate_values(cur, conn):
    """Migriert world_indicators nach data_values."""
    print("\n=== Werte migrieren ===")

    # Code → indicator_id Mapping holen
    cur.execute("SELECT id, code FROM data_indicators")
    code_to_id = {code: id_ for id_, code in cur.fetchall()}
    print(f"  {len(code_to_id)} Indikatoren registriert")

    # Total Anzahl in alter Tabelle
    cur.execute("SELECT COUNT(*) FROM world_indicators")
    total = cur.fetchone()[0]
    print(f"  {total:,} Werte zu migrieren")

    # In Batches lesen und schreiben
    BATCH = 5000
    offset = 0
    inserted_total = 0
    skipped_total = 0
    start_time = time.time()

    while True:
        cur.execute("""
            SELECT country_code, indicator_code, year, value
              FROM world_indicators
             WHERE country_code IS NOT NULL
               AND indicator_code IS NOT NULL
               AND year IS NOT NULL
             ORDER BY id
             LIMIT %s OFFSET %s
        """, (BATCH, offset))
        rows = cur.fetchall()
        if not rows:
            break

        batch_data = []
        for country_code, indicator_code, year, value in rows:
            ind_id = code_to_id.get(indicator_code)
            if not ind_id:
                skipped_total += 1
                continue
            batch_data.append((ind_id, country_code, year, value))

        if batch_data:
            cur.executemany("""
                INSERT INTO data_values (indicator_id, country_code, year, value, quality)
                VALUES (%s, %s, %s, %s, 'measured')
                ON DUPLICATE KEY UPDATE
                  value=VALUES(value),
                  fetched_at=CURRENT_TIMESTAMP
            """, batch_data)
            conn.commit()
            inserted_total += len(batch_data)

        offset += BATCH
        elapsed = time.time() - start_time
        rate = offset / elapsed if elapsed > 0 else 0
        pct = (offset / total) * 100 if total else 0
        print(f"  {offset:,}/{total:,} ({pct:.1f}%) – {rate:.0f} Zeilen/s – {inserted_total:,} migriert, {skipped_total:,} übersprungen")

    print(f"\n  Fertig. {inserted_total:,} Werte migriert, {skipped_total:,} übersprungen")


def migrate_trade(cur, conn):
    """Kopiert trade_flows nach trade_flows_v2 mit korrekter source_id."""
    print("\n=== Handelsdaten migrieren ===")

    comtrade_id = get_source_id(cur, "un_comtrade")

    cur.execute("SELECT COUNT(*) FROM trade_flows")
    total = cur.fetchone()[0]
    print(f"  {total} Trade-Flow-Einträge zu migrieren")

    cur.execute("""
        SELECT reporter_code, partner_code, flow, year, value_usd, hs_code
          FROM trade_flows
    """)
    rows = cur.fetchall()

    batch = []
    for reporter, partner, flow, year, value, hs in rows:
        if not reporter or not partner or not flow:
            continue
        batch.append((reporter, partner, flow, year, value, hs or 'TOTAL', comtrade_id))

    if batch:
        cur.executemany("""
            INSERT INTO trade_flows_v2
              (reporter_iso3, partner_iso3, flow, year, value_usd, hs_section, source_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              value_usd=VALUES(value_usd),
              fetched_at=CURRENT_TIMESTAMP
        """, batch)
        conn.commit()
        print(f"  {len(batch)} Trade-Flows migriert")


def update_stats(cur, conn):
    """Aktualisiert year_min, year_max, country_count je Indikator."""
    print("\n=== Statistik-Felder aktualisieren ===")

    cur.execute("""
        UPDATE data_indicators i
          JOIN (
            SELECT indicator_id,
                   MIN(year) AS y_min,
                   MAX(year) AS y_max,
                   COUNT(DISTINCT country_code) AS cnt
              FROM data_values
             WHERE value IS NOT NULL
             GROUP BY indicator_id
          ) s ON s.indicator_id = i.id
           SET i.year_min = s.y_min,
               i.year_max = s.y_max,
               i.country_count = s.cnt
    """)
    conn.commit()
    print(f"  {cur.rowcount} Indikatoren aktualisiert")


def main():
    load_env()
    conn = get_db(autocommit=False)
    cur = conn.cursor()

    migrate_indicators(cur)
    conn.commit()

    migrate_values(cur, conn)
    migrate_trade(cur, conn)
    update_stats(cur, conn)

    # Endstand
    print("\n=== Endstand ===")
    cur.execute("SELECT COUNT(*) FROM data_sources");      print(f"  data_sources:    {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM data_indicators");   print(f"  data_indicators: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM data_countries");    print(f"  data_countries:  {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM data_values");       print(f"  data_values:     {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM trade_flows_v2");    print(f"  trade_flows_v2:  {cur.fetchone()[0]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
