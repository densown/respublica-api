#!/usr/bin/env python3
"""
V-Dem Import Script v16
Importiert 8 Kern-Indikatoren in world_indicators / world_indicator_meta
"""
import csv
import sys
import mysql.connector

DB = dict(
    unix_socket="/var/run/mysqld/mysqld.sock",
    use_pure=True,
    user="root",
    password="",
    database="respublica_gesetze",
)

INDICATORS = {
    "v2x_libdem":        ("Liberal Democracy Index",     "democracy", "Index 0-1", "Liberaler Demokratie-Index (0=autoritär, 1=voll demokratisch)", "Liberal Democracy Index (0=authoritarian, 1=fully democratic)"),
    "v2x_polyarchy":     ("Electoral Democracy Index",   "democracy", "Index 0-1", "Wahldemokratie-Index", "Electoral Democracy Index"),
    "v2x_civlib":        ("Civil Liberties Index",       "democracy", "Index 0-1", "Bürgerrechte-Index", "Civil Liberties Index"),
    "v2x_freexp":        ("Freedom of Expression",       "democracy", "Index 0-1", "Meinungsfreiheits-Index", "Freedom of Expression Index"),
    "v2x_frassoc_thick": ("Freedom of Association",      "democracy", "Index 0-1", "Vereinigungsfreiheits-Index", "Freedom of Association Index"),
    "v2xel_frefair":     ("Free and Fair Elections",     "democracy", "Index 0-1", "Index für freie und faire Wahlen", "Free and Fair Elections Index"),
    "v2x_rule":          ("Rule of Law Index",           "democracy", "Index 0-1", "Rechtsstaatlichkeits-Index", "Rule of Law Index"),
    "v2x_corr":          ("Political Corruption Index",  "democracy", "Index 0-1", "Korruptionsindex (höher = mehr Korruption)", "Political Corruption Index (higher = more corruption)"),
}

SOURCE = "V-Dem Institute (v16)"
SOURCE_URL = "https://v-dem.net/data/the-v-dem-dataset/"
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "/root/data/vdem.csv"

def main():
    print(f"Verbinde mit DB...")
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    print("Schreibe Metadaten...")
    for code, (name, category, unit, desc_de, desc_en) in INDICATORS.items():
        cur.execute("""
            INSERT INTO world_indicator_meta
              (indicator_code, indicator_name, category, unit, description_de, description_en, source, source_url)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              indicator_name=VALUES(indicator_name),
              category=VALUES(category),
              unit=VALUES(unit),
              description_de=VALUES(description_de),
              description_en=VALUES(description_en),
              source=VALUES(source),
              source_url=VALUES(source_url)
        """, (code, name, category, unit, desc_de, desc_en, SOURCE, SOURCE_URL))
    conn.commit()
    print("Metadaten OK.")

    print(f"Lese {CSV_PATH} ...")
    rows_inserted = 0
    rows_skipped = 0
    batch = []
    BATCH_SIZE = 5000

    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Anführungszeichen in Spaltennamen entfernen
        reader.fieldnames = [f.strip('"') for f in reader.fieldnames]
        for row in reader:
            country_code = row.get("country_text_id", "").strip().strip('"')
            country_name = row.get("country_name", "").strip().strip('"')
            year_str = row.get("year", "").strip().strip('"')
            if not country_code or not year_str:
                continue
            try:
                year = int(float(year_str))
            except ValueError:
                continue
            if year < 1950:
                continue

            for code, (name, *_) in INDICATORS.items():
                val_str = row.get(code, "").strip().strip('"')
                if not val_str or val_str in ("", "NA", "NaN", "."):
                    rows_skipped += 1
                    continue
                try:
                    value = float(val_str)
                except ValueError:
                    rows_skipped += 1
                    continue

                batch.append((country_code, country_name, None, None, code, name, year, value))

                if len(batch) >= BATCH_SIZE:
                    cur.executemany("""
                        INSERT INTO world_indicators
                          (country_code, country_name, region, income_level,
                           indicator_code, indicator_name, year, value)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE value=VALUES(value)
                    """, batch)
                    conn.commit()
                    rows_inserted += len(batch)
                    print(f"  {rows_inserted} Zeilen importiert...")
                    batch = []

    if batch:
        cur.executemany("""
            INSERT INTO world_indicators
              (country_code, country_name, region, income_level,
               indicator_code, indicator_name, year, value)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE value=VALUES(value)
        """, batch)
        conn.commit()
        rows_inserted += len(batch)

    print(f"\nFertig. {rows_inserted} Zeilen importiert, {rows_skipped} übersprungen.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
