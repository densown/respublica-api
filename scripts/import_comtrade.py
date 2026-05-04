#!/usr/bin/env python3
"""
UN Comtrade Import Script v2
Lädt Top-Handelspartner für wichtige Länder
"""
import time, sys, json
import mysql.connector
try:
    import requests
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "requests", "--break-system-packages"])
    import requests

API_KEY = "809bb81f740e4045a0d7338a02a934c2"
BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

DB = dict(
    unix_socket="/var/run/mysqld/mysqld.sock",
    user="root",
    password="",
    database="respublica_gesetze",
    use_pure=True,
)

COUNTRIES = {
    "DEU": "276", "USA": "842", "CHN": "156", "FRA": "250", "GBR": "826",
    "JPN": "392", "IND": "356", "ITA": "380", "CAN": "124", "KOR": "410",
    "RUS": "643", "AUS": "036", "BRA": "076", "ESP": "724", "MEX": "484",
    "NLD": "528", "SAU": "682", "TUR": "792", "CHE": "756", "POL": "616",
}

YEAR = 2023

def fetch_trade(reporter_iso, reporter_num, flow_code):
    params = {
        "reporterCode": reporter_num,
        "period": str(YEAR),
        "cmdCode": "TOTAL",
        "flowCode": flow_code,
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": "20",
        "format": "JSON",
        "breakdownMode": "classic",
        "subscription-key": API_KEY,
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        if r.status_code == 429:
            print("  Rate limit - warte 60s...")
            time.sleep(60)
            r = requests.get(BASE_URL, params=params, timeout=30)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}: {r.text[:200]}")
            return []
        data = r.json()
        return data.get("data", []) or []
    except Exception as e:
        print(f"  Fehler: {e}")
        return []

def num_to_iso3(num_code):
    """Comtrade numeric → ISO3 lookup"""
    lookup = {v: k for k, v in COUNTRIES.items()}
    return lookup.get(str(num_code).zfill(3), str(num_code))

def main():
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()
    total = 0
    countries = list(COUNTRIES.items())

    for i, (iso3, num_code) in enumerate(countries):
        print(f"[{i+1}/{len(countries)}] {iso3}...")

        for flow_code, flow_name in [("X", "export"), ("M", "import")]:
            rows = fetch_trade(iso3, num_code, flow_code)
            print(f"  {flow_name}: {len(rows)} Einträge von API")

            batch = []
            for r in rows:
                partner_code = r.get("partnerCode", 0)
                if partner_code in (0, 1):  # 0=Welt, 1=andere
                    continue
                partner_iso = r.get("partnerISO") or num_to_iso3(partner_code)
                partner_name = r.get("partnerDesc") or partner_iso
                value = r.get("primaryValue") or r.get("fobvalue") or r.get("cifvalue")
                if not value or value <= 0:
                    continue
                batch.append((
                    iso3, r.get("reporterDesc", iso3),
                    str(partner_iso)[:3], str(partner_name)[:200],
                    flow_name, "TOTAL", "Total trade",
                    YEAR, int(value)
                ))

            if batch:
                cur.executemany("""
                    INSERT INTO trade_flows
                      (reporter_code, reporter_name, partner_code, partner_name,
                       flow, hs_code, hs_desc, year, value_usd)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      value_usd=VALUES(value_usd),
                      updated_at=CURRENT_TIMESTAMP
                """, batch)
                conn.commit()
                total += len(batch)
                print(f"  {flow_name}: {len(batch)} Partner gespeichert")
            else:
                print(f"  {flow_name}: keine verwertbaren Daten")

            time.sleep(2)

    print(f"\nFertig. {total} Handelsbeziehungen importiert.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
