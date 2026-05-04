#!/usr/bin/env python3
import time, sys
import mysql.connector
try:
    import requests
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "requests", "--break-system-packages"])
    import requests

# ISO3 mapping laden
exec(open('/root/apps/gesetze/scripts/comtrade_country_codes.py').read())

API_KEY = "809bb81f740e4045a0d7338a02a934c2"
BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

DB = dict(
    unix_socket="/var/run/mysqld/mysqld.sock",
    user="root", password="",
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

def fetch_trade(reporter_num, flow_code):
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
            print(f"  HTTP {r.status_code}: {r.text[:100]}")
            return []
        return r.json().get("data", []) or []
    except Exception as e:
        print(f"  Fehler: {e}")
        return []

def main():
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    # Alte Daten löschen
    cur.execute("DELETE FROM trade_flows")
    conn.commit()
    print("Alte Daten gelöscht.")

    total = 0
    for i, (iso3, num_code) in enumerate(COUNTRIES.items()):
        print(f"[{i+1}/{len(COUNTRIES)}] {iso3}...")

        for flow_code, flow_name in [("X", "export"), ("M", "import")]:
            rows = fetch_trade(num_code, flow_code)
            batch = []
            for r in rows:
                p_num = str(r.get("partnerCode", 0))
                if p_num in ("0", "1"):
                    continue
                # ISO3 aus Mapping
                p_iso3 = COMTRADE_TO_ISO3.get(p_num, p_num)
                # Name aus Mapping oder Code
                p_name = p_iso3
                value = r.get("primaryValue") or r.get("fobvalue") or r.get("cifvalue")
                if not value or float(value) <= 0:
                    continue
                batch.append((
                    iso3, iso3, p_iso3, p_name,
                    flow_name, "TOTAL", "Total trade",
                    YEAR, int(float(value))
                ))

            if batch:
                cur.executemany("""
                    INSERT INTO trade_flows
                      (reporter_code, reporter_name, partner_code, partner_name,
                       flow, hs_code, hs_desc, year, value_usd)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE value_usd=VALUES(value_usd)
                """, batch)
                conn.commit()
                total += len(batch)
                print(f"  {flow_name}: {len(batch)} Partner")

            time.sleep(1.5)

    print(f"\nFertig. {total} Handelsbeziehungen importiert.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
