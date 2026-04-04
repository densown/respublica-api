#!/usr/bin/env python3
import os, sys, time, logging
from pathlib import Path
import mysql.connector, requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler('/root/apps/gesetze/logs/backfill_eu_betreff.log', encoding='utf-8')])
log = logging.getLogger('backfill')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept-Language': 'de,en;q=0.9'}
TRIGGER = ['Rechtssache', 'Beschluss', 'Urteil', 'Gutachten', 'Stellungnahme']

def get_db():
    return mysql.connector.connect(host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME'))

def fetch_betreff(celex):
    try:
        r = requests.get(f'https://eur-lex.europa.eu/legal-content/DE/ALL/?uri=CELEX:{celex}',
            headers=HEADERS, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, 'html.parser')
        for p in soup.find_all('p'):
            txt = p.text.strip()
            if any(w in txt for w in TRIGGER): return txt[:1000]
        return None
    except Exception as e:
        log.warning(f'Fehler bei {celex}: {e}')
        return None

def main():
    db = get_db(); cur = db.cursor(dictionary=True)
    cur.execute('SELECT id, celex FROM eu_urteile WHERE betreff = celex')
    rows = cur.fetchall()
    log.info(f'{len(rows)} Urteile ohne echten Betreff')
    updated = 0
    for i, row in enumerate(rows):
        celex = row['celex']
        log.info(f'[{i+1}/{len(rows)}] {celex}')
        betreff = fetch_betreff(celex)
        if betreff:
            cur.execute('UPDATE eu_urteile SET betreff = %s WHERE id = %s', (betreff, row['id']))
            db.commit(); updated += 1
            log.info(f'  -> {betreff[:80]}...')
        else:
            log.info(f'  -> kein Betreff')
        time.sleep(1.5)
    log.info(f'Betreffs: {updated}/{len(rows)} aktualisiert')
    cur.execute("""UPDATE eu_urteile SET zusammenfassung_de=NULL, zusammenfassung_en=NULL
        WHERE zusammenfassung_de LIKE '%%benötige%%mehr Informationen%%'
        OR zusammenfassung_de LIKE '%%kein Experte%%'
        OR zusammenfassung_de LIKE '%%Bitte gib mir%%'
        OR zusammenfassung_de LIKE '%%Bitte teilen Sie%%'
        OR zusammenfassung_de LIKE '%%nicht möglich%%zusammenfassen%%'
        OR zusammenfassung_de LIKE '%%nicht möglich%%zusammenzufassen%%'
        OR zusammenfassung_de LIKE '%%benötige jedoch%%'
        OR zusammenfassung_de LIKE '%%I need the information%%'
        OR zusammenfassung_de LIKE '%%provide the full text%%'
        OR zusammenfassung_de LIKE '%%provide more details%%'""")
    nulled = cur.rowcount; db.commit()
    log.info(f'{nulled} kaputte Summaries auf NULL gesetzt')
    cur.close(); db.close()
    log.info(f'Jetzt: python3 scripts/summarize_eu_urteile.py')

if __name__ == '__main__':
    main()
