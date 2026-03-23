#!/usr/bin/env python3
import os, re, requests, mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

FEEDS = {
    'BVerfG': 'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bverfg.xml',
    'BGH':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bgh.xml',
    'BVerwG': 'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bverwg.xml',
    'BFH':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bfh.xml',
    'BAG':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bag.xml',
    'BSG':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bsg.xml',
    'BPatG':  'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bpatg.xml',
}

def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )

def parse_title(title, gericht):
    # "BGH 6a. Zivilsenat, Urteil vom 17.03.2026, VIa ZR 110/23"
    senat, typ, datum, az = None, None, None, None
    m = re.match(r'.+?(?:' + gericht + r')\s+(.+?),\s+(Urteil|Beschluss|Gerichtsbescheid)\s+vom\s+(\d{2}\.\d{2}\.\d{4}),\s+(.+)', title)
    if m:
        senat = m.group(1).strip()
        typ   = m.group(2).strip()
        datum = datetime.strptime(m.group(3), '%d.%m.%Y').date()
        az    = m.group(4).strip()
    return senat, typ, datum, az

def fetch_tenor(doc_id):
    url = f'https://www.rechtsprechung-im-internet.de/jportal/portal/page/bsjrsprod?showdoccase=1&doc.id={doc_id}&doc.part=L'
    try:
        r = requests.get(url, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        # Tenor-Abschnitt finden
        for h4 in soup.find_all('h4'):
            if 'Tenor' in h4.get_text():
                parent_div = h4.find_parent('div', class_='docLayoutMarginTopMore')
                next_div = parent_div.find_next_sibling('div') if parent_div else None
                if next_div:
                    return next_div.get_text(separator=' ', strip=True)[:2000]
    except Exception as e:
        print(f'  Tenor-Fehler {doc_id}: {e}')
    return None

def main():
    db = get_db()
    cur = db.cursor()
    neu = 0

    for gericht, feed_url in FEEDS.items():
        print(f'Abrufen: {gericht}')
        try:
            r = requests.get(feed_url, timeout=15)
            soup = BeautifulSoup(r.content, 'xml')
            items = soup.find_all('item')
            print(f'  {len(items)} Einträge im Feed')
        except Exception as e:
            print(f'  Feed-Fehler: {e}')
            continue

        for item in items:
            doc_id = item.find('guid').get_text().replace('jb-', '')
            full_doc_id = item.find('guid').get_text()

            # Schon in DB?
            cur.execute('SELECT id FROM urteile WHERE doc_id = %s', (doc_id,))
            if cur.fetchone():
                continue

            title     = item.find('title').get_text()
            leitsatz  = item.find('description').get_text() if item.find('description') else None
            senat, typ, datum, az = parse_title(title, gericht)

            # ECLI aus Link extrahieren
            link = item.find('link').get_text() if item.find('link') else ''

            # Tenor abrufen
            print(f'  Neu: {az or doc_id}')
            tenor = fetch_tenor(full_doc_id)

            cur.execute('''
                INSERT INTO urteile (doc_id, gericht, senat, typ, datum, aktenzeichen, leitsatz, tenor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (doc_id, gericht, senat, typ, datum, az, leitsatz, tenor))
            db.commit()
            neu += 1

    print(f'\nFertig. {neu} neue Urteile importiert.')
    cur.close()
    db.close()

if __name__ == '__main__':
    main()
