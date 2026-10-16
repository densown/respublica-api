#!/usr/bin/env python3
import re
import sys
from pathlib import Path

import requests
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env

FEEDS = {
    'BVerfG': 'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bverfg.xml',
    'BGH':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bgh.xml',
    'BVerwG': 'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bverwg.xml',
    'BFH':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bfh.xml',
    'BAG':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bag.xml',
    'BSG':    'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bsg.xml',
    'BPatG':  'https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bpatg.xml',
}

def parse_title(title, gericht):
    # "BGH 6a. Zivilsenat, Urteil vom 17.03.2026, VIa ZR 110/23"
    # "BVerfG 2. Senat 1. Kammer, Ablehnung einstweilige Anordnung vom 01.10.2026, 2 BvR 1/26"
    # Seit Ende Maerz 2026 beginnt der Titel direkt mit dem Gerichtskuerzel. Der
    # alte Ausdruck verlangte davor mindestens ein Zeichen und fand nichts mehr;
    # die Urteile landeten ohne Datum, Senat, Typ und Aktenzeichen in der DB.
    # Als Typ gilt alles zwischen Komma und "vom", nicht nur Urteil/Beschluss.
    senat, typ, datum, az = None, None, None, None
    m = re.match(
        r'^(?:.*?\s)?' + re.escape(gericht)
        + r'\s+(.+?),\s+([^,]+?)\s+vom\s+(\d{2}\.\d{2}\.\d{4}),\s+(.+)$',
        title.strip(),
    )
    if m:
        try:
            datum = datetime.strptime(m.group(3), '%d.%m.%Y').date()
        except ValueError:
            return None, None, None, None
        # "BPatG München 6. Senat": Ort gehoert nicht zum Senat (im XML steht er in gerort)
        senat = re.sub(r'^[A-ZÄÖÜ][a-zäöüß]+\s+(?=\d)', '', m.group(1).strip())[:100]
        typ   = m.group(2).strip()[:100]
        az    = m.group(4).strip()[:100]
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
    load_env()
    db = get_db(autocommit=False)
    cur = db.cursor()
    neu = 0
    nachgetragen = 0

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

            title = item.find('title').get_text()
            senat, typ, datum, az = parse_title(title, gericht)

            # Schon in DB?
            cur.execute('SELECT id, datum FROM urteile WHERE doc_id = %s', (doc_id,))
            vorhanden = cur.fetchone()
            if vorhanden:
                # Zeilen aus der Zeit, in der parse_title nichts fand, nachtragen
                if vorhanden[1] is None and datum:
                    cur.execute('''
                        UPDATE urteile SET senat = %s, typ = %s, datum = %s, aktenzeichen = %s
                        WHERE id = %s AND datum IS NULL
                    ''', (senat, typ, datum, az, vorhanden[0]))
                    db.commit()
                    nachgetragen += 1
                continue

            if not datum:
                print(f'  WARNUNG: Titel nicht erkannt: {title.strip()}')
            leitsatz  = item.find('description').get_text() if item.find('description') else None

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

    print(f'\nFertig. {neu} neue Urteile importiert, {nachgetragen} ohne Datum nachgetragen.')
    cur.close()
    db.close()

if __name__ == '__main__':
    main()
