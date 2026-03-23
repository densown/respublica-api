#!/usr/bin/env python3
import os, requests, mysql.connector
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME')
    )

def fetch_tenor(doc_id):
    url = f'https://www.rechtsprechung-im-internet.de/jportal/portal/page/bsjrsprod?showdoccase=1&doc.id=jb-{doc_id}&doc.part=L'
    try:
        r = requests.get(url, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        for h4 in soup.find_all('h4'):
            if 'Tenor' in h4.get_text():
                parent_div = h4.find_parent('div', class_='docLayoutMarginTopMore')
                next_div = parent_div.find_next_sibling('div') if parent_div else None
                if next_div:
                    return next_div.get_text(separator=' ', strip=True)[:2000]
    except Exception as e:
        print(f'  Fehler {doc_id}: {e}')
    return None

db = get_db()
cur = db.cursor()
cur.execute('SELECT id, doc_id FROM urteile WHERE tenor IS NULL')
rows = cur.fetchall()
print(f'{len(rows)} Einträge ohne Tenor')

for i, (uid, doc_id) in enumerate(rows):
    tenor = fetch_tenor(doc_id)
    if tenor:
        cur.execute('UPDATE urteile SET tenor = %s WHERE id = %s', (tenor, uid))
        db.commit()
        print(f'  [{i+1}/{len(rows)}] {doc_id} ✓')
    else:
        print(f'  [{i+1}/{len(rows)}] {doc_id} — kein Tenor')

print('Fertig.')
cur.close()
db.close()
