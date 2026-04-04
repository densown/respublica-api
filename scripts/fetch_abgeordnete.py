#!/usr/bin/env python3
"""Abgeordnete (21. Bundestag) von Abgeordnetenwatch API v2 in Tabelle abgeordnete."""
import os
import re
import time
from datetime import datetime
from urllib.parse import urlencode

import mysql.connector
import requests
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

BASE = 'https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates'
PARLIAMENT_PERIOD = 161
PAGER_LIMIT = 100
LOG_PATH = '/root/apps/gesetze/logs/cron.log'
PAGE_SLEEP_SEC = 2

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'ResPublicaGesetze/1.0 (+https://respublica.media)',
}


def log_line(msg):
    line = f'[{datetime.now().isoformat(timespec="seconds")}] fetch_abgeordnete: {msg}\n'
    print(line, end='')
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line)
    except OSError:
        pass


def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )


def clean_fraction_label(label):
    if not label:
        return None
    s = str(label).strip()
    s = re.sub(r'\s*\(\s*Bundestag\s+[^)]*\)\s*$', '', s, flags=re.I).strip()
    return s or str(label).strip()


def vorname_nachname_from_politician(politician):
    if not isinstance(politician, dict):
        return None, None, None
    fn = politician.get('first_name')
    ln = politician.get('last_name')
    label = (politician.get('label') or '').strip()
    name = label or None
    if fn and ln:
        return fn, ln, name
    if label and ' ' in label:
        vor, nach = label.rsplit(' ', 1)
        return vor.strip(), nach.strip(), name
    if label:
        return label, None, name
    return None, None, name


def wahlkreis_felder(constituency):
    if not isinstance(constituency, dict):
        return None, None
    wlabel = constituency.get('label')
    if wlabel is not None:
        wlabel = str(wlabel).strip() or None
    nr = constituency.get('number')
    if nr is not None:
        try:
            return wlabel, int(nr)
        except (TypeError, ValueError):
            pass
    if wlabel:
        m = re.match(r'^\s*(\d+)\s*-\s*', wlabel)
        if m:
            try:
                return wlabel, int(m.group(1))
            except ValueError:
                pass
    return wlabel, None


def extract_row(item):
    aw_id = item.get('id')
    politician = item.get('politician') or {}
    politiker_id = politician.get('id')
    vorname, nachname, name = vorname_nachname_from_politician(politician)

    fracs = item.get('fraction_membership') or []
    fraktion = None
    if fracs and isinstance(fracs[0], dict):
        frac = (fracs[0].get('fraction') or {}).get('label')
        fraktion = clean_fraction_label(frac)

    ed = item.get('electoral_data') or {}
    listenplatz = ed.get('list_position')
    try:
        listenplatz = int(listenplatz) if listenplatz is not None else None
    except (TypeError, ValueError):
        listenplatz = None

    wahlkreis, wahlkreis_nr = wahlkreis_felder(ed.get('constituency'))

    profil_url = politician.get('abgeordnetenwatch_url')
    if profil_url:
        profil_url = str(profil_url)[:500]

    try:
        aw_id = int(aw_id) if aw_id is not None else None
    except (TypeError, ValueError):
        aw_id = None
    try:
        politiker_id = int(politiker_id) if politiker_id is not None else None
    except (TypeError, ValueError):
        politiker_id = None

    return (
        aw_id,
        politiker_id,
        vorname,
        nachname,
        name,
        fraktion,
        wahlkreis,
        wahlkreis_nr,
        listenplatz,
        profil_url,
    )


UPSERT_SQL = '''
INSERT INTO abgeordnete (
  aw_id, politiker_id, vorname, nachname, name, fraktion,
  wahlkreis, wahlkreis_nr, listenplatz, profil_url, foto_url
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL
)
ON DUPLICATE KEY UPDATE
  politiker_id = VALUES(politiker_id),
  vorname = VALUES(vorname),
  nachname = VALUES(nachname),
  name = VALUES(name),
  fraktion = VALUES(fraktion),
  wahlkreis = VALUES(wahlkreis),
  wahlkreis_nr = VALUES(wahlkreis_nr),
  listenplatz = VALUES(listenplatz),
  profil_url = VALUES(profil_url),
  foto_url = COALESCE(VALUES(foto_url), foto_url)
'''


def fetch_page(page):
    q = urlencode({
        'parliament_period': PARLIAMENT_PERIOD,
        'page': page,
        'pager_limit': PAGER_LIMIT,
    })
    url = f'{BASE}?{q}'
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def main():
    log_line('Start')
    db = get_db()
    cur = db.cursor()
    page = 0
    total_pages = None
    processed = 0

    try:
        while True:
            payload = fetch_page(page)
            items = payload.get('data') or []
            if not items:
                break

            meta = (payload.get('meta') or {}).get('result') or {}
            if total_pages is None:
                total = int(meta.get('total') or 0)
                total_pages = max(1, (total + PAGER_LIMIT - 1) // PAGER_LIMIT)

            log_line(f'Seite {page + 1}/{total_pages}: {len(items)} MdBs geholt')

            batch = []
            for item in items:
                row = extract_row(item)
                if row[0] is None:
                    continue
                batch.append(row)

            for row in batch:
                cur.execute(UPSERT_SQL, row)
                processed += 1

            db.commit()

            if len(items) < PAGER_LIMIT:
                break
            page += 1
            time.sleep(PAGE_SLEEP_SEC)

        log_line(f'Fertig, {processed} Datensätze verarbeitet (INSERT/UPDATE)')
        return 0
    except requests.RequestException as e:
        log_line(f'API-Fehler: {e}')
        return 1
    except mysql.connector.Error as e:
        log_line(f'DB-Fehler: {e}')
        return 1
    finally:
        cur.close()
        db.close()


if __name__ == '__main__':
    raise SystemExit(main())
