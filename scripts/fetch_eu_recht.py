#!/usr/bin/env python3
"""Fetch EU legal acts from EUR-Lex SPARQL, store in eu_rechtsakte."""
import os
import re
from datetime import date, timedelta
from urllib.parse import urlencode

import mysql.connector
import requests
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

SPARQL_URL = 'https://publications.europa.eu/webapi/rdf/sparql'
LOG_PATH = '/root/apps/gesetze/logs/cron.log'

SPARQL_TEMPLATE = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?celex ?title ?date ?type ?force
WHERE {{
  ?work cdm:work_has_resource-type ?type.
  FILTER(?type IN (
    <http://publications.europa.eu/resource/authority/resource-type/REG>,
    <http://publications.europa.eu/resource/authority/resource-type/REG_IMPL>,
    <http://publications.europa.eu/resource/authority/resource-type/REG_DEL>,
    <http://publications.europa.eu/resource/authority/resource-type/DIR>,
    <http://publications.europa.eu/resource/authority/resource-type/DIR_IMPL>,
    <http://publications.europa.eu/resource/authority/resource-type/DIR_DEL>,
    <http://publications.europa.eu/resource/authority/resource-type/DEC>,
    <http://publications.europa.eu/resource/authority/resource-type/DEC_IMPL>
  ))
  FILTER NOT EXISTS {{
    ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/CORRIGENDUM>
  }}
  ?work cdm:resource_legal_id_celex ?celex.
  ?work cdm:work_date_document ?date.
  ?exp cdm:expression_belongs_to_work ?work.
  ?exp cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/{lang}>.
  ?exp cdm:expression_title ?title.
  OPTIONAL {{ ?work cdm:resource_legal_in-force ?force. }}
  FILTER NOT EXISTS {{
    ?work cdm:do_not_index "true"^^xsd:boolean
  }}
  FILTER(?date >= "{cutoff}"^^xsd:date)
}}
ORDER BY DESC(?date)
LIMIT 500
"""


def log_line(msg):
    from datetime import datetime
    line = f'[{datetime.now().isoformat(timespec="seconds")}] fetch_eu_recht: {msg}\n'
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


def binding_val(bindings, key):
    if key not in bindings:
        return None
    v = bindings[key]
    if isinstance(v, dict) and 'value' in v:
        return v['value']
    return None


def normalize_celex(raw):
    if not raw:
        return None
    s = str(raw).strip()
    s = re.sub(r'^http://[^#]+#', '', s)
    if s.upper().startswith('CELEX:'):
        s = s.split(':', 1)[-1].strip()
    s = re.sub(r'[^\w\d:]+', '', s.replace(' ', ''))
    if len(s) > 50:
        s = s[:50]
    return s or None


def extract_typ_from_uri(type_uri):
    if not type_uri:
        return 'OTHER', 'Sonstiges'
    u = str(type_uri)
    tail = u.rstrip('/').split('/')[-1]
    base = tail.split('_')[0].upper()
    mapping = {
        'REG': ('REG', 'Verordnung'),
        'DIR': ('DIR', 'Richtlinie'),
        'DEC': ('DEC', 'Beschluss'),
        'REC': ('REC', 'Empfehlung'),
    }
    if base in mapping:
        return mapping[base]
    if 'REG' in tail.upper():
        return 'REG', 'Verordnung'
    if 'DIR' in tail.upper():
        return 'DIR', 'Richtlinie'
    if 'DEC' in tail.upper():
        return 'DEC', 'Beschluss'
    return 'OTHER', 'Sonstiges'


def heuristik_rechtsgebiet(titel):
    if not titel:
        return 'Sonstiges'
    t = titel.lower()
    rules = [
        ('Umwelt', ['umwelt', 'klima', 'emission', 'naturschutz', 'meer', 'abfall', 'wasser', 'luftqual', 'öko', 'oeko', 'biodivers']),
        ('Handel', ['handel', 'zoll', 'antidumping', 'import', 'export', 'einfuhr', 'ausfuhr', 'waren']),
        ('Finanzen', ['finanz', 'bankenunion', 'kapitalmarkt', 'wirtschaftliche finanz', 'haushalt', 'eu-haushalt', 'gelder']),
        ('Migration', ['migration', 'asyl', 'grenz', 'aufenthalt', 'visum', 'rückführung', 'rueckfuehr']),
        ('Digitales', ['digital', 'datenschutz', 'cyber', 'elektronisch ident', 'online-plattform', ' ki ', 'künstliche intelligenz', 'kuenstliche intelligenz', 'algorithm']),
        ('Landwirtschaft', ['landwirtschaft', 'agrar', 'fischerei', 'wein ', 'milch', 'agrarmarkt']),
        ('Energie', ['energie', 'erneuerbar', 'gas', 'strom', 'emissionshandel']),
        ('Verkehr', ['verkehr', 'transport', 'luftfahrt', 'schiene', 'schifffahrt']),
        ('Gesundheit', ['gesundheit', 'arzneimittel', 'medizin', 'seuche', 'patient']),
        ('Justiz', ['strafrecht', 'justiz', 'gericht', 'zivilrechtliche', 'rechtsbehelf', 'europäischer haftbefehl', 'europaeischer haftbefehl']),
    ]
    for rg, kws in rules:
        for kw in kws:
            if kw in t:
                return rg
    return 'Sonstiges'


def sparql_fetch(lang_code, cutoff):
    q = SPARQL_TEMPLATE.format(lang=lang_code, cutoff=cutoff)
    body = urlencode({'query': q})
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/sparql-results+json',
        'User-Agent': 'ResPublicaGesetze/1.0 (+https://respublica.media)',
    }
    r = requests.post(SPARQL_URL, data=body, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def merge_rows(de_data, en_data):
    by_celex = {}
    for batch, lang in ((de_data, 'de'), (en_data, 'en')):
        results = (batch or {}).get('results', {}).get('bindings', [])
        for b in results:
            cx = normalize_celex(binding_val(b, 'celex'))
            if not cx:
                continue
            row = by_celex.setdefault(cx, {
                'celex': cx,
                'titel_de': None,
                'titel_en': None,
                'datum': None,
                'type_uri': None,
                'force': None,
            })
            title = binding_val(b, 'title')
            dat = binding_val(b, 'date')
            typ = binding_val(b, 'type')
            force = binding_val(b, 'force')
            if lang == 'de' and title:
                row['titel_de'] = title
            if lang == 'en' and title:
                row['titel_en'] = title
            if dat:
                row['datum'] = dat[:10] if len(dat) >= 10 else dat
            if typ:
                row['type_uri'] = typ
            if force:
                row['force'] = force
    return list(by_celex.values())


def main():
    cutoff = (date.today() - timedelta(days=365)).isoformat()
    log_line(f'Start, cutoff {cutoff}')

    try:
        log_line('SPARQL DE …')
        de_json = sparql_fetch('DEU', cutoff)
        log_line('SPARQL ENG …')
        en_json = sparql_fetch('ENG', cutoff)
    except requests.RequestException as e:
        log_line(f'SPARQL Fehler: {e}')
        return 1

    rows = merge_rows(de_json, en_json)
    log_line(f'{len(rows)} eindeutige CELEX nach Merge')

    db = get_db()
    cur = db.cursor()
    inserted = 0
    for r in rows:
        celex = r['celex']
        typ, typ_label = extract_typ_from_uri(r.get('type_uri'))
        titel_de = r.get('titel_de')
        titel_en = r.get('titel_en')
        titel_for_rg = titel_de or titel_en or ''
        rechtsgebiet = heuristik_rechtsgebiet(titel_for_rg)
        datum = r.get('datum')
        in_kraft = r.get('force')
        if in_kraft and len(str(in_kraft)) > 20:
            in_kraft = str(in_kraft)[:20]
        eurlex_url = f'https://eur-lex.europa.eu/legal-content/DE/TXT/?uri=CELEX:{celex}'

        try:
            cur.execute(
                '''
                INSERT IGNORE INTO eu_rechtsakte
                (celex, titel_de, titel_en, typ, typ_label, datum, in_kraft, rechtsgebiet, eurlex_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (celex, titel_de, titel_en, typ, typ_label, datum, in_kraft, rechtsgebiet, eurlex_url[:500]),
            )
            if cur.rowcount:
                inserted += 1
        except mysql.connector.Error as e:
            log_line(f'DB Insert Fehler {celex}: {e}')

    db.commit()
    cur.close()
    db.close()
    log_line(f'Fertig, neu eingefügt (INSERT IGNORE Zeilen): {inserted}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
