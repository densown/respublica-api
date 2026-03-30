#!/usr/bin/env python3
"""
EuGH/EuG: SPARQL (EUR-Lex Cellar) mit Fallback Scraping.
"""
import logging
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode

import mysql.connector
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

LOG_DIR = Path('/root/apps/gesetze/logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / 'fetch_eu_urteile.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ],
)
log = logging.getLogger('fetch_eu_urteile')

SPARQL_ENDPOINT = 'https://publications.europa.eu/webapi/rdf/sparql'
CURIA_SEARCH = 'https://curia.europa.eu/juris/recherche.jsf?language=en'
EURLEX_SEARCH = (
    'https://eur-lex.europa.eu/search.html?type=named&name=caselaw&qid='
    '&DD_YEAR=2025&DD_YEAR=2026'
)

# Ohne Browser-UA liefern publications.europa.eu und eur-lex oft 403/500.
HTTP_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'de,en;q=0.9',
}


def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )


def sparql_date_min():
    d = date.today() - timedelta(days=365)
    return d.isoformat()


def build_sparql():
    dmin = sparql_date_min()
    return f'''PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?work ?celex ?ecli ?date ?court_label ?type_label ?title_de ?title_en
WHERE {{
  ?work cdm:work_has_resource-type ?type .
  FILTER(?type IN (
    <http://publications.europa.eu/resource/authority/resource-type/JUDG>,
    <http://publications.europa.eu/resource/authority/resource-type/ORDER>,
    <http://publications.europa.eu/resource/authority/resource-type/VIEW_AG>
  ))

  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL {{ ?work cdm:work_id_document ?ecli . FILTER(STRSTARTS(STR(?ecli), "ECLI")) }}
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{ ?work cdm:work_created_by_agent ?court . ?court skos:prefLabel ?court_label . FILTER(LANG(?court_label) = "en") }}
  OPTIONAL {{ ?work cdm:resource_legal_type ?rtype . ?rtype skos:prefLabel ?type_label . FILTER(LANG(?type_label) = "en") }}

  OPTIONAL {{ ?work cdm:work_has_expression ?expr_de . ?expr_de cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/DEU> . ?expr_de cdm:expression_title ?title_de . }}
  OPTIONAL {{ ?work cdm:work_has_expression ?expr_en . ?expr_en cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> . ?expr_en cdm:expression_title ?title_en . }}

  FILTER(?date >= "{dmin}"^^xsd:date)
}}
ORDER BY DESC(?date)
LIMIT 500'''


def run_sparql():
    query = build_sparql()
    try:
        r = requests.post(
            SPARQL_ENDPOINT,
            data=urlencode({'query': query}),
            headers={
                **HTTP_HEADERS,
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/sparql-results+json',
            },
            timeout=120,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning('SPARQL fehlgeschlagen: %s', e)
        return []
    bindings = data.get('results', {}).get('bindings', [])
    out = []
    for b in bindings:
        def val(k):
            return b.get(k, {}).get('value')

        celex = val('celex')
        if not celex:
            continue
        out.append({
            'celex': celex.strip(),
            'ecli': (val('ecli') or '').strip() or None,
            'datum': val('date'),
            'court_label': (val('court_label') or '').strip() or None,
            'type_label': (val('type_label') or '').strip() or None,
            'title_de': (val('title_de') or '').strip() or None,
            'title_en': (val('title_en') or '').strip() or None,
        })
    return out


def extract_case_ref(text):
    if not text:
        return None
    m = re.search(r'\b([CT])-(\d+)/(\d{2,4})\b', text, re.I)
    if m:
        return f"{m.group(1).upper()}-{m.group(2)}/{m.group(3)}"
    return None


def court_from_celex_ecli_labels(celex, ecli, court_label):
    s = f'{celex or ""} {ecli or ""} {court_label or ""}'.upper()
    if court_label:
        cl = court_label.lower()
        if 'general court' in cl or 'gericht erster instanz' in cl:
            return 'EuG'
        if 'court of justice' in cl or 'eugh' in cl or 'cjeu' in cl:
            return 'EuGH'
    if re.search(r'\bT-\d+', s):
        return 'EuG'
    if re.search(r'\bC-\d+', s):
        return 'EuGH'
    if celex and celex.startswith('6'):
        if 'TJ' in celex.upper() or re.search(r'6\d{3}TJ', celex):
            return 'EuG'
        return 'EuGH'
    return 'EuGH'


def rechtsgebiet_heuristic(title_de, title_en, type_label):
    blob = f'{title_de or ""} {title_en or ""} {type_label or ""}'.lower()
    pairs = [
        (('competition', 'wettbewerb'), 'Wettbewerbsrecht'),
        (('tax', 'steuer', 'mehrwertsteuer', 'vat'), 'Steuerrecht'),
        (('asylum', 'refugee', 'asyl'), 'Migrationsrecht'),
        (('environment', 'umwelt', 'climate'), 'Umweltrecht'),
        (('data protection', 'datenschutz', 'gdpr', 'ds-gvo'), 'Datenschutzrecht'),
        (('consumer', 'verbraucher'), 'Verbraucherrecht'),
        (('trade', 'handel', 'customs', 'zoll'), 'Handelsrecht'),
        (('labour', 'labor', 'worker', 'arbeit', 'employment'), 'Arbeitsrecht'),
    ]
    for keys, rg in pairs:
        if any(k in blob for k in keys):
            return rg
    return 'EU-Recht allgemein'


def eurlex_url(celex):
    return f'https://eur-lex.europa.eu/legal-content/DE/TXT/?uri=CELEX:{celex}'


def curia_url(case_ref):
    if not case_ref:
        return None
    from urllib.parse import quote
    return f'https://curia.europa.eu/juris/liste.jsf?num={quote(case_ref, safe="")}&language=en'


def fallback_scrape_curia_html():
    """Sehr grobe Extraktion von Suchergebnis-Links (falls SPARQL leer)."""
    rows = []
    try:
        r = requests.get(CURIA_SEARCH, timeout=60, headers=HTTP_HEADERS)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(' ', strip=True)
            if 'document' not in href.lower() and 'liste.jsf' not in href.lower():
                continue
            m = re.search(r'\b([CT])-(\d+)/(\d{2,4})\b', f'{href} {text}', re.I)
            if not m:
                continue
            case_ref = f"{m.group(1).upper()}-{m.group(2)}/{m.group(3)}"
            celex_guess = None
            # CELEX in URL
            mc = re.search(r'CELEX[:(]?([0-9A-Za-z]{6,})', href, re.I)
            if mc:
                celex_guess = mc.group(1)
            if not celex_guess:
                continue
            rows.append({
                'celex': celex_guess,
                'ecli': None,
                'datum': None,
                'court_label': 'Court of Justice' if m.group(1).upper() == 'C' else 'General Court',
                'type_label': None,
                'title_de': text or None,
                'title_en': text or None,
            })
    except Exception as e:
        log.warning('CURIA HTML-Fallback fehlgeschlagen: %s', e)
    dedup = {}
    for row in rows:
        dedup[row['celex']] = row
    return list(dedup.values())[:150]


def fallback_scrape_eurlex():
    rows = []
    try:
        r = requests.get(EURLEX_SEARCH, timeout=60, headers=HTTP_HEADERS)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.select('a[href*="CELEX"]'):
            href = a.get('href') or ''
            m = re.search(r'CELEX[:(]?([0-9A-Za-z]{6,})', href, re.I)
            if not m:
                continue
            celex = m.group(1)
            title = a.get_text(' ', strip=True) or None
            rows.append({
                'celex': celex,
                'ecli': None,
                'datum': None,
                'court_label': None,
                'type_label': None,
                'title_de': title,
                'title_en': title,
            })
    except Exception as e:
        log.warning('EUR-Lex Fallback scrape fehlgeschlagen: %s', e)
    dedup = {}
    for row in rows:
        dedup[row['celex']] = row
    return list(dedup.values())[:200]


def insert_row(cur, db, row):
    celex = row['celex']
    cur.execute('SELECT id FROM eu_urteile WHERE celex = %s', (celex,))
    if cur.fetchone():
        return False
    ecli = row.get('ecli')
    court_label = row.get('court_label')
    title_de = row.get('title_de')
    title_en = row.get('title_en')
    type_label = row.get('type_label')
    gericht = court_from_celex_ecli_labels(celex, ecli, court_label)
    case_ref = extract_case_ref(f'{title_de or ""} {title_en or ""} {ecli or ""}')
    rg = rechtsgebiet_heuristic(title_de, title_en, type_label)
    datum = row.get('datum')
    if datum:
        try:
            datum = datum[:10]
        except Exception:
            datum = None
    betreff = title_de or title_en or celex
    elu = eurlex_url(celex)
    cu = curia_url(case_ref)
    cur.execute(
        '''INSERT INTO eu_urteile (
            celex, ecli, gericht, typ, datum, parteien, betreff, keywords, leitsatz,
            zusammenfassung_de, zusammenfassung_en, auswirkung_de, auswirkung_en,
            rechtsgebiet, eurlex_url, curia_url
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (
            celex, ecli, gericht, type_label, datum, None, betreff, None, None,
            None, None, None, None, rg, elu, cu,
        ),
    )
    db.commit()
    return True


def main():
    log.info('Start fetch_eu_urteile')
    rows = run_sparql()
    log.info('SPARQL: %s Treffer', len(rows))
    if len(rows) < 10:
        log.info('Fallback: EUR-Lex Suche')
        extra = fallback_scrape_eurlex()
        seen = {r['celex'] for r in rows}
        for e in extra:
            if e['celex'] not in seen:
                rows.append(e)
                seen.add(e['celex'])
        log.info('Nach EUR-Lex-Fallback: %s Treffer', len(rows))
    if len(rows) < 10:
        log.info('Fallback: CURIA HTML')
        extra = fallback_scrape_curia_html()
        seen = {r['celex'] for r in rows}
        for e in extra:
            if e['celex'] not in seen:
                rows.append(e)
                seen.add(e['celex'])
        log.info('Nach CURIA-Fallback: %s Treffer', len(rows))

    db = get_db()
    cur = db.cursor()
    neu = 0
    for row in rows:
        try:
            if insert_row(cur, db, row):
                neu += 1
                log.info('Neu: %s', row['celex'])
        except Exception as e:
            log.exception('Fehler bei %s: %s', row.get('celex'), e)
    cur.close()
    db.close()
    log.info('Fertig. %s neue Urteile.', neu)


if __name__ == '__main__':
    main()
