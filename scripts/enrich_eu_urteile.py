#!/usr/bin/env python3
"""
Reichert eu_urteile an (Titel, Leitsatz, Parteien, Keywords, Rechtsgebiet)
über EUR-Lex-HTML, EUR-Lex-Suche, Cellar RDF und SPARQL.
"""
from __future__ import annotations

import html as html_module
import logging
import os
import re
import sys
import time
from html import unescape
from pathlib import Path
from urllib.parse import quote, urlencode

import mysql.connector
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

LOG_DIR = Path('/root/apps/gesetze/logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / 'enrich_eu_urteile.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ],
)
log = logging.getLogger('enrich_eu_urteile')

SPARQL_ENDPOINT = 'https://publications.europa.eu/webapi/rdf/sparql'

HTTP_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'de,en;q=0.9',
}

REQUEST_PAUSE = float(os.getenv('EURLEX_ENRICH_PAUSE_SEC', '1.0'))


def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )


def sleep_pause():
    time.sleep(REQUEST_PAUSE)


def normalize_title(s: str | None) -> str | None:
    if not s:
        return None
    t = html_module.unescape(s).replace('\xa0', ' ').strip()
    t = re.sub(r'\s+', ' ', t)
    t = t.replace('#', '. ').replace('..', '.')
    return t.strip(' .') or None


def parse_eurlex_page(html: str) -> str | None:
    soup = BeautifulSoup(html, 'html.parser')
    for sel in ['p#title', '#title', '#PPTitle', 'div#PPTitle']:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return normalize_title(el.get_text(' ', strip=True))
    m = soup.find('meta', attrs={'name': 'DC.title'})
    if m and m.get('content'):
        return normalize_title(m['content'])
    m = soup.find('meta', property='og:title')
    if m and m.get('content'):
        ct = normalize_title(m['content'])
        if ct and not ct.startswith('EUR-Lex'):
            return ct
    h1 = soup.find('h1')
    if h1 and h1.get_text(strip=True):
        return normalize_title(h1.get_text(' ', strip=True))
    return None


def fetch_eurlex_html_title(celex: str, lang: str) -> str | None:
    """lang: DE oder EN"""
    url = (
        f'https://eur-lex.europa.eu/legal-content/{lang}/ALL/'
        f'?uri=CELEX:{quote(celex, safe="")}'
    )
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        sleep_pause()
        if r.status_code != 200:
            log.debug('EUR-Lex %s %s HTTP %s', lang, celex, r.status_code)
            return None
        return parse_eurlex_page(r.text)
    except Exception as e:
        log.warning('EUR-Lex %s %s: %s', lang, celex, e)
        sleep_pause()
        return None


def fetch_eurlex_quick_search(celex: str) -> str | None:
    q = urlencode(
        {
            'qid': '',
            'DTA': '',
            'DB_TYPE_OF_ACT': '',
            'DTS_DOM': '',
            'DTS_SUBDOM': '',
            'CELEX': celex,
            'text': '',
            'scope': 'EURLEX',
            'type': 'quick',
        }
    )
    url = f'https://eur-lex.europa.eu/search.html?{q}'
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        sleep_pause()
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.select('a[href*="legal-content"]'):
            h = a.get('href') or ''
            if celex in h.replace('%', ''):
                t = a.get_text(' ', strip=True)
                if t and len(t) > 15 and not t.lower().startswith('eur-lex'):
                    return normalize_title(t)
    except Exception as e:
        log.debug('Quick search %s: %s', celex, e)
        sleep_pause()
    return None


def parse_cellar_expression_title(xml: str) -> tuple[str | None, str | None]:
    """Aus RDF/XML expression_title mit xml:lang de/en."""
    de = en = None
    for m in re.finditer(
        r'<[^>]*expression_title[^>]*xml:lang="(de|en)"[^>]*>([^<]+)</',
        xml,
        re.I,
    ):
        lang, text = m.group(1).lower(), unescape(m.group(2).strip())
        text = normalize_title(text)
        if not text:
            continue
        if lang == 'de' and not de:
            de = text
        elif lang == 'en' and not en:
            en = text
    if not de and not en:
        for m in re.finditer(
            r'<[^>]*expression_title[^>]*>([^<]+)</', xml, re.I
        ):
            t = normalize_title(unescape(m.group(1).strip()))
            if t:
                de = t
                break
    return de, en


def fetch_cellar_expression(celex: str, lang_suffix: str) -> tuple[str | None, str | None]:
    path = f'{quote(celex, safe="")}.{lang_suffix}'
    url = f'https://publications.europa.eu/resource/celex/{path}'
    headers = {**HTTP_HEADERS, 'Accept': 'application/rdf+xml'}
    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        sleep_pause()
        if r.status_code != 200:
            return None, None
        return parse_cellar_expression_title(r.text)
    except Exception as e:
        log.debug('Cellar expr %s.%s: %s', celex, lang_suffix, e)
        sleep_pause()
        return None, None


def discover_cellar_deu_url(celex: str) -> str | None:
    url = f'https://publications.europa.eu/resource/celex/{quote(celex, safe="")}'
    headers = {**HTTP_HEADERS, 'Accept': 'application/rdf+xml'}
    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        sleep_pause()
        if r.status_code != 200:
            return None
        m = re.search(
            r'resource="(https://publications\.europa\.eu/resource/celex/[^"]+\.DEU)"',
            r.text,
        )
        if m:
            return m.group(1)
        m = re.search(r'resource="([^"]+\.DEU)"', r.text)
        if m and m.group(1).startswith('http'):
            return m.group(1)
    except Exception as e:
        log.debug('Cellar work %s: %s', celex, e)
        sleep_pause()
    return None


def fetch_cellar_by_url(url: str) -> tuple[str | None, str | None]:
    headers = {**HTTP_HEADERS, 'Accept': 'application/rdf+xml'}
    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        sleep_pause()
        if r.status_code != 200:
            return None, None
        return parse_cellar_expression_title(r.text)
    except Exception as e:
        log.debug('Cellar URL %s: %s', url[:80], e)
        sleep_pause()
        return None, None


def fetch_cellar_xml_language_param(celex: str) -> str | None:
    url = f'https://publications.europa.eu/resource/celex/{quote(celex, safe="")}?language=deu'
    headers = {**HTTP_HEADERS, 'Accept': 'application/xml'}
    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        sleep_pause()
        if r.status_code != 200:
            return None
        de, en = parse_cellar_expression_title(r.text)
        return de or en
    except Exception as e:
        log.debug('Cellar ?language=deu %s: %s', celex, e)
        sleep_pause()
        return None


def run_sparql_row(celex: str) -> tuple[str | None, str | None, list[str]]:
    subjects: list[str] = []
    q = f'''PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?title_de ?title_en ?subject
WHERE {{
  ?work cdm:resource_legal_id_celex "{celex}" .
  OPTIONAL {{ ?work cdm:work_has_expression ?expr_de .
             ?expr_de cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/DEU> .
             ?expr_de cdm:expression_title ?title_de . }}
  OPTIONAL {{ ?work cdm:work_has_expression ?expr_en .
             ?expr_en cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
             ?expr_en cdm:expression_title ?title_en . }}
  OPTIONAL {{ ?work cdm:work_has_subject ?subj . ?subj skos:prefLabel ?subject . FILTER(LANG(?subject) = "de") }}
}}
LIMIT 20'''
    try:
        r = requests.post(
            SPARQL_ENDPOINT,
            data=urlencode({'query': q}),
            headers={
                **HTTP_HEADERS,
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/sparql-results+json',
            },
            timeout=60,
        )
        sleep_pause()
        if r.status_code != 200:
            return None, None, []
        data = r.json()
        bindings = data.get('results', {}).get('bindings', [])
        td = te = None
        seen_subj = set()
        for b in bindings:
            if not td and b.get('title_de', {}).get('value'):
                td = normalize_title(b['title_de']['value'])
            if not te and b.get('title_en', {}).get('value'):
                te = normalize_title(b['title_en']['value'])
            sv = b.get('subject', {}).get('value')
            if sv and sv not in seen_subj:
                seen_subj.add(sv)
                subjects.append(sv)
        return td, te, subjects
    except Exception as e:
        log.warning('SPARQL %s: %s', celex, e)
        sleep_pause()
        return None, None, []


def extract_parteien(title: str | None) -> str | None:
    if not title:
        return None
    t = title.replace('#', ' ')
    m = re.search(
        r'([\w\s\-\',äöüÄÖÜß]+?)\s+gegen\s+([\w\s\-\',äöüÄÖÜß]+?)(?:\.|,|\s+Rechtssache|\s+Case|\Z)',
        t,
        re.I,
    )
    if m:
        a, b = m.group(1).strip(' .'), m.group(2).strip(' .')
        if len(a) > 2 and len(b) > 2:
            return f'{a} / {b}'
    m = re.search(
        r'([\w\s\-\',äöüÄÖÜß]+?)\s+v\.\s+([\w\s\-\',äöüÄÖÜß]+?)(?:\.|,|\s+Case|\Z)',
        t,
        re.I,
    )
    if m:
        return f'{m.group(1).strip(" .")} / {m.group(2).strip(" .")}'
    return None


def classify_rechtsgebiet(title: str | None) -> str:
    if not title:
        return 'EU-Recht allgemein'
    s = title.lower()
    rules = [
        (
            (
                'competition',
                'wettbewerb',
                'kartell',
                'cartel',
                'state aid',
                'beihilfe',
            ),
            'Wettbewerbsrecht',
        ),
        (
            ('tax', 'steuer', 'vat', 'mehrwertsteuer', 'umsatzsteuer'),
            'Steuerrecht',
        ),
        (
            (
                'asylum',
                'refugee',
                'asyl',
                'flüchtling',
                'flucht',
                'migration',
            ),
            'Migrationsrecht',
        ),
        (
            ('environment', 'umwelt', 'emission', 'waste', 'abfall', 'klima'),
            'Umweltrecht',
        ),
        (
            (
                'data protection',
                'datenschutz',
                'privacy',
                'gdpr',
                'dsgvo',
                'ds-gvo',
            ),
            'Datenschutzrecht',
        ),
        (('consumer', 'verbraucher'), 'Verbraucherrecht'),
        (
            (
                'trade',
                'handel',
                'customs',
                'zoll',
                'dumping',
                'anti-dumping',
            ),
            'Handelsrecht',
        ),
        (
            ('labour', 'labor', 'worker', 'arbeit', 'employment', 'beschäftigung'),
            'Arbeitsrecht',
        ),
        (
            (
                'trademark',
                'patent',
                'intellectual property',
                'marke',
                'urheber',
            ),
            'IP-Recht',
        ),
        (
            ('agriculture', 'landwirtschaft', 'fisheries', 'fisch'),
            'Agrarrecht',
        ),
        (
            ('transport', 'verkehr', 'aviation', 'luftfahrt', 'seeverkehr'),
            'Verkehrsrecht',
        ),
    ]
    for keys, label in rules:
        if any(k in s for k in keys):
            return label
    return 'EU-Recht allgemein'


def is_only_celex_betreff(betreff: str | None, celex: str) -> bool:
    if not betreff:
        return True
    b = betreff.strip()
    return b == celex or b.replace(' ', '') == celex


def enrich_one(celex: str) -> dict | None:
    """
    Liefert dict für UPDATE oder None.
    Quellen-Reihenfolge: EUR-Lex DE → EN → Quick-Suche → Cellar → SPARQL.
    """
    title_de = title_en = None
    subjects: list[str] = []

    # 1) EUR-Lex DE
    title_de = fetch_eurlex_html_title(celex, 'DE')
    if is_only_celex_betreff(title_de, celex):
        title_de = None

    # 2) EUR-Lex EN (nur wenn DE kein brauchbarer Titel)
    if not title_de:
        t_en_page = fetch_eurlex_html_title(celex, 'EN')
        if t_en_page and not is_only_celex_betreff(t_en_page, celex):
            title_en = t_en_page
            title_de = t_en_page

    # 3) Quick search
    if not title_de or is_only_celex_betreff(title_de, celex):
        qt = fetch_eurlex_quick_search(celex)
        if qt and not is_only_celex_betreff(qt, celex):
            title_de = qt

    # 4) Cellar
    cd, ce = fetch_cellar_expression(celex, 'DEU')
    if cd and (not title_de or is_only_celex_betreff(title_de, celex)):
        title_de = cd
    if ce:
        title_en = title_en or ce
    if not title_de or is_only_celex_betreff(title_de, celex):
        cde, cee = fetch_cellar_expression(celex, 'ENG')
        if cde and (not title_de or is_only_celex_betreff(title_de, celex)):
            title_de = cde
        if cee:
            title_en = title_en or cee

    if not title_de or is_only_celex_betreff(title_de, celex):
        deu_url = discover_cellar_deu_url(celex)
        if deu_url:
            cd2, ce2 = fetch_cellar_by_url(deu_url)
            if cd2:
                title_de = cd2
            if ce2:
                title_en = title_en or ce2

    if not title_de or is_only_celex_betreff(title_de, celex):
        cx = fetch_cellar_xml_language_param(celex)
        if cx:
            title_de = cx

    # 5) SPARQL (Titel + Subjects)
    sd, se, subs = run_sparql_row(celex)
    subjects.extend(subs)
    if sd and (not title_de or is_only_celex_betreff(title_de, celex)):
        title_de = sd
    if se:
        title_en = title_en or se

    final_title = title_de or title_en
    if not final_title or is_only_celex_betreff(final_title, celex):
        return None

    kw = '; '.join(dict.fromkeys(subjects)) if subjects else None

    return {
        'betreff': final_title,
        'parteien': extract_parteien(final_title),
        'leitsatz': final_title,
        'keywords': kw,
        'rechtsgebiet': classify_rechtsgebiet(final_title),
    }


def main():
    db = get_db()
    cur = db.cursor()
    cur.execute(
        '''
        SELECT id, celex, betreff FROM eu_urteile
        WHERE leitsatz IS NULL OR leitsatz = ''
        ORDER BY id
        '''
    )
    rows = cur.fetchall()
    total = len(rows)
    log.info('%s Urteile ohne Leitsatz', total)
    ok = 0
    fail = 0

    for i, (uid, celex, betreff) in enumerate(rows):
        try:
            if i % 10 == 0 and i > 0:
                log.info('Fortschritt %s/%s (ok=%s fail=%s)', i, total, ok, fail)
            data = enrich_one(celex)
            if not data:
                log.debug('Keine Daten: id=%s celex=%s', uid, celex)
                fail += 1
                continue
            cur.execute(
                '''
                UPDATE eu_urteile SET
                  betreff = %s, parteien = %s, leitsatz = %s, keywords = %s,
                  rechtsgebiet = %s
                WHERE id = %s
                ''',
                (
                    data['betreff'],
                    data['parteien'],
                    data['leitsatz'],
                    data['keywords'],
                    data['rechtsgebiet'],
                    uid,
                ),
            )
            db.commit()
            ok += 1
        except Exception as e:
            log.exception('id=%s celex=%s: %s', uid, celex, e)
            fail += 1
            db.rollback()

    log.info('Fertig: %s angereichert, %s übersprungen/fehler, total=%s', ok, fail, total)
    cur.close()
    db.close()


if __name__ == '__main__':
    main()
