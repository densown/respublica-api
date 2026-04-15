#!/usr/bin/env python3
"""Fetch Lobbyregister entries and upsert into MariaDB."""
import argparse
import json
import math
import os
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

API_URL = 'https://www.lobbyregister.bundestag.de/sucheDetailJson'
API_KEY = '5bHB2zrUuHR6YdPoZygQhWfg2CBrjUOi'
PAGE_SIZE = 100
PAGE_SLEEP_SEC = 0.5
LOG_PATH = '/root/apps/gesetze/logs/fetch_lobbyregister.log'

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lobbyregister (
  id INT AUTO_INCREMENT PRIMARY KEY,
  register_number VARCHAR(20) UNIQUE,
  name TEXT,
  legal_form VARCHAR(100),
  city VARCHAR(100),
  country VARCHAR(100),
  active BOOLEAN,
  members_count INT,
  employee_fte DECIMAL(10,2),
  financial_expenses_euro BIGINT,
  financial_year_start DATE,
  financial_year_end DATE,
  fields_of_interest JSON,
  activity_description TEXT,
  regulatory_projects_count INT,
  statements_count INT,
  details_url TEXT,
  first_publication DATE,
  last_update DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""

UPSERT_SQL = """
INSERT INTO lobbyregister (
  register_number,
  name,
  legal_form,
  city,
  country,
  active,
  members_count,
  employee_fte,
  financial_expenses_euro,
  financial_year_start,
  financial_year_end,
  fields_of_interest,
  activity_description,
  regulatory_projects_count,
  statements_count,
  details_url,
  first_publication,
  last_update
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  legal_form = VALUES(legal_form),
  city = VALUES(city),
  country = VALUES(country),
  active = VALUES(active),
  members_count = VALUES(members_count),
  employee_fte = VALUES(employee_fte),
  financial_expenses_euro = VALUES(financial_expenses_euro),
  financial_year_start = VALUES(financial_year_start),
  financial_year_end = VALUES(financial_year_end),
  fields_of_interest = VALUES(fields_of_interest),
  activity_description = VALUES(activity_description),
  regulatory_projects_count = VALUES(regulatory_projects_count),
  statements_count = VALUES(statements_count),
  details_url = VALUES(details_url),
  first_publication = VALUES(first_publication),
  last_update = VALUES(last_update);
"""


def log_line(msg: str) -> None:
    line = f'[{datetime.now().isoformat(timespec="seconds")}] fetch_lobbyregister: {msg}\n'
    print(line, end='')
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as fh:
            fh.write(line)
    except OSError:
        pass


def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME', 'respublica_gesetze'),
    )


def parse_date(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    return s[:10]


def to_int(raw: Any) -> int | None:
    if raw is None or raw == '':
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def to_decimal(raw: Any) -> Decimal | None:
    if raw is None or raw == '':
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None


def normalize_name(identity: dict[str, Any]) -> str | None:
    val = identity.get('name')
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, dict):
        nested = val.get('name')
        if nested is not None:
            return str(nested).strip() or None
    return None


def fetch_page(page: int) -> dict[str, Any]:
    query = urlencode(
        {
            'pageSize': PAGE_SIZE,
            'page': page,
            'sort': 'FINANCIALEXPENSES_DESC',
        }
    )
    headers = {
        'Accept': 'application/json',
        'X-API-Key': API_KEY,
        'User-Agent': 'ResPublicaGesetze/1.0 (+https://respublica.media)',
    }
    url = f'{API_URL}?{query}'
    req = Request(url, headers=headers)
    with urlopen(req, timeout=90) as res:
        return json.loads(res.read().decode('utf-8'))


def extract_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get('results'), list):
        return payload['results']
    if isinstance(payload.get('content'), list):
        return payload['content']
    if isinstance(payload.get('items'), list):
        return payload['items']
    if isinstance(payload.get('data'), list):
        return payload['data']
    return []


def extract_result_count(payload: dict[str, Any], fallback_len: int) -> int:
    keys = ('resultCount', 'totalElements', 'total', 'count')
    for key in keys:
        val = payload.get(key)
        n = to_int(val)
        if n is not None:
            return n
    page_meta = payload.get('page') if isinstance(payload.get('page'), dict) else {}
    for key in ('totalElements', 'total'):
        n = to_int(page_meta.get(key))
        if n is not None:
            return n
    return fallback_len


def row_from_item(item: dict[str, Any]) -> tuple[Any, ...] | None:
    register_number = item.get('registerNumber')
    if register_number is None:
        return None
    register_number = str(register_number).strip()
    if not register_number:
        return None

    identity = item.get('lobbyistIdentity') if isinstance(item.get('lobbyistIdentity'), dict) else {}
    address = identity.get('address') if isinstance(identity.get('address'), dict) else {}
    account = item.get('accountDetails') if isinstance(item.get('accountDetails'), dict) else {}
    employees = (
        item.get('employeesInvolvedInLobbying')
        if isinstance(item.get('employeesInvolvedInLobbying'), dict)
        else {}
    )
    expenses = item.get('financialExpenses') if isinstance(item.get('financialExpenses'), dict) else {}
    activities = (
        item.get('activitiesAndInterests')
        if isinstance(item.get('activitiesAndInterests'), dict)
        else {}
    )
    projects = item.get('regulatoryProjects') if isinstance(item.get('regulatoryProjects'), dict) else {}
    statements = item.get('statements') if isinstance(item.get('statements'), dict) else {}
    details = (
        item.get('registerEntryDetails')
        if isinstance(item.get('registerEntryDetails'), dict)
        else {}
    )

    fields_of_interest = activities.get('fieldsOfInterest')
    if fields_of_interest is None:
        fields_json = json.dumps([], ensure_ascii=False)
    else:
        fields_json = json.dumps(fields_of_interest, ensure_ascii=False)

    legal_form = identity.get('legalForm')
    if isinstance(legal_form, dict):
        legal_form_value = json.dumps(legal_form, ensure_ascii=False)
    elif legal_form is None:
        legal_form_value = None
    else:
        legal_form_value = str(legal_form).strip() or None

    fin_exp = expenses.get('financialExpensesEuro')
    if isinstance(fin_exp, dict):
        from_val = fin_exp.get('from', 0) or 0
        to_val = fin_exp.get('to', 0) or 0
        financial_expenses_euro = (int(from_val) + int(to_val)) // 2
    elif isinstance(fin_exp, (int, float)):
        financial_expenses_euro = int(fin_exp)
    else:
        financial_expenses_euro = None

    return (
        register_number[:20],
        normalize_name(identity),
        legal_form_value,
        (str(address.get('city')).strip() if address.get('city') is not None else None),
        (str(address.get('country')).strip() if address.get('country') is not None else None),
        bool(account.get('activeLobbyist')) if account.get('activeLobbyist') is not None else None,
        to_int(identity.get('membersCount')),
        to_decimal(employees.get('employeeFTE')),
        financial_expenses_euro,
        parse_date(expenses.get('relatedFiscalYearStart')),
        parse_date(expenses.get('relatedFiscalYearEnd')),
        fields_json,
        (str(activities.get('activityDescription')).strip() if activities.get('activityDescription') is not None else None),
        to_int(projects.get('regulatoryProjectsCount')),
        to_int(statements.get('statementsCount')),
        (str(details.get('detailsPageUrl')).strip() if details.get('detailsPageUrl') is not None else None),
        parse_date(account.get('firstPublicationDate')),
        parse_date(account.get('lastUpdateDate')),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fetch Lobbyregister data and upsert into DB.')
    parser.add_argument(
        '--limit',
        type=int,
        default=0,
        help='Limit pages for test runs (e.g. --limit 2). 0 = no limit.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_line('Start')

    try:
        db = get_db()
        cur = db.cursor()
    except mysql.connector.Error as err:
        log_line(f'DB-Verbindung fehlgeschlagen: {err}')
        return 1

    processed = 0
    page = 0

    try:
        cur.execute(CREATE_TABLE_SQL)
        db.commit()

        first = fetch_page(0)
        first_items = extract_entries(first)
        result_count = extract_result_count(first, len(first_items))
        total_pages = max(1, math.ceil(result_count / PAGE_SIZE))

        if args.limit > 0:
            total_pages = min(total_pages, args.limit)
            log_line(f'Testmodus aktiv: max. {total_pages} Seiten')

        # Fallback: some API responses contain all results in one payload.
        if len(first_items) > PAGE_SIZE:
            log_line('Hinweis: API liefert Vollmenge in "results" - nutze lokale Seitenschnitte')
            while page < total_pages:
                start = page * PAGE_SIZE
                end = start + PAGE_SIZE
                items = first_items[start:end]
                if not items:
                    break

                log_line(
                    f'Seite {page + 1}/{total_pages}: {len(items)} Einträge '
                    f'(Fortschritt {(page + 1) / max(total_pages, 1):.0%})'
                )

                for item in items:
                    row = row_from_item(item)
                    if row is None:
                        continue
                    cur.execute(UPSERT_SQL, row)
                    processed += 1

                db.commit()
                page += 1
                if page < total_pages:
                    time.sleep(PAGE_SLEEP_SEC)
        else:
            while page < total_pages:
                payload = first if page == 0 else fetch_page(page)
                items = extract_entries(payload)
                if not items:
                    log_line(f'Seite {page + 1}/{total_pages}: 0 Einträge (Abbruch)')
                    break

                log_line(
                    f'Seite {page + 1}/{total_pages}: {len(items)} Einträge '
                    f'(Fortschritt {(page + 1) / max(total_pages, 1):.0%})'
                )

                for item in items:
                    row = row_from_item(item)
                    if row is None:
                        continue
                    cur.execute(UPSERT_SQL, row)
                    processed += 1

                db.commit()
                page += 1
                if page < total_pages:
                    time.sleep(PAGE_SLEEP_SEC)

        log_line(f'Fertig: {processed} Datensätze verarbeitet (INSERT/UPDATE)')
        return 0
    except (HTTPError, URLError) as err:
        log_line(f'API-Fehler: {err}')
        return 1
    except mysql.connector.Error as err:
        log_line(f'DB-Fehler: {err}')
        return 1
    finally:
        cur.close()
        db.close()


if __name__ == '__main__':
    raise SystemExit(main())
