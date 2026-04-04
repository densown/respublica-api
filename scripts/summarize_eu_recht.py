#!/usr/bin/env python3
"""KI-Zusammenfassungen für eu_rechtsakte (Groq)."""
import os
import time
import requests
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions'


def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )


def summarize_titel(titel_de):
    prompt = (
        'Fasse den folgenden EU-Rechtsakt in 2-3 Sätzen auf Deutsch zusammen. '
        'Erkläre kurz was er regelt und wen er betrifft. '
        f'Titel: {titel_de}'
    )
    for attempt in range(3):
        r = requests.post(
            GROQ_URL,
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': 'llama-3.1-8b-instant',
                'max_tokens': 400,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=60,
        )
        if r.status_code == 429:
            if attempt == 2:
                return None
            time.sleep(5 if attempt == 0 else 15)
            continue
        r.raise_for_status()
        data = r.json()
        content = data['choices'][0]['message']['content']
        return content.strip()
    return None


def main():
    if not GROQ_API_KEY:
        print('GROQ_API_KEY fehlt in .env')
        return 1
    db = get_db()
    cur = db.cursor()
    cur.execute(
        '''
        SELECT id, titel_de
        FROM eu_rechtsakte
        WHERE zusammenfassung IS NULL
          AND titel_de IS NOT NULL
          AND TRIM(titel_de) != ''
        '''
    )
    rows = cur.fetchall()
    y_total = len(rows)
    print(f'{y_total} EU-Rechtsakte zu Zusammenfassungen')

    written = 0
    errors = 0

    for i, (eid, titel_de) in enumerate(rows):
        print(f'  [{i + 1}/{y_total}] id={eid}')
        try:
            text = summarize_titel(titel_de)
            if text:
                cur.execute(
                    'UPDATE eu_rechtsakte SET zusammenfassung = %s WHERE id = %s',
                    (text, eid),
                )
                db.commit()
                written += 1
                if written % 25 == 0:
                    time.sleep(30)
            else:
                errors += 1
        except Exception as e:
            print(f'  Fehler id={eid}: {e}')
            errors += 1
            db.rollback()
        time.sleep(2)

    cur.close()
    db.close()
    print(f'{written} von {y_total} Zusammenfassungen geschrieben, {errors} Fehler')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
