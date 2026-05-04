#!/usr/bin/env python3
"""
KI-Zusammenfassungen (DE + EN) für eu_urteile via Groq.
"""
import argparse
import json
import os
import sys
import time
import requests
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions'
MODEL = 'llama-3.1-8b-instant'

PROMPT_SUMMARY_DE = """Du bist ein juristischer Experte. Fasse das folgende EU-Gerichtsurteil in exakt 2-3 Sätzen auf Deutsch zusammen. Verwende NUR die unten angegebenen Informationen. Sage NIEMALS 'Ich konnte nichts finden' oder 'Ich habe keine Informationen' oder 'Leider'. Wenn der Betreff auf Französisch ist, übersetze ihn ins Deutsche.

Gericht: {gericht}
Datum: {datum}
Betreff: {betreff}
Volltext:
{fulltext}

Zusammenfassung:"""

PROMPT_SUMMARY_EN = """You are a legal expert. Summarize the following EU court ruling in exactly 2-3 sentences in English. Use ONLY the information provided below. NEVER say 'I could not find' or 'I have no information' or 'Unfortunately'. If the subject is in French or German, translate it to English.

Court: {gericht}
Date: {datum}
Subject: {betreff}
Full text:
{fulltext}

Summary:"""

PROMPT_IMPACT_DE = """Erkläre in exakt 1-2 Sätzen auf Deutsch die praktische Auswirkung dieses EU-Urteils. Verwende NUR die folgenden Informationen. Sage NIEMALS dass du keine Informationen hast.

Gericht: {gericht}, Datum: {datum}, Betreff: {betreff}
Volltext:
{fulltext}

Praktische Auswirkung:"""

PROMPT_IMPACT_EN = """Explain in exactly 1-2 sentences in English the practical impact of this EU ruling. Use ONLY the information below. NEVER say you have no information.

Court: {gericht}, Date: {datum}, Subject: {betreff}
Full text:
{fulltext}

Practical impact:"""

LOG_DIR = '/root/apps/gesetze/logs'
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'summarize_eu_urteile.log')


def log(msg):
    line = f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )


def groq_chat(prompt):
    # Groq on_demand ~30 RPM; 4 Aufrufe pro Urteil → min. ~2 s zwischen Requests
    pause = float(os.getenv('GROQ_CALL_PAUSE_SEC', '2.1'))
    for attempt in range(6):
        time.sleep(pause)
        r = requests.post(
            GROQ_URL,
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': MODEL,
                'max_tokens': 500,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=60,
        )
        data = r.json()
        if 'choices' in data:
            msg = data['choices'][0].get('message') or {}
            content = msg.get('content')
            return (content or '').strip()
        err = data.get('error')
        if isinstance(err, dict):
            err_s = json.dumps(err, ensure_ascii=False)[:400]
            if err.get('code') == 'rate_limit_exceeded':
                log(f'  Groq rate limit, Versuch {attempt + 1}: {err_s[:200]}')
                time.sleep(3 + attempt * 2)
                continue
        elif err is not None:
            err_s = str(err)[:400]
        else:
            err_s = json.dumps(data, ensure_ascii=False)[:400]
        log(f'  Groq error: {err_s}')
        return None
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None, help='Max. Anzahl Urteile')
    args = ap.parse_args()

    db = get_db()
    cur = db.cursor()
    cur.execute("SHOW COLUMNS FROM eu_urteile")
    available_cols = {row[0] for row in cur.fetchall()}
    fulltext_candidates = [c for c in ("volltext", "text_de", "text_en") if c in available_cols]
    if not fulltext_candidates:
        log("Keine Volltext-Spalte in eu_urteile gefunden (volltext/text_de/text_en) - nichts zu summarizen.")
        cur.close()
        db.close()
        return

    fulltext_sql = ", ".join(fulltext_candidates)
    cur.execute(
        f'''
        SELECT id, gericht, datum, betreff, leitsatz, {fulltext_sql}
        FROM eu_urteile
        WHERE zusammenfassung_de IS NULL
        ORDER BY datum DESC, id DESC
        '''
    )
    rows = cur.fetchall()
    if args.limit is not None:
        rows = rows[: args.limit]
    log(f'{len(rows)} Urteile zu verarbeiten')

    for i, row in enumerate(rows):
        uid, gericht, datum, betreff, _ = row[:5]
        fulltext_values = row[5:]
        log(f'  [{i + 1}/{len(rows)}] id={uid}')
        try:
            fulltext = ""
            for ft in fulltext_values:
                val = (ft or "").strip() if isinstance(ft, str) else ""
                if val:
                    fulltext = val
                    break
            if not fulltext:
                log(f'  überspringe id={uid} (kein Volltext vorhanden)')
                continue

            g = gericht or '—'
            d = str(datum) if datum else '—'
            b = (betreff or '').strip() or '—'

            z_de = groq_chat(PROMPT_SUMMARY_DE.format(gericht=g, datum=d, betreff=b, fulltext=fulltext))
            z_en = groq_chat(PROMPT_SUMMARY_EN.format(gericht=g, datum=d, betreff=b, fulltext=fulltext))
            a_de = groq_chat(PROMPT_IMPACT_DE.format(gericht=g, datum=d, betreff=b, fulltext=fulltext))
            a_en = groq_chat(PROMPT_IMPACT_EN.format(gericht=g, datum=d, betreff=b, fulltext=fulltext))

            if not all([z_de, z_en, a_de, a_en]):
                log(f'  überspringe id={uid} (unvollständige Groq-Antwort)')
                continue

            cur.execute(
                '''
                UPDATE eu_urteile
                SET zusammenfassung_de=%s, zusammenfassung_en=%s,
                    auswirkung_de=%s, auswirkung_en=%s, quality_ok=1
                WHERE id=%s
                ''',
                (z_de, z_en, a_de, a_en, uid),
            )
            db.commit()
        except Exception as e:
            log(f'  Fehler id={uid}: {e}')

    cur.close()
    db.close()
    log('Fertig.')


if __name__ == '__main__':
    if not GROQ_API_KEY:
        print('GROQ_API_KEY fehlt', file=sys.stderr)
        sys.exit(1)
    main()
