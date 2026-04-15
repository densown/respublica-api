#!/usr/bin/env python3
"""
Bessere Zusammenfassungen für eu_urteile via Claude CLI (claude --print).
"""
import argparse
import json
import os
import subprocess
import sys
import time
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

LOG_DIR = '/root/apps/gesetze/logs'
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'resummarize_claude.log')

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

def build_prompt(row):
    uid, celex, betreff, parteien, keywords, leitsatz = row
    parts = []
    parts.append(f'CELEX: {celex or "—"}')
    parts.append(f'Betreff: {(betreff or "").strip() or "—"}')
    parts.append(f'Parteien: {(parteien or "").strip() or "—"}')
    parts.append(f'Keywords: {(keywords or "").strip() or "—"}')
    parts.append(f'Leitsatz: {(leitsatz or "").strip() or "—"}')
    urteil_block = '\n'.join(parts)
    return f"""Du bist ein juristischer Experte fuer EU-Recht. Ich gebe dir Daten zu einem EU-Gerichtsurteil.
Antworte AUSSCHLIESSLICH mit einem JSON-Objekt (kein Markdown, kein Codeblock, kein Text davor/danach).

Das JSON muss exakt diese zwei Schluessel haben:
- "zusammenfassung_de": Zusammenfassung in 2-3 Saetzen auf Deutsch.
- "zusammenfassung_en": Summary in 2-3 sentences in English.

Regeln:
- Verwende NUR die unten angegebenen Informationen.
- Sage NIEMALS "Ich habe keine Informationen" oder "Leider".
- Wenn der Betreff auf Franzoesisch ist, uebersetze ihn.
- Schreibe klar und praezise fuer ein juristisches Fachpublikum.

--- URTEIL ---
{urteil_block}
--- ENDE ---

JSON:"""

def call_claude(prompt):
    try:
        env = os.environ.copy()
        env.pop('ANTHROPIC_API_KEY', None)
        result = subprocess.run(
            ['claude', '--print', '-p', prompt],
            capture_output=True, text=True, timeout=120, env=env,
        )
        if result.returncode != 0:
            log(f'  claude stderr: {result.stderr.strip()[:300]}')
            return None
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log('  claude timeout (120s)')
        return None
    except FileNotFoundError:
        log('  claude CLI nicht gefunden')
        return None

def parse_response(raw):
    if not raw:
        return None
    text = raw.strip()
    if text.startswith('```'):
        lines = text.split('\n')
        lines = [l for l in lines if not l.strip().startswith('```')]
        text = '\n'.join(lines).strip()
    # JSON aus dem Text extrahieren (auch wenn Text davor/danach steht)
    import re
    match = re.search(r'\{[^{}]*"zusammenfassung_de"[^{}]*\}', text, re.DOTALL)
    if match:
        text = match.group(0)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        log(f'  JSON parse error: {e}')
        log(f'  Rohtext: {text[:300]}')
        return None
    z_de = (data.get('zusammenfassung_de') or '').strip()
    z_en = (data.get('zusammenfassung_en') or '').strip()
    if not z_de or not z_en:
        log(f'  Fehlende Felder in JSON: de={bool(z_de)}, en={bool(z_en)}')
        return None
    return z_de, z_en

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--all', action='store_true', help='Alle Urteile neu zusammenfassen')
    ap.add_argument('--dry-run', action='store_true', help='Nur Prompt anzeigen')
    args = ap.parse_args()

    db = get_db()
    cur = db.cursor()
    where = '1=1' if args.all else 'quality_ok = 0 OR zusammenfassung_de IS NULL'
    cur.execute(f'SELECT id, celex, betreff, parteien, keywords, leitsatz FROM eu_urteile WHERE {where} ORDER BY datum DESC, id DESC')
    rows = cur.fetchall()
    if args.limit:
        rows = rows[:args.limit]

    log(f'{len(rows)} Urteile zu verarbeiten')
    ok = fail = 0

    for i, row in enumerate(rows):
        uid, celex = row[0], row[1]
        log(f'  [{i+1}/{len(rows)}] id={uid} celex={celex}')
        prompt = build_prompt(row)

        if args.dry_run:
            print(prompt[:500])
            continue

        raw = call_claude(prompt)
        parsed = parse_response(raw)
        if parsed is None:
            fail += 1
            log(f'  FEHLER id={uid}')
            continue

        z_de, z_en = parsed
        try:
            cur.execute('UPDATE eu_urteile SET zusammenfassung_de=%s, zusammenfassung_en=%s, quality_ok=1 WHERE id=%s', (z_de, z_en, uid))
            db.commit()
            ok += 1
            log(f'  OK id={uid}')
        except Exception as e:
            fail += 1
            log(f'  DB-Fehler id={uid}: {e}')

    cur.close()
    db.close()
    log(f'Fertig. Erfolg: {ok}, Fehler: {fail}, Gesamt: {len(rows)}')

if __name__ == '__main__':
    main()
