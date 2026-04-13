#!/usr/bin/env python3
"""EU-Rechtsakte mit Claude neu zusammenfassen."""
import json, os, re, subprocess, time
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')
LOG_FILE = '/root/apps/gesetze/logs/resummarize_rechtsakte.log'

def log(msg):
    line = f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME'))

def build_prompt(row):
    uid, celex, titel_de, titel_en, typ_label, eurovoc_tags = row
    return f"""Du bist ein juristischer Experte fuer EU-Recht. Fasse den folgenden EU-Rechtsakt zusammen.
Antworte AUSSCHLIESSLICH mit einem JSON-Objekt (kein Markdown, kein Codeblock, kein Text davor/danach).

Das JSON muss exakt diese zwei Schluessel haben:
- "zusammenfassung_de": Maximal 2 kurze Saetze auf Deutsch. Maximal 200 Zeichen.
- "zusammenfassung_en": Maximum 2 short sentences in English. Maximum 200 characters.

Regeln:
- Verwende NUR die unten angegebenen Informationen.
- Beginne NICHT mit Floskeln.
- Schreibe klar und praezise.
- Erklaere was der Rechtsakt regelt und welchen Zweck er verfolgt.
- Verwende keine typografischen Anfuehrungszeichen wie „ oder ".

--- RECHTSAKT ---
CELEX: {celex or '—'}
Typ: {typ_label or '—'}
Titel (DE): {(titel_de or '').strip() or '—'}
Titel (EN): {(titel_en or '').strip() or '—'}
EuroVoc-Tags: {(eurovoc_tags or '').strip() or '—'}
--- ENDE ---

JSON:"""

def call_claude(prompt):
    try:
        env = os.environ.copy()
        env.pop('ANTHROPIC_API_KEY', None)
        result = subprocess.run(
            ['claude', '--print', '-p', prompt],
            capture_output=True, text=True, timeout=120, env=env)
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
    if not raw: return None
    text = raw.strip()
    if text.startswith('```'):
        lines = text.split('\n')
        lines = [l for l in lines if not l.strip().startswith('```')]
        text = '\n'.join(lines).strip()
    text = text.replace('\u201e', '"').replace('\u201c', '"').replace('\u201d', '"')
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{.*?"zusammenfassung_de".*?\}', text, re.DOTALL)
        if match:
            candidate = match.group(0)
            candidate = candidate.replace('\u201e', '"').replace('\u201c', '"').replace('\u201d', '"')
            try:
                data = json.loads(candidate)
            except json.JSONDecodeError as e:
                log(f'  JSON parse error: {e}')
                log(f'  Rohtext: {text[:300]}')
                return None
        else:
            log(f'  Kein JSON gefunden in: {text[:300]}')
            return None
    z_de = (data.get('zusammenfassung_de') or '').strip()
    z_en = (data.get('zusammenfassung_en') or '').strip()
    if not z_de or not z_en:
        log(f'  Fehlende Felder: de={bool(z_de)}, en={bool(z_en)}')
        return None
    return z_de, z_en

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    db = get_db()
    cur = db.cursor()
    cur.execute('''SELECT id, celex, titel_de, titel_en, typ_label, eurovoc_tags
        FROM eu_rechtsakte WHERE zusammenfassung_de IS NULL ORDER BY datum DESC''')
    rows = cur.fetchall()
    if args.limit: rows = rows[:args.limit]

    log(f'{len(rows)} Rechtsakte zu verarbeiten')
    ok = fail = 0

    for i, row in enumerate(rows):
        uid, celex = row[0], row[1]
        log(f'  [{i+1}/{len(rows)}] id={uid} celex={celex}')
        if args.dry_run:
            print(build_prompt(row)[:500])
            continue
        raw = call_claude(build_prompt(row))
        parsed = parse_response(raw)
        if parsed is None:
            fail += 1
            log(f'  FEHLER id={uid}')
            continue
        z_de, z_en = parsed
        try:
            cur.execute('UPDATE eu_rechtsakte SET zusammenfassung_de=%s, zusammenfassung_en=%s WHERE id=%s',
                (z_de, z_en, uid))
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
