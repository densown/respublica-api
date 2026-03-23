#!/usr/bin/env python3
import os, json, time, requests, mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions'

def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME')
    )

def summarize(gericht, typ, aktenzeichen, leitsatz, tenor):
    text = f"Gericht: {gericht}\nTyp: {typ}\nAktenzeichen: {aktenzeichen}\n"
    if leitsatz:
        text += f"Leitsatz: {leitsatz}\n"
    if tenor:
        text += f"Tenor: {tenor}\n"

    prompt = f"""Du analysierst ein deutsches Bundesgerichtsurteil für das politische Nachrichtenmagazin Res.Publica.

{text}

Antworte NUR mit einem JSON-Objekt, kein Markdown, keine Erklärung:
{{
  "zusammenfassung": "2-3 Sätze: Was hat das Gericht entschieden? Sachlich, präzise, für gebildete Laien verständlich.",
  "auswirkung": "1-2 Sätze: Was bedeutet das konkret für betroffene Bürger, Unternehmen oder die Rechtspraxis?",
  "rechtsgebiet": "Eines von: Zivilrecht, Strafrecht, Öffentliches Recht, Steuerrecht, Arbeitsrecht, Sozialrecht, Verfassungsrecht, Patentrecht, Verwaltungsrecht",
  "gesetze": ["Liste der zitierten Gesetze als Kürzel, z.B. BGB, StGB, GG, AO -- maximal 5, nur die wichtigsten"]
}}"""

    for attempt in range(3):
        try:
            time.sleep(10)
            r = requests.post(GROQ_URL, headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json'
            }, json={
                'model': 'llama-3.1-8b-instant',
                'max_tokens': 400,
                'messages': [{'role': 'user', 'content': prompt}]
            }, timeout=30)
            data = r.json()
            if 'choices' not in data:
                print(f'  Groq kein choices (attempt {attempt+1}): {data.get("error", {}).get("message", str(data)[:100])}')
                time.sleep(10)
                continue
            content = data['choices'][0]['message']['content']
            content = content.replace('```json', '').replace('```', '').strip()
            return json.loads(content)
        except Exception as e:
            print(f'  Fehler (attempt {attempt+1}): {e}')
            time.sleep(10)
    return None

db = get_db()
cur = db.cursor()
cur.execute('''
    SELECT id, gericht, typ, aktenzeichen, leitsatz, tenor
    FROM urteile
    WHERE zusammenfassung IS NULL AND (tenor IS NOT NULL OR leitsatz IS NOT NULL)
''')
rows = cur.fetchall()
print(f'{len(rows)} Urteile zu verarbeiten')

for i, (uid, gericht, typ, az, leitsatz, tenor) in enumerate(rows):
    print(f'  [{i+1}/{len(rows)}] {gericht} {az}')
    result = summarize(gericht, typ, az, leitsatz, tenor)
    if not result:
        continue

    cur.execute('''
        UPDATE urteile SET zusammenfassung = %s, auswirkung = %s, rechtsgebiet = %s
        WHERE id = %s
    ''', (result.get('zusammenfassung'), result.get('auswirkung'), result.get('rechtsgebiet'), uid))

    for kuerzel in result.get('gesetze', []):
        kuerzel = kuerzel.strip()[:50]
        if kuerzel:
            try:
                cur.execute('INSERT IGNORE INTO urteil_gesetze (urteil_id, gesetz_kuerzel) VALUES (%s, %s)', (uid, kuerzel))
            except:
                pass

    db.commit()

print('Fertig.')
cur.close()
db.close()
