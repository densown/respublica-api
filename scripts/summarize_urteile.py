#!/usr/bin/env python3
"""Groq-Zusammenfassungen fuer Bundesgerichts-Urteile (Cron 07:10)."""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env
from lib.groq import GroqError, chat_completion

GROQ_MODEL = "llama-3.1-8b-instant"
PAUSE_SEC = 10


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

    time.sleep(PAUSE_SEC)
    try:
        content = chat_completion(
            [{"role": "user", "content": prompt}],
            model=GROQ_MODEL,
            max_tokens=400,
        )
    except GroqError as e:
        print(f'  Groq-Fehler: {e}')
        return None

    content = content.replace('```json', '').replace('```', '').strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f'  JSON parse error: {e}')
        return None


def main() -> int:
    load_env()
    db = get_db(autocommit=False)
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
                except Exception as e:
                    print(f'  WARN urteil_gesetze mapping {uid}->{kuerzel} failed: {e}', file=sys.stderr)

        db.commit()

    print('Fertig.')
    cur.close()
    db.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
