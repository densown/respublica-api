#!/usr/bin/env python3
"""
Verknüpft EU-Rechtsakte mit deutschen Gesetzen über Kürzel-Erkennung im Titel/Zusammenfassung.
Schreibt in eu_rechtsakt_gesetze.
"""
import os, re, time
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

LOG = '/root/apps/gesetze/logs/match_eu_recht_gesetze.log'

def log(msg):
    line = f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}'
    print(line)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

def get_db():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME'))

def main():
    conn = get_db()
    cur = conn.cursor()

    # Alle deutschen Gesetzes-Kürzel holen (sortiert nach Länge, damit längere zuerst matchen)
    cur.execute("SELECT id, kuerzel FROM gesetze")
    gesetze = sorted(cur.fetchall(), key=lambda x: -len(x[1]))
    log(f'{len(gesetze)} deutsche Gesetze geladen')

    # Alle EU-Rechtsakte
    cur.execute("""
        SELECT id, titel_de, zusammenfassung_de
        FROM eu_rechtsakte
    """)
    rechtsakte = cur.fetchall()
    log(f'{len(rechtsakte)} EU-Rechtsakte zu prüfen')

    # Bestehende Verknüpfungen laden um Duplikate zu vermeiden
    cur.execute("SELECT eu_rechtsakt_id, gesetz_id FROM eu_rechtsakt_gesetze")
    existing = set((r[0], r[1]) for r in cur.fetchall())
    log(f'{len(existing)} Verknüpfungen existieren bereits')

    new_links = 0
    for eu_id, titel, zusammenfassung in rechtsakte:
        text = f'{titel or ""} {zusammenfassung or ""}'
        if not text.strip():
            continue
        for gesetz_id, kuerzel in gesetze:
            # Kürzel mit Wortgrenze (nicht mitten in anderem Wort)
            pattern = r'\b' + re.escape(kuerzel) + r'\b'
            if re.search(pattern, text):
                key = (eu_id, gesetz_id)
                if key not in existing:
                    cur.execute(
                        "INSERT INTO eu_rechtsakt_gesetze (eu_rechtsakt_id, gesetz_id) VALUES (%s, %s)",
                        (eu_id, gesetz_id)
                    )
                    existing.add(key)
                    new_links += 1

    conn.commit()
    log(f'Fertig. {new_links} neue Verknüpfungen erstellt.')
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
