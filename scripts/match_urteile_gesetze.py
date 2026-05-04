#!/usr/bin/env python3
"""
Verknüpft deutsche Gerichtsurteile mit Gesetzen über Kürzel-Erkennung.
Scannt leitsatz, tenor, zusammenfassung, auswirkung nach bekannten Gesetzeskürzeln.
Schreibt in urteil_gesetze (urteil_id, gesetz_kuerzel).
"""
import os, re, time
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')
LOG = '/root/apps/gesetze/logs/match_urteile_gesetze.log'

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

    # Alle Kürzel laden (längere zuerst, damit z.B. "VwGO" vor "GO" matcht)
    cur.execute("SELECT kuerzel FROM gesetze ORDER BY LENGTH(kuerzel) DESC")
    kuerzel_list = [row[0] for row in cur.fetchall()]
    log(f'{len(kuerzel_list)} Gesetzeskürzel geladen')

    # Regex vorkompilieren mit Wortgrenze
    patterns = [(k, re.compile(r'\b' + re.escape(k) + r'\b')) for k in kuerzel_list]

    # Alle Urteile holen
    cur.execute("""
        SELECT id, leitsatz, tenor, zusammenfassung, auswirkung
        FROM urteile
    """)
    urteile = cur.fetchall()
    log(f'{len(urteile)} Urteile zu prüfen')

    # Bestehende Verknüpfungen
    cur.execute("SELECT urteil_id, gesetz_kuerzel FROM urteil_gesetze")
    existing = set((u, k) for u, k in cur.fetchall())
    log(f'{len(existing)} Verknüpfungen existieren bereits')

    new_links = 0
    urteile_with_matches = 0

    for urteil_id, leitsatz, tenor, zus, auswirkung in urteile:
        text = ' '.join(filter(None, [leitsatz, tenor, zus, auswirkung]))
        if not text.strip():
            continue

        matched_here = set()
        for kuerzel, pattern in patterns:
            if pattern.search(text):
                matched_here.add(kuerzel)

        added_here = False
        for kuerzel in matched_here:
            key = (urteil_id, kuerzel)
            if key not in existing:
                cur.execute(
                    "INSERT IGNORE INTO urteil_gesetze (urteil_id, gesetz_kuerzel) VALUES (%s, %s)",
                    (urteil_id, kuerzel)
                )
                existing.add(key)
                new_links += 1
                added_here = True
        if added_here:
            urteile_with_matches += 1

    conn.commit()
    log(f'Fertig. {new_links} neue Verknüpfungen, {urteile_with_matches} Urteile neu gematched.')
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
