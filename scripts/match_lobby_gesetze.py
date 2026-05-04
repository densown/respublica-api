#!/usr/bin/env python3
"""
Verknüpft Lobbyregister-Projekte mit deutschen Gesetzen.
Nutzt das affected_laws JSON-Feld aus lobby_regulatory_projects.
Matching über shortTitle (kuerzel) und title (name).
Schreibt in neue Tabelle lobby_gesetze.
"""
import os, json, time
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')
LOG = '/root/apps/gesetze/logs/match_lobby_gesetze.log'

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

    # Verknüpfungstabelle anlegen falls nicht existiert
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lobby_gesetze (
            id INT AUTO_INCREMENT PRIMARY KEY,
            project_id INT NOT NULL,
            gesetz_id INT NOT NULL,
            UNIQUE KEY uniq_link (project_id, gesetz_id),
            KEY idx_project (project_id),
            KEY idx_gesetz (gesetz_id)
        )
    """)
    conn.commit()

    # Alle deutschen Gesetze laden (Kürzel + Name)
    cur.execute("SELECT id, kuerzel, name FROM gesetze")
    gesetze_rows = cur.fetchall()
    # Lookup-Dicts, case-insensitive
    kuerzel_map = {k.upper(): gid for gid, k, n in gesetze_rows}
    name_map = {n.upper(): gid for gid, k, n in gesetze_rows}
    log(f'{len(gesetze_rows)} deutsche Gesetze geladen')

    # Alle Lobby-Projekte mit affected_laws
    cur.execute("""
        SELECT id, affected_laws
        FROM lobby_regulatory_projects
        WHERE affected_laws IS NOT NULL
          AND affected_laws != '[]'
          AND affected_laws != ''
    """)
    projects = cur.fetchall()
    log(f'{len(projects)} Lobby-Projekte mit affected_laws')

    # Bestehende Verknüpfungen laden
    cur.execute("SELECT project_id, gesetz_id FROM lobby_gesetze")
    existing = set((p, g) for p, g in cur.fetchall())
    log(f'{len(existing)} Verknüpfungen existieren bereits')

    new_links = 0
    parse_errors = 0
    projects_with_matches = 0

    for project_id, affected_json in projects:
        try:
            laws = json.loads(affected_json)
        except (json.JSONDecodeError, TypeError):
            parse_errors += 1
            continue

        if not isinstance(laws, list):
            continue

        matched_here = False
        for law in laws:
            if not isinstance(law, dict):
                continue
            short_title = (law.get('shortTitle') or '').strip().upper()
            title = (law.get('title') or '').strip().upper()

            gesetz_id = None
            # Zuerst über shortTitle (exakter)
            if short_title and short_title in kuerzel_map:
                gesetz_id = kuerzel_map[short_title]
            # Fallback über vollen Namen
            elif title and title in name_map:
                gesetz_id = name_map[title]

            if gesetz_id:
                key = (project_id, gesetz_id)
                if key not in existing:
                    cur.execute(
                        "INSERT IGNORE INTO lobby_gesetze (project_id, gesetz_id) VALUES (%s, %s)",
                        (project_id, gesetz_id)
                    )
                    existing.add(key)
                    new_links += 1
                    matched_here = True

        if matched_here:
            projects_with_matches += 1

    conn.commit()
    log(f'Fertig. {new_links} neue Verknüpfungen, {projects_with_matches} Projekte mit Matches, {parse_errors} JSON-Fehler.')
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
