#!/usr/bin/env python3
"""
remap_lobby_gesetze.py

Baut die Tabelle lobby_gesetze komplett neu aus den affected_laws JSON-Feldern
in lobby_regulatory_projects. Matched URL-Slug gegen gesetze.gii_slug.

Workflow:
1. Backup lobby_gesetze -> lobby_gesetze_backup_YYYYMMDD_HHMMSS
2. Lade alle gesetze (id, gii_slug) in dict
3. Lade alle projects mit affected_laws
4. Parse JSON, extrahiere URL-Slugs, match gegen gesetze-dict
5. Logging: matched / unmatched URLs mit Sample
6. TRUNCATE lobby_gesetze, INSERT neu
7. Stats-Report am Ende

Usage:
  python3 remap_lobby_gesetze.py --dry-run    # zeigt nur was passieren wuerde
  python3 remap_lobby_gesetze.py --execute    # fuehrt tatsaechlich aus
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from urllib.parse import urlparse

import pymysql

DB_CONFIG = {
    "unix_socket": "/var/run/mysqld/mysqld.sock",
    "user": "root",
    "database": "respublica_gesetze",
    "charset": "utf8mb4",
}


def normalize_slug(url):
    """Extrahiert den slug aus einer gesetze-im-internet.de URL.
    
    Beispiele:
      https://www.gesetze-im-internet.de/sgb_5  -> sgb_5
      https://www.gesetze-im-internet.de/bgb/   -> bgb
      http://gesetze-im-internet.de/estg        -> estg
    """
    if not url:
        return None
    try:
        parsed = urlparse(url.strip())
        path = parsed.path.strip("/").lower()
        if not path:
            return None
        slug = path.split("/")[0]
        return slug if slug else None
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Remap lobby_gesetze via affected_laws")
    parser.add_argument("--dry-run", action="store_true", help="Nur zeigen was passieren wuerde")
    parser.add_argument("--execute", action="store_true", help="Tatsaechlich ausfuehren")
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("FEHLER: Bitte --dry-run ODER --execute angeben")
        sys.exit(1)

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"=== Lobby-Remapping ({mode}) ===")
    print(f"Start: {datetime.now().isoformat()}\n")

    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # Schritt 1: gesetze in Dict laden (slug -> id)
    print("Schritt 1: Lade gesetze (gii_slug -> id) ...")
    cursor.execute("""
        SELECT id, gii_slug, kuerzel, titel_offiziell
        FROM gesetze
        WHERE gii_slug IS NOT NULL AND gii_slug != ''
    """)
    slug_to_gesetz = {}
    slug_to_info = {}
    for row in cursor.fetchall():
        slug = row["gii_slug"].strip().lower()
        slug_to_gesetz[slug] = row["id"]
        slug_to_info[slug] = (row["kuerzel"], row["titel_offiziell"])
    print(f"  -> {len(slug_to_gesetz)} Gesetze mit gii_slug geladen\n")

    # Schritt 2: Projects mit affected_laws laden
    print("Schritt 2: Lade lobby_regulatory_projects mit affected_laws ...")
    cursor.execute("""
        SELECT id, project_number, title, affected_laws
        FROM lobby_regulatory_projects
        WHERE affected_laws IS NOT NULL
          AND affected_laws != '[]'
          AND affected_laws != ''
    """)
    projects = cursor.fetchall()
    print(f"  -> {len(projects)} Projects mit affected_laws geladen\n")

    # Schritt 3: Parsen und matchen
    print("Schritt 3: Parse affected_laws und matche gegen gesetze ...")
    new_links = []          # (project_id, gesetz_id) Tupel
    seen_pairs = set()      # Dedup
    matched_urls = Counter()
    unmatched_urls = Counter()
    parse_errors = 0
    projects_with_at_least_one_match = 0

    for proj in projects:
        try:
            laws = json.loads(proj["affected_laws"])
            if not isinstance(laws, list):
                continue
        except (json.JSONDecodeError, TypeError):
            parse_errors += 1
            continue

        project_matched = False
        for law in laws:
            if not isinstance(law, dict):
                continue
            url = law.get("url")
            slug = normalize_slug(url)
            if not slug:
                continue

            if slug in slug_to_gesetz:
                gesetz_id = slug_to_gesetz[slug]
                pair = (proj["id"], gesetz_id)
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    new_links.append(pair)
                matched_urls[slug] += 1
                project_matched = True
            else:
                unmatched_urls[slug] += 1
        
        if project_matched:
            projects_with_at_least_one_match += 1

    print(f"  -> {parse_errors} JSON-Parse-Fehler")
    print(f"  -> {projects_with_at_least_one_match} von {len(projects)} Projects mit mind. 1 Match")
    print(f"  -> {len(new_links)} eindeutige Project-Gesetz-Paare\n")

    # Schritt 4: Coverage-Stats
    print("Schritt 4: Coverage-Statistik")
    unique_matched_gesetze = len(set(g for _, g in new_links))
    unique_matched_slugs = len(matched_urls)
    unique_unmatched_slugs = len(unmatched_urls)
    print(f"  -> Verlinkte Gesetze (unique): {unique_matched_gesetze}")
    print(f"  -> Davor: 94 / Jetzt: {unique_matched_gesetze} (Faktor {unique_matched_gesetze/94:.1f}x)")
    print(f"  -> Unique Slugs erfolgreich gematched: {unique_matched_slugs}")
    print(f"  -> Unique Slugs NICHT gematched: {unique_unmatched_slugs}\n")

    # Top 15 unmatched (interessant: was fehlt uns?)
    if unmatched_urls:
        print("Top 15 unmatched URLs (Slugs die nicht in gesetze gefunden wurden):")
        for slug, count in unmatched_urls.most_common(15):
            print(f"  - {slug:40s} ({count}x referenziert)")
        print()

    # Top 10 matched zur Kontrolle
    print("Top 10 matched Slugs:")
    for slug, count in matched_urls.most_common(10):
        info = slug_to_info.get(slug, ("?", "?"))
        kuerzel, titel = info
        titel_short = (titel[:50] + "...") if titel and len(titel) > 50 else (titel or "")
        print(f"  - {slug:25s} ({count}x) -> {kuerzel} / {titel_short}")
    print()

    # Schritt 5: Schreiben (nur bei --execute)
    if args.dry_run:
        print("=== DRY-RUN: Nichts geschrieben. ===")
        print(f"Mit --execute wuerden {len(new_links)} Eintraege in lobby_gesetze geschrieben.")
        cursor.close()
        conn.close()
        return

    # EXECUTE Mode
    print("Schritt 5: Backup + Rebuild lobby_gesetze ...")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_table = f"lobby_gesetze_backup_{ts}"

    cursor.execute(f"CREATE TABLE {backup_table} LIKE lobby_gesetze")
    cursor.execute(f"INSERT INTO {backup_table} SELECT * FROM lobby_gesetze")
    cursor.execute(f"SELECT COUNT(*) AS c FROM {backup_table}")
    backup_count = cursor.fetchone()["c"]
    print(f"  -> Backup: {backup_table} ({backup_count} Zeilen)")

    cursor.execute("TRUNCATE TABLE lobby_gesetze")
    print(f"  -> lobby_gesetze geleert")

    # Batch-Insert in 1000er Bloecken
    BATCH = 1000
    inserted = 0
    for i in range(0, len(new_links), BATCH):
        batch = new_links[i:i+BATCH]
        values = ",".join(f"({p},{g})" for p, g in batch)
        cursor.execute(f"INSERT INTO lobby_gesetze (project_id, gesetz_id) VALUES {values}")
        inserted += len(batch)
        if inserted % 10000 == 0 or inserted == len(new_links):
            print(f"  -> {inserted} / {len(new_links)} eingefuegt")

    conn.commit()
    print(f"  -> Commit\n")

    # Final stats
    cursor.execute("SELECT COUNT(*) AS c FROM lobby_gesetze")
    final_count = cursor.fetchone()["c"]
    cursor.execute("SELECT COUNT(DISTINCT gesetz_id) AS c FROM lobby_gesetze")
    final_unique_gesetze = cursor.fetchone()["c"]

    print(f"=== Fertig ===")
    print(f"  Eintraege in lobby_gesetze:  {final_count}")
    print(f"  Unique Gesetze verlinkt:     {final_unique_gesetze}")
    print(f"  Backup-Tabelle:              {backup_table}")
    print(f"  Ende: {datetime.now().isoformat()}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
