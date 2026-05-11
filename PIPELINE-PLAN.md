# Res.Publica — Neue Gesetze-Pipeline auf Basis gesetze-im-internet.de

**Stand:** 11. Mai 2026  
**Ziel:** Komplette Überarbeitung der Gesetzes-Datenpipeline mit autoritativer Quelle und vollständigen, lesbaren Metadaten.

---

## 1. Warum dieser Umbau

### Probleme der aktuellen Pipeline

- Datenquelle ist `bundestag/gesetze` (GitHub-Repo einer Einzelperson), unzuverlässig
- DB enthält nur Kürzel (`kuerzel`) und Pfade, keine Vollnamen
- Encoding-Bug hat 1798 Zeilen kaputt gemacht (heute repariert)
- Frontend zeigt für Bürger unbenutzbare Datei-Kürzel wie `1-DM-GoldmünzG-BJNR204500000`
- Lobbyregister-Verknüpfungen werden durch unzuverlässige Daten kompromittiert

### Lösung

Migration auf **gesetze-im-internet.de** als Primärquelle:

- Betrieben vom **Bundesamt für Justiz (BfJ)**
- Seit 25+ Jahren stabil, autoritative Quelle
- Tagesaktuelles XML-Inhaltsverzeichnis (gii-toc.xml)
- ~6.787 deutsche Bundesgesetze und Verordnungen
- Frei nutzbar, Deep-Links explizit erlaubt
- DTD-validiertes XML mit allen relevanten Metadaten

---

## 2. Architektur-Übersicht

```
gesetze-im-internet.de
        │
        ├── gii-toc.xml (Index aller Gesetze, täglich)
        │
        └── {slug}/xml.zip (einzelne Gesetze)
                │
                ▼
        Python Pipeline (cronjob, täglich 04:00)
                │
                ├── Phase 1: Download gii-toc.xml
                ├── Phase 2: Detect Changes (über doknr + builddate)
                ├── Phase 3: Download geänderte XMLs
                ├── Phase 4: Parse Metadaten (langue, jurabk, amtabk, ...)
                ├── Phase 5: Update gesetze_v2 Tabelle
                └── Phase 6: Logging
                │
                ▼
        MariaDB: gesetze_v2
                │
                ▼
        Node.js API (/api/gesetze, /api/gesetzgebung)
                │
                ▼
        React Frontend (Gesetzgebungs-Liste mit Vollnamen)
```

---

## 3. Datenquelle: gesetze-im-internet.de

### Inhaltsverzeichnis

URL: `https://www.gesetze-im-internet.de/gii-toc.xml`  
Größe: ~1.4 MB  
Updates: täglich

Struktur:

```xml
<items>
  <item>
    <title>Bürgerliches Gesetzbuch</title>
    <link>http://www.gesetze-im-internet.de/bgb/xml.zip</link>
  </item>
  ...
</items>
```

### Einzelne Gesetze

URL-Pattern: `https://www.gesetze-im-internet.de/{slug}/xml.zip`  
Beispiel: `https://www.gesetze-im-internet.de/bgb/xml.zip`

Enthält eine XML-Datei nach dem Schema `gii-norm.dtd` mit:

```xml
<dokumente builddate="20260506174502" doknr="BJNR001950896">
  <norm builddate="..." doknr="BJNR001950896">
    <metadaten>
      <jurabk>BGB</jurabk>                       <!-- Juristische Abkürzung -->
      <amtabk>BGB</amtabk>                       <!-- Amtliche Abkürzung -->
      <ausfertigung-datum>1896-08-18</...>       <!-- Ausfertigungsdatum -->
      <fundstelle typ="amtlich">
        <periodikum>RGBl</periodikum>
        <zitstelle>1896, 195</zitstelle>
      </fundstelle>
      <langue>Bürgerliches Gesetzbuch</langue>   <!-- VOLLNAME -->
      <standangabe>
        <standtyp>Neuf</standtyp>
        <standkommentar>Neugefasst durch...</standkommentar>
      </standangabe>
      <standangabe>
        <standtyp>Stand</standtyp>
        <standkommentar>zuletzt geändert durch Art. 1 G v. 29.3.2026...</standkommentar>
      </standangabe>
    </metadaten>
    ...
  </norm>
</dokumente>
```

**Die Felder die wir extrahieren:**

| XML-Tag | Bedeutung | Beispiel |
|---|---|---|
| `doknr` (Attribut) | BJNR-Nummer, eindeutige ID | `BJNR001950896` |
| `langue` | Vollständiger Titel | `Bürgerliches Gesetzbuch` |
| `jurabk` | Juristische Abkürzung | `BGB` |
| `amtabk` | Amtliche Abkürzung | `BGB` |
| `ausfertigung-datum` | Datum der Ausfertigung | `1896-08-18` |
| `fundstelle/periodikum` | Veröffentlichungsorgan | `RGBl`, `BGBl` |
| `fundstelle/zitstelle` | Fundstelle | `1896, 195` |
| `standangabe[standtyp=Stand]/standkommentar` | Letzter Änderungsstand | `zuletzt geändert durch...` |
| `builddate` (Attribut auf `dokumente`) | XML-Erstellungsdatum | `20260506174502` |

---

## 4. Mapping zur bestehenden DB

### Aktuelle gesetze-Tabelle

```sql
DESCRIBE gesetze;
+-------------------+--------------+
| id                | int          |
| kuerzel           | varchar(50)  |  -- z.B. "1-DM-GoldmünzG-BJNR204500000"
| name              | varchar(255) |  -- LEER
| pfad              | varchar(255) |  -- z.B. "laws/EBewMG.md"
| zuletzt_geaendert | datetime     |
| created_at        | datetime     |
+-------------------+--------------+
```

### Mapping-Strategie

**Primär-Match über BJNR** (eindeutig):

Aus `kuerzel` die BJNR-Nummer extrahieren:
- `1-DM-GoldmünzG-BJNR204500000` → BJNR-Teil: `BJNR204500000`
- `BGB` (ohne BJNR-Suffix) → fallback notwendig

Im XML aus gesetze-im-internet.de steht `doknr="BJNR001950896"` als Primary Identifier.

**Match per BJNR ist deterministisch und zuverlässig.**

### Fallback für Kürzel ohne BJNR

Etwa 100-200 Kürzel in deiner DB haben kein BJNR-Suffix (BGB, GG, StGB, etc.). Für diese:
1. Versuche `LOWER(kuerzel)` als Slug
2. Falls fehlschlägt, manuelles Mapping in `gesetze_mapping_overrides.json`

---

## 5. Neues DB-Schema

### Migration: neue Spalten in `gesetze` hinzufügen

Wir erweitern die bestehende Tabelle, kein Schema-Bruch:

```sql
ALTER TABLE gesetze
  ADD COLUMN titel_offiziell VARCHAR(500) NULL AFTER name,
  ADD COLUMN amtliche_abkuerzung VARCHAR(50) NULL AFTER titel_offiziell,
  ADD COLUMN ausfertigung_datum DATE NULL,
  ADD COLUMN fundstelle_periodikum VARCHAR(20) NULL,
  ADD COLUMN fundstelle_zitstelle VARCHAR(50) NULL,
  ADD COLUMN letzter_stand TEXT NULL,
  ADD COLUMN gii_slug VARCHAR(100) NULL,
  ADD COLUMN gii_doknr VARCHAR(50) NULL,
  ADD COLUMN gii_builddate VARCHAR(20) NULL,
  ADD COLUMN gii_last_synced DATETIME NULL,
  ADD COLUMN status ENUM('aktiv', 'aufgehoben', 'unbekannt') DEFAULT 'unbekannt',
  ADD INDEX idx_gii_doknr (gii_doknr),
  ADD INDEX idx_gii_slug (gii_slug),
  ADD INDEX idx_titel (titel_offiziell(255));
```

### Neue Tabelle: gesetze_sync_log

```sql
CREATE TABLE gesetze_sync_log (
  id INT PRIMARY KEY AUTO_INCREMENT,
  run_started_at DATETIME NOT NULL,
  run_ended_at DATETIME NULL,
  status ENUM('running', 'success', 'failed') NOT NULL,
  gesetze_total INT DEFAULT 0,
  gesetze_neu INT DEFAULT 0,
  gesetze_geaendert INT DEFAULT 0,
  gesetze_fehler INT DEFAULT 0,
  error_message TEXT NULL,
  INDEX idx_run_started (run_started_at)
);
```

---

## 6. Pipeline-Code-Struktur

### Verzeichnisstruktur

```
/root/apps/gesetze/
├── scripts/
│   ├── gii_sync.py              # Hauptscript (Cronjob)
│   ├── gii_initial_import.py    # Einmaliger Vollimport
│   ├── gii_parse.py             # XML-Parsing-Funktionen
│   ├── gii_match.py             # BJNR-Matching gegen DB
│   └── gesetze_mapping_overrides.json  # Manuelle Mappings für Ausnahmen
├── data/
│   ├── gii_toc.xml              # Cache vom letzten Sync
│   └── xml_cache/               # Lokaler Cache der Einzel-XMLs
├── logs/
│   └── gii_sync_YYYY-MM-DD.log
```

### Pseudo-Code für gii_sync.py

```python
#!/usr/bin/env python3
"""
Synchronisiert deutsche Bundesgesetze von gesetze-im-internet.de
in die respublica_gesetze Datenbank.

Läuft täglich via Cronjob um 04:00.
"""

import requests
import zipfile
import io
from lxml import etree
import mysql.connector
from datetime import datetime
import logging
import json

# Setup logging
logging.basicConfig(
    filename=f'/root/apps/gesetze/logs/gii_sync_{datetime.now():%Y-%m-%d}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# DB connection
def get_db():
    return mysql.connector.connect(
        unix_socket='/var/run/mysqld/mysqld.sock',
        user='root',
        database='respublica_gesetze',
        charset='utf8mb4'
    )

def log_run_start(cur):
    """Erstelle Eintrag im sync_log"""
    cur.execute(
        "INSERT INTO gesetze_sync_log (run_started_at, status) VALUES (NOW(), 'running')"
    )
    return cur.lastrowid

def fetch_toc():
    """Hole gii-toc.xml und parse alle Einträge"""
    url = "https://www.gesetze-im-internet.de/gii-toc.xml"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    root = etree.fromstring(response.content)
    items = []
    for item in root.findall('item'):
        link = item.findtext('link', '')
        title = item.findtext('title', '')
        # Slug aus URL extrahieren: http://www.gesetze-im-internet.de/SLUG/xml.zip
        slug = link.replace('http://www.gesetze-im-internet.de/', '').replace('/xml.zip', '')
        items.append({'slug': slug, 'title': title, 'link': link})
    return items

def fetch_law_xml(slug):
    """Lade ZIP für ein Gesetz herunter, gib parsedes XML zurück"""
    url = f"https://www.gesetze-im-internet.de/{slug}/xml.zip"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Eine XML-Datei pro ZIP
        xml_files = [n for n in z.namelist() if n.endswith('.xml')]
        if not xml_files:
            return None
        with z.open(xml_files[0]) as f:
            return etree.parse(f)

def extract_metadata(tree):
    """Extrahiere relevante Metadaten aus dem XML"""
    root = tree.getroot()
    
    # Top-level Attribute
    doknr = root.get('doknr', '')
    builddate = root.get('builddate', '')
    
    # Erstes <norm> hat die Gesetz-Metadaten
    first_norm = root.find('norm')
    if first_norm is None:
        return None
    
    meta = first_norm.find('metadaten')
    if meta is None:
        return None
    
    return {
        'doknr': doknr,
        'builddate': builddate,
        'langue': meta.findtext('langue', ''),
        'jurabk': meta.findtext('jurabk', ''),
        'amtabk': meta.findtext('amtabk', ''),
        'ausfertigung_datum': meta.findtext('ausfertigung-datum', ''),
        'fundstelle_periodikum': meta.findtext('fundstelle/periodikum', ''),
        'fundstelle_zitstelle': meta.findtext('fundstelle/zitstelle', ''),
        'letzter_stand': extract_letzter_stand(meta),
    }

def extract_letzter_stand(meta):
    """Hole den 'Stand'-Kommentar wenn vorhanden"""
    for standangabe in meta.findall('standangabe'):
        typ = standangabe.findtext('standtyp', '')
        if typ == 'Stand':
            return standangabe.findtext('standkommentar', '')
    return None

def extract_bjnr_from_kuerzel(kuerzel):
    """Extrahiere BJNR-Nummer aus dem Kürzel-String"""
    import re
    match = re.search(r'BJNR\w+', kuerzel or '')
    return match.group(0) if match else None

def match_to_db(cur, doknr):
    """Finde gesetze.id für ein doknr (= BJNR aus XML)"""
    # Strategie 1: kuerzel enthält doknr als BJNR-Suffix
    cur.execute(
        "SELECT id FROM gesetze WHERE kuerzel LIKE %s LIMIT 1",
        (f'%{doknr}%',)
    )
    result = cur.fetchone()
    if result:
        return result[0]
    
    # Strategie 2: Override-Tabelle prüfen (manuelle Mappings)
    overrides = load_overrides()
    if doknr in overrides:
        cur.execute(
            "SELECT id FROM gesetze WHERE kuerzel = %s LIMIT 1",
            (overrides[doknr],)
        )
        result = cur.fetchone()
        if result:
            return result[0]
    
    return None

def load_overrides():
    """Lade manuelle BJNR → kuerzel Mappings"""
    try:
        with open('/root/apps/gesetze/scripts/gesetze_mapping_overrides.json') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def update_law_in_db(cur, gesetz_id, metadata, slug):
    """Update gesetze-Tabelle mit Metadaten"""
    cur.execute("""
        UPDATE gesetze SET
            titel_offiziell = %s,
            amtliche_abkuerzung = %s,
            ausfertigung_datum = NULLIF(%s, ''),
            fundstelle_periodikum = %s,
            fundstelle_zitstelle = %s,
            letzter_stand = %s,
            gii_slug = %s,
            gii_doknr = %s,
            gii_builddate = %s,
            gii_last_synced = NOW(),
            status = 'aktiv'
        WHERE id = %s
    """, (
        metadata['langue'],
        metadata['amtabk'] or metadata['jurabk'],
        metadata['ausfertigung_datum'],
        metadata['fundstelle_periodikum'],
        metadata['fundstelle_zitstelle'],
        metadata['letzter_stand'],
        slug,
        metadata['doknr'],
        metadata['builddate'],
        gesetz_id
    ))

def needs_update(cur, doknr, builddate):
    """Prüfe ob ein Gesetz aktualisiert werden muss"""
    cur.execute(
        "SELECT gii_builddate FROM gesetze WHERE gii_doknr = %s LIMIT 1",
        (doknr,)
    )
    result = cur.fetchone()
    if result is None or result[0] is None:
        return True  # Noch nie synchronisiert
    return result[0] != builddate  # Builddate hat sich geändert

def main():
    logging.info("=== Starting gii_sync ===")
    
    db = get_db()
    cur = db.cursor()
    log_id = log_run_start(cur)
    db.commit()
    
    stats = {'total': 0, 'neu': 0, 'geaendert': 0, 'fehler': 0}
    
    try:
        # Phase 1: Get TOC
        toc = fetch_toc()
        stats['total'] = len(toc)
        logging.info(f"TOC enthält {len(toc)} Gesetze")
        
        # Phase 2-5: Pro Gesetz
        for i, entry in enumerate(toc):
            if i % 100 == 0:
                logging.info(f"Progress: {i}/{len(toc)}")
            
            try:
                # XML laden
                tree = fetch_law_xml(entry['slug'])
                if tree is None:
                    stats['fehler'] += 1
                    continue
                
                metadata = extract_metadata(tree)
                if metadata is None:
                    stats['fehler'] += 1
                    continue
                
                doknr = metadata['doknr']
                
                # Schon synchronisiert und nicht geändert?
                if not needs_update(cur, doknr, metadata['builddate']):
                    continue
                
                # Match in DB finden
                gesetz_id = match_to_db(cur, doknr)
                if gesetz_id is None:
                    # Neues Gesetz - INSERT in gesetze
                    cur.execute("""
                        INSERT INTO gesetze (kuerzel, name, pfad, titel_offiziell, 
                            amtliche_abkuerzung, ausfertigung_datum, 
                            fundstelle_periodikum, fundstelle_zitstelle, letzter_stand,
                            gii_slug, gii_doknr, gii_builddate, gii_last_synced, status)
                        VALUES (%s, '', %s, %s, %s, NULLIF(%s, ''), %s, %s, %s, %s, %s, %s, NOW(), 'aktiv')
                    """, (
                        metadata['jurabk'] or metadata['amtabk'],
                        f"gii/{entry['slug']}",
                        metadata['langue'],
                        metadata['amtabk'] or metadata['jurabk'],
                        metadata['ausfertigung_datum'],
                        metadata['fundstelle_periodikum'],
                        metadata['fundstelle_zitstelle'],
                        metadata['letzter_stand'],
                        entry['slug'],
                        doknr,
                        metadata['builddate']
                    ))
                    stats['neu'] += 1
                else:
                    update_law_in_db(cur, gesetz_id, metadata, entry['slug'])
                    stats['geaendert'] += 1
                
                # Commit alle 50 Zeilen
                if (stats['neu'] + stats['geaendert']) % 50 == 0:
                    db.commit()
                    
            except Exception as e:
                logging.error(f"Fehler bei {entry['slug']}: {e}")
                stats['fehler'] += 1
                continue
        
        db.commit()
        
        # Log success
        cur.execute("""
            UPDATE gesetze_sync_log 
            SET run_ended_at = NOW(), status = 'success',
                gesetze_total = %s, gesetze_neu = %s, 
                gesetze_geaendert = %s, gesetze_fehler = %s
            WHERE id = %s
        """, (stats['total'], stats['neu'], stats['geaendert'], stats['fehler'], log_id))
        db.commit()
        
        logging.info(f"=== Done: {stats} ===")
        
    except Exception as e:
        logging.error(f"FATAL: {e}")
        cur.execute("""
            UPDATE gesetze_sync_log 
            SET run_ended_at = NOW(), status = 'failed', error_message = %s
            WHERE id = %s
        """, (str(e), log_id))
        db.commit()
        raise
    finally:
        cur.close()
        db.close()

if __name__ == '__main__':
    main()
```

---

## 7. Cronjob

```cron
# /etc/cron.d/gesetze-sync
# Täglich 04:00 — synchronisiert gesetze-im-internet.de
0 4 * * * root /usr/bin/python3 /root/apps/gesetze/scripts/gii_sync.py >> /root/apps/gesetze/logs/cron.log 2>&1
```

---

## 8. API-Anpassung

In `/root/apps/gesetze/api/index.js` den Endpoint für Gesetze erweitern. Die SELECT-Statements müssen die neuen Spalten zurückgeben:

```javascript
// /api/gesetze
app.get('/api/gesetze', async (req, res) => {
  const sql = `
    SELECT 
      id,
      COALESCE(titel_offiziell, kuerzel) AS titel,
      kuerzel,
      amtliche_abkuerzung,
      ausfertigung_datum,
      fundstelle_periodikum,
      fundstelle_zitstelle,
      letzter_stand,
      zuletzt_geaendert,
      status,
      gii_slug
    FROM gesetze
    WHERE status = 'aktiv' OR status = 'unbekannt'
    ORDER BY zuletzt_geaendert DESC
    LIMIT ?, ?
  `;
  // ...
});
```

---

## 9. Frontend-Anpassung

In `/root/apps/dashboard/src/pages/Legislation.tsx`:

```tsx
// Statt:
<div className="gesetz-row">
  <span>{gesetz.kuerzel}</span>  // "ZPersAbk_Genf-BJNR209170954"
</div>

// Neu:
<div className="gesetz-row">
  <div className="gesetz-titel">{gesetz.titel}</div>      // "Genfer Personalstatutsabkommen"
  <div className="gesetz-meta">
    <span className="kuerzel">{gesetz.amtliche_abkuerzung}</span>  // "ZPersAbk"
    {gesetz.ausfertigung_datum && (
      <span className="datum">{gesetz.ausfertigung_datum}</span>
    )}
  </div>
</div>
```

CSS:
```css
.gesetz-titel {
  font-family: 'Source Serif 4', serif;
  font-size: 1rem;
  font-weight: 500;
}
.gesetz-meta {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  display: flex;
  gap: 0.5rem;
}
```

---

## 10. Test-Strategie

### Phase 1: Sample-Test (10 Gesetze)

```bash
cd /root/apps/gesetze/scripts
python3 gii_initial_import.py --limit 10
```

Erwartetes Ergebnis: 10 Gesetze in DB mit Vollnamen.

### Phase 2: Verifikation gegen Bekannte

```sql
SELECT id, kuerzel, titel_offiziell, amtliche_abkuerzung
FROM gesetze
WHERE kuerzel IN ('BGB', 'StGB', 'GG', 'AktG')
ORDER BY id;
```

Erwartetes Ergebnis: Klartext-Titel für alle.

### Phase 3: Vollimport

```bash
python3 gii_initial_import.py
```

Dauer: 30-90 Min (geschätzt). 7000+ HTTP-Requests, ZIP-Downloads, XML-Parsing.

### Phase 4: Match-Rate-Analyse

```sql
SELECT 
  COUNT(*) AS total,
  SUM(titel_offiziell IS NOT NULL) AS matched,
  SUM(titel_offiziell IS NULL) AS unmatched
FROM gesetze;
```

Ziel: ≥95% Match-Rate.

### Phase 5: Cronjob aktivieren

```bash
cp /root/apps/gesetze/scripts/gesetze-sync.cron /etc/cron.d/gesetze-sync
systemctl restart cron
```

---

## 11. Was Cursor tun soll — Aufgabenliste

### Setup-Phase
1. Python-Dependencies installieren: `pip3 install lxml requests mysql-connector-python --break-system-packages`
2. Verzeichnisstruktur anlegen
3. Schema-Migration ausführen (ALTER TABLE)
4. Neue Tabelle gesetze_sync_log erstellen

### Implementation-Phase
5. `gii_sync.py` schreiben gemäß Pseudo-Code oben
6. `gii_initial_import.py` als Variante für ersten Vollimport (kein needs_update-Check)
7. `gesetze_mapping_overrides.json` mit ~20 manuellen Mappings für Sonderfälle (BGB, GG, StGB, etc.)
8. API anpassen (`/root/apps/gesetze/api/index.js`)
9. Frontend anpassen (`Legislation.tsx`)

### Test-Phase
10. Sample-Test mit 10 Gesetzen
11. Verifikation gegen bekannte Gesetze (BGB, StGB, GG)
12. Vollimport ausführen
13. Match-Rate-Analyse
14. Frontend-Test (Browser, DE/EN, Light/Dark, Mobile)

### Deployment-Phase
15. Cronjob installieren
16. Backup-Strategie dokumentieren
17. SERVER-DOKU.md aktualisieren

---

## 12. Risiken und Mitigation

| Risiko | Wahrscheinlichkeit | Mitigation |
|---|---|---|
| Rate-Limiting durch gesetze-im-internet.de | Niedrig (kein bekanntes Limit) | Sleep 100ms zwischen Requests |
| Match-Rate <90% | Mittel | Override-JSON für Sonderfälle, manuelles Review |
| ZIP-Parsing schlägt fehl bei manchen Gesetzen | Niedrig | try/except pro Gesetz, weiter zum nächsten |
| Schema-Migration bricht bestehende API | Niedrig | Neue Spalten sind NULLABLE, alte Queries laufen weiter |
| Cronjob bricht Server-Last | Niedrig | 4 Uhr morgens, single-threaded, ~30-90 Min Laufzeit |

---

## 13. Cursor-Prompt für den Run

```
Wir implementieren die neue Gesetze-Pipeline gemäß /mnt/user-data/outputs/gesetze-pipeline-plan.md.

Lies das Dokument komplett durch und arbeite die Aufgabenliste in Sektion 11 ab.

WICHTIG:
- Plan-Mode zuerst: zeige mir den Implementierungsplan bevor du Code schreibst.
- Pro Datei separater Commit für saubere Historie.
- Backup vor Schema-Migration: mysqldump der gesetze-Tabelle nach /root/backup/.
- Nach Sample-Test (10 Gesetze) STOP und melde Ergebnis, bevor Vollimport läuft.
- Python: lxml für XML-Parsing (nicht ElementTree, lxml hat besseres XPath).
- Connections: unix_socket '/var/run/mysqld/mysqld.sock', user 'root', database 'respublica_gesetze'.
- API neu starten nach Code-Änderungen: pm2 restart api.
- Frontend Build prüfen: cd /root/apps/dashboard && npm run build muss grün sein.

Architektur, Schema und Code-Struktur sind im Plan-Dokument vorgegeben. Halte dich strikt daran.
```

---

## 14. Geschätzter Aufwand

| Phase | Zeitaufwand |
|---|---|
| Setup + Schema-Migration | 30 Min |
| Implementation Python | 1.5 - 2h |
| Implementation API-Anpassung | 30 Min |
| Implementation Frontend | 1h |
| Sample-Test + Debugging | 30 Min |
| Vollimport (Laufzeit) | 30-90 Min |
| Frontend-Test + Bugfixes | 30 Min |
| Cronjob-Setup + Doku | 15 Min |
| **Gesamt** | **5-7h Cursor-Zeit** |

---

## 15. Erfolg messen

Die Pipeline ist erfolgreich wenn:

1. ✅ ≥95% der 7127 Gesetze haben `titel_offiziell` befüllt
2. ✅ Frontend zeigt lesbare Klartext-Titel statt Datei-Kürzel
3. ✅ Cronjob läuft täglich ohne Fehler
4. ✅ `gesetze_sync_log` zeigt erfolgreiche Sync-Runs
5. ✅ Lobbyregister-Verknüpfungen bleiben intakt
6. ✅ DE/EN i18n funktioniert (Titel auf Deutsch, UI je nach Sprache)
7. ✅ Mobile-Responsive (Titel umbricht sauber)
8. ✅ Light/Dark Mode beide funktional

---

## 16. Was passiert mit rechtsinformationen.bund.de?

Die offizielle NeuRIS-API (testphase.rechtsinformationen.bund.de) ist aktuell unvollständig (nur 2.424 von 7.127 Gesetzen). Wenn sie in 2. HJ 2026 produktiv geht, können wir parallel beide Quellen nutzen:

- **Primärquelle:** weiterhin gesetze-im-internet.de (vollständig)
- **Erweiterung:** rechtsinformationen.bund.de für moderne Features (ELI-IDs, Versionierung, Volltextsuche)

Das ist eine Erweiterung, kein Ersatz. Die jetzt aufgebaute Pipeline bleibt das Fundament.

---

*Ende des Plan-Dokuments. Stand: 11. Mai 2026.*