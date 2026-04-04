# Server-Dokumentation – respublica_gesetze

## 1. Übersicht

Dieses Projekt liefert eine Express-API (Port standardmäßig `3002`, aus `.env`), Cron-Skripte und eine MariaDB-Datenbank `respublica_gesetze`.

## 2. Umgebung

- Konfiguration: `.env` im Projektroot (`DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `PORT`).
- API-Start: typischerweise `pm2` (z. B. Prozessname `api`).

## 3. Datenbank

### Tabelle `abgeordnete`

Bundestagsabgeordnete (21. Wahlperiode), befüllt per `scripts/fetch_abgeordnete.py` aus der Abgeordnetenwatch API v2.

| Spalte         | Bedeutung |
|----------------|-----------|
| `id`           | Interner Primärschlüssel (AUTO_INCREMENT) |
| `aw_id`        | Eindeutige Mandats-ID von Abgeordnetenwatch (Candidacy/Mandate) |
| `politiker_id` | Abgeordnetenwatch Politiker-ID |
| `vorname`      | Vorname |
| `nachname`     | Nachname |
| `name`         | Anzeigename / Label aus der API |
| `fraktion`     | Fraktionsbezeichnung ( gekürzt ohne „(Bundestag …)“-Suffix ) |
| `wahlkreis`    | Wahlkreis-Label aus der API |
| `wahlkreis_nr` | Wahlkreisnummer (falls in der Response vorhanden oder aus dem Label ableitbar) |
| `listenplatz`  | Listenplatz |
| `profil_url`   | Profil-URL auf abgeordnetenwatch.de |
| `foto_url`     | Reserviert für späteren Foto-Import |
| `created_at`   | Anlagezeitpunkt |
| `updated_at`   | Letzte Aktualisierung |

DDL: `migrations/002_abgeordnete.sql`

## 4. HTTP-API (Auszug Bundestag)

| Methode | Pfad | Beschreibung |
|---------|------|--------------|
| GET | `/api/bundestag/abgeordnete` | Alle Einträge aus `abgeordnete`, sortiert nach `fraktion`, `nachname` |
| GET | `/api/bundestag/abgeordnete/:id` | Ein Eintrag; `:id` ist die Abgeordnetenwatch-Mandats-ID (`aw_id`) |

Weitere Endpunkte siehe `api/index.js` (Gesetze, Abstimmungen, EU-Recht, Urteile, …).

## 5. Logs

Cron-/Import-Logs werden u. a. unter `logs/cron.log` geschrieben (siehe Skripte).

## 6. Cronjobs

Empfohlene / bestehende periodische Aufgaben (Zeiten nach Bedarf anpassen):

- **EU-Recht:** `scripts/fetch_eu_recht.py` (EUR-Lex / SPARQL)
- **Abgeordnete Bundestag:** `python3 /root/apps/gesetze/scripts/fetch_abgeordnete.py` – aktualisiert Tabelle `abgeordnete` aus `candidacies-mandates` (Wahlperiode 21, `parliament_period=161`). Zwischen API-Seiten 2 s Pause; wiederholbar per `INSERT … ON DUPLICATE KEY UPDATE`.

Nach Schemaänderungen: Migration ausführen, Import-Skript testen, API neu starten (`pm2 restart api`), Endpunkte prüfen (z. B. `curl http://localhost:3002/api/bundestag/abgeordnete`).
