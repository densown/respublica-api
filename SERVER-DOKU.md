# Server-Dokumentation – respublica_gesetze

## 1. Übersicht

Express-API (Port aus `.env`, auf diesem Server: **3002**), Cron-Skripte unter `scripts/`, MariaDB-Datenbank **`respublica_gesetze`**. Produktivbetrieb API: **PM2**, Prozessname **`api`**, Skript `/root/apps/gesetze/api/index.js`.

## 2. Umgebung

- Konfiguration: `.env` im Projektroot (`DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `PORT`, SMTP, optional KI-Keys).
- API-Start: `pm2 restart api` (Logs: `/root/.pm2/logs/api-*.log`).

## 3. Datenbank `respublica_gesetze`

### Alle Tabellen (aus `SHOW TABLES`)

| Tabelle | Zeilen (Stand 7. April 2026) |
|---------|------------------------------|
| `abgeordnete` | 629 |
| `abstimmungen` | 276 |
| `aenderungen` | 262 |
| `eu_rechtsakte` | 591 |
| `eu_rechtsakt_gesetze` | 6 |
| `eu_urteile` | 347 |
| `eu_urteil_rechtsakte` | 0 |
| `gesetze` | 229 |
| `gesetze_sync_log` | neu (Sync-Log fuer `gii_sync.py`) |
| `lobbyregister` | neu (Befüllung via Script/Cron) |
| `lobby_regulatory_projects` | neu (Regelungsvorhaben pro Lobbyeintrag) |
| `urteile` | 578 |
| `urteil_gesetze` | 866 |
| `votes` | neu (Einzelstimmen pro `poll_id`/`mandate_id`) |
| `wahlen` | 49 857 |
| `world_indicators` | 249 781 |
| `world_indicator_meta` | 51 |

Kernzählen (wie Monitoring-Query):

| Kennzahl | Anzahl |
|----------|--------|
| Gesetze (`gesetze`) | 229 |
| Änderungen (`aenderungen`) | 262 |
| Urteile (`urteile`) | 578 |
| EU-Urteile (`eu_urteile`) | 347 |
| EU-Rechtsakte (`eu_rechtsakte`) | 591 |
| Abgeordnete (`abgeordnete`) | 629 |
| Abstimmungen (`abstimmungen`) | 276 |

### Tabelle `gesetze` (Metadaten gesetze-im-internet.de)

Zusätzliche Spalten (Migration `migrations/005_gii_gesetze.sql`): u. a. `titel_offiziell`, `amtliche_abkuerzung`, `ausfertigung_datum`, `fundstelle_periodikum`, `fundstelle_zitstelle`, `letzter_stand`, `gii_slug`, `gii_doknr`, `gii_builddate`, `gii_last_synced`, `status`. Manuelle BJNR-zu-Kuerzel-Zuordnung: `scripts/gesetze_mapping_overrides.json`.

**Backup vor Schema-Aenderungen:** z. B. `mysqldump -uroot --socket=/var/run/mysqld/mysqld.sock respublica_gesetze gesetze > /root/backup/respublica_gesetze_gesetze_YYYY-MM-DD.sql`

Zusätzliche GII-Migrationen: `migrations/006_gii_titel_text.sql` (`titel_offiziell` TEXT), `migrations/007_gii_fundstelle_widen.sql` (längere Fundstellen-Felder). Täglicher Cron nutzt `.venv/bin/python3` (siehe `config/gesetze-gii-sync.cron.fragment`).

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
| `fraktion`     | Fraktionsbezeichnung (gekürzt ohne „(Bundestag …)“-Suffix) |
| `wahlkreis`    | Wahlkreis-Label aus der API |
| `wahlkreis_nr` | Wahlkreisnummer (falls in der Response vorhanden oder aus dem Label ableitbar) |
| `listenplatz`  | Listenplatz |
| `profil_url`   | Profil-URL auf abgeordnetenwatch.de |
| `foto_url`     | Reserviert für späteren Foto-Import |
| `created_at`   | Anlagezeitpunkt |
| `updated_at`   | Letzte Aktualisierung |

DDL: `migrations/002_abgeordnete.sql`

## 4. HTTP-API

Alle Routen in `api/index.js` sind **GET**-Endpunkte (`app.get`); keine `POST`/`PUT`/`DELETE`-Routen in dieser Datei.

Fehlerbehandlung zentral über `api/lib/errors.js` (Refactoring Phase 2, C1): jeder async Handler ist in `asyncHandler` gewickelt, unbekannte Routen liefern JSON-404 (`{"error":"Nicht gefunden"}`), Fehler landen in der zentralen Error-Middleware (Log mit Methode+URL, generischer 500 `{"error":"Datenbankfehler"}`). 400er-Validierungen bleiben in den Handlern. Verifikation per Response-Snapshot: `scripts/api_snapshot.sh <out-dir>` curlt alle Routen (Happy Path + Invalid-Cases) und legt Body+HTTP-Code als Dateien ab — zwei Läufe vor/nach einer Änderung per `diff -r` vergleichen.

| Methode | Pfad | Kurzbeschreibung |
|---------|------|------------------|
| GET | `/api/gesetze` | Liste Gesetzesänderungen (ohne Diff), inkl. GII-Felder `titel`, `amtliche_abkuerzung`, `ausfertigung_datum`, … |
| GET | `/api/gesetze/stats` | Zähler Gesetze / Änderungen |
| GET | `/api/gesetze/:id` | Einzeländerung inkl. Diff und GII-Metadaten (`letzter_stand`, …) |
| GET | `/api/abstimmungen/latest` | Neueste namentliche Abstimmungen (limit query) |
| GET | `/api/abstimmungen/:poll_id` | Abstimmung nach poll_id |
| GET | `/api/bundestag/sitzverteilung` | Feste Sitzverteilung WP21 |
| GET | `/api/bundestag/abgeordnete` | Alle Abgeordneten |
| GET | `/api/bundestag/abgeordnete/:id` | Ein Abgeordneter (`aw_id`) |
| GET | `/api/bundestag/abstimmungen` | Abstimmungen Übersicht |
| GET | `/api/bundestag/abstimmungen/:pollId` | Abstimmung nach pollId |
| GET | `/api/bundestag/poll-votes/:poll_id` | Einzelstimmen einer Abstimmung (`mandate_id` → `vote`) |
| GET | `/api/abgeordnete` | Alle Abgeordneten (id, aw_id, Name, Fraktion, Wahlkreis, Foto, Profil) |
| GET | `/api/abgeordnete/:aw_id/votes` | Abstimmungshistorie eines Abgeordneten |
| GET | `/api/urteile` | Urteile Liste |
| GET | `/api/urteile/:id` | Urteil Detail |
| GET | `/api/eu-recht/stats` | Statistik EU-Rechtsakte |
| GET | `/api/eu-recht` | EU-Rechtsakte Liste |
| GET | `/api/eu-recht/:id` | EU-Rechtsakt Detail |
| GET | `/api/eu-urteile/stats` | Statistik EU-Urteile |
| GET | `/api/eu-urteile` | EU-Urteile Liste |
| GET | `/api/eu-urteile/:id` | EU-Urteil Detail |
| GET | `/api/lobbyregister` | Lobby-Liste mit Pagination, Suche und Sortierung |
| GET | `/api/lobbyregister/stats` | Lobby-Statistiken inkl. Top-10 nach Ausgaben |
| GET | `/api/lobbyregister/by-field` | Aggregierte Lobby-Ausgaben pro Interessensgebiet (Top 15) |
| GET | `/api/lobbyregister/by-city` | Aggregierte Lobby-Kennzahlen pro Stadt (Top 50, nur aktiv) |
| GET | `/api/lobbyregister/by-time` | Registrierungen pro Monat inkl. kumulierter Summe |
| GET | `/api/lobbyregister/:register_number/projects` | Alle Gesetzesprojekte eines Lobbyeintrags (`lobby_regulatory_projects`) |
| GET | `/api/lobbyregister/:register_number` | Lobby-Detail inkl. Tätigkeitsbeschreibung |
| GET | `/api/lobby-projects/by-law` | Lobbyisten zu einem Gesetz (Suche via `q`, Top 20 nach Ausgaben) |
| GET | `/api/lobby-projects/stats` | Top 10 meistkommentierte Gesetzesprojekte inkl. Lobbybudget |
| GET | `/api/wahlen/types` | Wahlen-Typen |
| GET | `/api/wahlen/years` | Jahre |
| GET | `/api/wahlen/states` | Bundesländer |
| GET | `/api/wahlen/map` | Kartendaten |
| GET | `/api/wahlen/timeseries` | Zeitreihen |
| GET | `/api/wahlen/compare` | Vergleich |
| GET | `/api/wahlen/scatter` | Scatter |
| GET | `/api/wahlen/ranking` | Ranking |
| GET | `/api/wahlen/change` | Wechsel |
| GET | `/api/wahlen/national-average` | Bundesdurchschnitt |
| GET | `/api/wahlen/stats` | Statistik |
| GET | `/api/wahlen/region/:ags` | Region nach AGS |
| GET | `/api/world/categories` | Indikator-Kategorien (`data_indicators` + `world_indicator_meta` für Anzeige-Texte; optional `lang=de` oder `lang=en`, Default `en`) |
| GET | `/api/world/indicators` | Indikatoren (wie oben) |
| GET | `/api/world/map` | Weltkarte (`data_values` + `data_indicators`; Länder-/Aggregat-Felder wie zuvor aus `world_indicators` pro Zeile) |
| GET | `/api/world/country/:code` | Land (Werte aus `data_values`; Kopfzeile wie zuvor erste Zeile aus `world_indicators`) |
| GET | `/api/world/timeseries` | Zeitreihe (`data_values`) |
| GET | `/api/world/compare` | Vergleich (`data_values` + `world_indicators` für `country_name` pro Jahr) |
| GET | `/api/world/ranking` | Ranking (`data_values` + `world_indicators` für Namen) |
| GET | `/api/world/scatter` | Scatter (wie Ranking/Map) |
| GET | `/api/world/stats` | Statistik (`data_values`, `data_indicators`) |
| GET | `/api/world/sources` | Alle Zeilen aus `data_sources` mit `indicator_count` (distinct `data_indicators`), `value_count` = Zeilen in `data_values` über zugehörige Indikatoren plus Zeilen in `trade_flows_v2` mit gleicher `source_id` (für CEPII BACI); Feld `domain` u. a. `worldmap` für bekannte Slugs (`worldbank_wdi`, `vdem`, …, `cepii_baci_hs17`) |
| GET | `/api/world/trade/:iso3` | Handel Top 10 (`trade_flows_v2` + `data_countries`, `partner_name` lokalisiert via `lang=de|en` statt ISO3-Code); optional `?breakdown=sections` liefert zusätzlich `sections_export`/`sections_import`, optional `&partner=ISO3` filtert diese Sections auf ein Reporter-Partner-Landpaar |
| GET | `/api/world/trade/:iso3/timeseries` | Handels-Zeitreihe je Jahr (Exports/Imports aus `hs_section='TOTAL'`, Query `yearMin`, `yearMax`) |

Hinweis: Die Tabellen `world_indicators` und `world_indicator_meta` bleiben als Referenz/Backup bestehen; die API liest Kennzahlen aus `data_values` / `data_indicators` / `trade_flows_v2`. Die Legacy-Tabelle `trade_flows` (v1) wurde am 11.06.2026 gedroppt (Backup: `/root/backups/gesetze/dropped_tables_20260611.sql.gz`). SQL-Vorbereitung: `sql/2026-05-04-worldmap-api-prep.sql` (Indikator `EN.ATM.CO2E.PC`, `data_update_log.context` als JSON, neue Datenquelle `cepii_baci_hs17`).

Hinweis News: Die News-Pipeline (`/api/news*`, `modules/newsFetcher.js`, `modules/newsSummarizer.js`, Tabelle `news_items`) wurde am 11.06.2026 vollständig entfernt (tot seit 17.04.2026, vom Dashboard nie genutzt). Finales Daten-Backup: `/root/backups/gesetze/news_items_final_20260611.sql.gz`.

## 5. Cronjobs (root, Stand 7. April 2026)

## DB-Performance-Hinweis

- Tabelle `abstimmungen`: zusätzlicher Index `idx_poll_id (poll_id)` für schnellere Detailabfragen und Join auf `votes`.

| Zeit (UTC) | Skript | Beschreibung |
|------------|--------|--------------|
| 04:00 | `gii_sync.py` | Metadaten von gesetze-im-internet.de (`gii-toc.xml`, `xml.zip`) nach `gesetze` (Vorschlag: `config/gesetze-gii-sync.cron.fragment` nach `/etc/cron.d/` kopieren) |
| 06:00 | `bundestag_gesetze_diffs.py` | Repo `kmein/gesetze`, Diffs letzte 24 h → JSON unter `data/diffs/` |
| 06:05 | `import_diffs_to_db.py` | Import Tages-JSON → `gesetze` / `aenderungen` |
| 06:10 | `fetch_abstimmungen.py` | Namentliche Abstimmungen WP 161 → `abstimmungen` |
| 06:15 | `fetch_bgbl.py` | BGBl-Aktualitätendienst → `bgbl_referenz` an `aenderungen` |
| 06:20 | `fetch_lobbyregister.py` | Lobbyregister-Import (`sucheDetailJson`) → `lobbyregister` |
| 06:25 | `fetch_urteile.py` | RSS Bundesgerichte → `urteile` |
| 06:30 | `fetch_eu_recht.py` | EU-Rechtsakte SPARQL → `eu_rechtsakte` |
| 06:35 | `fetch_eu_urteile.py` | EU-Urteile (SPARQL/Scraping) → `eu_urteile` |
| 06:45 | `match_abstimmungen.py` | Verknüpfung Abstimmungen mit `aenderungen` (DIP/API) |
| 06:50 | `match_lobby_gesetze.py` | Verknüpfung Lobbyregister ↔ `gesetze` |
| 06:55 | `match_urteile_gesetze.py` | Verknüpfung Urteile ↔ `gesetze` |
| 07:00 | `summarize_gesetze_resilient.py` | Groq-Zusammenfassungen für Änderungen (Retry + Backoff, per-Row-Commit) |
| 07:10 | `summarize_urteile.py` | Groq-Zusammenfassungen Urteile |
| So 03:00 | `weekly_resummarize.sh` | Weekly Resummarize (Claude CLI, bilingual, Qualitätskorrektur) |
| 03:30 | `backup_gesetze_db.sh` | Tägliches `mysqldump`-Backup `respublica_gesetze` nach `/root/backups/gesetze/` (7 Tage Retention; `trade_flows_v2` sonntags separat, 28 Tage) |
| */5 | `pm2 jlist` | Schreibt `/root/apps/gesetze/data/pm2-status.json` |

Zusätzlich (nicht Gesetze-Repo): 03:00 `/srv/respublica/scripts/backup_wordpress.sh`.

Logrotation: `/etc/logrotate.d/respublica-gesetze` rotiert `logs/*.log` wöchentlich (4 Generationen, compress, copytruncate).

## 6. Skripte unter `scripts/` (Einzeiler)

### Gemeinsame Library `scripts/lib/`

Geteilte Infrastruktur für alle Pipeline-Skripte (Refactoring Phase 1, B1). Nutzung: `sys.path` muss `scripts/` enthalten, dann `from lib.db import get_db` etc.

| Datei | Inhalt |
|-------|--------|
| `lib/env.py` | `ROOT` (Projektroot) und `load_env()` (lädt `ROOT/.env`) |
| `lib/db.py` | `get_db(autocommit=True)` mit Unix-Socket-Fallback (Vorlage `gii_sync.py`), `with_connection()` Contextmanager |
| `lib/log.py` | `get_logger(name)` (Datei `logs/{name}.log` + stdout), `acquire_lock(name)`/`release_lock(name)` (PID-File), `install_signal_handlers(callback)` |
| `lib/groq.py` | `chat_completion(messages, model, max_tokens, temperature)` mit voller Retry-Logik (5 Versuche, 429-Backoff, Anti-Abuse-Erkennung) |

| Datei | Zweck |
|-------|--------|
| `backfill_diffs.py` | Letzte 30 Tage Git-Commits im Gesetze-Repo, Diffs nach DB backfillen |
| `backfill_eu_betreff.py` | Betreffzeilen zu EU-Akten aus EUR-Lex-HTML nachziehen |
| `backfill_tenors.py` | Tenor-Felder für Urteile nachziehen (BeautifulSoup) |
| `bundestag_gesetze_diffs.py` | Tages-Diffs aus Git-Repo als JSON |
| `gii_parse.py` | TOC und Norm-XML parsen (lxml) |
| `gii_match.py` | `doknr` / Slug zu `gesetze.id` |
| `gii_sync.py` | Taeglicher inkrementeller Sync inkl. `gesetze_sync_log` |
| `gii_initial_import.py` | Erstimport oder Stichprobe (`--limit N`) |
| `gesetze_mapping_overrides.json` | BJNR zu bestehendem `kuerzel` |
| `enrich_eu_urteile.py` | EU-Urteile anreichern (EUR-Lex, RDF, SPARQL) |
| `fetch_abgeordnete.py` | Abgeordnete AW-API → `abgeordnete` |
| `fetch_abgeordnete_fotos.py` | Fehlende `foto_url`/`politiker_id` in `abgeordnete` per AW-API nachziehen |
| `fetch_abstimmungen.py` | Namentliche Abstimmungen → `abstimmungen` |
| `fetch_votes.py` | Einzelstimmen aus Poll-Details (`related_data=votes`) → `votes` |
| `fetch_bgbl.py` | BGBl-Ticker → Zuordnung zu `aenderungen` |
| `fetch_eu_recht.py` | EU-Rechtsakte SPARQL → `eu_rechtsakte` |
| `fetch_lobbyregister.py` | Lobbyregister API (`sucheDetailJson`) → `lobbyregister` + `lobby_regulatory_projects` (Upsert) |
| `fetch_eu_urteile.py` | EU-Gerichte EuGH/EuG per SPARQL + Fallback |
| `fetch_urteile.py` | RSS rechtsprechung-im-internet → `urteile` |
| `fix_geojson_winding.py` | GeoJSON-Winding für Karten (RFC 7946) |
| `import_diffs_to_db.py` | `data/diffs/YYYY-MM-DD.json` → MariaDB |
| `import_wahlen.py` | GERDA/Wahldaten-CSV → `wahlen` |
| `import_world_indicators.py` | World-Bank-artige Indikatoren → `world_indicators` |
| `import_baci.py` | CEPII BACI HS17 (2017-2024) → `trade_flows_v2` inkl. `TOTAL`, Sections I-XXI und `OTHER` mit Audit-Context in `data_update_log` |
| `match_abstimmungen.py` | DIP-Abgleich `aenderungen` ↔ `abstimmungen` |
| `migrate_eu_urteile.py` | Schema-Hilfe `eu_urteile` / `eu_urteil_rechtsakte` |
| `resummarize_claude.py` | EU-Urteile neu zusammenfassen (Claude CLI) |
| `resummarize_rechtsakte.py` | EU-Rechtsakte neu zusammenfassen (Claude) |
| `summarize_eu_recht.py` | KI-Zusammenfassungen `eu_rechtsakte` (Groq) |
| `summarize_eu_urteile.py` | KI-Zusammenfassungen `eu_urteile` DE/EN (Groq) |
| `summarize_gesetze_resilient.py` | Kurz-Zusammenfassungen `aenderungen` (Groq, Retry + Backoff, `--limit N`) — Produktions-Cron 07:00 |
| `summarize_gesetze_claude.py` | DE+EN-Zusammenfassungen `aenderungen` (Claude CLI, Max Plan) — manuelles Qualitäts-Tool |
| `summarize_urteile.py` | Kurz-Zusammenfassungen Bundesgerichte (Groq) |
| `_archive/summarize_gesetze.py` | Alte Basis-Variante ohne Retry (ersetzt durch `summarize_gesetze_resilient.py`) |
| `backup_gesetze_db.sh` | Tägliches DB-Backup `respublica_gesetze` (Cron 03:30, Retention 7/28 Tage) |
| `api_snapshot.sh` | Response-Snapshot aller API-Routen für Refactoring-Verifikation (`diff -r` vor/nach) |

## 7. Logs

Cron-/Import-Ausgaben: `logs/cron.log`; GII-Sync: `logs/gii_sync_YYYY-MM-DD.log`; Lobbyregister-Sync: `logs/fetch_lobbyregister.log`; Stimmen-Sync: `logs/fetch_votes.log`; Foto-Sync Abgeordnete: `logs/fetch_abgeordnete_fotos.log`; weitere Logdateien z. B. in `logs/` pro Skript. TOC-Cache: `data/gii_toc.xml`.

---

**Zuletzt aktualisiert:** 12. Juni 2026 (Refactoring Phase 2 C1: zentrale Error-Middleware `api/lib/errors.js` [asyncHandler, JSON-404-Fallback, zentraler 500er], ~54 try/catch-Blöcke aus `api/index.js` entfernt, `e.message`-Leak der 2 Trade-Routen gefixt, Snapshot-Skript `scripts/api_snapshot.sh` für Refactoring-Verifikation; davor Phase 0: News-Pipeline entfernt, Legacy-Tabellen gedroppt, DB-Backup + Logrotation; Phase 1 B1: `scripts/lib/` [env, db, log, groq]; Phase 1 B5: Summarizer konsolidiert — `summarize_gesetze_resilient.py` ist Produktions-Cron 07:00, Quota-Abbruch eingebaut)
