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
| `lobbyregister` | neu (Befüllung via Script/Cron) |
| `lobby_regulatory_projects` | neu (Regelungsvorhaben pro Lobbyeintrag) |
| `urteile` | 578 |
| `urteil_gesetze` | 866 |
| `votes` | neu (Einzelstimmen pro `poll_id`/`mandate_id`) |
| `wahlen` | 49 857 |
| `news_items` | neu (RSS-News inkl. Groq-Zusammenfassungen) |
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

| Methode | Pfad | Kurzbeschreibung |
|---------|------|------------------|
| GET | `/api/gesetze` | Liste Gesetzesänderungen (ohne Diff) |
| GET | `/api/gesetze/stats` | Zähler Gesetze / Änderungen |
| GET | `/api/gesetze/:id` | Einzeländerung inkl. Diff |
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
| GET | `/api/world/trade/:iso3` | Handel Top 10 (`trade_flows_v2`, `partner_name` = ISO3 wie bisher) |

Hinweis: Die Tabellen `world_indicators`, `world_indicator_meta` und `trade_flows` bleiben als Referenz/Backup bestehen; die API liest Kennzahlen aus `data_values` / `data_indicators` / `trade_flows_v2`. SQL-Vorbereitung: `sql/2026-05-04-worldmap-api-prep.sql` (Indikator `EN.ATM.CO2E.PC` in `data_indicators`).
| GET | `/api/news` | News-Liste mit Filter (category/lang/source/since), Pagination und Redis-Cache |
| GET | `/api/news/sources` | Konfigurierte RSS-Quellen aus `config/news-sources.json` |
| GET | `/api/news/briefing` | Tagesbriefing (Groq), Redis-Cache (1h) |

## 5. Cronjobs (root, Stand 7. April 2026)

## DB-Performance-Hinweis

- Tabelle `abstimmungen`: zusätzlicher Index `idx_poll_id (poll_id)` für schnellere Detailabfragen und Join auf `votes`.

| Zeit (UTC) | Skript | Beschreibung |
|------------|--------|--------------|
| 06:00 | `bundestag_gesetze_diffs.py` | Repo `kmein/gesetze`, Diffs letzte 24 h → JSON unter `data/diffs/` |
| 06:05 | `import_diffs_to_db.py` | Import Tages-JSON → `gesetze` / `aenderungen` |
| 06:10 | `fetch_abstimmungen.py` | Namentliche Abstimmungen WP 161 → `abstimmungen` |
| 06:15 | `fetch_bgbl.py` | BGBl-Aktualitätendienst → `bgbl_referenz` an `aenderungen` |
| 06:20 | `match_abstimmungen.py` | Verknüpfung Abstimmungen mit `aenderungen` (DIP/API) |
| 06:25 | `summarize_gesetze.py` | Groq-Zusammenfassungen für Änderungen |
| 06:30 | `fetch_urteile.py` | RSS Bundesgerichte → `urteile` |
| 06:40 | `summarize_urteile.py` | Groq-Zusammenfassungen Urteile |
| 06:45 | `fetch_eu_urteile.py` | EU-Urteile (SPARQL/Scraping) → `eu_urteile` |
| 06:55 | `summarize_eu_urteile.py` | Groq-Zusammenfassungen EU-Urteile |
| 05:30 | `fetch_lobbyregister.py` | Lobbyregister-Import (`sucheDetailJson`) → `lobbyregister` |
| */30 | `node modules/newsFetcher.js` | RSS-Feeds einlesen, deduplizieren (`news_items`), Feed-Cache 15m |
| 06:00 | `node modules/newsSummarizer.js` | Offene News der letzten 48h via Groq zusammenfassen (max. 50) |
| 07:00 | `GET /api/news/briefing` | Briefing generieren und in Redis vorwärmen |
| 03:00 | `DELETE news_items < 30 Tage` | Tägliche Bereinigung alter News |
| */5 | `pm2 jlist` | Schreibt `/root/apps/gesetze/data/pm2-status.json` |

Zusätzlich (nicht Gesetze-Repo): 03:00 `/srv/respublica/scripts/backup_wordpress.sh`.

## 6. Skripte unter `scripts/` (Einzeiler)

| Datei | Zweck |
|-------|--------|
| `backfill_diffs.py` | Letzte 30 Tage Git-Commits im Gesetze-Repo, Diffs nach DB backfillen |
| `backfill_eu_betreff.py` | Betreffzeilen zu EU-Akten aus EUR-Lex-HTML nachziehen |
| `backfill_tenors.py` | Tenor-Felder für Urteile nachziehen (BeautifulSoup) |
| `bundestag_gesetze_diffs.py` | Tages-Diffs aus Git-Repo als JSON |
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
| `match_abstimmungen.py` | DIP-Abgleich `aenderungen` ↔ `abstimmungen` |
| `migrate_eu_urteile.py` | Schema-Hilfe `eu_urteile` / `eu_urteil_rechtsakte` |
| `resummarize_claude.py` | EU-Urteile neu zusammenfassen (Claude CLI) |
| `resummarize_rechtsakte.py` | EU-Rechtsakte neu zusammenfassen (Claude) |
| `summarize_eu_recht.py` | KI-Zusammenfassungen `eu_rechtsakte` (Groq) |
| `summarize_eu_urteile.py` | KI-Zusammenfassungen `eu_urteile` DE/EN (Groq) |
| `summarize_gesetze.py` | Kurz-Zusammenfassungen `aenderungen` (Groq) |
| `summarize_urteile.py` | Kurz-Zusammenfassungen Bundesgerichte (Groq) |
| `modules/newsFetcher.js` | RSS-Aggregator: Feeds aus `config/news-sources.json` laden, speichern, deduplizieren |
| `modules/newsSummarizer.js` | Groq-Summaries für aktuelle News (`news_items.groq_summary`) |

## 7. Logs

Cron-/Import-Ausgaben: `logs/cron.log`; Lobbyregister-Sync: `logs/fetch_lobbyregister.log`; Stimmen-Sync: `logs/fetch_votes.log`; Foto-Sync Abgeordnete: `logs/fetch_abgeordnete_fotos.log`; weitere Logdateien z. B. in `logs/` pro Skript.

---

**Zuletzt aktualisiert:** 4. Mai 2026
