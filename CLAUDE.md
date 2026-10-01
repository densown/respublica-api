# respublica-api

Node.js-API (`api/`, PM2-Prozess `api`, Port 3002), Python-Pipelines (`scripts/`), MariaDB `respublica_gesetze`. Auf dem Server: `/root/apps/gesetze`.

- Server, Cronjobs, Tabellen, Skripte: `SERVER-DOKU.md`. Neue oder geänderte Cronjobs dort in Abschnitt 5 eintragen: das Brain vergleicht diese Tabelle jeden Morgen mit der Crontab und warnt bei Abweichungen.
- Pipeline-Planung: `PIPELINE-PLAN.md`

## Dokumentation im Brain

Dieses Repo beschreibt, **wie** etwas funktioniert. Warum etwas entschieden wurde, was gerade kaputt ist und was offen ist, gehört ins Brain (Repo `densown/respublica-brain`, auf dem Server `/root/apps/brain`, Regeln in dessen `CLAUDE.md`).

- Entscheidung getroffen: oben in `projekte/api/entscheidungen.md` unter `## JJJJ-MM-TT`
- Fehler oder Auffälligkeit gefunden: oben in `projekte/api/befunde.md` (Was, Ursache, Fix)
- Etwas bleibt offen: als Aufgabe `- [ ] ...` in `projekte/api/api.md`
- Wikilinks mit vollem Pfad, z. B. `[[projekte/api/befunde|Befunde]]`. Keine Gedankenstriche, keine Emojis.
- In Cloud-Sitzungen auf den zugewiesenen `claude/*`-Branch des Brain-Repos pushen; reine Notizen landen automatisch in `main`.
