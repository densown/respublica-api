# Alerting-Setup (M-005)

Status: **wartet auf Account-Anlage durch Luca.** Sobald die unten markierten
Werte vorliegen, verdrahtet Claude Cron + Doku (Pipeline-Pings final mit M-009).

## Entscheidung (begründet im Refactoring-Plan)

- **Healthchecks.io** = Dead-Man-Switch für Cron-Jobs (meldet *ausbleibende* Pings —
  fängt „Job lief gar nicht / hing / lief leer durch", was reine Fehler-Mails nicht können).
- **UptimeRobot** = externer Check auf `https://api.respublica.media/api/health`
  (fängt den Totalausfall des VPS, den ein lokaler Monitor nicht sehen kann).
- Zustellung an `res.publica.magazin@gmail.com`; für „API down" zusätzlich Telegram
  in UptimeRobot aktivieren (Push wird eher gesehen als Mail).

## Was Luca anlegen muss

### 1. Healthchecks.io (kostenlos, https://healthchecks.io)
Account anlegen, dann **3 Checks** erstellen und je die Ping-URL kopieren:

| Check-Name | Schedule (Grace) | Ping-URL hier eintragen |
|---|---|---|
| `respublica-daily-pipeline` | täglich, erwartet ~07:15 UTC (Grace 30 min) | `PING_URL_PIPELINE=` |
| `respublica-gii-sync`       | täglich, erwartet ~04:15 UTC (Grace 30 min) | `PING_URL_GII=` |
| `respublica-db-backup`      | täglich, erwartet ~03:45 UTC (Grace 30 min) | `PING_URL_BACKUP=` |

E-Mail-Integration (Standard) aktiv lassen; optional Telegram hinzufügen.

### 2. UptimeRobot (kostenlos, https://uptimerobot.com)
Einen **HTTP(s)-Monitor** anlegen:
- URL: `https://api.respublica.media/api/health`
- Intervall: 5 min
- Alert-Kontakte: Gmail (+ optional Telegram)
- Optional „Keyword"-Monitor: erwartet den String `"ok"` im Body.

## Was Claude dann verdrahtet (sobald URLs vorliegen)

- **DB-Backup** (`scripts/backup_gesetze_db.sh`, 03:30): am Ende
  `curl -fsS "$PING_URL_BACKUP" || curl -fsS "$PING_URL_BACKUP/fail"`.
- **gii_sync** (04:00, `/etc/cron.d/gesetze-sync`): analog mit `PING_URL_GII`.
- **Tagespipeline** (06:00–07:10, root-crontab): `/start`-Ping am ersten Job,
  Erfolgs-Ping am letzten Job; die Job-Kette nutzt die mit **M-009** eingeführten
  sauberen Exit-Codes, sodass ein harter Fehler den Erfolgs-Ping verhindert.

Granularität pro Einzeljob bleibt bewusst zurückgestellt (Alert-Fatigue), bis M-009
verlässliche Exit-Codes liefert und ein Job real flaky wird.
