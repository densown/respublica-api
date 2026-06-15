# Alerting-Setup (M-005)

Status:
- **Healthchecks.io (Cron): ✅ verdrahtet & getestet** (3 Checks, Pings erreichen HC mit 200).
- **UptimeRobot (API-Uptime): ⏳ offen** — von Luca anzulegen (rein externe Config, kein Code).

> **Sicherheit:** Die Ping-URLs (UUIDs) sind **bewusst NICHT in Git**. Wer eine Ping-URL
> kennt, kann gefälschte success/fail-Pings senden und so das Monitoring aushebeln.
> Sie liegen daher nur in der System-Crontab (`crontab -e`), in `/etc/cron.d/gesetze-sync`
> und – fürs Backup – als `HC_PING_BACKUP=`-Env in der crontab-Zeile. **Nicht** in dieses
> (versionierte) Dokument eintragen.

## Entscheidung (begründet im Refactoring-Plan)

- **Healthchecks.io** = Dead-Man-Switch für Cron-Jobs (meldet *ausbleibende* Pings —
  fängt „Job lief gar nicht / hing / lief leer durch", was reine Fehler-Mails nicht können).
- **UptimeRobot** = externer Check auf `https://api.respublica.media/api/health`
  (fängt den Totalausfall des VPS, den ein lokaler Monitor nicht sehen kann).
- Zustellung an `res.publica.magazin@gmail.com`; für „API down" zusätzlich Telegram
  in UptimeRobot aktivieren (Push wird eher gesehen als Mail).

## ✅ Healthchecks.io (erledigt)

3 Checks angelegt; die Pings sind verdrahtet:

| Check | Schedule (Grace) | Verdrahtung |
|---|---|---|
| `respublica-daily-pipeline` | täglich ~07:15 UTC (Grace 30 min) | crontab: `/start` am ersten Job (06:00 `bundestag_gesetze_diffs.py`), Erfolg/`/fail` am letzten Job (07:10 `summarize_urteile.py`) |
| `respublica-gii-sync` | täglich ~04:15 UTC (Grace 30 min) | `/etc/cron.d/gesetze-sync`: Erfolg/`/fail` nach `gii_sync.py` |
| `respublica-db-backup` | täglich ~03:45 UTC (Grace 30 min) | `HC_PING_BACKUP`-Env in der crontab-Zeile; `backup_gesetze_db.sh` pingt selbst (Trap → `/fail`, Erfolg am Ende) |

**Hinweis (mit M-009 finalisieren):** Erfolg/`/fail` am letzten Pipeline-Job hängt am
Exit-Code von `summarize_urteile.py`. Bis M-009 verlässliche Exit-Codes liefert, fungiert
das v.a. als Liveness-Signal (Pipeline hat ~07:10 erreicht, Server/Cron leben). Echte
Mid-Pipeline-Fehlererkennung folgt mit M-009.

## ⏳ Noch zu tun: UptimeRobot (kostenlos, https://uptimerobot.com)

Rein externe Konfiguration, kein Code/Cron nötig:
- HTTP(s)-Monitor auf `https://api.respublica.media/api/health`, Intervall 5 min.
- Optional „Keyword"-Monitor: erwartet `ok` im Body (`{"status":"ok"}`).
- Alert-Kontakte: Gmail (+ optional Telegram).
- `/api/health` ist vom Rate-Limit ausgenommen (M-002) → die 5-min-Checks werden nicht gedrosselt.

## Granularität

Pro-Einzeljob-Checks bleiben bewusst zurückgestellt (Alert-Fatigue), bis M-009
verlässliche Exit-Codes liefert und ein Job real flaky wird.
