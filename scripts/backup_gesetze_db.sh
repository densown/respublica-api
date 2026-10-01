#!/bin/bash
# Tägliches Backup respublica_gesetze, 7 Tage Retention
# trade_flows_v2 (5,3M rows) separat wöchentlich (Sonntag)
set -e
set -o pipefail  # mysqldump-Fehler in der Pipe (| gzip) sollen das Skript abbrechen
BACKUP_DIR=/root/backups/gesetze
DATE=$(date +%Y%m%d)

# Healthchecks.io-Monitoring (M-005): /fail bei Fehler via Trap, Erfolg am Ende.
# Die Ping-URL kommt aus der Umgebung (in der crontab gesetzt) -> nicht im Git.
ping_fail() { [ -n "$HC_PING_BACKUP" ] && curl -fsS --max-time 10 "$HC_PING_BACKUP/fail" >/dev/null 2>&1 || true; }
trap ping_fail ERR

# Tägliches Backup ohne die Riesen-Tabelle
mysqldump --single-transaction --routines --triggers \
  --ignore-table=respublica_gesetze.trade_flows_v2 \
  respublica_gesetze | gzip > "$BACKUP_DIR/gesetze_$DATE.sql.gz"

# Sonntags: trade_flows_v2 separat
if [ "$(date +%u)" = "7" ]; then
  mysqldump --single-transaction \
    respublica_gesetze trade_flows_v2 | gzip > "$BACKUP_DIR/trade_flows_$DATE.sql.gz"
fi

# Retention: tägliche 7 Tage, trade_flows 28 Tage
find "$BACKUP_DIR" -name "gesetze_*.sql.gz" -mtime +7 -delete
find "$BACKUP_DIR" -name "trade_flows_*.sql.gz" -mtime +28 -delete

# Erfolgszeile ins Log (stdout geht per Crontab nach logs/backup.log), damit der
# letzte Lauf auch ohne Healthchecks nachvollziehbar ist
echo "$(date '+%F %T') Backup ok: gesetze_$DATE.sql.gz ($(du -h "$BACKUP_DIR/gesetze_$DATE.sql.gz" | cut -f1))"

# Erfolg an Healthchecks.io melden (no-op falls HC_PING_BACKUP nicht gesetzt)
[ -n "$HC_PING_BACKUP" ] && curl -fsS --max-time 10 "$HC_PING_BACKUP" >/dev/null 2>&1 || true
