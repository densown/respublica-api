#!/bin/bash
# Tägliches Backup respublica_gesetze, 7 Tage Retention
# trade_flows_v2 (5,3M rows) separat wöchentlich (Sonntag)
set -e
BACKUP_DIR=/root/backups/gesetze
DATE=$(date +%Y%m%d)

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
