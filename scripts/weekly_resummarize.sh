#!/bin/bash
# Wöchentlicher Claude-Resummarize-Lauf.
# Füllt fehlende und korrigiert schlechte Zusammenfassungen bilingual.
# Läuft Sonntags früh, nutzt Claude Max Plan.

cd /root/apps/gesetze

# API-Key entfernen damit Claude Code den Max Plan nutzt
unset ANTHROPIC_API_KEY

LOG="/root/apps/gesetze/logs/weekly_resummarize.log"
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Weekly Resummarize Start ===" >> $LOG

# EU-Rechtsakte: alle mit fehlenden DE oder EN Summaries
python3 scripts/resummarize_rechtsakte.py >> $LOG 2>&1

# EU-Urteile: alle mit quality_ok=0 oder fehlenden Summaries
python3 scripts/resummarize_claude.py >> $LOG 2>&1

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Weekly Resummarize Ende ===" >> $LOG
