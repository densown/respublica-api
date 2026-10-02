#!/usr/bin/env bash
# Automatisches Deployment der API (Pull statt Push).
#
# Laeuft per systemd-Timer alle 5 Minuten auf dem Server. Holt origin/main und
# deployt nur, wenn die GitHub-CI fuer genau diesen Commit gruen ist. Danach
# pm2 reload und Healthcheck; schlaegt der fehl, geht es auf den alten Commit
# zurueck. Neue Migrationen spielt das Skript nie selbst ein, es haelt an.
#
# Aufruf:
#   deploy/autodeploy.sh                         normaler Lauf (Timer)
#   deploy/autodeploy.sh --migrationen-erledigt  nach Einspielen der Migration von Hand
#
# Konfiguration ueber Umgebung (systemd: /etc/respublica/autodeploy.env):
#   DEPLOY_ALERT_URL  Ping-URL, z. B. healthchecks.io. Erfolg pingt die URL,
#                     Fehler pingen URL/fail mit dem Grund. Leer: nur Logdatei.
#   GITHUB_TOKEN      optional, nur gegen das API-Limit (60 Abfragen/Stunde)
set -euo pipefail

REPO_SLUG=densown/respublica-api
APP_DIR=${APP_DIR:-/root/apps/gesetze}
BRANCH=${BRANCH:-main}
PM2_NAME=${PM2_NAME:-api}
HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:3002/api/health}
STATE_DIR=${STATE_DIR:-/var/lib/respublica-autodeploy}
LOG=${LOG:-$APP_DIR/logs/autodeploy.log}
DEPLOY_ALERT_URL=${DEPLOY_ALERT_URL:-}
GITHUB_API=${GITHUB_API:-https://api.github.com}

MIGRATIONEN_ERLEDIGT=0
[ "${1:-}" = "--migrationen-erledigt" ] && MIGRATIONEN_ERLEDIGT=1

mkdir -p "$STATE_DIR" "$(dirname "$LOG")"
BLOCKED_FILE=$STATE_DIR/api.blocked
LAST_FILE=$STATE_DIR/api.last

exec 9>"$STATE_DIR/api.lock"
flock -n 9 || exit 0

log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
ping_ok() { [ -z "$DEPLOY_ALERT_URL" ] || curl -fsS -m 10 -o /dev/null "$DEPLOY_ALERT_URL" || true; }
ping_fail() { [ -z "$DEPLOY_ALERT_URL" ] || curl -fsS -m 10 -o /dev/null --data-raw "api: $1" "$DEPLOY_ALERT_URL/fail" || true; }

# Haelt an und meldet einmal pro Commit und Grund, nicht bei jedem Timerlauf.
# halt:  Grund kann sich ohne neuen Commit erledigen (lokale Aenderungen,
#        Migration), wird also beim naechsten Lauf erneut geprueft.
# block: Commit ist endgueltig durchgefallen (CI rot, Healthcheck), wird erst
#        mit einem neuen Commit auf main wieder versucht.
halt() {
  if [ "$(cat "$LAST_FILE" 2>/dev/null)" != "$NEW $1" ]; then
    log "ANGEHALTEN bei ${NEW:0:7}: $1"
    ping_fail "$1"
    echo "$NEW $1" > "$LAST_FILE"
  fi
  exit 1
}
block() {
  echo "$NEW" > "$BLOCKED_FILE"
  halt "$1"
}

health() {
  for _ in $(seq 1 15); do
    curl -fsS -m 5 -o /dev/null "$HEALTH_URL" && return 0
    sleep 2
  done
  return 1
}

# success | pending | failure fuer alle Check-Runs eines Commits
ci_state() {
  curl -fsS -m 20 \
    -H "Accept: application/vnd.github+json" \
    ${GITHUB_TOKEN:+-H "Authorization: Bearer $GITHUB_TOKEN"} \
    "$GITHUB_API/repos/$REPO_SLUG/commits/$1/check-runs?per_page=100" |
    python3 -c '
import json, sys
runs = json.load(sys.stdin).get("check_runs", [])
if not runs or any(r["status"] != "completed" for r in runs):
    print("pending")
elif all(r["conclusion"] in ("success", "skipped", "neutral") for r in runs):
    print("success")
else:
    print("failure")
'
}

cd "$APP_DIR"
git fetch -q origin "$BRANCH" || { log "git fetch fehlgeschlagen, naechster Versuch beim naechsten Lauf"; exit 0; }
CUR=$(git rev-parse HEAD)
NEW=$(git rev-parse "origin/$BRANCH")

if [ "$CUR" = "$NEW" ]; then
  rm -f "$BLOCKED_FILE" "$LAST_FILE"
  ping_ok
  exit 0
fi
if [ "$MIGRATIONEN_ERLEDIGT" = 0 ] && [ "$(cat "$BLOCKED_FILE" 2>/dev/null)" = "$NEW" ]; then
  exit 1
fi

# Von Hand geaenderte Dateien auf dem Server wuerden still ueberschrieben.
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  halt "lokale Aenderungen in $APP_DIR (git status), bitte committen oder verwerfen"
fi
if ! git merge-base --is-ancestor "$CUR" "$NEW"; then
  halt "HEAD ${CUR:0:7} ist nicht Teil von origin/$BRANCH (lokale Commits auf dem Server?)"
fi

STATE=$(ci_state "$NEW") || { log "GitHub nicht erreichbar, naechster Versuch beim naechsten Lauf"; exit 0; }
case "$STATE" in
  pending) log "CI fuer ${NEW:0:7} laeuft noch"; exit 0 ;;
  failure) block "CI fuer ${NEW:0:7} ist rot, wird nicht deployt" ;;
esac

CHANGED=$(git diff --name-only "$CUR" "$NEW")
MIGRATIONEN=$(grep -E '^migrations/[0-9][^/]*\.sql$' <<< "$CHANGED" | tr '\n' ' ' || true)
if [ "$MIGRATIONEN_ERLEDIGT" = 0 ] && [ -n "$MIGRATIONEN" ]; then
  halt "neue Migration ($MIGRATIONEN). Backup, von Hand einspielen, dann: deploy/autodeploy.sh --migrationen-erledigt"
fi

install_deps() {
  if grep -qx 'api/package-lock.json' <<< "$CHANGED"; then
    log "npm ci in api/"
    (cd api && npm ci --omit=dev --no-audit --no-fund >> "$LOG" 2>&1)
  fi
  if grep -qx 'requirements.txt' <<< "$CHANGED" && [ -x .venv/bin/pip ]; then
    log "pip install -r requirements.txt"
    .venv/bin/pip install -q -r requirements.txt >> "$LOG" 2>&1
  fi
}

log "Deploy ${CUR:0:7} -> ${NEW:0:7}"
git merge -q --ff-only "$NEW"
if install_deps && pm2 reload "$PM2_NAME" >> "$LOG" 2>&1 && health; then
  rm -f "$BLOCKED_FILE" "$LAST_FILE"
  log "OK, ${NEW:0:7} laeuft"
  ping_ok
  exit 0
fi

log "Healthcheck oder Installation fehlgeschlagen, zurueck auf ${CUR:0:7}"
git reset -q --hard "$CUR"
install_deps || true
pm2 reload "$PM2_NAME" >> "$LOG" 2>&1 || true
if health; then
  block "${NEW:0:7} fiel durch den Healthcheck, zurueckgerollt auf ${CUR:0:7}"
fi
block "${NEW:0:7} fiel durch, und auch ${CUR:0:7} antwortet nicht. API pruefen: pm2 logs $PM2_NAME"
