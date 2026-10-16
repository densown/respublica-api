#!/usr/bin/env bash
# API auf eine fruehere Version zuruecksetzen.
#
# Setzt den Code (api/ und die Python-Skripte) auf einen frueheren Commit von
# main zurueck, installiert bei Bedarf die passenden Abhaengigkeiten, laedt
# pm2 neu und prueft /api/health. Antwortet die API danach nicht, geht es auf
# den Stand vor dem Aufruf zurueck.
#
# Aufruf (auf dem Server, als root):
#   deploy/rollback.sh                  letzte Versionen von main auflisten, nichts aendern
#   deploy/rollback.sh zurueck          auf die vorige Version von main
#   deploy/rollback.sh <commit>         auf diesen Commit (muss auf main liegen)
#   dazu --trotz-migration              auch wenn dazwischen eine Migration liegt
#
# Die Datenbank bleibt, wie sie ist. Liegt zwischen den beiden Staenden eine
# neue Datei unter migrations/, bricht das Skript ab: erst pruefen, ob der alte
# Code mit dem neuen Schema laeuft (taegliche Dumps: /root/backups/gesetze).
#
# Der Timer liefert den verlassenen Stand von main danach nicht erneut aus,
# erst den naechsten neuen Commit, also typischerweise den Fix. Die verlassene
# Version gilt als zurueckgenommen und wird bei "zurueck" uebersprungen.
set -euo pipefail

APP_DIR=${APP_DIR:-/root/apps/gesetze}
BRANCH=${BRANCH:-main}
PM2_NAME=${PM2_NAME:-api}
HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:3002/api/health}
STATE_DIR=${STATE_DIR:-/var/lib/respublica-autodeploy}
LOG=${LOG:-$APP_DIR/logs/autodeploy.log}
BLOCKED_FILE=$STATE_DIR/api.blocked
ZURUECK_FILE=$STATE_DIR/api.zurueckgenommen

TROTZ_MIGRATION=0
ARG=""
for a in "$@"; do
  case "$a" in
    --trotz-migration) TROTZ_MIGRATION=1 ;;
    *) ARG=$a ;;
  esac
done

mkdir -p "$STATE_DIR" "$(dirname "$LOG")"
exec 9>"$STATE_DIR/api.lock"
flock -w 900 9 || { echo "Ein Deploy laeuft noch. Bitte spaeter erneut versuchen." >&2; exit 1; }

log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
zurueckgenommen() { grep -qx "$1" "$ZURUECK_FILE" 2>/dev/null; }

health() {
  for _ in $(seq 1 15); do
    curl -fsS -m 5 -o /dev/null "$HEALTH_URL" && return 0
    sleep 2
  done
  return 1
}

# Wie in autodeploy.sh: neu installieren, wenn sich die Sperrdateien zwischen
# den beiden Staenden unterscheiden.
install_deps() {
  local changed
  changed=$(git diff --name-only "$1" "$2")
  if grep -qx 'api/package-lock.json' <<< "$changed"; then
    log "npm ci in api/"
    (cd api && npm ci --omit=dev --no-audit --no-fund >> "$LOG" 2>&1)
  fi
  if grep -qx 'requirements.txt' <<< "$changed" && [ -x .venv/bin/pip ]; then
    log "pip install -r requirements.txt"
    .venv/bin/pip install -q -r requirements.txt >> "$LOG" 2>&1
  fi
}

auflisten() {
  echo "Live: ${CUR:0:7}  $(git log -1 --format='%cs %s' "$CUR")"
  echo
  echo "Letzte Versionen von $BRANCH, neueste oben:"
  git log --first-parent -n 12 --format='%H %h %cs %s' "$MAIN" | while read -r voll kurz datum text; do
    marke=""
    [ "$voll" != "$CUR" ] || marke="  <- live"
    ! zurueckgenommen "$voll" || marke="$marke  (zurueckgenommen)"
    printf '  %s  %s  %s%s\n' "$kurz" "$datum" "$text" "$marke"
  done
  git merge-base --is-ancestor "$CUR" "$MAIN" || echo "Hinweis: Der laufende Stand liegt nicht auf $BRANCH."
}

# Gibt den vollen Commit des Ziels aus.
ziel_finden() {
  local c
  if [ "$1" = zurueck ]; then
    # Vorgaenger auf main (jeweils erster Elternteil), der nicht zurueckgenommen ist
    for c in $(git rev-list --first-parent --skip=1 -n 50 "$CUR"); do
      zurueckgenommen "$c" || { echo "$c"; return 0; }
    done
    echo "Keine fruehere Version gefunden." >&2
    return 1
  fi
  c=$(git rev-parse -q --verify "$1^{commit}") || { echo "Commit \"$1\" ist unbekannt." >&2; return 1; }
  git merge-base --is-ancestor "$c" "$MAIN" || { echo "Commit ${c:0:7} liegt nicht auf $BRANCH." >&2; return 1; }
  echo "$c"
}

cd "$APP_DIR"
git fetch -q origin "$BRANCH" || echo "git fetch fehlgeschlagen, es gilt der lokale Stand von origin/$BRANCH." >&2
CUR=$(git rev-parse HEAD)
MAIN=$(git rev-parse "origin/$BRANCH")

if [ -z "$ARG" ]; then
  auflisten
  echo
  echo "Eine Version zurueck:  deploy/rollback.sh zurueck"
  echo "Bestimmter Stand:      deploy/rollback.sh <commit>"
  exit 0
fi

ZIEL=$(ziel_finden "$ARG")
if [ "$ZIEL" = "$CUR" ]; then
  echo "${ZIEL:0:7} laeuft schon."
  exit 0
fi
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "Lokale Aenderungen in $APP_DIR (git status). Erst committen oder verwerfen." >&2
  exit 1
fi
MIGRATIONEN=$(git diff --name-only "$ZIEL" "$CUR" | grep -E '^migrations/[0-9][^/]*\.sql$' | tr '\n' ' ' || true)
if [ -n "$MIGRATIONEN" ] && [ "$TROTZ_MIGRATION" = 0 ]; then
  echo "Zwischen ${ZIEL:0:7} und ${CUR:0:7} liegen Migrationen: $MIGRATIONEN" >&2
  echo "Die Datenbank wird nicht zurueckgesetzt. Erst pruefen, ob der alte Code damit laeuft, dann erneut mit --trotz-migration." >&2
  exit 1
fi

log "Zuruecksetzen von Hand: ${CUR:0:7} -> ${ZIEL:0:7}"
git reset -q --hard "$ZIEL"
if ! { install_deps "$CUR" "$ZIEL" && pm2 reload "$PM2_NAME" >> "$LOG" 2>&1 && health; }; then
  log "${ZIEL:0:7} antwortet nicht, zurueck auf ${CUR:0:7}"
  git reset -q --hard "$CUR"
  install_deps "$ZIEL" "$CUR" || true
  pm2 reload "$PM2_NAME" >> "$LOG" 2>&1 || true
  if health; then
    echo "Zuruecksetzen abgebrochen, ${CUR:0:7} laeuft wieder." >&2
  else
    echo "Zuruecksetzen abgebrochen, und auch ${CUR:0:7} antwortet nicht. API pruefen: pm2 logs $PM2_NAME" >&2
  fi
  exit 1
fi

# Der Timer soll den verlassenen Stand von main nicht gleich wieder ausliefern.
echo "$MAIN" > "$BLOCKED_FILE"
# Ging es rueckwaerts, gilt der verlassene Stand als zurueckgenommen. Wer
# bewusst auf einen Stand wechselt, nimmt ihn von dieser Liste.
if git merge-base --is-ancestor "$ZIEL" "$CUR"; then
  echo "$CUR" >> "$ZURUECK_FILE"
fi
if [ -f "$ZURUECK_FILE" ]; then
  grep -vx "$ZIEL" "$ZURUECK_FILE" > "$ZURUECK_FILE.neu" || true
  mv "$ZURUECK_FILE.neu" "$ZURUECK_FILE"
fi

log "ZURUECKGESETZT von Hand: ${ZIEL:0:7} laeuft"
echo "Live ist jetzt ${ZIEL:0:7} ($(git log -1 --format='%cs %s' "$ZIEL"))."
echo "Der Timer liefert erst den naechsten neuen Commit auf $BRANCH wieder aus."
