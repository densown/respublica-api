# Automatisches Deployment

Der Server holt sich neue Staende selbst (Pull statt Push): Ein systemd-Timer
prueft alle 5 Minuten `origin/main` und deployt nur Commits, deren GitHub-CI
gruen ist. Es liegt kein Server-Schluessel bei GitHub.

| Repo | Skript | Was passiert |
|------|--------|--------------|
| respublica-api | `deploy/autodeploy.sh` | `git merge --ff-only`, bei Bedarf `npm ci` in `api/` und `pip install`, `pm2 reload api`, Healthcheck auf `/api/health`. Faellt der durch: zurueck auf den alten Commit. |
| respublica-dashboard | `deploy/autodeploy.sh` | Build nach `dist.next`, Tausch gegen `dist` erst nach vollstaendigem Build, alte Fassung bleibt als `dist.prev`. Danach Abruf von app.respublica.media, sonst zurueck. |

Das Skript haelt an und meldet sich, statt zu deployen, wenn
- die CI fuer den Commit rot ist,
- auf dem Server Dateien von Hand geaendert wurden (`git status` nicht sauber),
- auf dem Server Commits liegen, die nicht auf main sind,
- (nur API) der Commit eine neue Datei unter `migrations/` bringt. Migrationen
  spielt das Skript nie selbst ein: Backup, Migration von Hand einspielen, dann
  `deploy/autodeploy.sh --migrationen-erledigt`.

Gemeldet wird einmal pro Commit und Grund in die Logdatei
(`logs/autodeploy.log` bzw. `deploy.log` im Dashboard) und, falls gesetzt, an
`DEPLOY_ALERT_URL` (z. B. ein Check bei healthchecks.io: jeder erfolgreiche
Lauf pingt, jeder Fehler pingt `/fail` mit dem Grund).

## Einrichten auf dem Server (einmalig, als root)

```bash
# 1. Arbeitskopien muessen sauber auf main stehen
git -C /root/apps/gesetze status
git -C /root/apps/dashboard status

# 2. Optionale Konfiguration
mkdir -p /etc/respublica
cat > /etc/respublica/autodeploy.env <<'ENV'
# DEPLOY_ALERT_URL=https://hc-ping.com/<uuid>
# GITHUB_TOKEN=<fine-grained, nur lesen>   # nur noetig, falls das API-Limit greift
# PATH=/root/.nvm/versions/node/v18.x.x/bin:/usr/local/bin:/usr/bin:/bin  # falls node per nvm
ENV
chmod 600 /etc/respublica/autodeploy.env

# 3. Pruefen, wo nginx das Dashboard ausliefert: Das Skript erwartet
#    root /root/apps/dashboard/dist;
grep -rn "apps/dashboard" /etc/nginx/sites-enabled/

# 4. Einmal von Hand laufen lassen
/root/apps/gesetze/deploy/autodeploy.sh
/root/apps/dashboard/deploy/autodeploy.sh

# 5. Timer einschalten
cp /root/apps/gesetze/deploy/respublica-autodeploy-api.{service,timer} /etc/systemd/system/
cp /root/apps/dashboard/deploy/respublica-autodeploy-dashboard.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now respublica-autodeploy-api.timer respublica-autodeploy-dashboard.timer
systemctl list-timers 'respublica-autodeploy-*'
```

Abschalten: `systemctl disable --now respublica-autodeploy-api.timer` (bzw. `-dashboard`).
Status und letzte Laeufe: `journalctl -u respublica-autodeploy-api -n 50`.

## Quality Gate auf GitHub

Damit nur gepruefter Code auf main landet, in beiden Repos unter
Settings, Branches, Branch protection rule fuer `main`:
- Require a pull request before merging (Approvals: 0 reicht)
- Require status checks to pass: in respublica-api `API smoke tests` und
  `Python parsing tests`, in respublica-dashboard `Typecheck, build, lint`
- Do not allow bypassing the above settings (sonst gilt es fuer Admins nicht)

Danach geht jede Aenderung, auch aus Claude-Sitzungen, per PR nach main.
Das Skript prueft die CI trotzdem selbst, falls doch einmal direkt gepusht wird.
