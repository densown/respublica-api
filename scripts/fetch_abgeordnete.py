#!/usr/bin/env python3
"""Mandate und Kandidaturen von Abgeordnetenwatch API v2 in Tabelle abgeordnete.

Standard ist der 21. Bundestag (parliament_period 161). Ueber
--parliament-period laesst sich jede andere Wahlperiode holen, etwa 168 fuer
die Landtagswahl Sachsen-Anhalt 2026. Mandate und Kandidaturen landen in
derselben Tabelle und werden ueber die Spalte `typ` unterschieden — so
fuehrt es auch die Quelle.

Laeuft taeglich (config/fetch-abgeordnete.cron.fragment), damit Nachruecker
dazukommen und Ausgeschiedene ein Mandatsende (`mandat_bis`) bekommen.
Ausgeschiedene fuehrt abgeordnetenwatch nicht mehr in der Liste der
Wahlperiode; ihr Enddatum kommt aus dem Einzelabruf des Mandats.
"""
import argparse
import os
import re
import time
from datetime import date, datetime
from urllib.parse import urlencode

import mysql.connector
from lib import aw
from lib.db import get_db as lib_get_db
import requests
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

BASE = 'https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates'
PARLIAMENT_PERIOD = 161
PAGER_LIMIT = 100
LOG_PATH = '/root/apps/gesetze/logs/cron.log'
PAGE_SLEEP_SEC = 2
# Fehlen mehr Mandate in der Liste, stimmt eher mit der Liste etwas nicht als
# mit den Abgeordneten: dann nichts nachtragen.
MAX_AUSGESCHIEDENE = 30

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'ResPublicaGesetze/1.0 (+https://respublica.media)',
}


def log_line(msg):
    line = f'[{datetime.now().isoformat(timespec="seconds")}] fetch_abgeordnete: {msg}\n'
    print(line, end='')
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line)
    except OSError:
        pass


def get_db():
    return lib_get_db(autocommit=False)


def clean_fraction_label(label):
    if not label:
        return None
    s = str(label).strip()
    s = re.sub(r'\s*\(\s*Bundestag\s+[^)]*\)\s*$', '', s, flags=re.I).strip()
    return s or str(label).strip()


def datum_oder_none(raw):
    """'2026-03-01' als date, sonst None."""
    if not raw or not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def aktuelle_fraktion(fracs):
    """Fraktion, der jemand jetzt angehoert oder zuletzt angehoert hat.

    Fraktionswechsel fuehrt abgeordnetenwatch als mehrere Eintraege mit
    valid_from und valid_until. Bisher galt der erste Eintrag, das kann die
    alte Fraktion sein.
    """
    eintraege = [f for f in fracs or [] if isinstance(f, dict)]
    if not eintraege:
        return None
    offen = [f for f in eintraege if not f.get('valid_until')]
    if offen:
        f = max(offen, key=lambda f: f.get('valid_from') or '')
    else:
        f = max(eintraege, key=lambda f: f.get('valid_until') or '')
    return clean_fraction_label((f.get('fraction') or {}).get('label'))


def vorname_nachname_from_politician(politician):
    if not isinstance(politician, dict):
        return None, None, None
    fn = politician.get('first_name')
    ln = politician.get('last_name')
    label = (politician.get('label') or '').strip()
    name = label or None
    if fn and ln:
        return fn, ln, name
    if label and ' ' in label:
        vor, nach = label.rsplit(' ', 1)
        return vor.strip(), nach.strip(), name
    if label:
        return label, None, name
    return None, None, name


def wahlkreis_felder(constituency):
    if not isinstance(constituency, dict):
        return None, None
    wlabel = constituency.get('label')
    if wlabel is not None:
        wlabel = str(wlabel).strip() or None
    nr = constituency.get('number')
    if nr is not None:
        try:
            return wlabel, int(nr)
        except (TypeError, ValueError):
            pass
    if wlabel:
        m = re.match(r'^\s*(\d+)\s*-\s*', wlabel)
        if m:
            try:
                return wlabel, int(m.group(1))
            except ValueError:
                pass
    return wlabel, None


def extract_row(item):
    aw_id = item.get('id')
    politician = item.get('politician') or {}
    politiker_id = politician.get('id')
    vorname, nachname, name = vorname_nachname_from_politician(politician)

    fraktion = aktuelle_fraktion(item.get('fraction_membership'))

    ed = item.get('electoral_data') or {}
    listenplatz = ed.get('list_position')
    try:
        listenplatz = int(listenplatz) if listenplatz is not None else None
    except (TypeError, ValueError):
        listenplatz = None

    wahlkreis, wahlkreis_nr = wahlkreis_felder(ed.get('constituency'))

    profil_url = politician.get('abgeordnetenwatch_url')
    if profil_url:
        profil_url = str(profil_url)[:500]

    try:
        aw_id = int(aw_id) if aw_id is not None else None
    except (TypeError, ValueError):
        aw_id = None
    try:
        politiker_id = int(politiker_id) if politiker_id is not None else None
    except (TypeError, ValueError):
        politiker_id = None

    # Kandidaturen haben keine Fraktion, sondern eine Partei.
    partei = ((item.get('party') or {}).get('label') or None)
    if partei:
        partei = str(partei).strip()[:96] or None

    # abgeordnetenwatch unterscheidet Mandat und Kandidatur ueber `type`.
    typ = 'kandidatur' if str(item.get('type') or '').lower() == 'candidacy' else 'mandat'

    # Nur gesetzt, wenn abweichend von der Wahlperiode: Nachruecken, Verzicht, Tod
    mandat_von = datum_oder_none(item.get('start_date'))
    mandat_bis = datum_oder_none(item.get('end_date'))

    periode = ((item.get('parliament_period') or {}).get('id'))
    try:
        periode = int(periode) if periode is not None else None
    except (TypeError, ValueError):
        periode = None

    return (
        aw_id,
        politiker_id,
        periode,
        typ,
        mandat_von,
        mandat_bis,
        vorname,
        nachname,
        name,
        fraktion,
        partei,
        wahlkreis,
        wahlkreis_nr,
        listenplatz,
        profil_url,
    )


UPSERT_SQL = '''
INSERT INTO abgeordnete (
  aw_id, politiker_id, parliament_period, typ, mandat_von, mandat_bis, vorname,
  nachname, name, fraktion, partei, wahlkreis, wahlkreis_nr, listenplatz,
  profil_url, foto_url
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL
)
ON DUPLICATE KEY UPDATE
  politiker_id = VALUES(politiker_id),
  parliament_period = VALUES(parliament_period),
  typ = VALUES(typ),
  mandat_von = VALUES(mandat_von),
  mandat_bis = VALUES(mandat_bis),
  partei = VALUES(partei),
  vorname = VALUES(vorname),
  nachname = VALUES(nachname),
  name = VALUES(name),
  fraktion = VALUES(fraktion),
  wahlkreis = VALUES(wahlkreis),
  wahlkreis_nr = VALUES(wahlkreis_nr),
  listenplatz = VALUES(listenplatz),
  profil_url = VALUES(profil_url),
  foto_url = COALESCE(VALUES(foto_url), foto_url)
'''


def abrufen(url):
    """GET bei abgeordnetenwatch, bei 429 und 5xx mit Pausen wiederholt (lib/aw.py).

    Gibt die letzte Antwort zurueck; den Status prueft der Aufrufer.
    """
    for versuch in range(aw.VERSUCHE):
        r = requests.get(url, headers=HEADERS, timeout=60)
        if versuch == aw.VERSUCHE - 1 or not aw.wiederholen_bei(r.status_code):
            return r
        time.sleep(aw.wartezeit(r.headers.get('Retry-After'), versuch))
    raise AssertionError('nicht erreichbar')


def fetch_page(page, periode=PARLIAMENT_PERIOD):
    q = urlencode({
        'parliament_period': periode,
        'page': page,
        'pager_limit': PAGER_LIMIT,
    })
    r = abrufen(f'{BASE}?{q}')
    r.raise_for_status()
    return r.json()


def ausgeschiedene_nachtragen(cur, periode, aktuelle_ids):
    """Mandatsende fuer Abgeordnete, die nicht mehr in der Liste der Wahlperiode stehen.

    Wer ausscheidet, faellt bei abgeordnetenwatch aus der Liste; der Einzelabruf
    liefert weiter end_date ("Ronja Kemmer ist am 21.05.2026 ausgeschieden").
    Ohne diesen Schritt bliebe die alte Zeile ohne mandat_bis stehen und zaehlte
    als im Amt (am 17.10.2026: 634 statt 630). Scheitert ein Einzelabruf auch
    nach den Wiederholungen, bleibt dieses Mandat bis zum naechsten Lauf offen;
    die uebrigen laufen weiter. Gibt die Zahl der nachgetragenen Mandate zurueck.
    """
    cur.execute(
        "SELECT aw_id FROM abgeordnete"
        " WHERE parliament_period = %s AND typ = 'mandat' AND mandat_bis IS NULL",
        (periode,),
    )
    fehlend = [r[0] for r in cur.fetchall() if r[0] not in aktuelle_ids]
    if len(fehlend) > MAX_AUSGESCHIEDENE:
        log_line(f'{len(fehlend)} Mandate fehlen in der Liste, mehr als {MAX_AUSGESCHIEDENE}: '
                 'kein Mandatsende nachgetragen, Liste pruefen')
        return 0
    nachgetragen = 0
    for aw_id in fehlend:
        # Pause auch vor dem ersten Abruf: direkt nach der letzten Listenseite
        # antwortete abgeordnetenwatch am 17. und 18.10.2026 mit 429
        time.sleep(PAGE_SLEEP_SEC)
        try:
            r = abrufen(f'{BASE}/{aw_id}')
        except requests.RequestException as e:
            log_line(f'Einzelabruf fuer Mandat {aw_id} gescheitert ({e}), naechster Lauf versucht es wieder')
            continue
        if r.status_code == 404:
            log_line(f'Mandat {aw_id} fehlt in der Liste und im Einzelabruf')
            continue
        if not r.ok:
            log_line(f'Einzelabruf fuer Mandat {aw_id} gescheitert (HTTP {r.status_code}), '
                     'naechster Lauf versucht es wieder')
            continue
        ende = datum_oder_none((r.json().get('data') or {}).get('end_date'))
        if ende is None:
            log_line(f'Mandat {aw_id} fehlt in der Liste, hat aber kein end_date')
        else:
            cur.execute('UPDATE abgeordnete SET mandat_bis = %s WHERE aw_id = %s', (ende, aw_id))
            nachgetragen += 1
    return nachgetragen


def main():
    ap = argparse.ArgumentParser(description='Mandate/Kandidaturen importieren')
    ap.add_argument('--parliament-period', type=int, default=PARLIAMENT_PERIOD,
                    help=f'abgeordnetenwatch-Wahlperiode (Default {PARLIAMENT_PERIOD} = 21. Bundestag)')
    args = ap.parse_args()
    periode = args.parliament_period
    log_line(f'Start (parliament_period={periode})')
    db = get_db()
    cur = db.cursor()
    page = 0
    total_pages = None
    total = 0
    processed = 0
    aktuelle_ids = set()

    try:
        while True:
            payload = fetch_page(page, periode)
            items = payload.get('data') or []
            if not items:
                break

            meta = (payload.get('meta') or {}).get('result') or {}
            if total_pages is None:
                total = int(meta.get('total') or 0)
                total_pages = max(1, (total + PAGER_LIMIT - 1) // PAGER_LIMIT)

            log_line(f'Seite {page + 1}/{total_pages}: {len(items)} Datensaetze geholt')

            batch = []
            for item in items:
                row = extract_row(item)
                if row[0] is None:
                    continue
                batch.append(row)

            for row in batch:
                cur.execute(UPSERT_SQL, row)
                aktuelle_ids.add(row[0])
                processed += 1

            db.commit()

            if len(items) < PAGER_LIMIT:
                break
            page += 1
            time.sleep(PAGE_SLEEP_SEC)

        # Nur mit vollstaendiger Liste, sonst gaelten Fehlende als ausgeschieden
        if total and len(aktuelle_ids) >= total:
            ausgeschieden = ausgeschiedene_nachtragen(cur, periode, aktuelle_ids)
            db.commit()
        else:
            ausgeschieden = 0
            log_line(f'Liste unvollstaendig ({len(aktuelle_ids)} von {total}), Ausgeschiedene nicht geprueft')

        log_line(f'Fertig, {processed} Datensätze verarbeitet (INSERT/UPDATE), '
                 f'Mandatsende fuer {ausgeschieden} Ausgeschiedene nachgetragen')
        return 0
    except requests.RequestException as e:
        log_line(f'API-Fehler: {e}')
        return 1
    except mysql.connector.Error as e:
        log_line(f'DB-Fehler: {e}')
        return 1
    finally:
        cur.close()
        db.close()


if __name__ == '__main__':
    raise SystemExit(main())
