#!/usr/bin/env python3
"""Importiert die Themenfeld-Taxonomie und verknuepft sie mit Abstimmungen.

Quelle: abgeordnetenwatch API v2 (/topics, /polls). Dort werden die
DIP21-Sachgebiete des Bundestags gefuehrt — zweistufig und bereits an die
namentlichen Abstimmungen gehaengt.

Arbeitsteilung:
  - Quelle liefert  : deutscher Name, Eltern-Kind-Struktur, Poll-Verknuepfung
  - Redaktionell    : Slug, englischer Name, Sortierung, fuer_positionen

Die redaktionellen Entscheidungen stehen als KURATIERT unten im Klartext.
Slugs sind bewusst hier festgelegt und nicht aus den Labels generiert: sie
sind Teil der oeffentlichen API und duerfen sich nicht aendern, wenn
abgeordnetenwatch ein Label umbenennt.

Modi:
  --dry-run   Nichts schreiben
  --quiet     Nur Zusammenfassung

Cron-tauglich, aber als einmaliger bzw. seltener Lauf gedacht: die Taxonomie
aendert sich kaum, die Poll-Verknuepfungen wachsen mit neuen Abstimmungen.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env
from lib.log import (
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)

LOCK_NAME = "import_themenfelder"
BASE = "https://www.abgeordnetenwatch.de/api/v2"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "ResPublicaGesetze/1.0 (+https://respublica.media)",
}
PAGE = 100
PAGE_SLEEP = 1.0
TIMEOUT = 60

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


# ---------------------------------------------------------------------------
# Redaktionelle Zuordnung: (abgeordnetenwatch-Topic-ID, Slug, Name EN,
# fuer_positionen). Die Reihenfolge bestimmt die Sortierung.
#
# fuer_positionen = 0 bei parlamentarischem Verfahren und Historischem. Solche
# Felder taugen nicht als Ziel fuer Wahlprogramm-Positionen; die spaetere
# Extraktion validiert dagegen.
# ---------------------------------------------------------------------------
KURATIERT: list[tuple[int, str, str, int]] = [
    (2,    "arbeit-beschaeftigung",        "Labour and employment", 1),
    (14,   "soziale-sicherung",            "Social security", 1),
    (28,   "gesundheit",                   "Health", 1),
    (3,    "bildung",                      "Education", 1),
    (15,   "wissenschaft-forschung",       "Science, research and technology", 1),
    (49,   "forschung",                    "Research", 1),
    (50,   "reaktorsicherheit",            "Nuclear safety", 1),
    (51,   "technologiefolgenabschaetzung", "Technology assessment", 1),
    (19,   "wirtschaft",                   "Economy", 1),
    (22,   "finanzen-steuern",             "Public finance and taxation", 1),
    (41,   "finanzen",                     "Finance", 1),
    (42,   "haushalt",                     "Budget", 1),
    (20,   "energie",                      "Energy", 1),
    (9,    "umwelt",                       "Environment", 1),
    (2900, "klima",                        "Climate", 1),
    (48,   "naturschutz",                  "Nature conservation", 1),
    (5,    "landwirtschaft-ernaehrung",    "Agriculture and food", 1),
    (10,   "verkehr",                      "Transport", 1),
    (18,   "bauen-wohnen",                 "Planning, construction and housing", 1),
    (25,   "migration",                    "Migration and residence law", 1),
    (23,   "innere-sicherheit",            "Internal security", 1),
    (38,   "innere-angelegenheiten",       "Home affairs", 1),
    (8,    "recht",                        "Law and justice", 1),
    (43,   "menschenrechte",               "Human rights", 1),
    (44,   "verbraucherschutz",            "Consumer protection", 1),
    (16,   "gesellschaftspolitik",         "Social policy and groups", 1),
    (34,   "familie",                      "Family", 1),
    (35,   "frauen",                       "Women", 1),
    (36,   "jugend",                       "Youth", 1),
    (37,   "senioren",                     "Older people", 1),
    (7,    "kultur",                       "Culture", 1),
    (1,    "medien-kommunikation",         "Media, communication and IT", 1),
    (39,   "digitale-agenda",              "Digital agenda", 1),
    (40,   "digitale-infrastruktur",       "Digital infrastructure", 1),
    (45,   "medien",                       "Media", 1),
    (12,   "sport-freizeit-tourismus",     "Sport, leisure and tourism", 1),
    (46,   "sport",                        "Sport", 1),
    (47,   "tourismus",                    "Tourism", 1),
    (21,   "aussenpolitik",                "Foreign policy and international relations", 1),
    (4,    "europapolitik",                "European policy and the EU", 1),
    (13,   "verteidigung",                 "Defence", 1),
    (17,   "entwicklungspolitik",          "Development policy", 1),
    (33,   "humanitaere-hilfe",            "Humanitarian aid", 1),
    (11,   "aussenwirtschaft",             "Foreign trade", 1),
    (24,   "staat-verwaltung",             "State and public administration", 1),
    (27,   "politisches-leben-parteien",   "Political life and parties", 1),
    (53,   "lobbyismus-transparenz",       "Lobbying and transparency", 1),
    # Parlamentarisches Verfahren — kein Ziel fuer Wahlprogramm-Positionen
    (6,    "bundestag",                    "Bundestag", 0),
    (29,   "geschaeftsordnung",            "Rules of procedure", 0),
    (30,   "immunitaet",                   "Parliamentary immunity", 0),
    (31,   "petitionen",                   "Petitions", 0),
    (32,   "wahlpruefung",                 "Scrutiny of elections", 0),
    # Historisch abgeschlossen
    (26,   "deutsche-einheit",             "German unification (to 1990)", 0),
]


def parent_id_of(topic: dict) -> int | None:
    """`parent` kommt je nach Eintrag als dict, als Liste oder als None."""
    p = topic.get("parent")
    if isinstance(p, list):
        p = p[0] if p else None
    if isinstance(p, dict):
        return p.get("id")
    return p


def hole(pfad: str, params: dict) -> dict:
    r = requests.get(f"{BASE}/{pfad}", params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def hole_alle(pfad: str, extra: dict | None = None) -> list[dict]:
    """Blaettert die API vollstaendig durch."""
    raus: list[dict] = []
    start = 0
    while _running:
        params = {"range_start": start, "range_end": PAGE, **(extra or {})}
        d = hole(pfad, params)
        teil = d.get("data") or []
        raus.extend(teil)
        gesamt = (d.get("meta") or {}).get("result", {}).get("total", 0)
        start += PAGE
        if start >= gesamt or not teil:
            break
        time.sleep(PAGE_SLEEP)
    return raus


def main() -> int:
    ap = argparse.ArgumentParser(description="Themenfelder importieren")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = None
    try:
        topics = hole_alle("topics")
        log.info("%d Themen von abgeordnetenwatch geladen", len(topics))

        kuratiert = {aw: (slug, en, fp) for aw, slug, en, fp in KURATIERT}
        quelle = {t["id"]: t for t in topics}

        fehlend = [t["id"] for t in topics if t["id"] not in kuratiert]
        if fehlend:
            log.warning(
                "Nicht kuratierte Themen (werden uebersprungen): %s",
                ", ".join(f'{i}={quelle[i]["label"]}' for i in fehlend),
            )
        verschwunden = [aw for aw in kuratiert if aw not in quelle]
        if verschwunden:
            log.warning("Kuratierte Themen fehlen in der Quelle: %s", verschwunden)

        if args.dry_run:
            log.info(
                "[dry-run] wuerde %d Themenfelder schreiben",
                len(set(kuratiert) & set(quelle)),
            )
            return 0

        load_env()
        conn = get_db(autocommit=True)
        cur = conn.cursor()

        # --- Durchgang 1: Themen ohne Elternbezug schreiben -------------------
        # Der Baum wird erst danach gesetzt, weil der Fremdschluessel sonst
        # von der Reihenfolge der Quelle abhinge.
        for i, (aw, slug, name_en, fuer_pos) in enumerate(KURATIERT):
            t = quelle.get(aw)
            if not t:
                continue
            cur.execute(
                """
                INSERT INTO themenfelder
                  (slug, name_de, name_en, aw_topic_id, fuer_positionen, sortierung)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  name_de = VALUES(name_de), name_en = VALUES(name_en),
                  fuer_positionen = VALUES(fuer_positionen),
                  sortierung = VALUES(sortierung)
                """,
                (slug, t["label"], name_en, aw, fuer_pos, i * 10),
            )

        cur.execute("SELECT aw_topic_id, id FROM themenfelder WHERE aw_topic_id IS NOT NULL")
        nach_aw = {int(a): int(i) for a, i in cur.fetchall()}

        # --- Durchgang 2: Baum verdrahten ------------------------------------
        eltern = 0
        for aw, eigene_id in nach_aw.items():
            t = quelle.get(aw)
            if not t:
                continue
            p_aw = parent_id_of(t)
            p_id = nach_aw.get(p_aw) if p_aw is not None else None
            cur.execute(
                "UPDATE themenfelder SET parent_id = %s WHERE id = %s", (p_id, eigene_id)
            )
            if p_id:
                eltern += 1

        log.info("%d Themenfelder geschrieben, davon %d Unterthemen", len(nach_aw), eltern)

        # --- Abstimmungen verknuepfen ----------------------------------------
        polls = hole_alle("polls")
        log.info("%d Abstimmungen von abgeordnetenwatch geladen", len(polls))

        verknuepft = ohne_thema = 0
        for p in polls:
            themen = p.get("field_topics") or []
            if not themen:
                ohne_thema += 1
                continue
            for th in themen:
                ziel = nach_aw.get(th.get("id"))
                if ziel is None:
                    continue
                cur.execute(
                    "INSERT IGNORE INTO poll_themenfelder (poll_id, themenfeld_id) VALUES (%s,%s)",
                    (int(p["id"]), ziel),
                )
                verknuepft += 1

        log.info(
            "Fertig: %d Poll-Themen-Verknuepfungen, %d Abstimmungen ohne Thema",
            verknuepft, ohne_thema,
        )
        return 0

    except Exception as exc:  # noqa: BLE001
        log.exception("Abbruch: %s", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
