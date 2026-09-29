#!/usr/bin/env python3
"""Ordnet Versammlungsthemen ohne Kategorie einem Themenfeld zu (Claude CLI).

Grundlage ist allein das Thema, wie es die Quelle veroeffentlicht. Die
Kategorie beschreibt, WORUM es geht, nicht wer demonstriert oder wie das
politisch einzuordnen ist. Eine Wertung (etwa "extremistisch") vergibt das
Skript bewusst nicht; das waere eine redaktionelle Entscheidung, keine
Klassifikation.

Klassifiziert werden verschiedene Themen (`versammlung_themen`, Migration
017), nicht Zeilen: eine taegliche Mahnwache kostet einen Eintrag statt 365,
und ein --rebuild der Versammlungen verliert keine Kategorie. Anschliessend
werden die Kategorien per thema_hash auf `versammlungen` uebertragen.

Batches von BATCH_SIZE Themen pro Aufruf, hoechstens MAX_CALLS Aufrufe pro
Lauf (Brain-Konvention: max. 5 Claude-Aufrufe). Der Rest folgt im naechsten
Lauf. Claude CLI im Max-Plan, ANTHROPIC_API_KEY wird in lib.claude entfernt.

Modi:
  --dry-run   Klassifizieren, aber nichts schreiben
  --limit N   hoechstens N Themen

Cron-tauglich: Exit 0 bei Erfolg, Exit 1 bei Fehler.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.claude import call_claude
from lib.db import get_db
from lib.env import load_env
from lib.log import (
    acquire_lock,
    get_logger,
    install_signal_handlers,
    release_lock,
)
from lib.versammlungen import sync_kategorien

LOCK_NAME = "classify_versammlungen"
BATCH_SIZE = 120
MAX_CALLS = 5

# Slug -> Beschreibung fuer den Prompt. Aenderungen hier wirken nur auf neue
# Klassifikationen; bestehende bei Bedarf per DELETE FROM versammlung_themen
# (WHERE kategorie = ...) zuruecksetzen.
KATEGORIEN = {
    "klima_umwelt": "Klima, Umwelt, Energie, Artenschutz",
    "nahost": "Israel, Palaestina, Gaza, Libanon, Iran-Israel-Konflikt",
    "ukraine_russland": "Krieg in der Ukraine, Russland",
    "internationales": "Lage in anderen Laendern (z. B. Iran, Kurdistan, Tuerkei, Sudan), Diaspora-Anliegen",
    "frieden_militaer": "Frieden, Abruestung, Wehrpflicht, Waffenlieferungen allgemein",
    "gegen_rechts": "gegen Rechtsextremismus, Rassismus, Antisemitismus, fuer Demokratie",
    "migration_asyl": "Migration, Asyl, Abschiebung, Grenzpolitik (jede Richtung)",
    "arbeit_soziales": "Loehne, Streiks, Tarif, Rente, Buergergeld, Armut, Sozialkuerzungen",
    "wohnen_stadt": "Mieten, Wohnen, Verdraengung, Stadtentwicklung",
    "verkehr": "Verkehr, Radfahren, OePNV, Autobahn",
    "gleichstellung_queer": "Frauenrechte, Feminismus, LGBTQ, CSD, Selbstbestimmung",
    "gesundheit": "Gesundheitswesen, Pflege, Pandemie, Impfen",
    "bildung_wissenschaft": "Schule, Hochschule, Kita, Forschung",
    "landwirtschaft": "Landwirtschaft, Bauern, Ernaehrung",
    "tierschutz": "Tierrechte, Tierschutz, Tierversuche",
    "grundrechte_digitales": "Buergerrechte, Datenschutz, Ueberwachung, Polizei, Pressefreiheit",
    "religion": "religioese Veranstaltungen und Anliegen",
    "gedenken": "Gedenken, Mahnwache fuer Opfer, Jahrestage",
    "sonstiges": "passt in keine Kategorie oder Thema nicht erkennbar",
}

log = get_logger(LOCK_NAME)

_running = True


def _stop() -> None:
    global _running
    _running = False


def build_prompt(items: list[tuple[int, str]]) -> str:
    kategorien = "\n".join(f"- {slug}: {beschr}" for slug, beschr in KATEGORIEN.items())
    themen = "\n".join(f"{vid}\t{thema}" for vid, thema in items)
    return (
        "Ordne jede Versammlung genau einer Kategorie zu. Grundlage ist nur das "
        "angegebene Thema. Die Kategorie beschreibt den Gegenstand, keine "
        "politische Bewertung.\n\n"
        f"Kategorien:\n{kategorien}\n\n"
        "Antworte ausschliesslich mit einem JSON-Objekt der Form "
        '{"<id>": "<kategorie>", ...}, ohne Erklaerung, ohne Markdown.\n\n'
        f"Versammlungen (id<TAB>thema):\n{themen}\n"
    )


def parse_antwort(text: str | None, erlaubte_ids: set[int]) -> dict[int, str]:
    """Claude-Antwort -> {id: kategorie}. Unbekannte IDs/Kategorien fliegen raus."""
    if not text:
        return {}
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return {}
    try:
        data = json.loads(m.group(0))
    except ValueError:
        return {}
    ergebnis: dict[int, str] = {}
    for key, kat in data.items():
        try:
            vid = int(key)
        except (TypeError, ValueError):
            continue
        kat = str(kat).strip().lower()
        if vid in erlaubte_ids and kat in KATEGORIEN:
            ergebnis[vid] = kat
    return ergebnis


def main() -> int:
    ap = argparse.ArgumentParser(description="Versammlungen thematisch klassifizieren")
    ap.add_argument("--dry-run", action="store_true", help="nichts schreiben")
    ap.add_argument("--limit", type=int, default=BATCH_SIZE * MAX_CALLS)
    args = ap.parse_args()

    if not acquire_lock(LOCK_NAME, log):
        return 1
    install_signal_handlers(_stop)

    conn = None
    try:
        load_env()
        conn = get_db(autocommit=True)
        cur = conn.cursor()
        # Haeufigste Themen zuerst: sie bestimmen die Statistik am staerksten
        cur.execute(
            """
            SELECT v.thema_hash, MIN(v.thema)
              FROM versammlungen v
              LEFT JOIN versammlung_themen t ON t.thema_hash = v.thema_hash
             WHERE t.thema_hash IS NULL AND v.thema_hash IS NOT NULL
             GROUP BY v.thema_hash
             ORDER BY COUNT(*) DESC, MIN(v.datum)
             LIMIT %s
            """,
            (min(args.limit, BATCH_SIZE * MAX_CALLS),),
        )
        offen = [(h, thema) for h, thema in cur.fetchall()]
        if not offen:
            log.info("Nichts zu klassifizieren")
            return 0
        log.info("%d Themen ohne Kategorie", len(offen))

        gesamt = fehlend = 0
        for start in range(0, len(offen), BATCH_SIZE):
            if not _running:
                log.warning("Abbruch angefordert")
                break
            batch = offen[start:start + BATCH_SIZE]
            # Kurze laufende Nummern statt 40-stelliger Hashes im Prompt
            items = [(i + 1, thema[:300]) for i, (_, thema) in enumerate(batch)]
            antwort = call_claude(build_prompt(items), timeout=300, log=log.warning)
            zuordnung = parse_antwort(antwort, {i for i, _ in items})
            fehlend += len(batch) - len(zuordnung)
            if not zuordnung:
                log.error("Batch ab %d: keine verwertbare Antwort", start)
                continue
            if args.dry_run:
                for i, thema in items[:5]:
                    log.info("[dry-run] %s | %s", zuordnung.get(i), thema)
            else:
                jetzt = datetime.now().replace(microsecond=0)
                cur.executemany(
                    "INSERT INTO versammlung_themen (thema_hash, thema, kategorie, klassifiziert_am) "
                    "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                    "kategorie = VALUES(kategorie), klassifiziert_am = VALUES(klassifiziert_am)",
                    [(batch[i - 1][0], batch[i - 1][1], kat, jetzt) for i, kat in zuordnung.items()],
                )
            gesamt += len(zuordnung)

        zeilen = 0 if args.dry_run else sync_kategorien(cur)
        log.info(
            "Fertig%s: %d Themen klassifiziert, %d ohne Ergebnis (naechster Lauf), %d Zeilen aktualisiert",
            " [dry-run]" if args.dry_run else "", gesamt, fehlend, zeilen,
        )
        return 0

    except Exception as exc:  # noqa: BLE001 — Cron soll den Fehler im Log sehen
        log.exception("Abbruch: %s", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()
        release_lock(LOCK_NAME)


if __name__ == "__main__":
    sys.exit(main())
