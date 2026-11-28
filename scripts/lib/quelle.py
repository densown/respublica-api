"""Gemeinsamer Ablauf fuer jeden Abruf einer fremden Datenquelle.

Jede Quelle steht mit einer Zeile in ``data_sources`` (Name, Herausgeber,
Lizenz, Takt). Ein Lauf macht immer dasselbe:

  1. Eintrag ``started`` in ``data_update_log``
  2. abrufen: GET mit Wiederholung bei 429 und 5xx, Rohantwort auf Platte
     (``data/raw/<slug>/``), damit ein Fehler spaeter nachvollziehbar ist
  3. pruefen: Zeilenzahl, Wertebereich, Luecken. Faellt eine Pruefung durch,
     wird nichts geschrieben, der alte Stand bleibt stehen
  4. schreiben (macht das Skript selbst)
  5. Abschluss: ``data_update_log`` auf success oder failed, in
     ``data_sources`` ``last_fetched`` und bei Erfolg ``last_success``

Die API liest daraus den Stand (``/api/quellen``, ``/api/stand``), die App
zeigt ihn als Stempel, das Brain meldet veraltete Quellen im Briefing.

Verwendung::

    from lib.quelle import Lauf

    with Lauf("gie-agsi", conn, schreiben=args.schreiben) as lauf:
        roh = lauf.abrufen(url, name="speicher.json")
        zeilen = lesen(roh)
        lauf.pruefen(len(zeilen) >= 20, f"nur {len(zeilen)} Laender")
        if lauf.schreiben:
            neu, geaendert = speichern(conn, zeilen)
            lauf.zaehlen(neu=neu, geaendert=geaendert)

Ohne ``schreiben=True`` ist der Lauf ein Trockenlauf: er ruft ab und prueft,
fasst die Datenbank aber nicht an (auch kein Protokoll).
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .aw import MAX_WARTEN, USER_AGENT, wartezeit, wiederholen_bei
from .env import ROOT

ROH_DIR = ROOT / "data" / "raw"
VERSUCHE = 5
# So viele Tage Rohantworten bleiben je Quelle liegen.
ROH_TAGE = 14

_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


class PruefungFehlgeschlagen(Exception):
    """Die Daten der Quelle sind nicht plausibel. Es wird nichts geschrieben."""


class QuelleUnbekannt(Exception):
    """Der Slug steht nicht in data_sources (Migration oder Seed fehlt)."""


def _jetzt() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Lauf:
    """Ein Abruf einer Quelle, als ``with``-Block.

    conn       offene DB-Verbindung (autocommit an) oder None im Trockenlauf
    schreiben  False = Trockenlauf, Datenbank bleibt unberuehrt
    oeffnen    Ersatz fuer urllib.request.urlopen (Tests)
    schlafen   Ersatz fuer time.sleep (Tests)
    """

    def __init__(
        self,
        slug: str,
        conn: Any = None,
        *,
        schreiben: bool = False,
        logger: Any = None,
        kontext: dict[str, Any] | None = None,
        roh_dir: Path | None = None,
        oeffnen: Callable[..., Any] = urllib.request.urlopen,
        schlafen: Callable[[float], None] = time.sleep,
    ) -> None:
        if schreiben and conn is None:
            raise ValueError("schreiben=True braucht eine Datenbankverbindung")
        self.slug = slug
        self.conn = conn
        self.schreiben = schreiben
        self.logger = logger
        self.kontext: dict[str, Any] = dict(kontext or {})
        self.roh_dir = (roh_dir or ROH_DIR) / slug
        self._oeffnen = oeffnen
        self._schlafen = schlafen
        self.source_id: int | None = None
        self.log_id: int | None = None
        self.neu = 0
        self.geaendert = 0
        self.uebersprungen = 0
        self.teilweise = False
        self._start = 0.0

    # --- Ablauf ---------------------------------------------------------

    def __enter__(self) -> "Lauf":
        self._start = time.monotonic()
        if self.conn is not None:
            cur = self.conn.cursor()
            cur.execute("SELECT id FROM data_sources WHERE slug = %s LIMIT 1", (self.slug,))
            row = cur.fetchone()
            cur.close()
            if not row:
                raise QuelleUnbekannt(f"Quelle '{self.slug}' steht nicht in data_sources")
            self.source_id = int(row[0])
        if self.schreiben:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO data_update_log (source_id, status, context) VALUES (%s, 'started', %s)",
                (self.source_id, json.dumps(self.kontext, ensure_ascii=False)),
            )
            self.log_id = cur.lastrowid
            cur.close()
        self._info(f"{self.slug}: Lauf beginnt ({'schreibt' if self.schreiben else 'Trockenlauf'})")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        dauer = int(time.monotonic() - self._start)
        fehler = None if exc is None else f"{exc_type.__name__}: {exc}"[:2000]
        if fehler:
            self._info(f"{self.slug}: FEHLER nach {dauer} s: {fehler}", fehler=True)
        else:
            self._info(
                f"{self.slug}: fertig in {dauer} s, neu {self.neu}, "
                f"geaendert {self.geaendert}, uebersprungen {self.uebersprungen}"
            )
        if self.schreiben:
            status = "failed" if fehler else ("partial" if self.teilweise else "success")
            jetzt = _jetzt()
            cur = self.conn.cursor()
            cur.execute(
                """UPDATE data_update_log
                   SET status = %s, records_added = %s, records_updated = %s,
                       records_skipped = %s, duration_seconds = %s,
                       error_message = %s, context = %s, finished_at = %s
                   WHERE id = %s""",
                (status, self.neu, self.geaendert, self.uebersprungen, dauer, fehler,
                 json.dumps(self.kontext, ensure_ascii=False), jetzt, self.log_id),
            )
            if fehler:
                cur.execute(
                    "UPDATE data_sources SET last_fetched = %s, last_error = %s WHERE id = %s",
                    (jetzt, fehler, self.source_id),
                )
            else:
                cur.execute(
                    """UPDATE data_sources
                       SET last_fetched = %s, last_success = %s, last_error = NULL
                       WHERE id = %s""",
                    (jetzt, jetzt, self.source_id),
                )
            cur.close()
        return False  # Fehler nicht verschlucken: das Skript endet mit Code != 0

    # --- Werkzeuge ------------------------------------------------------

    def abrufen(
        self,
        url: str,
        *,
        name: str | None = None,
        kopf: dict[str, str] | None = None,
        timeout: int = 120,
        versuche: int = VERSUCHE,
    ) -> bytes:
        """GET mit Wiederholung bei 429 und 5xx. Legt die Rohantwort ab, wenn ``name`` gesetzt ist.

        Schluessel gehoeren in ``kopf`` (Header), nicht in die Adresse: die
        Adresse steht im Log.
        """
        headers = {"User-Agent": USER_AGENT, **(kopf or {})}
        for versuch in range(versuche):
            req = urllib.request.Request(url, headers=headers)
            try:
                with self._oeffnen(req, timeout=timeout) as resp:
                    daten = resp.read()
                break
            except urllib.error.HTTPError as e:
                if versuch == versuche - 1 or not wiederholen_bei(e.code):
                    raise
                warten = wartezeit(e.headers.get("Retry-After") if e.headers else None, versuch)
                self._info(f"{self.slug}: HTTP {e.code}, neuer Versuch in {warten:.0f} s")
                self._schlafen(warten)
            except (urllib.error.URLError, TimeoutError):
                if versuch == versuche - 1:
                    raise
                warten = min(3 * 2**versuch, MAX_WARTEN)
                self._info(f"{self.slug}: nicht erreichbar, neuer Versuch in {warten:.0f} s")
                self._schlafen(warten)
        if name:
            self.roh_ablegen(name, daten)
        return daten

    def laden(self, url: str, ziel: Path, *, kopf: dict[str, str] | None = None,
              timeout: int = 300, versuche: int = VERSUCHE) -> Path:
        """Wie abrufen(), schreibt aber stueckweise in eine Datei. Fuer grosse Downloads,
        die nicht in den Arbeitsspeicher sollen. Die Datei entsteht erst vollstaendig
        (erst .teil, dann umbenannt), ein abgebrochener Download bleibt nicht liegen."""
        headers = {"User-Agent": USER_AGENT, **(kopf or {})}
        ziel.parent.mkdir(parents=True, exist_ok=True)
        teil = ziel.with_name(ziel.name + ".teil")
        for versuch in range(versuche):
            req = urllib.request.Request(url, headers=headers)
            try:
                with self._oeffnen(req, timeout=timeout) as resp, open(teil, "wb") as f:
                    while True:
                        stueck = resp.read(1 << 20)
                        if not stueck:
                            break
                        f.write(stueck)
                teil.replace(ziel)
                return ziel
            except urllib.error.HTTPError as e:
                teil.unlink(missing_ok=True)
                if versuch == versuche - 1 or not wiederholen_bei(e.code):
                    raise
                warten = wartezeit(e.headers.get("Retry-After") if e.headers else None, versuch)
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                teil.unlink(missing_ok=True)
                if versuch == versuche - 1:
                    raise
                warten = min(3 * 2**versuch, MAX_WARTEN)
            self._info(f"{self.slug}: Download unterbrochen, neuer Versuch in {warten:.0f} s")
            self._schlafen(warten)
        raise AssertionError("nicht erreichbar")

    def roh_ablegen(self, name: str, daten: bytes) -> Path:
        """Speichert eine Rohantwort unter data/raw/<slug>/<Zeit>_<name> und raeumt alte weg."""
        self.roh_dir.mkdir(parents=True, exist_ok=True)
        sauber = _NAME_RE.sub("_", name).strip("._") or "antwort"
        ziel = self.roh_dir / f"{_jetzt():%Y%m%dT%H%M%S}_{sauber}"
        ziel.write_bytes(daten)
        grenze = time.time() - ROH_TAGE * 86400
        for alt in self.roh_dir.iterdir():
            try:
                if alt.is_file() and alt.stat().st_mtime < grenze:
                    alt.unlink()
            except OSError:
                pass
        return ziel

    def pruefen(self, bedingung: bool, meldung: str) -> None:
        """Bricht den Lauf ab, wenn die Bedingung nicht gilt. Danach wird nichts geschrieben."""
        if not bedingung:
            raise PruefungFehlgeschlagen(meldung)

    def zaehlen(self, *, neu: int = 0, geaendert: int = 0, uebersprungen: int = 0) -> None:
        self.neu += neu
        self.geaendert += geaendert
        self.uebersprungen += uebersprungen

    def merken(self, **werte: Any) -> None:
        """Zusatzangaben fuer das Protokoll (context), z. B. Zeitraum oder Zahl der Zonen."""
        self.kontext.update(werte)

    def _info(self, text: str, fehler: bool = False) -> None:
        if self.logger is not None:
            (self.logger.error if fehler else self.logger.info)(text)
        else:
            print(text, flush=True)
