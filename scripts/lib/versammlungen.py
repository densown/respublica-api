"""Gemeinsame Logik fuer alle Versammlungsquellen (Demonstrations-Tracker).

Ein Skript pro Quelle (fetch_versammlungen_<stadt>.py) liefert nur Abruf und
Feldzuordnung und ruft dann `build_row` fuer jeden Eintrag. Alles andere,
also Themenbereinigung, Serien, IDs, Archiv, Upsert, Status und Rebuild,
liegt hier, damit jede Stadt gleich gezaehlt wird.

Schema: migrations/016_versammlungen.sql und 017_versammlungen_serien.sql.
"""
from __future__ import annotations

import gzip
import hashlib
import html
import re
import unicodedata
from datetime import date, datetime, time
from typing import Callable, Iterable

# Faellt die Liste gegenueber dem Bestand um mehr als diesen Anteil,
# wird nichts als "vor Termin entfernt" markiert. Schuetzt vor
# Teilantworten und Wartungsseiten, die sonst hunderte Absagen erzeugen.
MIN_ANTEIL_FUER_ENTFERNT = 0.5


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------

def normalize_key(key: str) -> str:
    """'Straße/Nr.' -> 'strassenr', 'Uhrzeit von' -> 'uhrzeitvon'."""
    s = str(key).strip().lower().replace("ß", "ss")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", s)


def clean_text(value) -> str | None:
    if value is None:
        return None
    s = re.sub(r"\s+", " ", str(value)).strip()
    return s or None


# Nach "Reise- und Versammlungsfreiheit" bleibt der Bindestrich stehen
_KEINE_TRENNUNG = {"und", "oder", "bis", "sowie", "bzw", "statt", "als", "wie"}
_TRENNUNG_RE = re.compile(r"(\w)- ([a-zäöüß][a-zäöüß]*)")


def _join_trennung(m: re.Match) -> str:
    if m[2] in _KEINE_TRENNUNG:
        return m[0]
    return m[1] + m[2]


def clean_thema(value) -> str | None:
    """Artefakte aus dem Satz der Quelle entfernen, Inhalt nicht veraendern.

    - HTML-Entitaeten aufloesen ("FRE&#304;HE&#304;T", Polizei-Berlin-XLSX)
    - Leerraum zusammenfassen
    - ",," als oeffnendes, "''" als schliessendes Anfuehrungszeichen
    - Silbentrennung "Hinrichtu- ngen" -> "Hinrichtungen", aber
      "Reise- und Versammlungsfreiheit" bleibt
    """
    s = clean_text(html.unescape(str(value)) if value is not None else None)
    if s is None:
        return None
    s = s.replace(",,", "„").replace("''", "“")
    s = _TRENNUNG_RE.sub(_join_trennung, s)
    return s.strip(" ,;") or None


# "(vom 01.08. bis 01.10.2026 - täglich)", "(vom 07.09.2026 bis 27.09.2027 -
# jeweils Mo.)", "(vom 06.11. bis 07.11.2026,Fr.,Sa.)". Fehlt beim Beginn das
# Jahr, gilt das Jahr des Endes (geprueft am Berliner Bestand 29.09.2026).
# Altdaten 2018-2022 zusaetzlich: "?" statt Gedankenstrich (Kodierungsfehler),
# zweistellige Jahre, "vom 01.01.2019 - 31.12.2019", "vom 01.01. 2019 bis".
_SERIE_RE = re.compile(
    r"\s*\(\s*vom\s+(\d{1,2})\.(\d{1,2})\.\s?(\d{4}|\d{2})?\s*(?:bis|-|–)\s*"
    r"(\d{1,2})\.(\d{1,2})\.\s?(\d{4}|\d{2})"
    r"\s*(?:[-,?–—]\s*([^)]*?))?\s*\)\s*$",
    re.I,
)


def _jahr(s: str) -> int:
    j = int(s)
    return j + 2000 if j < 100 else j


def split_serie(thema: str | None) -> tuple[str | None, date | None, date | None, str | None]:
    """Thema -> (Thema ohne Suffix, Serienbeginn, Serienende, Rhythmus).

    Ohne (lesbares) Serien-Suffix: (thema, None, None, None).
    """
    if not thema:
        return thema, None, None, None
    m = _SERIE_RE.search(thema)
    if not m:
        return thema, None, None, None
    try:
        ende = date(_jahr(m[6]), int(m[5]), int(m[4]))
        jahr = _jahr(m[3]) if m[3] else ende.year
        beginn = date(jahr, int(m[2]), int(m[1]))
        if not m[3] and beginn > ende:
            beginn = date(jahr - 1, int(m[2]), int(m[1]))
    except ValueError:
        return thema, None, None, None
    if beginn > ende:
        return thema, None, None, None

    rhythmus = clean_text(m[7])
    if rhythmus:
        rhythmus = re.sub(r"\s*,\s*", ", ", rhythmus.rstrip(" ,"))
        if not re.match(r"(?i)(täglich|jeweils)", rhythmus):
            rhythmus = "jeweils " + rhythmus
        if rhythmus.lower() == "jeweils":
            rhythmus = None
    rest = thema[: m.start()]
    # Manche Eintraege tragen das Suffix doppelt
    while (m2 := _SERIE_RE.search(rest)) is not None:
        rest = rest[: m2.start()]
    rest = clean_text(rest)
    return rest, beginn, ende, (rhythmus[:100] if rhythmus else None)


def normalize_thema(thema: str | None) -> str:
    """Fuer Hashes: Gross-/Kleinschreibung und Satzzeichen egal."""
    return re.sub(r"[^a-z0-9]", "", normalize_key(thema or ""))


def _sha1(*teile: str) -> str:
    return hashlib.sha1("|".join(teile).encode("utf-8")).hexdigest()


def thema_hash(thema: str | None) -> str | None:
    n = normalize_thema(thema)
    return _sha1(n) if n else None


# ---------------------------------------------------------------------------
# Datum/Zeit
# ---------------------------------------------------------------------------

def parse_datum(value: str | None) -> date | None:
    """'29.09.2026', '29.9.26' oder '2026-09-29'."""
    if not value:
        return None
    s = value.strip()
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
    else:
        m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", s)
        if not m:
            return None
        d, mo, y = int(m[1]), int(m[2]), int(m[3])
        if y < 100:
            y += 2000
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def parse_zeit(value: str | None) -> time | None:
    """'10:00', '10.00 Uhr', '9 Uhr' -> time; alles andere -> None."""
    if not value:
        return None
    m = re.search(r"(\d{1,2})(?:[:.](\d{2}))?", value)
    if not m:
        return None
    h, mi = int(m[1]), int(m[2] or 0)
    if h == 24 and mi == 0:
        return time(23, 59)
    if 0 <= h <= 23 and 0 <= mi <= 59:
        return time(h, mi)
    return None


def is_ganztaegig(von: time | None, bis: time | None) -> bool:
    """Dauerversammlungen: Berlin schreibt 00:00 bis 23:59 (oder 00:00)."""
    return von == time(0, 0) and bis in (None, time(0, 0), time(23, 59))


# ---------------------------------------------------------------------------
# Zeile
# ---------------------------------------------------------------------------

def make_quelle_id(datum: date, von: time | None, plz: str | None, thema: str | None) -> str:
    """Stabile ID eines Versammlungstags aus dem Inhalt.

    `thema` ist das Thema OHNE Serien-Suffix, sonst aendert jede
    Serienverlaengerung die IDs aller Termine.
    """
    return _sha1(
        datum.isoformat(),
        von.strftime("%H:%M") if von else "",
        (plz or "").strip(),
        normalize_thema(thema),
    )


def make_serie_id(thema: str | None, plz: str | None, serie_von: date) -> str:
    """Serie = gleiches Thema, gleiche PLZ, gleicher Beginn (Ende absichtlich nicht)."""
    return _sha1("serie", normalize_thema(thema), (plz or "").strip(), serie_von.isoformat())


def build_row(
    *,
    datum: date,
    von: time | None,
    bis: time | None,
    thema_roh: str | None,
    plz: str | None,
    ort: str | None,
    aufzugsstrecke: str | None,
    erfassung: str = "liste",
    typ_bekannt: bool = True,
) -> dict:
    """Normalisierte Zeile fuer `versammlungen` aus bereits zugeordneten Feldern.

    `typ_bekannt=False` fuer Quellen ohne Ort/Strecke: dann bleibt `typ` NULL,
    statt jede Versammlung als Kundgebung zu zaehlen.
    """
    thema, serie_von, serie_bis, rhythmus = split_serie(clean_thema(thema_roh))
    # '"Sylvester vor dem Knast" (vom ...)': Anfuehrungszeichen um das ganze Thema
    if thema and len(thema) > 2 and thema[0] == thema[-1] == '"' and thema.count('"') == 2:
        thema = thema[1:-1].strip() or None
    plz = plz.strip()[:10] if plz else None
    return {
        "quelle_id": make_quelle_id(datum, von, plz, thema),
        "datum": datum,
        "von": von,
        "bis": bis,
        "ganztaegig": is_ganztaegig(von, bis),
        "thema": thema,
        "thema_hash": thema_hash(thema),
        "plz": plz,
        "ort": ort[:500] if ort else None,
        "aufzugsstrecke": aufzugsstrecke,
        "typ": ("aufzug" if aufzugsstrecke else "kundgebung") if typ_bekannt else None,
        "serie_id": make_serie_id(thema, plz, serie_von) if serie_von else None,
        "serie_von": serie_von,
        "serie_bis": serie_bis,
        "serie_rhythmus": rhythmus,
        "erfassung": erfassung,
    }


def dedupe(rows: Iterable[dict], diag: dict | None = None) -> list[dict]:
    """Gleiche quelle_id zusammenfassen (Berlin listet manche Termine doppelt)."""
    out: dict[str, dict] = {}
    for r in rows:
        if r["quelle_id"] in out and diag is not None:
            diag.setdefault("duplikate", []).append(r)
        out[r["quelle_id"]] = r
    return list(out.values())


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def archive_raw(cur, quelle: str, raw: bytes, anzahl: int | None, jetzt: datetime) -> bool:
    """Rohdaten ablegen. True = neu archiviert, False = identisch mit Bestand."""
    sha = hashlib.sha256(raw).hexdigest()
    cur.execute(
        """
        INSERT IGNORE INTO versammlungen_rohdaten
          (quelle, abgerufen, sha256, anzahl, bytes, inhalt_gz)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (quelle, jetzt, sha, anzahl, len(raw), gzip.compress(raw, compresslevel=9)),
    )
    return cur.rowcount == 1


def apply_snapshot(
    cur, quelle: str, land: str, stadt: str, rows: list[dict], jetzt: datetime, log,
) -> dict:
    """Upsert einer vollstaendigen Liste und Status-Fortschreibung relativ zu `jetzt`.

    Nur fuer erfassung='liste': "nicht mehr gelistet" heisst hier etwas.
    """
    stats = {"neu": 0, "aktualisiert": 0, "vergangen": 0, "entfernt": 0}
    heute = jetzt.date()

    for r in rows:
        cur.execute(
            """
            INSERT INTO versammlungen
              (quelle, quelle_id, land, stadt, datum, von, bis, ganztaegig, thema,
               thema_hash, plz, ort, aufzugsstrecke, typ, serie_id, serie_von,
               serie_bis, serie_rhythmus, erfassung, status, erstmals_gesehen,
               zuletzt_gesehen)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    'angezeigt',%s,%s)
            ON DUPLICATE KEY UPDATE
              bis            = VALUES(bis),
              ganztaegig     = VALUES(ganztaegig),
              thema          = VALUES(thema),
              thema_hash     = VALUES(thema_hash),
              ort            = VALUES(ort),
              aufzugsstrecke = VALUES(aufzugsstrecke),
              typ            = VALUES(typ),
              serie_id       = VALUES(serie_id),
              serie_von      = VALUES(serie_von),
              serie_bis      = VALUES(serie_bis),
              serie_rhythmus = VALUES(serie_rhythmus),
              status         = IF(status = 'stattgefunden', status, 'angezeigt'),
              zuletzt_gesehen= VALUES(zuletzt_gesehen)
            """,
            (
                quelle, r["quelle_id"], land, stadt, r["datum"], r["von"], r["bis"],
                int(r["ganztaegig"]), r["thema"], r["thema_hash"], r["plz"], r["ort"],
                r["aufzugsstrecke"], r["typ"], r["serie_id"], r["serie_von"],
                r["serie_bis"], r["serie_rhythmus"], r["erfassung"], jetzt, jetzt,
            ),
        )
        if cur.rowcount == 1:
            stats["neu"] += 1
        elif cur.rowcount == 2:
            stats["aktualisiert"] += 1

    # Termin liegt zurueck -> vergangen. Heute bleibt 'angezeigt', weil die
    # Liste tagsueber aktualisiert wird und laufende Termine herausfallen.
    cur.execute(
        "UPDATE versammlungen SET status = 'vergangen' "
        "WHERE quelle = %s AND status = 'angezeigt' AND datum < %s",
        (quelle, heute),
    )
    stats["vergangen"] = cur.rowcount

    # Zukuenftige Termine, die nicht mehr gelistet sind
    cur.execute(
        "SELECT COUNT(*) FROM versammlungen "
        "WHERE quelle = %s AND status = 'angezeigt' AND datum > %s",
        (quelle, heute),
    )
    bestand = cur.fetchone()[0]
    aktuell = sum(1 for r in rows if r["datum"] > heute)
    if bestand and aktuell < bestand * MIN_ANTEIL_FUER_ENTFERNT:
        log.warning(
            "Nur %d von %d kommenden Terminen gelistet, Absage-Markierung ausgesetzt",
            aktuell, bestand,
        )
    else:
        cur.execute(
            "UPDATE versammlungen SET status = 'vor_termin_entfernt' "
            "WHERE quelle = %s AND status = 'angezeigt' AND datum > %s "
            "AND zuletzt_gesehen < %s",
            (quelle, heute, jetzt),
        )
        stats["entfernt"] = cur.rowcount
    return stats


def sync_kategorien(cur) -> int:
    """Kategorien aus dem Themen-Cache auf die Zeilen uebertragen."""
    cur.execute(
        """
        UPDATE versammlungen v
          JOIN versammlung_themen t ON t.thema_hash = v.thema_hash
           SET v.kategorie = t.kategorie
         WHERE NOT (v.kategorie <=> t.kategorie)
        """
    )
    return cur.rowcount


def rebuild(
    cur,
    quelle: str,
    land: str,
    stadt: str,
    parse_raw: Callable[[bytes], list[dict]],
    log,
    running: Callable[[], bool] = lambda: True,
    quiet: bool = False,
) -> None:
    """Alle Zeilen einer Quelle aus dem Rohdaten-Archiv neu aufbauen.

    Kategorien ueberleben (Themen-Cache). Teilnehmerzahlen haengen per FK an
    den Zeilen und wuerden mitgeloescht, deshalb bricht der Rebuild ab, sobald
    es welche gibt.
    """
    cur.execute(
        "SELECT COUNT(*) FROM versammlung_zahlen z "
        "JOIN versammlungen v ON v.id = z.versammlung_id WHERE v.quelle = %s",
        (quelle,),
    )
    if cur.fetchone()[0]:
        raise RuntimeError(
            "Rebuild wuerde Teilnehmerzahlen loeschen (versammlung_zahlen), abgebrochen"
        )
    cur.execute("DELETE FROM versammlungen WHERE quelle = %s", (quelle,))
    log.info("%d Zeilen geloescht, baue aus Archiv neu auf", cur.rowcount)
    cur.execute(
        "SELECT id FROM versammlungen_rohdaten WHERE quelle = %s ORDER BY abgerufen",
        (quelle,),
    )
    ids = [r[0] for r in cur.fetchall()]
    for rid in ids:
        if not running():
            raise RuntimeError("Abbruch angefordert, Rebuild unvollstaendig")
        cur.execute(
            "SELECT abgerufen, inhalt_gz FROM versammlungen_rohdaten WHERE id = %s",
            (rid,),
        )
        abgerufen, blob = cur.fetchone()
        rows = parse_raw(gzip.decompress(blob))
        stats = apply_snapshot(cur, quelle, land, stadt, rows, abgerufen, log)
        if not quiet:
            log.info("Snapshot %s (%s): %d Eintraege, %s", rid, abgerufen, len(rows), stats)
    log.info("Rebuild fertig: %d Snapshots, %d Kategorien aus Cache", len(ids), sync_kategorien(cur))
