#!/usr/bin/env python3
"""
Ruft alle namentlichen Abstimmungen der Wahlperiode 161 von abgeordnetenwatch.de ab,
aggregiert Stimmen je Fraktion und schreibt sie nach MariaDB (Tabelle abstimmungen).
Beschreibung, Links und Ergebnis je Abstimmung kommen nach abstimmung_details
(Migration 019), bei jedem Lauf fuer alle Abstimmungen, damit Korrekturen der
Quelle ankommen.

Voraussetzung: UNIQUE(poll_id, partei). Duplikate (zweiter Lauf) per IntegrityError 1062.
Nach jedem Poll: conn.commit(), am Ende ein weiteres commit().
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import mysql.connector

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db import get_db
from lib.env import load_env

BASE = "https://www.abgeordnetenwatch.de/api/v2"
AW_WEB = "https://www.abgeordnetenwatch.de"
LEGISLATURE = 161
POLL_PAGE = 100
VOTES_PAGE = 1000
VERSUCHE = 5

UA = "gesetze-fetch/1.0 (+https://github.com/bundestag/gesetze)"


def wartezeit(err: urllib.error.HTTPError, versuch: int) -> float:
    """Sekunden bis zum naechsten Versuch: Retry-After der Quelle, sonst 3, 6, 12, 24."""
    raw = err.headers.get("Retry-After") if err.headers else None
    if raw and str(raw).strip().isdigit():
        return min(int(str(raw).strip()), 60)
    return 3 * 2**versuch


def fetch_json(url: str) -> dict[str, Any]:
    """GET mit bis zu fuenf Versuchen bei 429 und 5xx.

    abgeordnetenwatch drosselt gelegentlich (429), ein zweiter Versuch nach ein
    paar Sekunden klappt. Ohne Wiederholung brach der Lauf beim ersten 429 ab.
    """
    for versuch in range(VERSUCHE):
        req = urllib.request.Request(
            url,
            headers={"User-Agent": UA, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if versuch == VERSUCHE - 1 or not (e.code == 429 or e.code >= 500):
                raise
            time.sleep(wartezeit(e, versuch))
    raise AssertionError("nicht erreichbar")


def fetch_all_polls() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    start = 0
    total: int | None = None
    while True:
        q = urllib.parse.urlencode(
            {
                "field_legislature": str(LEGISLATURE),
                "range_start": start,
                "range_end": POLL_PAGE,
            }
        )
        url = f"{BASE}/polls?{q}"
        try:
            j = fetch_json(url)
        except urllib.error.HTTPError as e:
            print(f"HTTP-Fehler beim Abruf der Abstimmungen: {e}", file=sys.stderr)
            raise
        data = j.get("data") or []
        meta = (j.get("meta") or {}).get("result") or {}
        total = meta.get("total", total)
        out.extend(data)
        if not data:
            break
        if total is not None and len(out) >= total:
            break
        if len(data) < POLL_PAGE:
            break
        start += POLL_PAGE
    return out


def fetch_all_votes(poll_id: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    start = 0
    total: int | None = None
    while True:
        q = urllib.parse.urlencode(
            {"poll": str(poll_id), "range_start": start, "range_end": VOTES_PAGE}
        )
        url = f"{BASE}/votes?{q}"
        j = fetch_json(url)
        data = j.get("data") or []
        meta = (j.get("meta") or {}).get("result") or {}
        total = meta.get("total", total)
        out.extend(data)
        if not data:
            break
        if total is not None and len(out) >= total:
            break
        start += len(data)
    return out


def fraction_label(vote_row: dict[str, Any]) -> str:
    fr = vote_row.get("fraction")
    if isinstance(fr, dict) and fr.get("label"):
        return str(fr["label"])
    return "(keine Fraktion)"


def aggregate_by_fraction(votes: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """
    Pro Fraktion: Zähler ja, nein, enthalten, abwesend.
    API vote: yes | no | abstain | no_show
    """
    agg: dict[str, dict[str, int]] = defaultdict(
        lambda: {"ja": 0, "nein": 0, "enthalten": 0, "abwesend": 0}
    )
    for v in votes:
        label = fraction_label(v)
        key = v.get("vote")
        bucket = agg[label]
        if key == "yes":
            bucket["ja"] += 1
        elif key == "no":
            bucket["nein"] += 1
        elif key == "abstain":
            bucket["enthalten"] += 1
        elif key == "no_show":
            bucket["abwesend"] += 1
        else:
            bucket["abwesend"] += 1
    return dict(agg)


def parse_poll_datum(poll: dict[str, Any]) -> date | None:
    raw = poll.get("field_poll_date")
    if not raw or not isinstance(raw, str):
        return None
    try:
        y, m, d = raw.split("-")
        return date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


# --- Beschreibung, Links und Ergebnis (abstimmung_details) -------------------

# Tags, an denen im Einleitungstext ein Absatz endet
ABSATZ_TAGS = {
    "p", "div", "li", "ul", "ol", "br", "blockquote", "table", "tr",
    "h1", "h2", "h3", "h4", "h5", "h6",
}
PDF_HINWEIS = re.compile(r"\s*\(\s*PDF\b[^)]{0,30}\)", re.I)
# Woerter, die als Linktext nichts ueber das Ziel sagen
LEERE_LINKTEXTE = {"", "pdf", "hier", "link", "dokument"}
# Links auf abgeordnetenwatch selbst (Profile, Ausschuesse, Fraktionslisten,
# fruehere Abstimmungen) sind keine Quellen. Die Seite der Abstimmung selbst
# steht ohnehin als quelle_url daneben.
AW_HOSTS = {"www.abgeordnetenwatch.de", "abgeordnetenwatch.de"}
# Dokumentarten, auf die ein Linktext wie "dritte und letzte Gesetzentwurf"
# gekuerzt wird. Zusammengesetzte zuerst, sonst gewinnt "Antrag".
DOKUMENTARTEN = (
    "Änderungsantrag", "Entschließungsantrag", "Beschlussempfehlung", "Gesetzentwurf",
    "Antrag", "Bericht", "Unterrichtung", "Verordnung", "Stellungnahme", "Plenarprotokoll",
)
# Schlussabsatz nennt eine Mehrheit, die mehr verlangt als mehr Ja als Nein
QUALIFIZIERTE_MEHRHEIT = re.compile(
    r"zwei\s*-?\s*drittel|kanzlermehrheit|mehrheit (?:der|aller) mitglieder"
    r"|erforderlichen? mehrheit|absoluten? mehrheit",
    re.I,
)

DETAILS_SQL = """
INSERT INTO abstimmung_details
  (poll_id, intro, links, intro_html, quelle_url, angenommen, aw_accepted)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  intro = VALUES(intro),
  links = VALUES(links),
  intro_html = VALUES(intro_html),
  quelle_url = VALUES(quelle_url),
  angenommen = VALUES(angenommen),
  aw_accepted = VALUES(aw_accepted)
"""


def _saeubern(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


class _IntroParser(HTMLParser):
    """Sammelt Absaetze und Links; je Link auch den Text davor im selben Absatz."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.absaetze: list[str] = []
        self.links: list[dict[str, str]] = []
        self._absatz: list[str] = []
        self._link: dict[str, Any] | None = None

    def _absatz_beenden(self) -> None:
        text = _saeubern("".join(self._absatz))
        if text:
            self.absaetze.append(text)
        self._absatz = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ABSATZ_TAGS:
            self._absatz_beenden()
        elif tag == "a":
            self._link = {
                "href": dict(attrs).get("href") or "",
                "text": [],
                "davor": "".join(self._absatz),
            }

    def handle_endtag(self, tag: str) -> None:
        if tag in ABSATZ_TAGS:
            self._absatz_beenden()
        elif tag == "a" and self._link is not None:
            self.links.append(
                {
                    "href": self._link["href"],
                    "text": "".join(self._link["text"]),
                    "davor": self._link["davor"],
                }
            )
            self._link = None

    def handle_data(self, data: str) -> None:
        self._absatz.append(data)
        if self._link is not None:
            self._link["text"].append(data)

    def close(self) -> None:
        super().close()
        self._absatz_beenden()


def dokument_aus_url(url: str) -> str | None:
    """Drucksache oder Plenarprotokoll aus einer Adresse auf dserver.bundestag.de."""
    m = re.search(r"dserver\.bundestag\.de/btd/(\d{2})/\d{3}/\1(\d{5})\.pdf", url)
    if m:
        return f"Drucksache {int(m.group(1))}/{int(m.group(2))}"
    m = re.search(r"dserver\.bundestag\.de/btp/(\d{2})/\1(\d{3})\.pdf", url)
    if m:
        return f"Plenarprotokoll {int(m.group(1))}/{int(m.group(2))}"
    return None


def dokumentart(text: str) -> str | None:
    """Dokumentart in einem Linktext, auch gebeugt ("des Antrags"); sonst None."""
    for art in DOKUMENTARTEN:
        if re.search(rf"\b{art}(?:e?s)?\b", text, re.I):
            return art
    return None


def link_eintrag(href: str, text: str, davor: str) -> dict[str, Any] | None:
    """Ein Link aus dem Einleitungstext als {titel, url, dokument}.

    None fuer Anker, Mail und Links auf abgeordnetenwatch selbst.
    """
    href = (href or "").strip()
    if not href or href.startswith("#"):
        return None
    url = urllib.parse.urljoin(AW_WEB + "/", href)
    if not url.startswith(("https://", "http://")):
        return None
    if (urllib.parse.urlsplit(url).hostname or "").lower() in AW_HOSTS:
        return None
    dokument = dokument_aus_url(url)
    titel = _saeubern(PDF_HINWEIS.sub("", text))
    if titel.lower() == "pdf" or not titel:
        # "Gesetzentwurf (<a>PDF</a>)": gemeint ist das Wort davor
        m = re.search(r"(\S+)\s*\(?\s*$", davor)
        titel = m.group(1).strip(",;:") if m else ""
    if titel.lower() in LEERE_LINKTEXTE:
        titel = dokument or "Dokument"
    art = dokumentart(titel)
    if art and (titel[:1].islower() or re.fullmatch(rf"{art}(?:e?s)?", titel, re.I)):
        # Satzteil als Linktext ("dritte und letzte Gesetzentwurf"): nur die Art
        titel = art
    return {"titel": titel[:200], "url": url[:500], "dokument": dokument}


def intro_zerlegen(roh: str | None) -> tuple[list[str], list[dict[str, Any]]]:
    """Einleitungstext von abgeordnetenwatch in Absaetze und Links zerlegen.

    Der Link auf eine Drucksache kann das Wort, den Hinweis "(PDF)" oder beides
    umfassen:
      <a href="...">Gesetzentwurf</a> (PDF)
      <a href="...">Gesetzentwurf (PDF)</a>
      Gesetzentwurf (<a href="...">PDF</a>)
    Im Text steht danach immer "Gesetzentwurf"; "(PDF)" faellt weg, weil die
    Links auf der Seite gesammelt unter den Quellen stehen.
    """
    if not roh or not isinstance(roh, str):
        return [], []
    parser = _IntroParser()
    parser.feed(roh)
    parser.close()
    absaetze = [a for a in (_saeubern(PDF_HINWEIS.sub("", x)) for x in parser.absaetze) if a]
    links: list[dict[str, Any]] = []
    gesehen: set[str] = set()
    for roh_link in parser.links:
        eintrag = link_eintrag(roh_link["href"], roh_link["text"], roh_link["davor"])
        if eintrag and eintrag["url"] not in gesehen:
            gesehen.add(eintrag["url"])
            links.append(eintrag)
    return absaetze, links


def ergebnis_aus_intro(absaetze: list[str]) -> bool | None:
    """Angenommen oder abgelehnt laut letztem Absatz; None, wenn er es nicht eindeutig sagt.

    abgeordnetenwatch schliesst den Text mit dem Ergebnis, etwa "Der dritte
    Gesetzentwurf wurde mit 132 Ja-Stimmen zu 445 Nein-Stimmen bei keinen
    Enthaltungen abgelehnt." Steht beides im Absatz oder keins, bleibt es offen.
    """
    if not absaetze:
        return None
    satz = absaetze[-1].replace("\xad", "").lower()
    satz = re.sub(r"\bnicht\s+angenommen\b", " abgelehnt ", satz)
    satz = re.sub(r"\bnicht\s+abgelehnt\b", " angenommen ", satz)
    ja = re.search(r"\bangenommen\b", satz) is not None
    nein = re.search(r"\babgelehnt\b", satz) is not None
    if ja == nein:
        return None
    return ja


def ergebnis(ja: int, nein: int, absaetze: list[str]) -> bool | None:
    """Ob angenommen ist, worueber namentlich abgestimmt wurde.

    Massgeblich sind die Stimmen: mehr Ja als Nein. Der Schlussabsatz nennt oft
    zwei Ergebnisse ("Die Beschlussempfehlung wurde mit 443 Ja-Stimmen ...
    angenommen. Der Antrag ... wurde demnach abgelehnt."), Ja und Nein gelten
    dann der Beschlussempfehlung. Der Text zaehlt nur, solange es noch keine
    Stimmen gibt, oder wenn er "abgelehnt" sagt und eine qualifizierte Mehrheit
    nennt: Dann kann trotz Ja-Mehrheit abgelehnt sein.
    """
    text = ergebnis_aus_intro(absaetze)
    if ja + nein == 0:
        return text
    if text is False and ja > nein and QUALIFIZIERTE_MEHRHEIT.search(absaetze[-1]):
        return False
    return ja > nein


def details_aus_poll(poll: dict[str, Any], ja: int = 0, nein: int = 0) -> tuple[Any, ...] | None:
    """Zeile fuer abstimmung_details; None, wenn die Liste kein field_intro mitliefert.

    ja und nein: Stimmen insgesamt, fuer das Ergebnis. Ohne das Feld bleibt
    eine vorhandene Zeile unangetastet, statt sie mit leeren Werten zu
    ueberschreiben.
    """
    pid = poll_id_as_int(poll.get("id"))
    if pid is None or "field_intro" not in poll:
        return None
    roh = poll.get("field_intro")
    roh = roh if isinstance(roh, str) and roh.strip() else None
    absaetze, links = intro_zerlegen(roh)
    angenommen = ergebnis(ja, nein, absaetze)
    quelle = poll.get("abgeordnetenwatch_url")
    quelle = quelle[:500] if isinstance(quelle, str) and quelle.startswith("http") else None
    accepted = poll.get("field_accepted")
    if isinstance(accepted, bool):
        aw_accepted: int | None = int(accepted)
    elif accepted in (0, 1, "0", "1"):
        aw_accepted = int(accepted)
    else:
        aw_accepted = None
    return (
        pid,
        "\n\n".join(absaetze) or None,
        json.dumps(links, ensure_ascii=False) if links else None,
        roh,
        quelle,
        None if angenommen is None else int(angenommen),
        aw_accepted,
    )


def poll_id_as_int(raw: Any) -> int | None:
    """API liefert id oft als int, manchmal als str — ohne int() werden alle Polls übersprungen."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def main() -> int:
    load_env()

    try:
        polls = fetch_all_polls()
    except Exception as e:
        print(f"Abstimmungen konnten nicht geladen werden: {e}", file=sys.stderr)
        return 1

    conn = get_db(autocommit=False)
    neue_abstimmungen_polls: set[int] = set()
    neue_votes_summe = 0
    details_abgeglichen = 0
    ohne_intro = 0

    try:
        cur = conn.cursor()
        for poll in polls:
            pid = poll_id_as_int(poll.get("id"))
            if pid is None:
                continue
            titel = poll.get("label") or ""
            if not isinstance(titel, str):
                titel = str(titel)
            poll_datum = parse_poll_datum(poll)

            votes = fetch_all_votes(pid)
            by_fr = aggregate_by_fraction(votes)

            details = details_aus_poll(
                poll,
                sum(z["ja"] for z in by_fr.values()),
                sum(z["nein"] for z in by_fr.values()),
            )
            if details is None:
                ohne_intro += 1
            else:
                cur.execute(DETAILS_SQL, details)
                details_abgeglichen += 1

            for partei, z in by_fr.items():
                ja, nein, enthalten, abwesend = (
                    z["ja"],
                    z["nein"],
                    z["enthalten"],
                    z["abwesend"],
                )
                row = (
                    partei,
                    ja,
                    nein,
                    enthalten,
                    abwesend,
                    pid,
                    titel,
                    poll_datum,
                )
                inserted = False
                try:
                    cur.execute(
                        """
                        INSERT INTO abstimmungen
                        (aenderung_id, partei, ja, nein, enthalten, abwesend,
                         poll_id, poll_titel, poll_datum)
                        VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        row,
                    )
                except mysql.connector.IntegrityError as err:
                    if getattr(err, "errno", None) != 1062:
                        raise
                else:
                    inserted = True
                _ = cur.warnings
                if inserted:
                    neue_abstimmungen_polls.add(pid)
                    neue_votes_summe += ja + nein + enthalten + abwesend

            conn.commit()

        conn.commit()
    finally:
        conn.close()

    print(
        f"Fertig: {len(neue_abstimmungen_polls)} Abstimmungen mit neuen Zeilen, "
        f"{neue_votes_summe} Stimmen neu gespeichert, Beschreibung zu "
        f"{details_abgeglichen} Abstimmungen abgeglichen, {ohne_intro} ohne field_intro."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
