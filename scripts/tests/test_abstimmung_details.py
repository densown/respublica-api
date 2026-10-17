"""Beschreibung, Links und Ergebnis je Abstimmung (fetch_abstimmungen.py, Migration 019).

Der Einleitungstext ist nachgebaut nach Abstimmung 6724 (Steuerfreiheit fuer
Kryptogewinne), wie abgeordnetenwatch ihn als Text zeigt. Das HTML dazu ist
angenommen, deshalb sind alle drei moeglichen Linkformen abgedeckt.
"""

import io
import json
import urllib.error

import pytest

import fetch_abstimmungen as fa

DRUCKSACHE = "https://dserver.bundestag.de/btd/21/012/2101234.pdf"
SCHLUSS = (
    "<p>Der dritte Gesetzentwurf wurde mit 132 Ja-Stimmen zu 445 Nein-Stimmen "
    "bei keinen Enthaltungen abgelehnt.</p>"
)


def intro(link_html):
    return (
        "<p>Im Rahmen einer Debatte zur Steuerpolitik hat die Fraktion "
        "BÜNDNIS 90/DIE GRÜNEN insgesamt vier Anträge und drei Gesetzentwürfe "
        "zur namentlichen Abstimmung vorgelegt.</p>\n"
        f"<p>Der dritte und letzte {link_html} befasst sich mit der "
        "Besteuerung von Kryptowerten.</p>\n" + SCHLUSS
    )


@pytest.mark.parametrize(
    "link_html",
    [
        f'<a href="{DRUCKSACHE}">Gesetzentwurf</a> (PDF)',
        f'<a href="{DRUCKSACHE}">Gesetzentwurf (PDF)</a>',
        f'Gesetzentwurf (<a href="{DRUCKSACHE}">PDF</a>)',
    ],
)
def test_linkformen_ergeben_denselben_text_und_link(link_html):
    absaetze, links = fa.intro_zerlegen(intro(link_html))
    assert len(absaetze) == 3
    assert absaetze[1] == (
        "Der dritte und letzte Gesetzentwurf befasst sich mit der Besteuerung von Kryptowerten."
    )
    assert links == [
        {"titel": "Gesetzentwurf", "url": DRUCKSACHE, "dokument": "Drucksache 21/1234"}
    ]


def test_entities_und_leerraum():
    absaetze, _ = fa.intro_zerlegen("<p>Gr&uuml;ne&nbsp;und\n  Linke</p><p> </p>")
    assert absaetze == ["Grüne und Linke"]


def test_relative_links_werden_absolut_anker_und_mail_fallen_weg():
    _, links = fa.intro_zerlegen(
        '<p><a href="/bundestag/21/abstimmungen/x">Abstimmung</a> '
        '<a href="#oben">oben</a> <a href="mailto:a@b.de">Mail</a> '
        '<a href="/bundestag/21/abstimmungen/x">nochmal</a></p>'
    )
    assert links == [
        {
            "titel": "Abstimmung",
            "url": "https://www.abgeordnetenwatch.de/bundestag/21/abstimmungen/x",
            "dokument": None,
        }
    ]


def test_nichtssagender_linktext_nimmt_dokument_aus_der_adresse():
    _, links = fa.intro_zerlegen(f'<p>Den Antrag finden Sie <a href="{DRUCKSACHE}">hier</a>.</p>')
    assert links[0]["titel"] == "Drucksache 21/1234"


@pytest.mark.parametrize(
    "url,erwartet",
    [
        ("https://dserver.bundestag.de/btd/21/007/2100712.pdf", "Drucksache 21/712"),
        ("https://dserver.bundestag.de/btp/21/21034.pdf", "Plenarprotokoll 21/34"),
        ("https://dserver.bundestag.de/btd/20/007/2100712.pdf", None),
        ("https://www.bundestag.de/dokumente/textarchiv", None),
    ],
)
def test_dokument_aus_url(url, erwartet):
    assert fa.dokument_aus_url(url) == erwartet


@pytest.mark.parametrize(
    "schluss,erwartet",
    [
        ("Der Gesetzentwurf wurde mit 132 Ja-Stimmen zu 445 Nein-Stimmen abgelehnt.", False),
        ("Der Antrag wurde mit 320 Ja-Stimmen angenommen.", True),
        ("Die Beschlussempfehlung wurde angenommen, der Antrag damit abgelehnt.", None),
        ("Die erforderliche Zweidrittelmehrheit wurde verfehlt, die Änderung ist nicht angenommen.", False),
        ("Mehr dazu im Plenarprotokoll.", None),
        ("Die angenommene Fassung tritt im Januar in Kraft.", None),
    ],
)
def test_ergebnis_aus_letztem_absatz(schluss, erwartet):
    assert fa.ergebnis_aus_intro(["Einleitung.", schluss]) is erwartet


def test_ergebnis_ohne_text():
    assert fa.ergebnis_aus_intro([]) is None


def test_details_aus_poll():
    poll = {
        "id": "6724",
        "label": "Dritter Gesetzentwurf: Steuerfreiheit für Kryptogewinne nach einem Jahr abschaffen",
        "field_intro": intro(f'<a href="{DRUCKSACHE}">Gesetzentwurf</a> (PDF)'),
        "field_accepted": True,
        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/21/abstimmungen/x",
    }
    pid, text, links, roh, quelle, angenommen, aw_accepted = fa.details_aus_poll(poll)
    assert pid == 6724
    assert text.count("\n\n") == 2 and "(PDF)" not in text
    assert json.loads(links)[0]["dokument"] == "Drucksache 21/1234"
    assert roh == poll["field_intro"]
    assert quelle == poll["abgeordnetenwatch_url"]
    # Text sagt abgelehnt, field_accepted sagt true: der Text gilt
    assert angenommen == 0
    assert aw_accepted == 1


def test_details_ohne_feld_ueberschreiben_nichts():
    assert fa.details_aus_poll({"id": 1, "label": "x"}) is None


def test_details_mit_leerem_intro():
    pid, text, links, roh, quelle, angenommen, aw_accepted = fa.details_aus_poll(
        {"id": 2, "field_intro": None, "field_accepted": None}
    )
    assert (pid, text, links, roh, quelle, angenommen, aw_accepted) == (2, None, None, None, None, None, None)


class _Antwort(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _http_fehler(code, retry_after=None):
    headers = {"Retry-After": retry_after} if retry_after else {}
    return urllib.error.HTTPError("https://x", code, "Fehler", headers, None)


def test_fetch_json_wiederholt_bei_429(monkeypatch):
    antworten = [_http_fehler(429, "2"), _http_fehler(503), _Antwort(b'{"data": []}')]
    pausen = []

    def urlopen(req, timeout):
        a = antworten.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    monkeypatch.setattr(fa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(fa.time, "sleep", pausen.append)
    assert fa.fetch_json("https://x") == {"data": []}
    assert pausen == [2, 6]


def test_fetch_json_gibt_bei_404_sofort_auf(monkeypatch):
    aufrufe = []

    def urlopen(req, timeout):
        aufrufe.append(req)
        raise _http_fehler(404)

    monkeypatch.setattr(fa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(fa.time, "sleep", lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        fa.fetch_json("https://x")
    assert len(aufrufe) == 1


def test_fetch_json_gibt_nach_fuenf_versuchen_auf(monkeypatch):
    aufrufe = []

    def urlopen(req, timeout):
        aufrufe.append(req)
        raise _http_fehler(429)

    monkeypatch.setattr(fa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(fa.time, "sleep", lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        fa.fetch_json("https://x")
    assert len(aufrufe) == fa.VERSUCHE
