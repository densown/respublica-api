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
from lib import aw

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


def test_links_auf_abgeordnetenwatch_anker_und_mail_fallen_weg():
    _, links = fa.intro_zerlegen(
        '<p><a href="/bundestag/21/abstimmungen/x">Abstimmung</a> '
        '<a class="link-profile" href="https://www.abgeordnetenwatch.de/profile/simone-fischer">Simone Fischer</a> '
        '<a href="#oben">oben</a> <a href="mailto:a@b.de">Mail</a> '
        f'<a href="{DRUCKSACHE}">Antrag</a> <a href="{DRUCKSACHE}">nochmal</a></p>'
    )
    assert links == [{"titel": "Antrag", "url": DRUCKSACHE, "dokument": "Drucksache 21/1234"}]


# Aufbau wie 6146 und 6724 am 17.10.2026, Text dazwischen gekuerzt
INTRO_6146 = (
    '<p>Der Bundestag hat über einen <a class="link-read-more" '
    'href="https://dserver.bundestag.de/btd/21/002/2100229.pdf" target="_blank">Antrag der '
    'Bundesregierung</a> abgestimmt. Das Mandat war zuletzt per <a class="link-read-more link-poll" '
    'href="https://www.abgeordnetenwatch.de/bundestag/20/abstimmungen/verlaengerung-unifil" '
    'target="_blank">namentlicher Abstimmung</a> verlängert worden.</p>'
    '<p>Zu dem Antrag lag eine <a class="link-read-more" '
    'href="https://dserver.bundestag.de/btd/21/004/2100442.pdf" target="_blank">Beschlussempfehlung</a> '
    'des <a class="link-committees" href="https://www.abgeordnetenwatch.de/bundestag/21/ausschuesse/'
    'auswaertiger-ausschuss" target="_blank">Auswärtigen Ausschusses</a> vor. Außerdem hatte der '
    'Haushaltsausschuss einen <a class="link-read-more" '
    'href="https://dserver.bundestag.de/btd/21/004/2100465.pdf" target="_blank">Bericht</a> vorgelegt.</p>'
    '<p>Die Beschlussempfehlung und damit der Antrag der Bundesregierung wurden mit 390 Ja-Stimmen '
    '<strong>angenommen</strong>. Es gab zwei Enthaltungen von <a class="link-profile" '
    'href="https://www.abgeordnetenwatch.de/profile/simone-fischer" target="_blank">Simone Fischer</a>.</p>'
)
INTRO_6724 = (
    '<p><span>Im Rahmen einer </span><a class="link-read-more" '
    'href="https://www.bundestag.de/dokumente/textarchiv/2026/kw41-de-steuersystem-1217522" '
    'target="_blank"><span>Debatte zur Steuerpolitik</span></a><span> hat die </span>'
    '<a class="link-committees" href="https://www.abgeordnetenwatch.de/bundestag/abgeordnete?'
    'fraction=420&amp;constituency=All" target="_blank"><span>Fraktion BÜNDNIS 90/DIE GRÜNEN</span></a>'
    '<span> drei Gesetzentwürfe vorgelegt.</span></p>'
    '<p><span>Der </span><a class="link-read-more" href="https://dserver.bundestag.de/btd/21/057/2105752.pdf" '
    'target="_blank"><span>dritte und letzte Gesetzentwurf (PDF)</span></a><span> befasst sich mit '
    'der Besteuerung von Kryptowerten.</span></p>'
)


def test_echte_links_nur_quellen_ausserhalb_von_abgeordnetenwatch():
    _, links = fa.intro_zerlegen(INTRO_6146)
    assert [(l["titel"], l["dokument"]) for l in links] == [
        ("Antrag der Bundesregierung", "Drucksache 21/229"),
        ("Beschlussempfehlung", "Drucksache 21/442"),
        ("Bericht", "Drucksache 21/465"),
    ]


def test_satzteil_als_linktext_wird_zur_dokumentart():
    absaetze, links = fa.intro_zerlegen(INTRO_6724)
    assert [(l["titel"], l["dokument"]) for l in links] == [
        ("Debatte zur Steuerpolitik", None),
        ("Gesetzentwurf", "Drucksache 21/5752"),
    ]
    assert absaetze[1] == "Der dritte und letzte Gesetzentwurf befasst sich mit der Besteuerung von Kryptowerten."


@pytest.mark.parametrize(
    "text,erwartet",
    [
        ("Antrags", "Antrag"),
        ("Gesetzentwurfs", "Gesetzentwurf"),
        ("Änderungsantrag", "Änderungsantrag"),
        ("geänderten Beschlussempfehlung", "Beschlussempfehlung"),
        ("Antrag der Bundesregierung", "Antrag der Bundesregierung"),
        ("Debatte zur Steuerpolitik", "Debatte zur Steuerpolitik"),
    ],
)
def test_linktitel(text, erwartet):
    assert fa.link_eintrag(DRUCKSACHE, text, "")["titel"] == erwartet


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


SCHLUSS_6451 = (
    "Namentlich abgestimmt wurde über die Beschlussempfehlung des Ausschusses für Wirtschaft und "
    "Energie zu dem Antrag der AfD-Fraktion. Mit Annahme der Beschlussempfehlung wurde der Antrag "
    "mit 445 Ja-Stimmen zu 138 Nein-Stimmen bei keiner Enthaltung abgelehnt."
)
SCHLUSS_6165 = (
    "Die Beschlussempfehlung wurde mit 443 Ja-Stimmen zu 85 Nein-Stimmen bei 55 Enthaltungen "
    "angenommen. Der Antrag der Fraktion BÜNDNIS 90/DIE GRÜNEN wurde demnach abgelehnt."
)
SCHLUSS_6662 = (
    "Ein abschließender Beschluss kam jedoch nicht zustande, weil nicht festgestellt wurde, ob die "
    "erforderliche Mehrheit aller Mitglieder des Bundestages erreicht worden war. Deshalb wurde die "
    "Schlussabstimmung nun wiederholt und der Gesetzentwurf mit 322 Ja-Stimmen zu 134 Nein-Stimmen "
    "bei 118 Enthaltungen angenommen. Für die endgültige Annahme waren mindestens 316 Ja-Stimmen nötig."
)


@pytest.mark.parametrize(
    "ja,nein,schluss,erwartet",
    [
        # Ja und Nein gelten der Beschlussempfehlung, der Satz meint den Antrag
        (445, 138, SCHLUSS_6451, True),
        (443, 85, SCHLUSS_6165, True),
        (132, 445, "Der dritte Gesetzentwurf wurde mit 132 Ja-Stimmen zu 445 Nein-Stimmen abgelehnt.", False),
        (322, 134, SCHLUSS_6662, True),
        # Zweidrittelmehrheit verfehlt: trotz Ja-Mehrheit abgelehnt
        (400, 200, "Die erforderliche Zweidrittelmehrheit wurde verfehlt, der Gesetzentwurf ist abgelehnt.", False),
        (300, 300, "Für den Antrag stimmten 300 Abgeordnete, 300 dagegen.", False),
        # Noch keine Stimmen: der Text entscheidet
        (0, 0, "Der Antrag wurde abgelehnt.", False),
        (0, 0, SCHLUSS_6165, None),
    ],
)
def test_ergebnis_aus_stimmen_und_text(ja, nein, schluss, erwartet):
    assert fa.ergebnis(ja, nein, ["Einleitung.", schluss]) is erwartet


def test_details_mit_stimmen():
    poll = {"id": 6451, "field_intro": f"<p>Einleitung.</p><p>{SCHLUSS_6451}</p>", "field_accepted": True}
    assert fa.details_aus_poll(poll, 445, 138)[5] == 1


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

    monkeypatch.setattr(aw.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(aw.time, "sleep", pausen.append)
    assert fa.fetch_json("https://x") == {"data": []}
    assert pausen == [2, 6]


def test_fetch_json_gibt_bei_404_sofort_auf(monkeypatch):
    aufrufe = []

    def urlopen(req, timeout):
        aufrufe.append(req)
        raise _http_fehler(404)

    monkeypatch.setattr(aw.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(aw.time, "sleep", lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        fa.fetch_json("https://x")
    assert len(aufrufe) == 1


def test_fetch_json_gibt_nach_fuenf_versuchen_auf(monkeypatch):
    aufrufe = []

    def urlopen(req, timeout):
        aufrufe.append(req)
        raise _http_fehler(429)

    monkeypatch.setattr(aw.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(aw.time, "sleep", lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        fa.fetch_json("https://x")
    assert len(aufrufe) == aw.VERSUCHE
