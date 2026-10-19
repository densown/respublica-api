"""Kurztexte je Abstimmung (kurztexte_abstimmungen.py, Migration 020).

Die Beispiele stammen aus einem Probelauf vom 19.10.2026 mit
openai/gpt-oss-120b auf echten Beschreibungen von abgeordnetenwatch.
"""

import hashlib

import pytest

import kurztexte_abstimmungen as kt

SAUBER = {
    "de": (
        "Die AfD-Fraktion beantragte, das Staatsbürgerschaftsrecht nicht weiter zu "
        "reformieren. Der Innenausschuss empfahl, den Antrag abzulehnen."
    ),
    "en": (
        "The AfD faction submitted a request to make no further reforms to the "
        "citizenship law. The Interior Committee recommended rejecting the proposal."
    ),
}


def test_hash_wie_sha1_in_mariadb():
    # SELECT SHA1('Grüne und Linke') in MariaDB mit utf8mb4
    assert kt.intro_hash("Grüne und Linke") == hashlib.sha1("Grüne und Linke".encode()).hexdigest()
    assert len(kt.intro_hash("x")) == 40


def test_antwort_mit_markdown_und_umbruechen():
    roh = '```json\n{"de": "Satz eins.\\n Satz zwei.", "en": "One.  Two."}\n```'
    assert kt.antwort_lesen(roh) == {"de": "Satz eins. Satz zwei.", "en": "One. Two."}


@pytest.mark.parametrize("roh", ["", "kein json", '{"de": "nur deutsch"}', '["de", "en"]', '{"de": 1, "en": "x"}'])
def test_unbrauchbare_antworten(roh):
    assert kt.antwort_lesen(roh) is None


def test_saubere_texte_mit_ausschussempfehlung_gehen_durch():
    assert kt.pruefen(SAUBER) is None


def test_ergebnis_im_text_faellt_durch():
    # 6733 im Probelauf: nennt das Ergebnis statt den Antragsteller
    texte = {
        "de": (
            "Der Bundestag beschloss die Novellierung des Brennstoffemissionshandelsgesetzes, "
            "um den CO₂-Preis 2027 unverändert zu lassen. Der Umweltausschuss begründete dies "
            "mit dem späteren Start des europäischen Emissionshandels."
        ),
        "en": "The Bundestag approved the amendment. The committee cited the later EU start.",
    }
    assert "Bundestag" in kt.pruefen(texte)
    texte["de"] = "Union und SPD wollten den CO₂-Preis halten. Der Antrag wurde angenommen."
    texte["en"] = "CDU/CSU and SPD wanted to keep the price. They cited the EU timeline."
    assert "angenommen" in kt.pruefen(texte)
    texte["de"] = "Union und SPD wollten den CO₂-Preis halten. Sie verwiesen auf den EU-Zeitplan."
    texte["en"] = "CDU/CSU and SPD wanted to keep the price. The motion passed."
    assert "passed" in kt.pruefen(texte)


@pytest.mark.parametrize(
    "de, grund",
    [
        ("", "leer"),
        ("Ein Satz ohne Ende", "Satzende"),
        ("Die Grünen wollten **mehr**. Sie begründeten das.", "Markdown"),
        ("Die Grünen wollten " + "sehr " * 80 + "viel.", "Zeichen"),
        ("Die Linke stellte einen umstrittenen Antrag. Sie begründete ihn.", "umstritten"),
    ],
)
def test_form_und_wertung(de, grund):
    assert grund in kt.pruefen({"de": de, "en": SAUBER["en"]})


def test_zweiter_versuch_bekommt_den_grund(monkeypatch):
    antworten = iter(
        [
            '{"de": "Der Bundestag beschloss etwas. Mehr nicht.", "en": "The Bundestag approved it. Nothing more."}',
            '{"de": "%s", "en": "%s"}' % (SAUBER["de"], SAUBER["en"]),
        ]
    )
    prompts = []

    def fake(messages, **kw):
        prompts.append(messages[0]["content"])
        return next(antworten)

    monkeypatch.setattr(kt, "chat_completion", fake)
    monkeypatch.setattr(kt.time, "sleep", lambda s: None)
    texte, grund = kt.erzeugen("Keine weitere Reform des Staatsbürgerschaftsrechts", "Beschreibung")
    assert texte == SAUBER and grund is None
    assert len(prompts) == 2
    assert "passte nicht" not in prompts[0]
    assert "passte nicht" in prompts[1] and "Bundestag" in prompts[1]


def test_zweimal_unbrauchbar_gibt_grund(monkeypatch):
    monkeypatch.setattr(kt, "chat_completion", lambda messages, **kw: "kein json")
    monkeypatch.setattr(kt.time, "sleep", lambda s: None)
    texte, grund = kt.erzeugen("Titel", "Beschreibung")
    assert texte is None and "JSON" in grund


def test_prompt_enthaelt_titel_und_beschreibung():
    p = kt.prompt_bauen(" Tankrabatt ", "Die Union wollte ... {nicht formatieren}")
    assert "Titel der Abstimmung: Tankrabatt\n" in p
    assert "{nicht formatieren}" in p
    assert p.endswith('{"de": "...", "en": "..."}')
