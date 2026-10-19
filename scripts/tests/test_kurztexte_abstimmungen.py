"""Kurztexte je Abstimmung (kurztexte_abstimmungen.py, Migration 020).

Die Beispiele stammen aus zwei Probelaeufen vom 19.10.2026 mit
openai/gpt-oss-120b auf echten Beschreibungen von abgeordnetenwatch. Die
Beschreibungen unten sind woertlich aus abstimmung_details.intro.
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
    roh = '```json\n{"6733": {"de": "Satz eins.\\n Satz zwei.", "en": "One.  Two."}, "6731": {"de": "A.", "en": "B."}}\n```'
    assert kt.antwort_lesen(roh) == {
        6733: {"de": "Satz eins. Satz zwei.", "en": "One. Two."},
        6731: {"de": "A.", "en": "B."},
    }


@pytest.mark.parametrize(
    "roh",
    ["", "kein json", '{"6733": {"de": "nur deutsch"}}', '["de", "en"]', '{"6733": {"de": 1, "en": "x"}}', '{"x": {"de": "a", "en": "b"}}'],
)
def test_unbrauchbare_antworten(roh):
    assert kt.antwort_lesen(roh) == {}


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


def _antwort(**texte):
    import json

    return json.dumps({k.lstrip("p"): v for k, v in texte.items()}, ensure_ascii=False)


def test_zweiter_versuch_nur_fuer_durchgefallene_mit_grund(monkeypatch):
    schlecht = {"de": "Der Bundestag beschloss etwas. Mehr nicht.", "en": "The Bundestag approved it. Nothing more."}
    antworten = iter([_antwort(p1=schlecht, p2=SAUBER), _antwort(p1=SAUBER)])
    prompts = []

    def fake(prompt, **kw):
        prompts.append(prompt)
        return next(antworten)

    monkeypatch.setattr(kt, "call_claude", fake)
    monkeypatch.setattr(kt.time, "sleep", lambda s: None)
    ergebnis = kt.erzeugen([(1, "Staatsbürgerschaft", "Beschreibung"), (2, "Zweiter Titel", "Beschreibung")])
    assert ergebnis == {1: (SAUBER, None), 2: (SAUBER, None)}
    assert len(prompts) == 2
    assert "### Abstimmung 1" in prompts[0] and "### Abstimmung 2" in prompts[0]
    assert "passte nicht" not in prompts[0]
    assert "### Abstimmung 1" in prompts[1] and "### Abstimmung 2" not in prompts[1]
    assert "passte nicht" in prompts[1] and "Bundestag" in prompts[1]


def test_zweimal_unbrauchbar_gibt_grund(monkeypatch):
    aufrufe = []
    monkeypatch.setattr(kt, "call_claude", lambda prompt, **kw: aufrufe.append(1) or "kein json")
    monkeypatch.setattr(kt.time, "sleep", lambda s: None)
    ergebnis = kt.erzeugen([(7, "Titel", "Beschreibung")])
    assert ergebnis[7][0] is None and "de und en" in ergebnis[7][1]
    assert len(aufrufe) == 2


def test_claude_ohne_antwort_laesst_alles_offen(monkeypatch):
    monkeypatch.setattr(kt, "call_claude", lambda prompt, **kw: None)
    with pytest.raises(kt.ClaudeFehler):
        kt.erzeugen([(7, "Titel", "Beschreibung")])


def test_hoechstens_vier_aufrufe_je_lauf():
    # Projektregel: hoechstens 5 Claude-CLI-Aufrufe je Skriptlauf
    assert kt.MAX_STAPEL * 2 <= 5
    assert kt.LIMIT == kt.STAPEL * kt.MAX_STAPEL


def test_prompt_enthaelt_titel_und_beschreibung():
    p = kt.prompt_bauen([(6733, " Tankrabatt ", "Die Union wollte ... {nicht formatieren}", None)])
    assert "### Abstimmung 6733\n\nTitel: Tankrabatt\n" in p
    assert "{nicht formatieren}" in p
    assert p.endswith('{"6733": {"de": "...", "en": "..."}}')


# --- Zweiter Probelauf 19.10.2026 auf dem Server, nach dem Deploy -----------

INTRO_6733 = """Der Bundestag hat über die Novellierung des Brenstoffemissionshandelsgesetzes beraten und der Beschlussempfehlung des Ausschusses für Umwelt, Klimaschutz, Naturschutz und nukleare Sicherheit zugestimmt. Zentraler Punkt ist, dass der CO₂-Preis für Benzin, Diesel, Heizöl und Erdgas im Jahr 2027 nicht erhöht wird. Dieser soll 2027 weiterhin zwischen 55 und 65 Euro pro Tonne CO₂ liegen und damit auf dem Niveau von 2026 bleiben.

Grund dafür ist, dass der neue europäische Emissionshandel für Gebäude und Verkehr (ETS2) erst 2028 starten soll. Für das Übergangsjahr 2027 soll deshalb der bisherige nationale CO₂-Preis weiter gelten. Damit sollen stärkere Preisschwankungen vermieden und Unternehmen sowie Verbraucher:innen mehr Planungssicherheit gegeben werden.

Der Umweltausschuss hat den Gesetzentwurf vor der Abstimmung noch geändert. Unter anderem sollen 2027 mehr CO₂-Zertifikate angeboten und bestimmte zusätzliche Preise für Zertifikate gesenkt werden. Dadurch soll der CO₂-Preis möglichst genau auf dem Niveau von 2026 bleiben. Für den Klima- und Transformationsfonds werden dadurch Mindereinnahmen von rund 400 Millionen Euro erwartet.

Namentlich abgestimmt wurde über die Beschlussempfehlung des Ausschusses für Umwelt, Klimaschutz, Naturschutz und nukleare Sicherheit. Diese wurde mit 303 Ja-Stimmen zu 210 Nein-Stimmen bei 52 Enthaltungen der Linksfraktion angenommen. Damit wurde die weitere Planung der Bahnstrecke beschlossen."""

INTRO_6731 = """Der Bundestag hat einen Antrag der Bundesregierung angenommen, der die Fortsetzung der Bundeswehrbeteiligung an der von den Vereinten Nationen geführten Mission im Südsudan (UNMISS) vorsieht. Das bisherige Mandat läuft Ende Oktober 2026 aus und soll um ein weiteres Jahr bis Ende Oktober 2027 verlängert werden. Wie bisher sollen bis zu 50 Soldat:innen eingesetzt werden können. Sie sollen unter anderem Führungs-, Beratungs-, Beobachtungs- und Unterstützungsaufgaben übernehmen sowie bei der technischen Ausrüstung und Ausbildung von Streitkräften anderer beteiligter Staaten helfen.

Ziel der UN-Mission ist es laut Bundesregierung, die Zivilbevölkerung zu schützen, die Menschenrechtslage zu beobachten, humanitäre Hilfe abzusichern sowie die Umsetzung des Friedensabkommens und den Friedensprozess zu unterstützen. Die einsatzbedingten Zusatzausgaben werden für den Zeitraum der Verlängerung auf rund 900.000 Euro geschätzt.

Der Auswärtige Ausschuss hat eine Beschlussempfehlung zu dem Antrag vorgelegt, über die namentlich abgestimmt wurde. Darin wird die Annahme des Antrags empfohlen. Außerdem hat der Haushaltsausschuss einen Bericht zur Vereinbarkeit des Einsatzes mit der Haushaltslage des Bundes eingebracht.

Namentlich abgestimmt wurde über die Beschlussempfehlung. Diese wurde mit 496 Ja-Stimmen zu 73 Nein-Stimmen bei vier Enthaltungen aus der AfD-Fraktion angenommen."""

INTRO_6727 = """Zusätzlich zur Abstimmung über den Antrag zur Kassenzulassung des nicht-invasiven Pränataltests (NIPT) hat der Bundestag auch über einen Änderungsantrag dazu abgestimmt. Ursprünglich war ein Änderungsantrag von 33 Abgeordneten aus den Fraktionen von SPD, Bündnis 90/Die Grünen und Die Linke vorgelegt worden. Dieser wurde jedoch kurz vor der Abstimmung zurückgezogen. Stattdessen brachten alle 110 Abgeordneten, die bereits den Hauptantrag gestellt hatten, gemeinsam einen neuen Änderungsantrag ein.

Der Änderungsantrag ergänzt den zur Abstimmung stehenden NIPT-Antrag um folgenden Satz: „Dabei wird sichergestellt, dass im Vergleich zu heute der Zugang der Schwangeren zum kassenfinanzierten NIPT bei einer gerechtfertigten medizinischen Indikation nicht erschwert wird.“ Dadurch soll sichergestellt werden, dass das Ergebnis des Monitorings keine vollständige Abschaffung des NIPTs als Kassenleistung zur Folge hat.

Der Änderungsantrag wurde mit den Stimmen von 511 Abgeordneten angenommen. 24 Abgeordnete stimmten dagegen, es gab 31 Enthaltungen."""

TITEL_6720 = "Beschlussempfehlung zu drittem Antrag: Organisierte Steuerhinterziehung wie bisher bekämpfen"
INTRO_6720 = """Im Rahmen einer Debatte zur Steuerpolitik hat die Fraktion BÜNDNIS 90/DIE GRÜNEN insgesamt vier Anträge und drei Gesetzentwürfe zur namentlichen Abstimmung vorgelegt.

Ein dritter Antrag der Grünen befasst sich mit der Bekämpfung organisierter Steuerhinterziehung. Dabei geht es unter anderem um sogenannte Cum-Ex- und Cum-Cum-Geschäfte sowie um Umsatzsteuerbetrug. Nach Ansicht der Grünen entstehen dem Staat durch solche Modelle jedes Jahr Schäden in Milliardenhöhe. Die Fraktion fordert deshalb eine spezialisierte Einheit auf Bundesebene, die besonders große und internationale Fälle von Steuerhinterziehung untersuchen und die Behörden der Länder bei ihren Ermittlungen unterstützen soll. Sie soll außerdem Informationen bündeln und die Zusammenarbeit mit europäischen Ermittlungsbehörden koordinieren.

Darüber hinaus wollen die Grünen Gesetzeslücken schließen und schwere Steuerhinterziehung härter bestrafen. Auch beim Umsatzsteuerbetrug soll früher eingegriffen werden. Geplant ist ein elektronisches Meldesystem, mit dem Geschäfte zwischen Unternehmen anhand ihrer Rechnungen schneller überprüft werden können. Dadurch sollen verdächtige Transaktionen früher auffallen und zugleich bestehende Meldepflichten vereinfacht werden.

Namentlich abgestimmt wurde über die Beschlussempfehlung des Finanzausschusses, welcher die Ablehnung des Antrags empfohlen hatte. Die Beschlussempfehlung wurde mit 445 Ja-Stimmen zu 132 Nein-Stimmen bei einer Enthaltung von Sieghard Knodel (fraktionslos) angenommen. Damit wurde der dritte Antrag der Grünen abgelehnt."""


def test_ohne_ergebnis_nimmt_stimmen_und_fremden_satz_heraus():
    q = kt.ohne_ergebnis(INTRO_6733)
    assert "Ja-Stimmen" not in q and "303" not in q
    # Kopierfehler bei abgeordnetenwatch: Satz aus der Bahnstrecken-Abstimmung
    assert "Bahnstrecke" not in q
    assert "Namentlich abgestimmt wurde über die Beschlussempfehlung" in q
    assert "Mindereinnahmen von rund 400 Millionen Euro" in q
    assert q.count("\n\n") == 3


def test_ohne_ergebnis_behaelt_antragsteller_und_empfehlung():
    q = kt.ohne_ergebnis(INTRO_6720)
    assert "welcher die Ablehnung des Antrags empfohlen hatte" in q
    assert "445" not in q and "Damit wurde" not in q
    q = kt.ohne_ergebnis(INTRO_6727)
    assert "alle 110 Abgeordneten" in q
    assert "511" not in q and "stimmten" not in q and "Enthaltungen" not in q
    assert "Antrag der Bundesregierung" in kt.ohne_ergebnis(INTRO_6731)


def test_ohne_ergebnis_laesst_reine_ergebniszeile_stehen():
    nur = "Der Antrag wurde mit 100 Ja-Stimmen angenommen."
    assert kt.ohne_ergebnis(nur) == nur


def test_hinweis_nur_bei_empfohlener_ablehnung():
    q = kt.ohne_ergebnis(INTRO_6720)
    assert "Achtung: Der Ausschuss empfahl, den Antrag abzulehnen" in kt.prompt_bauen([(6720, TITEL_6720, q, None)])
    p = kt.prompt_bauen(
        [
            (6728, "Neubau Bahnstrecke Hannover – Hamburg (Beschlussempfehlung)", "Der Verkehrsausschuss empfiehlt ...", None),
            (6731, "Verlängerung des Bundeswehreinsatzes im Südsudan (UNMISS)", kt.ohne_ergebnis(INTRO_6731), None),
        ]
    )
    assert "Achtung" not in p


def test_guter_text_aus_dem_probelauf_geht_durch():
    texte = {
        "de": (
            "Die Bundesregierung beantragte die Verlängerung des Bundeswehreinsatzes im Südsudan "
            "(UNMISS) um ein Jahr bis Ende Oktober 2027. Der Auswärtige Ausschuss empfahl, den Antrag "
            "anzunehmen, um die Zivilbevölkerung zu schützen und die Friedensprozesse zu unterstützen."
        ),
        "en": (
            "The German government requested an extension of the Bundeswehr deployment in South Sudan "
            "(UNMISS) for one year until the end of October 2027. The Foreign Affairs Committee "
            "recommended approving the request to protect civilians and support the peace process."
        ),
    }
    titel = "Verlängerung des Bundeswehreinsatzes im Südsudan (UNMISS)"
    assert kt.pruefen(texte, titel, kt.ohne_ergebnis(INTRO_6731)) is None


def test_bundesregierung_nur_wenn_sie_in_der_beschreibung_steht():
    # 6727 im Probelauf: Änderungsantrag von 110 Abgeordneten
    texte = {
        "de": (
            "Bundesregierung und die Abgeordneten von SPD, Grünen und Linken wollten den bisherigen "
            "Umfang der NIPT-Kassenleistung erhalten. Sie betonten, der Zugang solle nicht erschwert werden."
        ),
        "en": "110 MPs wanted to keep NIPT coverage. They said access should not become harder.",
    }
    titel = "Bisherigen Umfang der NIPT-Kassenleistung erhalten (Änderungsantrag)"
    assert "Bundesregierung" in kt.pruefen(texte, titel, kt.ohne_ergebnis(INTRO_6727))
    # 6733: die Beschreibung nennt keinen Antragsteller
    texte = {
        "de": "Die Bundesregierung wollte den CO₂-Preis 2027 halten. Der Umweltausschuss änderte den Entwurf.",
        "en": "The federal government wanted to keep the CO₂ price in 2027. The committee amended the bill.",
    }
    assert "Bundesregierung" in kt.pruefen(texte, "Änderung des Brennstoffemissionshandelsgesetzes", kt.ohne_ergebnis(INTRO_6733))


def test_titel_der_beschlussempfehlung_nicht_als_ziel_des_antrags():
    # 6720 im Probelauf: das Gegenteil dessen, was die Grünen wollten
    texte = {
        "de": "Die Grünen wollten organisierte Steuerhinterziehung wie bisher bekämpfen. Der Finanzausschuss empfahl, den Antrag abzulehnen.",
        "en": "The Greens wanted to fight organised tax evasion as before. The Finance Committee recommended rejecting the proposal.",
    }
    q = kt.ohne_ergebnis(INTRO_6720)
    assert "wie bisher" in kt.pruefen(texte, TITEL_6720, q)
    texte = {
        "de": (
            "Die Grünen wollten eine Bundeseinheit gegen große Fälle von Steuerhinterziehung wie Cum-Ex schaffen. "
            "Der Finanzausschuss empfahl, den Antrag abzulehnen."
        ),
        "en": (
            "The Greens wanted a federal unit to tackle large tax evasion cases such as cum-ex. "
            "The Finance Committee recommended rejecting the motion."
        ),
    }
    assert kt.pruefen(texte, TITEL_6720, q) is None


def test_parteinamen_und_gesetzentwurf():
    titel = "Erster Gesetzentwurf: Steuervergünstigung für Immobiliengesellschaften abschaffen"
    intro = "Die Grünen wollen diese Steuervergünstigung abschaffen."
    en = "The Greens wanted to abolish the tax break. They said it favours the property sector."
    de = "Die Fraktion Grüne wollte die erweiterte Grundstückskürzung abschaffen. Sie begründete das mit Gerechtigkeit."
    assert "Fraktion Grüne" in kt.pruefen({"de": de, "en": en}, titel, intro)
    de = "Die Fraktion BÜNDNIS 90/DIE GRÜNEN wollte die Kürzung abschaffen. Sie begründete das mit Gerechtigkeit."
    assert "BÜNDNIS" in kt.pruefen({"de": de, "en": en}, titel, intro)
    de = "Die Grünen beantragten, die Kürzung abzuschaffen. Sie begründeten das mit Gerechtigkeit."
    assert "beantragte" in kt.pruefen({"de": de, "en": en}, titel, intro)
    de = "Die Grünen legten einen Gesetzentwurf vor, um die Kürzung abzuschaffen. Sie begründeten das mit Gerechtigkeit."
    assert kt.pruefen({"de": de, "en": en}, titel, intro) is None


def test_zahlen_muessen_in_der_beschreibung_stehen():
    q = kt.ohne_ergebnis(INTRO_6731)
    texte = {
        "de": "Die Bundesregierung wollte bis zu 50 Soldat:innen bis Oktober 2027 einsetzen. Die Kosten schätzte sie auf 900.000 Euro.",
        "en": "The federal government wanted to deploy up to 50 soldiers until October 2027. It put the cost at €900,000.",
    }
    assert kt.pruefen(texte, "UNMISS", q) is None
    texte["de"] = texte["de"].replace("50", "80")
    assert "80" in kt.pruefen(texte, "UNMISS", q)
    # Stimmenzahlen aus der Ergebniszeile sind nach ohne_ergebnis nicht mehr erlaubt
    texte["de"] = "Die Bundesregierung wollte das Mandat bis 2027 verlängern. Dafür gab es 496 Ja."
    assert "496" in kt.pruefen(texte, "UNMISS", q)


def test_erzeugen_schickt_die_bereinigte_beschreibung(monkeypatch):
    prompts = []

    def fake(prompt, **kw):
        prompts.append(prompt)
        return _antwort(p6720=SAUBER)

    monkeypatch.setattr(kt, "call_claude", fake)
    kt.erzeugen([(6720, TITEL_6720, INTRO_6720)])
    assert "445 Ja-Stimmen" not in prompts[0]
    assert "Achtung: Der Ausschuss empfahl" in prompts[0]
