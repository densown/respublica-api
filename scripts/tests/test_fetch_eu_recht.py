"""Datum eines EU-Rechtsakts: Tippfehler in Cellar abfangen."""

from datetime import date

from fetch_eu_recht import datum_aus_titel, plausibles_datum

HEUTE = date(2026, 10, 16)
TITEL_DE = "Durchführungsverordnung (EU) 2026/2053 der Kommission vom 10. September 2026 zur Änderung der Verordnung (EU) Nr. 1/2013 vom 5. März 2013"
TITEL_EN = "Commission Implementing Regulation (EU) 2026/2053 of 10 September 2026 amending Regulation (EU) No 1/2013"


def test_datum_aus_titel_nimmt_das_erste_datum():
    assert datum_aus_titel(TITEL_DE, None) == "2026-09-10"
    assert datum_aus_titel(None, TITEL_EN) == "2026-09-10"
    assert datum_aus_titel("Beschluss vom 3. März 2026", None) == "2026-03-03"
    assert datum_aus_titel("Ohne Datum", "No date") is None


def test_plausibles_datum_laesst_normale_daten_stehen():
    assert plausibles_datum("2026-10-15", TITEL_DE, TITEL_EN, HEUTE) == "2026-10-15"
    assert plausibles_datum(None, TITEL_DE, TITEL_EN, HEUTE) is None


def test_plausibles_datum_ersetzt_zukunft_durch_titeldatum():
    # 32026R2053: Cellar meldet 2029-09-10
    assert plausibles_datum("2029-09-10", TITEL_DE, TITEL_EN, HEUTE) == "2026-09-10"


def test_plausibles_datum_ohne_titeldatum_wird_leer():
    assert plausibles_datum("2029-09-10", "Ohne Datum", None, HEUTE) is None
    # Auch ein Titeldatum in der Zukunft hilft nicht
    assert plausibles_datum("2029-09-10", "Beschluss vom 1. Januar 2030", None, HEUTE) is None
