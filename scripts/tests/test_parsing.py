"""Unit-Tests fuer reine Parsing-/Normalisierungs-Helfer der ETL-Skripte (M-012).

Bewusst nur seiteneffektfreie Funktionen (keine DB/Netz/IO) -> schnell, in CI
ohne Datenbank lauffaehig.
"""

import pytest

from gii_parse import slug_from_link
from fetch_eu_recht import normalize_celex, extract_typ_from_uri


@pytest.mark.parametrize(
    "link,expected",
    [
        ("https://www.gesetze-im-internet.de/bgb/xml.zip", "bgb"),
        ("http://www.gesetze-im-internet.de/estg/xml.zip", "estg"),
        ("https://www.gesetze-im-internet.de/sgb_5/xml.zip", "sgb_5"),
        ("", ""),
        (None, ""),
    ],
)
def test_slug_from_link(link, expected):
    assert slug_from_link(link) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CELEX:32016R0679", "32016R0679"),
        ("32016R0679", "32016R0679"),
        ("celex:32016R0679", "32016R0679"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_celex(raw, expected):
    assert normalize_celex(raw) == expected


def test_normalize_celex_truncates_long_input():
    long = "X" * 80
    out = normalize_celex(long)
    assert out is not None and len(out) == 50


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("http://publications.europa.eu/resource/authority/resource-type/REG", ("REG", "Verordnung")),
        ("http://publications.europa.eu/resource/authority/resource-type/DIR", ("DIR", "Richtlinie")),
        ("http://publications.europa.eu/resource/authority/resource-type/DEC", ("DEC", "Beschluss")),
        ("http://publications.europa.eu/resource/authority/resource-type/REC", ("REC", "Empfehlung")),
        (None, ("OTHER", "Sonstiges")),
        ("", ("OTHER", "Sonstiges")),
    ],
)
def test_extract_typ_from_uri(uri, expected):
    assert extract_typ_from_uri(uri) == expected
