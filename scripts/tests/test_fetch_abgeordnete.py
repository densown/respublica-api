"""Mandate aus abgeordnetenwatch: Mandatsdauer und Fraktion nach Wechsel."""

from datetime import date

import fetch_abgeordnete as fab


def mandat(**felder):
    item = {
        "id": 70129,
        "type": "mandate",
        "parliament_period": {"id": 161},
        "politician": {"id": 1, "label": "David Mandrella", "first_name": "David", "last_name": "Mandrella"},
        "start_date": None,
        "end_date": None,
        "electoral_data": {"list_position": 12, "constituency": None},
        "fraction_membership": [
            {"fraction": {"label": "SPD (Bundestag 2025 - 2029)"}, "valid_from": None, "valid_until": None}
        ],
    }
    item.update(felder)
    return item


def spalten(item):
    # Reihenfolge wie in UPSERT_SQL
    namen = [
        "aw_id", "politiker_id", "parliament_period", "typ", "mandat_von", "mandat_bis",
        "vorname", "nachname", "name", "fraktion", "partei", "wahlkreis", "wahlkreis_nr",
        "listenplatz", "profil_url",
    ]
    row = fab.extract_row(item)
    assert len(row) == len(namen) == fab.UPSERT_SQL.count("%s")
    return dict(zip(namen, row))


def test_nachruecker_mit_beginn():
    z = spalten(mandat(start_date="2026-09-03"))
    assert z["mandat_von"] == date(2026, 9, 3)
    assert z["mandat_bis"] is None
    assert z["fraktion"] == "SPD"


def test_ausgeschieden_mit_ende():
    z = spalten(mandat(end_date="2026-08-31"))
    assert z["mandat_bis"] == date(2026, 8, 31)


def test_unlesbares_datum_bleibt_leer():
    z = spalten(mandat(start_date="unbekannt", end_date=""))
    assert z["mandat_von"] is None and z["mandat_bis"] is None


def test_fraktionswechsel_gilt_die_offene_mitgliedschaft():
    z = spalten(
        mandat(
            fraction_membership=[
                {"fraction": {"label": "AfD (Bundestag 2025 - 2029)"}, "valid_from": None, "valid_until": "2026-02-01"},
                {"fraction": {"label": "fraktionslos (Bundestag 2025 - 2029)"}, "valid_from": "2026-02-02", "valid_until": None},
            ]
        )
    )
    assert z["fraktion"] == "fraktionslos"


def test_ohne_offene_mitgliedschaft_gilt_die_letzte():
    z = spalten(
        mandat(
            fraction_membership=[
                {"fraction": {"label": "B (Bundestag 2025 - 2029)"}, "valid_until": "2026-05-01"},
                {"fraction": {"label": "A (Bundestag 2025 - 2029)"}, "valid_until": "2026-01-01"},
            ]
        )
    )
    assert z["fraktion"] == "B"


def test_ohne_fraktion():
    assert spalten(mandat(fraction_membership=[]))["fraktion"] is None


class _Cursor:
    """Nimmt SELECT und UPDATE von ausgeschiedene_nachtragen entgegen."""

    def __init__(self, ohne_ende):
        self.ohne_ende = ohne_ende
        self.updates = []

    def execute(self, sql, params=()):
        if sql.lstrip().upper().startswith('SELECT'):
            self._zeilen = [(aw_id,) for aw_id in self.ohne_ende]
        else:
            self.updates.append(params)

    def fetchall(self):
        return self._zeilen


class _Antwort:
    def __init__(self, status, data=None):
        self.status_code = status
        self._data = data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise fab.requests.HTTPError(str(self.status_code))

    def json(self):
        return {'data': self._data}


def _abrufe(monkeypatch, antworten):
    urls = []

    def get(url, headers, timeout):
        urls.append(url)
        return antworten[url.rsplit('/', 1)[1]]

    monkeypatch.setattr(fab.requests, 'get', get)
    monkeypatch.setattr(fab.time, 'sleep', lambda s: None)
    return urls


def test_ausgeschiedene_bekommen_enddatum_aus_dem_einzelabruf(monkeypatch):
    # Wie Ronja Kemmer am 17.10.2026: nicht mehr in der Liste, im Einzelabruf mit end_date
    urls = _abrufe(monkeypatch, {
        '68639': _Antwort(200, {
            'id': 68639,
            'end_date': '2026-05-21',
            'info': 'Ronja Kemmer ist am 21.05.2026 ausgeschieden. Nachgerückt ist Christoph Naser am 27.05.2026.',
        }),
    })
    cur = _Cursor([68639, 70562])
    assert fab.ausgeschiedene_nachtragen(cur, 161, {70562}) == 1
    assert urls == [f'{fab.BASE}/68639']
    assert cur.updates == [(date(2026, 5, 21), 68639)]


def test_ohne_enddatum_oder_404_bleibt_alles_wie_es_ist(monkeypatch):
    _abrufe(monkeypatch, {'1': _Antwort(200, {'id': 1, 'end_date': None}), '2': _Antwort(404)})
    cur = _Cursor([1, 2])
    assert fab.ausgeschiedene_nachtragen(cur, 161, set()) == 0
    assert cur.updates == []


def test_fehlen_zu_viele_wird_nichts_nachgetragen(monkeypatch):
    urls = _abrufe(monkeypatch, {})
    cur = _Cursor(list(range(fab.MAX_AUSGESCHIEDENE + 1)))
    assert fab.ausgeschiedene_nachtragen(cur, 161, set()) == 0
    assert urls == [] and cur.updates == []
