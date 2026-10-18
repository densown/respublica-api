"""Mandate aus abgeordnetenwatch: Mandatsdauer und Fraktion nach Wechsel."""

from datetime import date

import pytest

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
    def __init__(self, status, data=None, headers=None):
        self.status_code = status
        self._data = data
        self.headers = headers or {}

    @property
    def ok(self):
        return self.status_code < 400

    def raise_for_status(self):
        if self.status_code >= 400:
            raise fab.requests.HTTPError(str(self.status_code))

    def json(self):
        return {'data': self._data}


def _abrufe(monkeypatch, antworten, pausen=None):
    """antworten: je Mandat eine Antwort oder eine Liste, die nacheinander kommt."""
    urls = []

    def get(url, headers, timeout):
        urls.append(url)
        a = antworten[url.rsplit('/', 1)[1]]
        return a.pop(0) if isinstance(a, list) else a

    monkeypatch.setattr(fab.requests, 'get', get)
    monkeypatch.setattr(fab.time, 'sleep', (pausen.append if pausen is not None else lambda s: None))
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


def test_einzelabruf_wiederholt_bei_429(monkeypatch):
    # Am 17. und 18.10.2026 kam auf den ersten Einzelabruf sofort 429, der Lauf brach ab
    pausen = []
    urls = _abrufe(monkeypatch, {
        '68639': [
            _Antwort(429, headers={'Retry-After': '5'}),
            _Antwort(429),
            _Antwort(200, {'id': 68639, 'end_date': '2026-05-21'}),
        ],
    }, pausen)
    cur = _Cursor([68639])
    assert fab.ausgeschiedene_nachtragen(cur, 161, set()) == 1
    assert cur.updates == [(date(2026, 5, 21), 68639)]
    assert len(urls) == 3
    # Pause vor dem Abruf, dann Retry-After der Quelle, dann 6 s
    assert pausen == [fab.PAGE_SLEEP_SEC, 5, 6]


def test_dauerhaft_gedrosselt_bleibt_offen_die_anderen_laufen_weiter(monkeypatch):
    _abrufe(monkeypatch, {
        '1': [_Antwort(429) for _ in range(fab.aw.VERSUCHE)],
        '2': _Antwort(200, {'id': 2, 'end_date': '2026-08-31'}),
    })
    cur = _Cursor([1, 2])
    assert fab.ausgeschiedene_nachtragen(cur, 161, set()) == 1
    assert cur.updates == [(date(2026, 8, 31), 2)]


def test_netzwerkfehler_beim_einzelabruf_bricht_nicht_ab(monkeypatch):
    def get(url, headers, timeout):
        if url.endswith('/1'):
            raise fab.requests.ConnectionError('weg')
        return _Antwort(200, {'id': 2, 'end_date': '2026-08-31'})

    monkeypatch.setattr(fab.requests, 'get', get)
    monkeypatch.setattr(fab.time, 'sleep', lambda s: None)
    cur = _Cursor([1, 2])
    assert fab.ausgeschiedene_nachtragen(cur, 161, set()) == 1


def test_listenseite_wiederholt_bei_429(monkeypatch):
    antworten = [_Antwort(503), _Antwort(200, [{'id': 1}])]
    pausen = []
    monkeypatch.setattr(fab.requests, 'get', lambda url, headers, timeout: antworten.pop(0))
    monkeypatch.setattr(fab.time, 'sleep', pausen.append)
    assert fab.fetch_page(0) == {'data': [{'id': 1}]}
    assert pausen == [3]


def test_listenseite_gibt_nach_allen_versuchen_auf(monkeypatch):
    monkeypatch.setattr(fab.requests, 'get', lambda url, headers, timeout: _Antwort(429))
    monkeypatch.setattr(fab.time, 'sleep', lambda s: None)
    with pytest.raises(fab.requests.HTTPError):
        fab.fetch_page(0)
