"""Gemeinsamer Abruf-Ablauf (lib/quelle.py): Protokoll, Wiederholung, Pruefung, Trockenlauf."""

import io
import urllib.error

import pytest

from lib import quelle
from lib.quelle import Lauf, PruefungFehlgeschlagen, QuelleUnbekannt


class _Antwort(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.lastrowid = None
        self._row = None

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        self.conn.sql.append((sql, params))
        if sql.startswith("SELECT id FROM data_sources"):
            self._row = (7,) if params[0] in self.conn.bekannt else None
        if sql.startswith("INSERT INTO data_update_log"):
            self.lastrowid = 42

    def fetchone(self):
        return self._row

    def close(self):
        pass


class _Conn:
    def __init__(self, bekannt=("test-quelle",)):
        self.sql = []
        self.bekannt = set(bekannt)

    def cursor(self):
        return _Cursor(self)

    def anweisungen(self, anfang):
        return [(s, p) for s, p in self.sql if s.startswith(anfang)]


def _http_fehler(code, retry_after=None):
    kopf = {"Retry-After": retry_after} if retry_after else {}
    return urllib.error.HTTPError("https://x", code, "Fehler", kopf, None)


def test_erfolg_schreibt_protokoll_und_stand():
    conn = _Conn()
    with Lauf("test-quelle", conn, schreiben=True, kontext={"lauf": "test"}) as lauf:
        lauf.zaehlen(neu=3, geaendert=2)
        lauf.merken(zonen=16)
    log = conn.anweisungen("UPDATE data_update_log")
    assert len(log) == 1
    assert log[0][1][0] == "success"
    assert log[0][1][1:4] == (3, 2, 0)
    assert '"zonen": 16' in log[0][1][6]
    stand = conn.anweisungen("UPDATE data_sources")
    assert len(stand) == 1 and "last_success" in stand[0][0] and "last_error = NULL" in stand[0][0]


def test_fehler_laesst_last_success_stehen_und_wird_nicht_verschluckt():
    conn = _Conn()
    with pytest.raises(PruefungFehlgeschlagen):
        with Lauf("test-quelle", conn, schreiben=True) as lauf:
            lauf.pruefen(False, "nur 3 von 27 Laendern")
    log = conn.anweisungen("UPDATE data_update_log")
    assert log[0][1][0] == "failed"
    assert "nur 3 von 27" in log[0][1][5]
    stand = conn.anweisungen("UPDATE data_sources")
    assert "last_success" not in stand[0][0]
    assert "last_error = %s" in stand[0][0]


def test_trockenlauf_fasst_die_datenbank_nicht_an():
    conn = _Conn()
    with Lauf("test-quelle", conn) as lauf:
        assert lauf.schreiben is False
        lauf.zaehlen(neu=5)
    assert not conn.anweisungen("INSERT")
    assert not conn.anweisungen("UPDATE")


def test_trockenlauf_geht_ohne_datenbank():
    with Lauf("irgendwas") as lauf:
        lauf.pruefen(True, "ok")


def test_schreiben_ohne_datenbank_ist_ein_fehler():
    with pytest.raises(ValueError):
        Lauf("test-quelle", None, schreiben=True)


def test_unbekannte_quelle_bricht_vor_dem_abruf_ab():
    with pytest.raises(QuelleUnbekannt):
        with Lauf("gibt-es-nicht", _Conn(), schreiben=True):
            pass


def test_abrufen_wiederholt_bei_429_und_legt_roh_ab(tmp_path):
    antworten = [_http_fehler(429, "7"), _http_fehler(503), _Antwort(b'{"ok": true}')]
    gewartet = []
    kopf_gesehen = []

    def oeffnen(req, timeout):
        kopf_gesehen.append(dict(req.header_items()))
        a = antworten.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    with Lauf("test-quelle", roh_dir=tmp_path, oeffnen=oeffnen, schlafen=gewartet.append) as lauf:
        daten = lauf.abrufen("https://x/daten", name="tag/2026-11-28.json", kopf={"X-Key": "geheim"})
    assert daten == b'{"ok": true}'
    assert gewartet == [7, 6]
    assert kopf_gesehen[0]["X-key"] == "geheim"
    assert "ResPublicaBot" in kopf_gesehen[0]["User-agent"]
    dateien = list((tmp_path / "test-quelle").iterdir())
    assert len(dateien) == 1 and dateien[0].name.endswith("_tag_2026-11-28.json")
    assert dateien[0].read_bytes() == daten


def test_abrufen_gibt_bei_404_sofort_auf(tmp_path):
    aufrufe = []

    def oeffnen(req, timeout):
        aufrufe.append(1)
        raise _http_fehler(404)

    with pytest.raises(urllib.error.HTTPError):
        with Lauf("test-quelle", roh_dir=tmp_path, oeffnen=oeffnen, schlafen=lambda s: None) as lauf:
            lauf.abrufen("https://x/fehlt")
    assert len(aufrufe) == 1


def test_abrufen_gibt_nach_fuenf_versuchen_auf(tmp_path):
    aufrufe = []

    def oeffnen(req, timeout):
        aufrufe.append(1)
        raise _http_fehler(500)

    with pytest.raises(urllib.error.HTTPError):
        with Lauf("test-quelle", roh_dir=tmp_path, oeffnen=oeffnen, schlafen=lambda s: None) as lauf:
            lauf.abrufen("https://x/kaputt")
    assert len(aufrufe) == quelle.VERSUCHE


def test_teilweise_wird_als_partial_protokolliert():
    conn = _Conn()
    with Lauf("test-quelle", conn, schreiben=True) as lauf:
        lauf.teilweise = True
    assert conn.anweisungen("UPDATE data_update_log")[0][1][0] == "partial"
    # ein Teil-Erfolg zaehlt als Erfolg fuer den Stand
    assert "last_success" in conn.anweisungen("UPDATE data_sources")[0][0]


def test_laden_schreibt_datei_und_laesst_keinen_rest(tmp_path):
    antworten = [_http_fehler(502), _Antwort(b"x" * 3_000_000)]

    def oeffnen(req, timeout):
        a = antworten.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    ziel = tmp_path / "gross" / "datei.csv.gz"
    with Lauf("test-quelle", roh_dir=tmp_path, oeffnen=oeffnen, schlafen=lambda s: None) as lauf:
        lauf.laden("https://x/gross", ziel)
    assert ziel.stat().st_size == 3_000_000
    assert [p.name for p in ziel.parent.iterdir()] == ["datei.csv.gz"]
