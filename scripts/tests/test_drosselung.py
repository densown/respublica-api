"""Drosselung durch abgeordnetenwatch (429): Wartezeiten, Lock und Reihenfolge der Laeufe."""

import io
import os
import subprocess
import urllib.error

import pytest

import fetch_abstimmungen as fa
import fetch_votes as fv
from lib import aw
from lib import log as lib_log


class _Antwort(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _http_fehler(code):
    return urllib.error.HTTPError("https://x", code, "Fehler", {}, None)


@pytest.mark.parametrize(
    "retry_after, versuch, sekunden",
    [
        (None, 0, 3),
        (None, 3, 24),
        (None, 6, aw.MAX_WARTEN),
        ("7", 0, 7),
        ("600", 0, aw.MAX_WARTEN),
        ("Sat, 18 Oct 2026 06:12:30 GMT", 1, 6),
    ],
)
def test_wartezeit(retry_after, versuch, sekunden):
    assert aw.wartezeit(retry_after, versuch) == sekunden


def test_votes_wiederholen_bei_429(monkeypatch):
    # Am 18.10.2026 scheiterten 6731 und 6733 beim ersten 429 ohne zweiten Versuch
    antworten = [_http_fehler(429), _Antwort(b'{"data": {"related_data": {"votes": []}}}')]
    pausen = []

    def urlopen(req, timeout):
        a = antworten.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    monkeypatch.setattr(aw.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(aw.time, "sleep", pausen.append)
    assert fv.fetch_json("https://x") == {"data": {"related_data": {"votes": []}}}
    assert pausen == [3]


def test_votes_warten_bis_die_abstimmungen_fertig_sind(monkeypatch):
    zustaende = [True, True, False]
    pausen = []
    monkeypatch.setattr(fv, "laeuft", lambda name: zustaende.pop(0))
    monkeypatch.setattr(fv.time, "sleep", pausen.append)
    fv.warten_auf_abstimmungen()
    assert pausen == [fv.WARTEN_TAKT_SEK, fv.WARTEN_TAKT_SEK]


def test_votes_warten_hoechstens_bis_zur_grenze(monkeypatch):
    pausen = []
    monkeypatch.setattr(fv, "laeuft", lambda name: True)
    monkeypatch.setattr(fv.time, "sleep", pausen.append)
    fv.warten_auf_abstimmungen()
    assert sum(pausen) == fv.WARTEN_MAX_SEK


def test_votes_warten_auf_das_lock_der_abstimmungen():
    assert fv.WARTEN_AUF == fa.LOCK_NAME


def test_abstimmungen_geben_das_lock_auch_nach_fehler_frei(monkeypatch):
    ereignisse = []
    monkeypatch.setattr(fa, "acquire_lock", lambda name: ereignisse.append(("lock", name)) or True)
    monkeypatch.setattr(fa, "release_lock", lambda name: ereignisse.append(("frei", name)))

    def abgleichen():
        ereignisse.append(("lauf",))
        raise RuntimeError("kaputt")

    monkeypatch.setattr(fa, "abgleichen", abgleichen)
    with pytest.raises(RuntimeError):
        fa.main()
    assert ereignisse == [("lock", fa.LOCK_NAME), ("lauf",), ("frei", fa.LOCK_NAME)]


def test_abstimmungen_laufen_nicht_doppelt(monkeypatch):
    monkeypatch.setattr(fa, "acquire_lock", lambda name: False)
    monkeypatch.setattr(fa, "abgleichen", lambda: pytest.fail("darf nicht laufen"))
    assert fa.main() == 1


def test_laeuft_nur_bei_lebendem_fremdem_prozess(monkeypatch, tmp_path):
    monkeypatch.setattr(lib_log, "LOG_DIR", tmp_path)
    pid_datei = tmp_path / "x.pid"
    assert not lib_log.laeuft("x")

    pid_datei.write_text(str(os.getppid()))
    assert lib_log.laeuft("x")

    pid_datei.write_text(str(os.getpid()))
    assert not lib_log.laeuft("x")

    beendet = subprocess.Popen(["true"])
    beendet.wait()
    pid_datei.write_text(str(beendet.pid))
    assert not lib_log.laeuft("x")

    pid_datei.write_text("kaputt")
    assert not lib_log.laeuft("x")
