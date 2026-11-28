"""Strompreise: Leser fuer api.energy-charts.info und Rechenregeln fuer Tageswerte.

Reine Funktionen ohne Netz und Datenbank. scripts/fetch_strom.py ruft ab,
prueft und schreibt.

Antwort von /price?bzn=DE-LU&start=JJJJ-MM-TT&end=JJJJ-MM-TT (laut
Dokumentation https://api.energy-charts.info/):

    {"license_info": "CC BY 4.0 (creativecommons.org/licenses/by/4.0) from Bundesnetzagentur | SMARD.de",
     "unix_seconds": [1795820400, 1795821300, ...],
     "price": [84.12, 83.9, ...],
     "unit": "EUR / MWh",
     "deprecated": false}

Der Leser ist streng: fehlt ein Feld, passt die Einheit nicht oder steht keine
CC-BY-Lizenz in der Antwort, bricht er ab, statt etwas Falsches zu speichern.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

MARKTZEIT = ZoneInfo("Europe/Berlin")
# Harte Grenzen der Day-Ahead-Auktion (SDAC): -500 bis +4000 EUR/MWh.
PREIS_MIN, PREIS_MAX = -500.0, 4000.0


class FormatFehler(ValueError):
    """Die Antwort der Quelle sieht anders aus als erwartet."""


def preise_lesen(antwort: dict[str, Any]) -> list[dict[str, Any]]:
    """Antwort von energy-charts -> Zeilen {zeit_utc, minuten, liefertag, stunde, eur_mwh}.

    Die Dauer eines Zeitraums ergibt sich aus dem Abstand zum naechsten Wert
    (letzter Wert: wie der vorige). Luecken (null) werden ausgelassen.
    """
    if not isinstance(antwort, dict):
        raise FormatFehler("Antwort ist kein Objekt")
    for feld in ("unix_seconds", "price", "unit", "license_info"):
        if feld not in antwort:
            raise FormatFehler(f"Feld {feld} fehlt")
    if antwort.get("deprecated"):
        raise FormatFehler("Endpunkt als veraltet markiert")
    if "eur" not in str(antwort["unit"]).lower() or "mwh" not in str(antwort["unit"]).lower():
        raise FormatFehler(f"unerwartete Einheit {antwort['unit']!r}")
    if "cc by 4.0" not in str(antwort["license_info"]).lower():
        raise FormatFehler(f"keine CC-BY-Lizenz in der Antwort: {antwort['license_info']!r}")
    zeiten, preise = antwort["unix_seconds"], antwort["price"]
    if not isinstance(zeiten, list) or not isinstance(preise, list) or len(zeiten) != len(preise):
        raise FormatFehler("unix_seconds und price passen nicht zusammen")
    if any(b <= a for a, b in zip(zeiten, zeiten[1:])):
        raise FormatFehler("Zeiten nicht aufsteigend")

    out = []
    for i, (t, p) in enumerate(zip(zeiten, preise)):
        if p is None:
            continue
        if not isinstance(p, (int, float)) or not PREIS_MIN <= p <= PREIS_MAX:
            raise FormatFehler(f"Preis ausserhalb der Boersengrenzen: {p!r}")
        if i + 1 < len(zeiten):
            sekunden = zeiten[i + 1] - t
        elif i > 0:
            sekunden = t - zeiten[i - 1]
        else:
            sekunden = 3600
        if sekunden not in (900, 1800, 3600):
            raise FormatFehler(f"unerwarteter Abstand von {sekunden} s")
        utc = datetime.fromtimestamp(t, tz=timezone.utc)
        lokal = utc.astimezone(MARKTZEIT)
        out.append({
            "zeit_utc": utc.replace(tzinfo=None),
            "minuten": sekunden // 60,
            "liefertag": lokal.date(),
            "stunde": lokal.hour,
            "eur_mwh": round(float(p), 2),
        })
    return out


def tag_vollstaendig(zeilen: list[dict[str, Any]], tag: date) -> bool:
    """Deckt die Zone den Liefertag ganz ab? 24 Stunden, an Umstellungstagen 23 oder 25."""
    minuten = sum(z["minuten"] for z in zeilen if z["liefertag"] == tag)
    return minuten == erwartete_minuten(tag)


def erwartete_minuten(tag: date) -> int:
    anfang = datetime(tag.year, tag.month, tag.day, tzinfo=MARKTZEIT)
    morgen = tag + timedelta(days=1)
    ende = datetime(morgen.year, morgen.month, morgen.day, tzinfo=MARKTZEIT)
    return int((ende.astimezone(timezone.utc) - anfang.astimezone(timezone.utc)).total_seconds() // 60)


def stundenmittel(zeilen: list[dict[str, Any]]) -> dict[int, float]:
    """Stunde (Marktzeit) -> Mittel der Werte dieser Stunde, nach Dauer gewichtet."""
    summe: dict[int, float] = {}
    dauer: dict[int, int] = {}
    for z in zeilen:
        summe[z["stunde"]] = summe.get(z["stunde"], 0.0) + float(z["eur_mwh"]) * z["minuten"]
        dauer[z["stunde"]] = dauer.get(z["stunde"], 0) + z["minuten"]
    return {h: round(summe[h] / dauer[h], 2) for h in sorted(summe)}


def guenstigstes_fenster(stunden: dict[int, float], laenge: int = 3) -> dict[str, Any] | None:
    """Die zusammenhaengenden ``laenge`` Stunden mit dem niedrigsten Mittel."""
    hs = sorted(stunden)
    beste = None
    for i in range(len(hs) - laenge + 1):
        fenster = hs[i : i + laenge]
        if fenster[-1] - fenster[0] != laenge - 1:
            continue
        mittel = sum(stunden[h] for h in fenster) / laenge
        if beste is None or mittel < beste["eur_mwh"]:
            beste = {"von": fenster[0], "bis": fenster[-1] + 1, "eur_mwh": round(mittel, 2)}
    return beste


def tagesbericht(je_zone: dict[str, dict[int, float]], heimat: str = "DE-LU") -> dict[str, Any] | None:
    """Kennwerte eines Liefertags aus den Stundenmitteln je Zone: Tagesmittel, billigste und
    teuerste Zone, teuerste und billigste Stunde der Heimatzone, guenstigstes 3-Stunden-Fenster."""
    mittel = {z: round(sum(h.values()) / len(h), 2) for z, h in je_zone.items() if h}
    if not mittel:
        return None
    billig = min(mittel, key=lambda z: (mittel[z], z))
    teuer = max(mittel, key=lambda z: (mittel[z], z))
    out: dict[str, Any] = {
        "zonen": len(mittel),
        "mittel": mittel,
        "billigste": {"zone": billig, "eur_mwh": mittel[billig]},
        "teuerste": {"zone": teuer, "eur_mwh": mittel[teuer]},
        "negativ": sorted(z for z, h in je_zone.items() if any(v < 0 for v in h.values())),
    }
    h = je_zone.get(heimat)
    if h:
        hmin = min(h, key=lambda k: (h[k], k))
        hmax = max(h, key=lambda k: (h[k], -k))
        out["heimat"] = {
            "zone": heimat,
            "mittel": mittel[heimat],
            "billigste_stunde": {"stunde": hmin, "eur_mwh": h[hmin]},
            "teuerste_stunde": {"stunde": hmax, "eur_mwh": h[hmax]},
            "fenster": guenstigstes_fenster(h),
            "negative_stunden": sum(1 for v in h.values() if v < 0),
        }
    return out
