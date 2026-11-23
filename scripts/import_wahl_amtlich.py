#!/usr/bin/env python3
"""Amtliche Kreisergebnisse von Bundestags- und Europawahl nach `wahlen`.

Quelle sind die offenen Dateien der Bundeswahlleiterin:
  Europawahl:     …/europawahlen/JJJJ/ergebnisse/opendata/ewJJ/csv/kerg2.csv
                  (enthält die Kreise, schon in der Wahlnacht als vorläufiges Ergebnis)
  Bundestagswahl: Kreisdatei btwJJJJkreis.csv unter "Weitere Ergebnisse"
                  (erscheint erst Wochen nach der Wahl; kerg2.csv dient als
                  amtliche Gesamtsumme zur Gegenprobe)

Ohne --schreiben wird nur geprüft und berichtet. Geschrieben wird nur, wenn
alle Prüfungen bestehen (Zahl der Kreise, Summen gegen die amtliche
Gesamtzeile, Plausibilität je Kreis). Der Lauf ist wiederholbar: aus einem
vorläufigen wird so das endgültige Ergebnis.

  python scripts/import_wahl_amtlich.py european 2024            # prüfen
  python scripts/import_wahl_amtlich.py european 2024 --schreiben
  python scripts/import_wahl_amtlich.py federal 2025 --datei btw2025kreis.csv --summe kerg2.csv
  python scripts/import_wahl_amtlich.py state 2026 --land 15     # Landtagswahl, Leser je Land in LAENDER
  python scripts/import_wahl_amtlich.py --auto --schreiben       # für Cron

--auto sieht in `wahltermine` nach Bundestags- und Europawahlen der letzten
90 Tage und versucht den Import. Außerhalb dieser Zeit tut der Lauf nichts.
"""
from __future__ import annotations

import argparse
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import wahl_amtlich as wa  # noqa: E402

BASIS = "https://www.bundeswahlleiterin.de"
AUTO_TAGE = 90
EBENE_TYP = {"bund": "federal", "eu": "european"}


def url_kerg2(typ: str, jahr: int) -> str:
    jj = f"{jahr % 100:02d}"
    if typ == "european":
        return f"{BASIS}/europawahlen/{jahr}/ergebnisse/opendata/ew{jj}/csv/kerg2.csv"
    return f"{BASIS}/bundestagswahlen/{jahr}/ergebnisse/opendata/btw{jj}/csv/kerg2.csv"


def hole(url: str) -> bytes | None:
    """Lädt eine Datei. 404 heißt: noch nicht veröffentlicht (None)."""
    anfrage = urllib.request.Request(url, headers={"User-Agent": "wahl-import/1.0"})
    try:
        with urllib.request.urlopen(anfrage, timeout=60) as antwort:
            return antwort.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def finde_kreisdatei(jahr: int) -> str | None:
    """Sucht den Link auf btwJJJJkreis.csv (die Adresse enthält eine Zufallskennung)."""
    for seite in ("ergebnisse/weitere-ergebnisse.html", "ergebnisse.html"):
        seiten_url = f"{BASIS}/bundestagswahlen/{jahr}/{seite}"
        roh = hole(seiten_url)
        if not roh:
            continue
        m = re.search(rf'href="([^"]*btw{jahr}kreis\.csv)"', wa.text_aus_bytes(roh))
        if m:
            # Der Link steht relativ auf der Seite ("../../../dam/jcr/…").
            return urllib.parse.urljoin(seiten_url, m.group(1))
    return None


# Landtagswahlen: jede Landeswahlleitung hat ein eigenes Format, also je Wahl ein Eintrag.
# `summe` ist eine zweite Datei mit der Landeszeile, wenn die Hauptdatei keine hat.
_MV = "https://www.laiv-mv.de/static/LAIV/Wahlen/2-Landtagswahlen/2026/Ergebnisse/"
LAENDER = {
    ("08", 2026): {"name": "Baden-Württemberg", "datum": date(2026, 3, 8), "kreise": 44,
                   "url": "https://wahlen.statistik-bw.de/ltw26/ltw26-ergebnisse.csv",
                   "lies": lambda roh, summe: wa.lies_bw(wa.text_aus_bytes(roh))},
    ("07", 2026): {"name": "Rheinland-Pfalz", "datum": date(2026, 3, 22), "kreise": 36,
                   "url": "https://www.wahlen.rlp.de/fileadmin/wahlen.rlp.de/dokumente-wahlen/ltw/Ergebnisdateien/2026/Endgueltiges_Ergebnis_LW_2026_LK_KS.xlsx",
                   "lies": lambda roh, summe: wa.lies_rp(wa.lies_xlsx(roh))},
    ("15", 2026): {"name": "Sachsen-Anhalt", "datum": date(2026, 9, 6), "kreise": 14,
                   "url": "https://wahlergebnisse.sachsen-anhalt.de/wahlen/lt26/downloads/Ergebnisse_Land_RKR_WKR_LT_2026.csv",
                   "lies": lambda roh, summe: wa.lies_st(wa.text_aus_bytes(roh))},
    ("13", 2026): {"name": "Mecklenburg-Vorpommern", "datum": date(2026, 9, 20), "kreise": 8,
                   "url": _MV + "l_gemeinden.csv", "summe": _MV + "l_wahlkreise.csv",
                   "lies": lambda roh, summe: wa.lies_mv(wa.text_aus_bytes(roh), wa.text_aus_bytes(summe) if summe else None)},
    ("11", 2026): {"name": "Berlin", "datum": date(2026, 9, 20), "kreise": 1,
                   "url": "https://www.wahlen-berlin.de/wahlen/BE2026/Afspraes/AGH/DL/DL_BE_AGHBVV2026.xlsx",
                   "lies": lambda roh, summe: wa.lies_be(wa.lies_xlsx(roh, "AGH_W2"))},
    # Der Pfad hat keine Jahreszahl. Bis zur Wahl am 18.04.2027 liegt dort das Ergebnis von 2022.
    ("10", 2022): {"name": "Saarland", "datum": date(2022, 3, 27), "kreise": 6,
                   "url": "https://wahlergebnis.saarland.de/LTW/KERG_SAARLAND.csv",
                   "lies": lambda roh, summe: wa.lies_sl(wa.text_aus_bytes(roh))},
}


def lade_land(land: str, jahr: int, datei: str | None, summe: str | None) -> dict | None:
    eintrag = LAENDER.get((land, jahr))
    if not eintrag:
        bekannt = ", ".join(f"{l} {j}" for l, j in sorted(LAENDER))
        raise SystemExit(f"Für Land {land} und Jahr {jahr} gibt es keinen Leser. Vorhanden: {bekannt}")
    roh = Path(datei).read_bytes() if datei else hole(eintrag["url"])
    if roh is None:
        return None
    roh_summe = None
    if summe:
        roh_summe = Path(summe).read_bytes()
    elif eintrag.get("summe") and not datei:
        roh_summe = hole(eintrag["summe"])
    wahl = eintrag["lies"](roh, roh_summe)
    wahl["datum"] = eintrag["datum"]
    wahl["quelle"] = datei or eintrag["url"]
    wahl["erwartete_kreise"] = eintrag["kreise"]
    fremd = sorted(a for a in wahl["kreise"] if not a.startswith(land))
    if fremd:
        raise wa.FormatFehler(f"Kreise außerhalb des Landes {land}: {fremd[:5]}")
    return wahl


def lade_wahl(typ: str, jahr: int, datei: str | None, summe: str | None, land: str | None = None) -> dict | None:
    """Liefert die gelesene Wahl oder None, wenn die Datei noch fehlt."""
    if typ == "state":
        return lade_land(land, jahr, datei, summe)
    def lies(pfad_oder_url: str | None, url: str | None) -> str | None:
        if pfad_oder_url:
            return wa.text_aus_bytes(Path(pfad_oder_url).read_bytes())
        roh = hole(url) if url else None
        return wa.text_aus_bytes(roh) if roh else None

    if typ == "european":
        quelle = url_kerg2(typ, jahr)
        text = lies(datei, quelle)
        if text is None:
            return None
        wahl = wa.lies_kerg2(text, gebietsart="Kreis", stimme="1")
    elif typ == "federal":
        quelle = None if datei else finde_kreisdatei(jahr)
        text = lies(datei, quelle)
        if text is None:
            return None
        wahl = wa.lies_btw_kreis(text)
        gesamt = lies(summe, None if datei and not summe else url_kerg2(typ, jahr))
        if gesamt:
            k2 = wa.lies_kerg2(gesamt, gebietsart="Kreis", stimme="2")
            wahl["soll"], wahl["datum"] = k2["soll"], k2["datum"]
            wahl["status"] = wahl["status"] or k2["status"]
    else:
        raise SystemExit(f"Unbekannte Wahlart: {typ} (federal, european oder state)")
    wahl["kreise"] = wa.fasse_zusammen(wahl["kreise"])
    wahl["quelle"] = datei or quelle
    return wahl


SPALTEN = ["typ", "ags", "election_year", "election_date", "state", "county", "eligible_voters",
           "number_voters", "valid_votes", "invalid_votes", "turnout", *wa.PARTY_COLS, "other",
           "winning_party", "election_type", "round"]
SCHLUESSEL = {"typ", "ags", "election_year", "election_type", "round"}
UPSERT = (
    f"INSERT INTO wahlen ({', '.join(SPALTEN)}) VALUES ({', '.join('%(' + s + ')s' for s in SPALTEN)}) "
    "ON DUPLICATE KEY UPDATE "
    + ", ".join(f"{s} = VALUES({s})" for s in SPALTEN if s not in SCHLUESSEL and s != "election_date")
    + ", election_date = COALESCE(VALUES(election_date), election_date)"
)


def bestand(db, typ: str, jahr: int, land: str | None = None) -> dict[str, dict]:
    cur = db.cursor(dictionary=True)
    cur.execute(
        f"SELECT ags, {', '.join(wa.WERT_SPALTEN)} FROM wahlen "
        "WHERE typ = %s AND election_year = %s AND election_type = '' AND round = 0 AND ags LIKE %s",
        (typ, jahr, (land or "") + "%"),
    )
    return {z["ags"]: z for z in cur.fetchall()}


def kreise_der_vorwahl(db, typ: str, jahr: int) -> int | None:
    cur = db.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM wahlen WHERE typ = %s AND election_year = "
        "(SELECT MAX(election_year) FROM wahlen WHERE typ = %s AND election_year < %s)",
        (typ, typ, jahr),
    )
    n = cur.fetchone()[0]
    return n or None


def laufe(db, typ: str, jahr: int, *, datei=None, summe=None, schreiben=False, land=None) -> int:
    """0 = in Ordnung oder nichts zu tun, 1 = Prüfung gescheitert."""
    name = f"{typ} {jahr}" + (f" Land {land}" if land else "")
    try:
        wahl = lade_wahl(typ, jahr, datei, summe, land)
    except wa.FormatFehler as e:
        print(f"FEHLER {name}: Datei hat ein unerwartetes Format: {e}")
        return 1
    if wahl is None:
        print(f"{name}: amtliche Datei noch nicht veröffentlicht, nichts zu tun")
        return 0

    erwartet = wahl.get("erwartete_kreise") or (kreise_der_vorwahl(db, typ, jahr) if db else None)
    fehler = wa.pruefe(wahl["kreise"], soll=wahl["soll"], erwartete_kreise=erwartet)
    if wahl["soll"] is None:
        fehler.append("keine amtliche Gesamtzeile zur Gegenprobe")
    k = wahl["kreise"]
    ber, wae = sum(x["eligible"] for x in k.values()), sum(x["voters"] for x in k.values())
    print(f"{name}: {len(k)} Kreise, Status {wahl['status'] or 'unbekannt'}, "
          f"Wahlberechtigte {ber}, Wählende {wae}, Beteiligung {wae / ber:.2%}" if ber else f"{name}: leer")
    print(f"  Quelle: {wahl['quelle']}")
    if fehler:
        print(f"FEHLER {name}: {len(fehler)} Prüfung(en) gescheitert, es wird nichts geschrieben")
        for f in fehler:
            print(f"  - {f}")
        return 1

    zeilen = wa.zu_zeilen(k, typ=typ, jahr=jahr, datum=wahl["datum"], land=land)
    if not db:
        print("  Prüfungen bestanden (ohne Datenbank, kein Vergleich)")
        return 0
    b = wa.vergleiche(zeilen, bestand(db, typ, jahr, land))
    print(f"  Vergleich mit der Datenbank: {b['neu']} neu, {b['geaendert']} geändert, {b['gleich']} gleich, "
          f"größte Abweichung {b['groesste_abweichung'] * 100:.2f} Punkte")
    for bsp in b["beispiele"]:
        print(f"    {bsp}")
    if b["nur_in_db"]:
        print(f"FEHLER {name}: Kreise nur in der Datenbank: {', '.join(b['nur_in_db'][:10])}. Nichts geschrieben.")
        return 1
    if not schreiben:
        print("  Nur geprüft. Zum Schreiben: --schreiben")
        return 0
    if b["neu"] + b["geaendert"] == 0:
        print("  Datenbank ist schon auf diesem Stand")
        return 0
    cur = db.cursor()
    cur.executemany(UPSERT, zeilen)
    db.commit()
    print(f"  GESCHRIEBEN: {b['neu']} neue und {b['geaendert']} geänderte Kreise")
    return 0


def auto_wahlen(db, heute: date) -> list[tuple[str, int]]:
    cur = db.cursor()
    cur.execute(
        "SELECT ebene, datum FROM wahltermine WHERE ebene IN ('bund','eu') AND datum BETWEEN %s AND %s",
        (heute - timedelta(days=AUTO_TAGE), heute),
    )
    return [(EBENE_TYP[e], d.year) for e, d in cur.fetchall()]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("typ", nargs="?", choices=["federal", "european", "state"])
    p.add_argument("jahr", nargs="?", type=int)
    p.add_argument("--land", help="Landesschlüssel für state, zweistellig (08, 07, 15, 13, 11, 10)")
    p.add_argument("--datei", help="lokale Datei statt Download")
    p.add_argument("--summe", help="lokale Datei mit der Gesamtsumme (Bundestagswahl: kerg2.csv, MV: l_wahlkreise.csv)")
    p.add_argument("--schreiben", action="store_true", help="in die Datenbank schreiben")
    p.add_argument("--ohne-db", action="store_true", help="nur Datei prüfen, keine Datenbank")
    p.add_argument("--auto", action="store_true", help="Wahlen der letzten 90 Tage aus wahltermine")
    a = p.parse_args(argv)
    if not a.auto and not (a.typ and a.jahr):
        p.error("Wahlart und Jahr angeben oder --auto")
    if a.typ == "state" and not a.land:
        p.error("state braucht --land")

    db = None
    if not a.ohne_db:
        from lib.db import get_db
        from lib.env import load_env
        load_env()
        db = get_db(autocommit=False)
    try:
        if a.auto:
            if not db:
                p.error("--auto braucht die Datenbank")
            status = 0
            wahlen = auto_wahlen(db, date.today())
            if not wahlen:
                print(f"{date.today()}: keine Bundestags- oder Europawahl in den letzten {AUTO_TAGE} Tagen, nichts zu tun")
            for typ, jahr in wahlen:
                status |= laufe(db, typ, jahr, schreiben=a.schreiben)
            return status
        return laufe(db, a.typ, a.jahr, datei=a.datei, summe=a.summe, schreiben=a.schreiben, land=a.land)
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    sys.exit(main())
