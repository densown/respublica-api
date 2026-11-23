"use strict";

/**
 * Kleine Anfragen: Antworten der Bundesregierung (Migration 021).
 *
 * Quelle DIP (Deutscher Bundestag), importiert von
 * scripts/fetch_kleine_anfragen.py. Zusammenfassung, Schlagzeile, Kernzahlen
 * und story_wert schreibt scripts/summarize_kleine_anfragen.py automatisch;
 * jede Kernzahl ist gegen den Volltext geprueft (Zitat steht woertlich in
 * einem Teil der Bundesregierung). `hinweis` geht bei jeder Antwort mit, damit
 * das Frontend die Kennzeichnung nicht vergessen kann.
 *
 * Endpunkte:
 *   GET /kleine-anfragen            Liste mit Suche und Filtern
 *   GET /kleine-anfragen/facetten   Zaehlungen fuer die Filter
 *   GET /kleine-anfragen/woche      Top-Antworten der letzten Tage nach story_wert
 *   GET /kleine-anfragen/:wp/:nr    eine Antwort (Drucksache wp/nr, z. B. 21/8937)
 *
 * Fragesteller werden per Name auf Abgeordnete der Wahlperiode abgebildet
 * (`fragesteller_mdb`, Link zum Profil), und `?abgeordneter=<aw_id>` filtert
 * auf die Anfragen einer Person. Ohne eigene Tabelle: der Name steht im Kopf
 * der Drucksache, die Zuordnung geschieht beim Ausliefern.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler, ValidationError } = require("../lib/errors");
const { parsePagination, parseIntParam } = require("../lib/validate");

const HINWEIS =
  "Zusammenfassungen und Kernzahlen sind automatisch erstellt. Jede Kernzahl ist gegen den " +
  "Wortlaut der Antwort geprüft; maßgeblich ist die Drucksache (PDF).";
const QUELLE = {
  name: "Dokumentations- und Informationssystem für Parlamentsmaterialien (DIP), Deutscher Bundestag",
  url: "https://dip.bundestag.de",
};

const FRAKTIONEN = Object.freeze({
  afd: "AfD",
  cdu_csu: "CDU/CSU",
  spd: "SPD",
  gruene: "Grüne",
  linke: "Die Linke",
  bsw: "BSW",
  fdp: "FDP",
  mehrere: "mehrere",
  sonstige: "sonstige",
});
const BUNDESTAG_PERIOD = 161;

/**
 * Name fuer den Vergleich: Kleinbuchstaben, ohne Akzente, ohne Titel
 * ("Dr.", "Prof.", "h. c."), Bindestrich ohne Leerraum ("Anne- Mieke").
 */
function nameSchluessel(name) {
  return String(name ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s*-\s*/g, "-")
    .split(/\s+/)
    .filter((w) => w && !w.endsWith(".") && !["prof", "dr"].includes(w))
    .join(" ");
}

/** Vorname und Nachname ohne zweite Vornamen ("udo theodor hemmelgarn" -> "udo hemmelgarn"). */
function kurzSchluessel(schluessel) {
  const w = schluessel.split(" ");
  return w.length > 2 ? `${w[0]} ${w[w.length - 1]}` : schluessel;
}

let mdbCache = { bis: 0, index: null };

/** Map Schluessel -> {aw_id, name}; mehrdeutige Schluessel fallen raus. */
async function mdbIndex(pool) {
  if (mdbCache.index && mdbCache.bis > Date.now()) return mdbCache.index;
  const [rows] = await pool.query(
    `SELECT aw_id, name FROM abgeordnete
      WHERE parliament_period = ? AND typ = 'mandat' AND aw_id IS NOT NULL AND name IS NOT NULL`,
    [BUNDESTAG_PERIOD],
  );
  const index = new Map();
  const doppelt = new Set();
  const setze = (k, wert) => {
    if (!k) return;
    const alt = index.get(k);
    if (alt && alt.aw_id !== wert.aw_id) doppelt.add(k);
    else index.set(k, wert);
  };
  for (const r of rows) {
    const k = nameSchluessel(r.name);
    const wert = { aw_id: r.aw_id, name: r.name };
    setze(k, wert);
    setze(kurzSchluessel(k), wert);
  }
  for (const k of doppelt) index.delete(k);
  mdbCache = { bis: Date.now() + 10 * 60 * 1000, index };
  return index;
}

function findeMdb(index, name) {
  const k = nameSchluessel(name);
  return index.get(k) || index.get(kurzSchluessel(k)) || null;
}

const SORTIERUNGEN = new Set(["datum", "wert", "relevanz"]);
const DATUM_RE = /^\d{4}-\d{2}-\d{2}$/;

const LISTE_FELDER = `
  k.id, k.dokumentnummer, k.wahlperiode, DATE_FORMAT(k.datum, '%Y-%m-%d') AS datum,
  k.titel, k.vorgang_id, k.frage_dokumentnummer, k.fraktion, k.initiative, k.fragesteller,
  k.ressort, k.sachgebiete, k.einzelfragen, k.pdf_url, k.text_zeichen,
  k.nicht_erhoben, k.hat_anlage, k.eingestuft,
  k.schlagzeile, k.zusammenfassung, k.zusammenfassung_en, k.kernzahlen,
  k.story_wert, k.story_grund, DATE_FORMAT(k.ki_am, '%Y-%m-%d') AS ki_am`;

function jsonListe(s) {
  if (!s) return [];
  try {
    const v = JSON.parse(s);
    return Array.isArray(v) ? v : [];
  } catch {
    return [];
  }
}

function mapAntwort(r, index) {
  const fragesteller = jsonListe(r.fragesteller).filter(
    (n) => typeof n === "string" && n.length <= 60 && !/\d/.test(n),
  );
  return {
    id: r.id,
    dokumentnummer: r.dokumentnummer,
    wahlperiode: r.wahlperiode,
    datum: r.datum,
    titel: r.titel,
    frage_dokumentnummer: r.frage_dokumentnummer,
    fraktion: r.fraktion,
    fraktion_name: FRAKTIONEN[r.fraktion] || r.initiative || null,
    // Altbestand kann Fliesstext statt Namen enthalten (Kopf falsch gelesen, z. B. 21/45)
    fragesteller,
    fragesteller_mdb: fragesteller.map((name) => {
      const m = index ? findeMdb(index, name) : null;
      return { name, aw_id: m ? m.aw_id : null };
    }),
    ressort: r.ressort,
    sachgebiete: jsonListe(r.sachgebiete),
    einzelfragen: r.einzelfragen,
    pdf_url: r.pdf_url,
    // DIP braucht einen Titel-Teil im Pfad, "-" genuegt (ohne: "Seite nicht gefunden")
    dip_url: r.vorgang_id ? `https://dip.bundestag.de/vorgang/-/${r.vorgang_id}` : null,
    volltext_vorhanden: Number(r.text_zeichen) > 0,
    nicht_erhoben: Number(r.nicht_erhoben) || 0,
    hat_anlage: Number(r.hat_anlage) === 1,
    eingestuft: Number(r.eingestuft) === 1,
    ausgewertet: r.zusammenfassung != null,
    schlagzeile: r.schlagzeile,
    zusammenfassung: r.zusammenfassung,
    zusammenfassung_en: r.zusammenfassung_en,
    kernzahlen: jsonListe(r.kernzahlen),
    story_wert: r.story_wert,
    story_grund: r.story_grund,
    ausgewertet_am: r.ki_am,
    ...(r.treffer != null && { treffer: String(r.treffer).replace(/\s+/g, " ").trim() }),
  };
}

function parseDatum(value, name) {
  const s = String(value ?? "").trim();
  if (!s) return null;
  const d = new Date(`${s}T00:00:00Z`);
  if (!DATUM_RE.test(s) || Number.isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== s) {
    throw new ValidationError(`${name} muss ein gültiges Datum JJJJ-MM-TT sein`);
  }
  return s;
}

/**
 * Suchbegriffe fuer MATCH ... IN BOOLEAN MODE: jedes Wort Pflicht, mit
 * Praefix-Suche ("Abschieb" findet "Abschiebungen"). Operatoren aus der
 * Eingabe werden entfernt. Woerter unter 3 Zeichen kennt der Index nicht.
 */
function suchbegriffe(q) {
  const woerter = String(q ?? "")
    .replace(/[+\-<>()~*"@]/g, " ")
    .split(/\s+/)
    .map((w) => w.trim())
    .filter((w) => w.length >= 3)
    .slice(0, 8);
  return woerter;
}

function buildFilter(query) {
  const where = [];
  const params = [];
  let match = null;
  let erstesWort = null;

  const q = String(query.q ?? "").trim();
  if (q.length > 200) throw new ValidationError("q zu lang");
  if (q) {
    const woerter = suchbegriffe(q);
    if (woerter.length) {
      match = woerter.map((w) => `+${w}*`).join(" ");
      erstesWort = woerter[0];
      where.push(
        "(MATCH(k.titel, k.zusammenfassung, k.deskriptoren) AGAINST(? IN BOOLEAN MODE) " +
          "OR MATCH(k.volltext) AGAINST(? IN BOOLEAN MODE))",
      );
      params.push(match, match);
    } else {
      where.push("k.titel LIKE ?");
      params.push(`%${q}%`);
    }
  }

  const fraktion = String(query.fraktion ?? "").trim().toLowerCase();
  if (fraktion) {
    if (!FRAKTIONEN[fraktion]) throw new ValidationError("fraktion ungültig");
    where.push("k.fraktion = ?");
    params.push(fraktion);
  }
  const ressort = String(query.ressort ?? "").trim();
  if (ressort) {
    if (ressort.length > 255) throw new ValidationError("ressort zu lang");
    where.push("k.ressort = ?");
    params.push(ressort);
  }
  const sachgebiet = String(query.sachgebiet ?? "").trim();
  if (sachgebiet) {
    if (sachgebiet.length > 120) throw new ValidationError("sachgebiet zu lang");
    where.push("JSON_CONTAINS(k.sachgebiete, JSON_QUOTE(?))");
    params.push(sachgebiet);
  }
  const von = parseDatum(query.von, "von");
  const bis = parseDatum(query.bis, "bis");
  if (von && bis && von > bis) throw new ValidationError("von liegt nach bis");
  if (von) {
    where.push("k.datum >= ?");
    params.push(von);
  }
  if (bis) {
    where.push("k.datum <= ?");
    params.push(bis);
  }
  if (query.wp != null && String(query.wp).trim() !== "") {
    const wp = parseIntParam(query.wp, "wp");
    where.push("k.wahlperiode = ?");
    params.push(wp);
  }
  if (query.min_wert != null && String(query.min_wert).trim() !== "") {
    const w = parseIntParam(query.min_wert, "min_wert");
    if (w < 0 || w > 10) throw new ValidationError("min_wert muss zwischen 0 und 10 liegen");
    where.push("k.story_wert >= ?");
    params.push(w);
  }
  if (["1", "true"].includes(String(query.nur_zahlen ?? "").toLowerCase())) {
    where.push("k.kernzahlen IS NOT NULL AND k.kernzahlen <> '[]'");
  }
  if (["1", "true"].includes(String(query.nicht_erhoben ?? "").toLowerCase())) {
    where.push("k.nicht_erhoben >= 3");
  }
  return { where, params, match, erstesWort };
}

/**
 * Filter auf die Anfragen einer oder eines Abgeordneten. Vorauswahl per
 * Nachname im JSON-Text, dann exakter Namensvergleich wie bei der Anzeige,
 * damit Titel, zweite Vornamen und Schreibweisen gleich behandelt werden.
 */
async function abgeordneterFilter(pool, index, awId, f) {
  const [[mdb]] = await pool.query(
    `SELECT aw_id, name, nachname, fraktion FROM abgeordnete
      WHERE aw_id = ? AND parliament_period = ? AND typ = 'mandat' LIMIT 1`,
    [awId, BUNDESTAG_PERIOD],
  );
  if (!mdb) throw new ValidationError("abgeordneter unbekannt");
  const nachname = String(mdb.nachname || String(mdb.name).split(" ").pop() || "").replace(/[%_\\]/g, "");
  const [kandidaten] = await pool.query(
    "SELECT id, fragesteller FROM kleine_anfragen WHERE fragesteller LIKE ?",
    [`%${nachname}%`],
  );
  const ids = kandidaten
    .filter((r) => jsonListe(r.fragesteller).some((n) => findeMdb(index, n)?.aw_id === mdb.aw_id))
    .map((r) => r.id);
  f.where.push(ids.length ? `k.id IN (${ids.map(() => "?").join(",")})` : "FALSE");
  f.params.push(...ids);
  f.mdb = { aw_id: mdb.aw_id, name: mdb.name, fraktion: mdb.fraktion };
}

const whereSql = (where) => (where.length ? `WHERE ${where.join(" AND ")}` : "");

router.get(
  "/kleine-anfragen",
  asyncHandler(async (req, res) => {
    const { limit, offset } = parsePagination(req.query, { defLimit: 30, maxLimit: 100 });
    const f = buildFilter(req.query);
    const pool = getPool();
    const index = await mdbIndex(pool);
    if (req.query.abgeordneter != null && String(req.query.abgeordneter).trim() !== "") {
      await abgeordneterFilter(pool, index, parseIntParam(req.query.abgeordneter, "abgeordneter"), f);
    }
    let sort = String(req.query.sort ?? "").trim().toLowerCase() || (f.match ? "relevanz" : "datum");
    if (!SORTIERUNGEN.has(sort)) throw new ValidationError("sort ungültig");
    if (sort === "relevanz" && !f.match) sort = "datum";

    const extraFelder = [];
    const extraParams = [];
    if (f.erstesWort) {
      // Ausschnitt um den ersten Treffer im Volltext, fuer die Trefferliste
      extraFelder.push(
        "IF(LOCATE(?, k.volltext) > 0, SUBSTRING(k.volltext, GREATEST(1, LOCATE(?, k.volltext) - 140), 320), NULL) AS treffer",
      );
      extraParams.push(f.erstesWort, f.erstesWort);
    }
    if (sort === "relevanz") {
      extraFelder.push(
        "(MATCH(k.titel, k.zusammenfassung, k.deskriptoren) AGAINST(? IN BOOLEAN MODE) * 3 " +
          "+ MATCH(k.volltext) AGAINST(? IN BOOLEAN MODE)) AS relevanz",
      );
      extraParams.push(f.match, f.match);
    }
    const order = {
      datum: "k.datum DESC, k.id DESC",
      wert: "k.story_wert IS NULL, k.story_wert DESC, k.datum DESC",
      relevanz: "relevanz DESC, k.datum DESC",
    }[sort];

    const [[{ total }]] = await pool.query(
      `SELECT COUNT(*) AS total FROM kleine_anfragen k ${whereSql(f.where)}`,
      f.params,
    );
    const felder = [LISTE_FELDER, ...extraFelder].join(", ");
    const [rows] = await pool.query(
      `SELECT ${felder} FROM kleine_anfragen k ${whereSql(f.where)} ORDER BY ${order} LIMIT ? OFFSET ?`,
      [...extraParams, ...f.params, limit, offset],
    );
    res.json({
      total: Number(total) || 0,
      limit,
      offset,
      sort,
      hinweis: HINWEIS,
      quelle: QUELLE,
      ...(f.mdb && { abgeordneter: f.mdb }),
      daten: rows.map((r) => mapAntwort(r, index)),
    });
  }),
);

router.get(
  "/kleine-anfragen/facetten",
  asyncHandler(async (req, res) => {
    const pool = getPool();
    const [[stand]] = await pool.query(
      `SELECT COUNT(*) AS gesamt, SUM(zusammenfassung IS NOT NULL) AS ausgewertet,
              DATE_FORMAT(MIN(datum), '%Y-%m-%d') AS von, DATE_FORMAT(MAX(datum), '%Y-%m-%d') AS bis
         FROM kleine_anfragen`,
    );
    const [fraktionen] = await pool.query(
      `SELECT fraktion, COUNT(*) AS anzahl FROM kleine_anfragen
        WHERE fraktion IS NOT NULL GROUP BY fraktion ORDER BY anzahl DESC`,
    );
    const [ressorts] = await pool.query(
      `SELECT ressort, COUNT(*) AS anzahl FROM kleine_anfragen
        WHERE ressort IS NOT NULL GROUP BY ressort ORDER BY anzahl DESC`,
    );
    const [sachgebiete] = await pool.query(
      `SELECT j.sachgebiet, COUNT(*) AS anzahl
         FROM kleine_anfragen k,
              JSON_TABLE(k.sachgebiete, '$[*]' COLUMNS (sachgebiet VARCHAR(120) PATH '$')) AS j
        WHERE k.sachgebiete IS NOT NULL AND JSON_VALID(k.sachgebiete)
        GROUP BY j.sachgebiet ORDER BY anzahl DESC`,
    );
    res.json({
      gesamt: Number(stand.gesamt) || 0,
      ausgewertet: Number(stand.ausgewertet) || 0,
      zeitraum: { von: stand.von, bis: stand.bis },
      fraktionen: fraktionen.map((r) => ({
        fraktion: r.fraktion,
        name: FRAKTIONEN[r.fraktion] || r.fraktion,
        anzahl: Number(r.anzahl),
      })),
      ressorts: ressorts.map((r) => ({ ressort: r.ressort, anzahl: Number(r.anzahl) })),
      sachgebiete: sachgebiete.map((r) => ({ sachgebiet: r.sachgebiet, anzahl: Number(r.anzahl) })),
      quelle: QUELLE,
    });
  }),
);

router.get(
  "/kleine-anfragen/woche",
  asyncHandler(async (req, res) => {
    let tage = req.query.tage != null ? parseIntParam(req.query.tage, "tage") : 7;
    let top = req.query.top != null ? parseIntParam(req.query.top, "top") : 10;
    if (tage < 1 || tage > 60) throw new ValidationError("tage muss zwischen 1 und 60 liegen");
    if (top < 1 || top > 30) throw new ValidationError("top muss zwischen 1 und 30 liegen");
    const pool = getPool();
    const [rows] = await pool.query(
      `SELECT ${LISTE_FELDER} FROM kleine_anfragen k
        WHERE k.datum >= CURDATE() - INTERVAL ? DAY AND k.story_wert IS NOT NULL
        ORDER BY k.story_wert DESC, k.datum DESC LIMIT ?`,
      [tage, top],
    );
    const [[{ neu }]] = await pool.query(
      "SELECT COUNT(*) AS neu FROM kleine_anfragen WHERE datum >= CURDATE() - INTERVAL ? DAY",
      [tage],
    );
    const index = await mdbIndex(pool);
    res.json({
      tage,
      neu: Number(neu) || 0,
      hinweis: HINWEIS,
      quelle: QUELLE,
      daten: rows.map((r) => mapAntwort(r, index)),
    });
  }),
);

router.get(
  "/kleine-anfragen/:wp/:nr",
  asyncHandler(async (req, res) => {
    const wp = parseIntParam(req.params.wp, "wp");
    const nr = parseIntParam(req.params.nr, "nr");
    const [rows] = await getPool().query(
      `SELECT ${LISTE_FELDER} FROM kleine_anfragen k WHERE k.dokumentnummer = ? LIMIT 1`,
      [`${wp}/${nr}`],
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const index = await mdbIndex(getPool());
    res.json({ hinweis: HINWEIS, quelle: QUELLE, daten: mapAntwort(rows[0], index) });
  }),
);

module.exports = router;
