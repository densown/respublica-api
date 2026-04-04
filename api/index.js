"use strict";

const path = require("path");
const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const PORT = Number.parseInt(process.env.PORT || "3002", 10);
const DB_NAME = process.env.DB_NAME || "respublica_gesetze";

let pool;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST || "localhost",
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD || "",
      database: DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      charset: "utf8mb4",
    });
  }
  return pool;
}

function formatDate(val) {
  if (val == null) return null;
  if (val instanceof Date) return val.toISOString().slice(0, 10);
  return String(val).slice(0, 10);
}

/** Aktuelle Sitzverteilung Bundestag, 21. Wahlperiode (fest codiert) */
const BUNDESTAG_SITZVERTEILUNG_WP21 = [
  { partei: "Linke", farbe: "#BE3075", sitze: 28, position: 0 },
  { partei: "BSW", farbe: "#6B2D5B", sitze: 10, position: 1 },
  { partei: "Grüne", farbe: "#1AA037", sitze: 117, position: 2 },
  { partei: "SPD", farbe: "#E3000F", sitze: 120, position: 3 },
  { partei: "FDP", farbe: "#FFED00", sitze: 91, position: 4 },
  { partei: "CDU/CSU", farbe: "#000000", sitze: 208, position: 5 },
  { partei: "AfD", farbe: "#009EE0", sitze: 78, position: 6 },
  { partei: "Fraktionslos", farbe: "#808080", sitze: 4, position: 7 },
];

const app = express();
app.use(cors());
app.use(express.json());

/** Liste: kein diff (kann sehr groß sein) */
app.get("/api/gesetze", async (req, res) => {
  try {
    const [rows] = await getPool().query(
      `SELECT
         a.id,
         g.kuerzel AS kuerzel,
         g.kuerzel AS name,
         a.datum,
         a.zusammenfassung,
         a.kontext,
         a.bgbl_referenz,
         a.poll_id
       FROM aenderungen a
       INNER JOIN gesetze g ON g.id = a.gesetz_id
       ORDER BY a.datum DESC, a.id DESC`
    );
    const out = rows.map((r) => ({
      id: r.id,
      kuerzel: r.kuerzel,
      name: r.name,
      datum: formatDate(r.datum),
      zusammenfassung: r.zusammenfassung,
      kontext: r.kontext,
      bgbl_referenz: r.bgbl_referenz,
      poll_id: r.poll_id,
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Statistik Gesetze / Änderungen (vor :id registrieren) */
app.get("/api/gesetze/stats", async (_req, res) => {
  try {
    const [[row]] = await getPool().query(
      `SELECT COUNT(DISTINCT g.id) AS gesetze_count,
              COUNT(DISTINCT a.id) AS aenderungen_count
       FROM gesetze g
       LEFT JOIN aenderungen a ON a.gesetz_id = g.id`
    );
    res.json({
      gesetze_count: Number(row.gesetze_count) || 0,
      aenderungen_count: Number(row.aenderungen_count) || 0,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Einzeln inkl. vollem diff */
app.get("/api/gesetze/:id", async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT
         a.id,
         g.kuerzel AS kuerzel,
         g.kuerzel AS name,
         a.datum,
         a.zusammenfassung,
         a.kontext,
         a.bgbl_referenz,
         a.diff
       FROM aenderungen a
       INNER JOIN gesetze g ON g.id = a.gesetz_id
       WHERE a.id = ?
       LIMIT 1`,
      [id]
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const r = rows[0];
    res.json({
      id: r.id,
      kuerzel: r.kuerzel,
      name: r.name,
      datum: formatDate(r.datum),
      zusammenfassung: r.zusammenfassung,
      kontext: r.kontext,
      bgbl_referenz: r.bgbl_referenz,
      poll_id: r.poll_id,
      diff: r.diff,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Neueste Abstimmungen (vor :poll_id registrieren) */
app.get("/api/abstimmungen/latest", async (req, res) => {
  let limit = Number.parseInt(String(req.query.limit ?? "3"), 10);
  if (!Number.isFinite(limit) || limit < 1) limit = 3;
  if (limit > 10) limit = 10;
  try {
    const [rows] = await getPool().query(
      `SELECT DISTINCT poll_id, poll_titel, poll_datum
       FROM abstimmungen
       ORDER BY poll_datum DESC
       LIMIT ?`,
      [limit]
    );
    const out = rows.map((r) => ({
      poll_id: r.poll_id,
      poll_titel: r.poll_titel,
      poll_datum: formatDate(r.poll_datum),
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/bundestag/sitzverteilung", (_req, res) => {
  res.json(BUNDESTAG_SITZVERTEILUNG_WP21);
});

/** Bundestag: alle Abgeordneten (DB), sortiert nach Fraktion, Nachname */
app.get("/api/bundestag/abgeordnete", async (_req, res) => {
  try {
    const [rows] = await getPool().query(
      `SELECT * FROM abgeordnete ORDER BY fraktion, nachname`
    );
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Bundestag: ein Abgeordneter nach Abgeordnetenwatch-Mandats-ID (aw_id) */
app.get("/api/bundestag/abgeordnete/:id", async (req, res) => {
  const awId = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(awId)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT * FROM abgeordnete WHERE aw_id = ? LIMIT 1`,
      [awId]
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    res.json(rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/bundestag/abstimmungen", async (_req, res) => {
  try {
    const [rows] = await getPool().query(
      `SELECT DISTINCT poll_id, poll_titel, poll_datum
       FROM abstimmungen
       ORDER BY poll_datum DESC`
    );
    const out = rows.map((r) => ({
      poll_id: r.poll_id,
      poll_titel: r.poll_titel,
      poll_datum: formatDate(r.poll_datum),
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/bundestag/abstimmungen/:pollId", async (req, res) => {
  const pollId = Number.parseInt(req.params.pollId, 10);
  if (!Number.isFinite(pollId)) {
    res.status(400).json({ error: "Ungültige poll_id" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT partei, ja, nein, enthalten, abwesend, poll_titel, poll_datum
       FROM abstimmungen
       WHERE poll_id = ?
       ORDER BY partei`,
      [pollId]
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const r0 = rows[0];
    let ja_gesamt = 0;
    let nein_gesamt = 0;
    let enthalten_gesamt = 0;
    let abwesend_gesamt = 0;
    const fraktionen = rows.map((r) => {
      const ja = Number(r.ja) || 0;
      const nein = Number(r.nein) || 0;
      const enthalten = Number(r.enthalten) || 0;
      const abwesend = Number(r.abwesend) || 0;
      ja_gesamt += ja;
      nein_gesamt += nein;
      enthalten_gesamt += enthalten;
      abwesend_gesamt += abwesend;
      return { partei: r.partei, ja, nein, enthalten, abwesend };
    });
    res.json({
      poll_id: pollId,
      poll_titel: r0.poll_titel,
      poll_datum: formatDate(r0.poll_datum),
      ergebnis: {
        ja_gesamt,
        nein_gesamt,
        enthalten_gesamt,
        abwesend_gesamt,
      },
      fraktionen,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/abstimmungen/:poll_id", async (req, res) => {
  const pollId = Number.parseInt(req.params.poll_id, 10);
  if (!Number.isFinite(pollId)) {
    res.status(400).json({ error: "Ungültige poll_id" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT
         partei,
         ja,
         nein,
         enthalten,
         abwesend,
         poll_titel,
         poll_datum
       FROM abstimmungen
       WHERE poll_id = ?
       ORDER BY partei`,
      [pollId]
    );
    const out = rows.map((r) => ({
      partei: r.partei,
      ja: r.ja,
      nein: r.nein,
      enthalten: r.enthalten,
      abwesend: r.abwesend,
      poll_titel: r.poll_titel,
      poll_datum: formatDate(r.poll_datum),
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Urteile: Liste */
app.get("/api/urteile", async (req, res) => {
  try {
    const rechtsgebiet = req.query.rechtsgebiet || null;
    const gericht = req.query.gericht || null;
    let query = `
      SELECT id, doc_id, gericht, senat, typ, datum,
             aktenzeichen, leitsatz, zusammenfassung,
             auswirkung, rechtsgebiet
      FROM urteile
      WHERE 1=1
    `;
    const params = [];
    if (rechtsgebiet) { query += ` AND rechtsgebiet LIKE ?`; params.push(`%${rechtsgebiet}%`); }
    if (gericht)      { query += ` AND gericht = ?`;         params.push(gericht); }
    query += ` ORDER BY datum DESC, id DESC`;
    const [rows] = await getPool().query(query, params);
    res.json(rows.map(r => ({ ...r, datum: formatDate(r.datum) })));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Urteile: Einzeln */
app.get("/api/urteile/:id", async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) { res.status(400).json({ error: "Ungültige id" }); return; }
  try {
    const [rows] = await getPool().query(
      `SELECT id, doc_id, gericht, senat, typ, datum,
              aktenzeichen, ecli, leitsatz, tenor,
              zusammenfassung, auswirkung, rechtsgebiet
       FROM urteile WHERE id = ? LIMIT 1`, [id]
    );
    if (!rows.length) { res.status(404).json({ error: "Nicht gefunden" }); return; }
    const [gesetze] = await getPool().query(
      `SELECT gesetz_kuerzel FROM urteil_gesetze WHERE urteil_id = ?`, [id]
    );
    const r = rows[0];
    res.json({
      ...r,
      datum: formatDate(r.datum),
      gesetze: gesetze.map(g => g.gesetz_kuerzel)
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** EU-Recht: Statistiken (vor :id registrieren) */
app.get("/api/eu-recht/stats", async (_req, res) => {
  try {
    const pool = getPool();
    const [[{ total }]] = await pool.query(
      "SELECT COUNT(*) AS total FROM eu_rechtsakte"
    );
    const [byTyp] = await pool.query(
      "SELECT typ, COUNT(*) AS c FROM eu_rechtsakte GROUP BY typ ORDER BY c DESC"
    );
    const [byRg] = await pool.query(
      `SELECT rechtsgebiet, COUNT(*) AS c FROM eu_rechtsakte
       GROUP BY rechtsgebiet ORDER BY c DESC LIMIT 30`
    );
    const [[latest]] = await pool.query(
      "SELECT MAX(datum) AS latest_datum, MAX(created_at) AS latest_created FROM eu_rechtsakte"
    );
    res.json({
      total,
      by_typ: byTyp,
      by_rechtsgebiet: byRg,
      latest_datum: formatDate(latest?.latest_datum),
      latest_created: latest?.latest_created,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** EU-Recht: Liste mit Paginierung */
app.get("/api/eu-recht", async (req, res) => {
  try {
    const typ = req.query.typ || null;
    const rechtsgebiet = req.query.rechtsgebiet || null;
    const search = req.query.search || null;
    let limit = Number.parseInt(String(req.query.limit || "50"), 10);
    let offset = Number.parseInt(String(req.query.offset || "0"), 10);
    if (!Number.isFinite(limit) || limit < 1) limit = 50;
    if (limit > 200) limit = 200;
    if (!Number.isFinite(offset) || offset < 0) offset = 0;

    const allowedTyp = new Set(["REG", "DIR", "DEC", "REC", "OTHER"]);
    let where = "WHERE 1=1";
    const params = [];
    if (typ && allowedTyp.has(String(typ))) {
      where += " AND typ = ?";
      params.push(String(typ));
    }
    if (rechtsgebiet) {
      where += " AND rechtsgebiet = ?";
      params.push(String(rechtsgebiet));
    }
    if (search) {
      const s = `%${String(search).trim()}%`;
      where += " AND (titel_de LIKE ? OR titel_en LIKE ? OR celex LIKE ?)";
      params.push(s, s, s);
    }

    const pool = getPool();
    const [[{ total }]] = await pool.query(
      `SELECT COUNT(*) AS total FROM eu_rechtsakte ${where}`,
      params
    );

    const [listRows] = await pool.query(
      `SELECT id, celex, titel_de, titel_en, typ, typ_label, datum, in_kraft,
              zusammenfassung, rechtsgebiet, eurlex_url, created_at
       FROM eu_rechtsakte
       ${where}
       ORDER BY datum DESC, id DESC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    const ids = listRows.map((r) => r.id);
    /** @type {Record<number, Array<{ id: number, kuerzel: string }>>} */
    const linked = {};
    if (ids.length) {
      const ph = ids.map(() => "?").join(",");
      const [linkRows] = await pool.query(
        `SELECT j.eu_rechtsakt_id AS eid, g.id AS gesetz_id, g.kuerzel
         FROM eu_rechtsakt_gesetze j
         INNER JOIN gesetze g ON g.id = j.gesetz_id
         WHERE j.eu_rechtsakt_id IN (${ph})
         ORDER BY g.kuerzel`,
        ids
      );
      for (const lr of linkRows) {
        const eid = lr.eid;
        if (!linked[eid]) linked[eid] = [];
        linked[eid].push({ id: lr.gesetz_id, kuerzel: lr.kuerzel });
      }
    }

    const items = listRows.map((r) => ({
      id: r.id,
      celex: r.celex,
      titel_de: r.titel_de,
      titel_en: r.titel_en,
      typ: r.typ,
      typ_label: r.typ_label,
      datum: formatDate(r.datum),
      in_kraft: r.in_kraft,
      zusammenfassung: r.zusammenfassung,
      rechtsgebiet: r.rechtsgebiet,
      eurlex_url: r.eurlex_url,
      linked_gesetze: linked[r.id] || [],
    }));

    res.json({ total, limit, offset, items });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** EU-Recht: Einzelansicht */
app.get("/api/eu-recht/:id", async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT id, celex, titel_de, titel_en, typ, typ_label, datum, in_kraft,
              eurovoc_tags, zusammenfassung, rechtsgebiet, eurlex_url, created_at
       FROM eu_rechtsakte WHERE id = ? LIMIT 1`,
      [id]
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const r = rows[0];
    const [linkRows] = await getPool().query(
      `SELECT g.id AS gesetz_id, g.kuerzel
       FROM eu_rechtsakt_gesetze j
       INNER JOIN gesetze g ON g.id = j.gesetz_id
       WHERE j.eu_rechtsakt_id = ?
       ORDER BY g.kuerzel`,
      [id]
    );
    let tags = r.eurovoc_tags;
    if (typeof tags === "string") {
      try {
        tags = JSON.parse(tags);
      } catch {
        /* bleibt String */
      }
    }
    res.json({
      id: r.id,
      celex: r.celex,
      titel_de: r.titel_de,
      titel_en: r.titel_en,
      typ: r.typ,
      typ_label: r.typ_label,
      datum: formatDate(r.datum),
      in_kraft: r.in_kraft,
      eurovoc_tags: tags,
      zusammenfassung: r.zusammenfassung,
      rechtsgebiet: r.rechtsgebiet,
      eurlex_url: r.eurlex_url,
      linked_gesetze: linkRows.map((x) => ({
        id: x.gesetz_id,
        kuerzel: x.kuerzel,
      })),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** EU-Urteile: Statistiken (vor :id registrieren) */
app.get("/api/eu-urteile/stats", async (_req, res) => {
  try {
    const pool = getPool();
    const [[{ total }]] = await pool.query(
      "SELECT COUNT(*) AS total FROM eu_urteile WHERE quality_ok = 1"
    );
    const [byGericht] = await pool.query(
      "SELECT gericht, COUNT(*) AS c FROM eu_urteile WHERE quality_ok = 1 GROUP BY gericht ORDER BY gericht"
    );
    const [byRg] = await pool.query(
      `SELECT rechtsgebiet, COUNT(*) AS c FROM eu_urteile
       WHERE rechtsgebiet IS NOT NULL AND rechtsgebiet != '' AND quality_ok = 1
       GROUP BY rechtsgebiet ORDER BY c DESC LIMIT 50`
    );
    const [[latest]] = await pool.query(
      "SELECT MAX(datum) AS latest_datum, MAX(created_at) AS latest_created FROM eu_urteile"
    );
    res.json({
      total,
      by_gericht: byGericht,
      by_rechtsgebiet: byRg,
      latest_datum: formatDate(latest?.latest_datum),
      latest_created: latest?.latest_created,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** EU-Urteile: Liste mit Paginierung */
app.get("/api/eu-urteile", async (req, res) => {
  try {
    const gericht = req.query.gericht || null;
    const rechtsgebiet = req.query.rechtsgebiet || null;
    const search = req.query.search || null;
    let limit = Number.parseInt(String(req.query.limit || "50"), 10);
    let offset = Number.parseInt(String(req.query.offset || "0"), 10);
    if (!Number.isFinite(limit) || limit < 1) limit = 50;
    if (limit > 200) limit = 200;
    if (!Number.isFinite(offset) || offset < 0) offset = 0;

    const allowedGericht = new Set(["EuGH", "EuG"]);
    let where = "WHERE 1=1";
    const params = [];
    if (gericht && allowedGericht.has(String(gericht))) {
      where += " AND gericht = ?";
      params.push(String(gericht));
    }
    if (rechtsgebiet) {
      where += " AND rechtsgebiet = ?";
      params.push(String(rechtsgebiet));
    }
    if (search) {
      const s = `%${String(search).trim()}%`;
      where +=
        " AND (betreff LIKE ? OR parteien LIKE ? OR celex LIKE ? OR ecli LIKE ? OR keywords LIKE ? OR leitsatz LIKE ? OR zusammenfassung_de LIKE ? OR zusammenfassung_en LIKE ?)";
      params.push(s, s, s, s, s, s, s, s);
    }
    where += " AND quality_ok = 1";

    const pool = getPool();
    const [[{ total }]] = await pool.query(
      `SELECT COUNT(*) AS total FROM eu_urteile ${where}`,
      params
    );

    const [listRows] = await pool.query(
      `SELECT id, celex, ecli, gericht, typ, datum, parteien, betreff,
              zusammenfassung_de, zusammenfassung_en, auswirkung_de, auswirkung_en,
              rechtsgebiet, eurlex_url, curia_url
       FROM eu_urteile
       ${where}
       ORDER BY datum DESC, id DESC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    const items = listRows.map((r) => ({
      id: r.id,
      celex: r.celex,
      ecli: r.ecli,
      gericht: r.gericht,
      typ: r.typ,
      datum: formatDate(r.datum),
      parteien: r.parteien,
      betreff: r.betreff,
      zusammenfassung_de: r.zusammenfassung_de,
      zusammenfassung_en: r.zusammenfassung_en,
      auswirkung_de: r.auswirkung_de,
      auswirkung_en: r.auswirkung_en,
      rechtsgebiet: r.rechtsgebiet,
      eurlex_url: r.eurlex_url,
      curia_url: r.curia_url,
    }));

    res.json({ total, limit, offset, items });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** EU-Urteile: Einzelansicht */
app.get("/api/eu-urteile/:id", async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT id, celex, ecli, gericht, typ, datum, parteien, betreff, keywords, leitsatz,
              zusammenfassung_de, zusammenfassung_en, auswirkung_de, auswirkung_en,
              rechtsgebiet, eurlex_url, curia_url, created_at
       FROM eu_urteile WHERE id = ? LIMIT 1`,
      [id]
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const r = rows[0];
    const [linkRows] = await getPool().query(
      `SELECT j.id AS link_id, j.eu_rechtsakt_id, j.rechtsakt_celex,
              e.celex AS akt_celex, e.titel_de AS akt_titel_de, e.titel_en AS akt_titel_en
       FROM eu_urteil_rechtsakte j
       LEFT JOIN eu_rechtsakte e ON e.id = j.eu_rechtsakt_id
       WHERE j.eu_urteil_id = ?
       ORDER BY j.id`,
      [id]
    );
    res.json({
      id: r.id,
      celex: r.celex,
      ecli: r.ecli,
      gericht: r.gericht,
      typ: r.typ,
      datum: formatDate(r.datum),
      parteien: r.parteien,
      betreff: r.betreff,
      keywords: r.keywords,
      leitsatz: r.leitsatz,
      zusammenfassung_de: r.zusammenfassung_de,
      zusammenfassung_en: r.zusammenfassung_en,
      auswirkung_de: r.auswirkung_de,
      auswirkung_en: r.auswirkung_en,
      rechtsgebiet: r.rechtsgebiet,
      eurlex_url: r.eurlex_url,
      curia_url: r.curia_url,
      created_at: r.created_at,
      linked_rechtsakte: linkRows.map((x) => ({
        link_id: x.link_id,
        eu_rechtsakt_id: x.eu_rechtsakt_id,
        rechtsakt_celex: x.rechtsakt_celex,
        akt_celex: x.akt_celex,
        titel_de: x.akt_titel_de,
        titel_en: x.akt_titel_en,
      })),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});



// --- Wahlen API ---
const WAHlen_TYPS = ["federal", "state", "municipal", "european", "mayoral"];
const WAHlen_TYP_SET = new Set(WAHlen_TYPS);

const WAHlen_NUM_COLS = new Set([
  "turnout",
  "cdu_csu",
  "spd",
  "gruene",
  "fdp",
  "linke_pds",
  "afd",
  "bsw",
  "npd",
  "freie_waehler",
  "piraten",
  "die_partei",
  "other",
  "far_right",
  "far_left",
  "winner_voteshare",
]);

const DE_STATES = [
  { code: "01", name: "Schleswig-Holstein" },
  { code: "02", name: "Hamburg" },
  { code: "03", name: "Niedersachsen" },
  { code: "04", name: "Bremen" },
  { code: "05", name: "Nordrhein-Westfalen" },
  { code: "06", name: "Hessen" },
  { code: "07", name: "Rheinland-Pfalz" },
  { code: "08", name: "Baden-Württemberg" },
  { code: "09", name: "Bayern" },
  { code: "10", name: "Saarland" },
  { code: "11", name: "Berlin" },
  { code: "12", name: "Brandenburg" },
  { code: "13", name: "Mecklenburg-Vorpommern" },
  { code: "14", name: "Sachsen" },
  { code: "15", name: "Sachsen-Anhalt" },
  { code: "16", name: "Thüringen" },
];

function wahlenParseTyp(raw) {
  const t = String(raw || "").trim().toLowerCase();
  return WAHlen_TYP_SET.has(t) ? t : null;
}

function wahlenAgsClause(param) {
  const s = String(param || "").trim();
  if (!s) return null;
  const five = s.length >= 5 ? s.slice(0, 5) : s;
  return { sql: "(ags = ? OR LEFT(ags, 5) = ?)", params: [s, five] };
}

function wahlenRowToElection(r) {
  return {
    year: r.election_year,
    typ: r.typ,
    election_date: formatDate(r.election_date),
    election_type: r.election_type,
    round: r.round,
    turnout: r.turnout != null ? Number(r.turnout) : null,
    cdu_csu: r.cdu_csu != null ? Number(r.cdu_csu) : null,
    spd: r.spd != null ? Number(r.spd) : null,
    gruene: r.gruene != null ? Number(r.gruene) : null,
    fdp: r.fdp != null ? Number(r.fdp) : null,
    linke_pds: r.linke_pds != null ? Number(r.linke_pds) : null,
    afd: r.afd != null ? Number(r.afd) : null,
    bsw: r.bsw != null ? Number(r.bsw) : null,
    npd: r.npd != null ? Number(r.npd) : null,
    freie_waehler: r.freie_waehler != null ? Number(r.freie_waehler) : null,
    piraten: r.piraten != null ? Number(r.piraten) : null,
    die_partei: r.die_partei != null ? Number(r.die_partei) : null,
    other: r.other != null ? Number(r.other) : null,
    winning_party: r.winning_party,
    winner_party: r.winner_party,
    winner_voteshare: r.winner_voteshare != null ? Number(r.winner_voteshare) : null,
  };
}



/** Spezifische /api/wahlen/* Routen vor /region/:ags registrieren */
app.get("/api/wahlen/types", (_req, res) => {
  res.json(WAHlen_TYPS);
});

app.get("/api/wahlen/years", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  if (!typ) {
    res.status(400).json({ error: "typ erforderlich oder ungültig" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      "SELECT DISTINCT election_year FROM wahlen WHERE typ = ? ORDER BY election_year ASC",
      [typ],
    );
    res.json(rows.map((r) => r.election_year));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/states", (_req, res) => {
  res.json(DE_STATES);
});

app.get("/api/wahlen/map", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const year = Number.parseInt(String(req.query.year || ""), 10);
  const metric = String(req.query.metric || "winning_party").trim().toLowerCase();
  if (!typ || !Number.isFinite(year)) {
    res.status(400).json({ error: "typ und year erforderlich" });
    return;
  }
  const useWinner = metric === "winning_party" || metric === "wahlsieger";
  const col = useWinner ? null : metric;
  if (!useWinner && (!col || !WAHlen_NUM_COLS.has(col))) {
    res.status(400).json({ error: "metric ungültig" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT ags, ags_name, winning_party, turnout,
              cdu_csu, spd, gruene, fdp, linke_pds, afd, bsw, npd,
              freie_waehler, piraten, die_partei, other
       FROM wahlen WHERE typ = ? AND election_year = ?`,
      [typ, year],
    );
    const out = rows.map((r) => {
      let value = null;
      if (useWinner) {
        value = r.winning_party;
      } else if (col && r[col] != null) {
        value = Number(r[col]);
      }
      return {
        ags: r.ags,
        ags_name: r.ags_name,
        value,
        winning_party: r.winning_party,
        turnout: r.turnout != null ? Number(r.turnout) : null,
      };
    });
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/timeseries", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const party = String(req.query.party || "").trim().toLowerCase();
  const agsParam = String(req.query.ags || "").trim();
  const clause = wahlenAgsClause(agsParam);
  if (!typ || !party || !clause || !WAHlen_NUM_COLS.has(party)) {
    res.status(400).json({ error: "ags, typ und party erforderlich" });
    return;
  }
  const sql = `SELECT election_year AS year, \`${party}\` AS value FROM wahlen WHERE typ = ? AND ${clause.sql} ORDER BY election_year ASC`;
  try {
    const [rows] = await getPool().query(sql, [typ, ...clause.params]);
    const out = rows.map((r) => ({
      year: r.year,
      value: r.value != null ? Number(r.value) : null,
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/compare", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const party = String(req.query.party || "").trim().toLowerCase();
  const agsList = String(req.query.ags || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!typ || !party || !agsList.length || !WAHlen_NUM_COLS.has(party)) {
    res.status(400).json({ error: "typ, party und ags (kommagetrennt) erforderlich" });
    return;
  }
  try {
    const pool = getPool();
    const regions = [];
    for (const ags of agsList) {
      const clause = wahlenAgsClause(ags);
      if (!clause) continue;
      const sql = `SELECT election_year AS year, ags_name, \`${party}\` AS value FROM wahlen WHERE typ = ? AND ${clause.sql} ORDER BY election_year ASC`;
      const [rows] = await pool.query(sql, [typ, ...clause.params]);
      const name = rows.length ? rows[rows.length - 1].ags_name : null;
      regions.push({
        ags,
        name,
        data: rows.map((r) => ({ year: r.year, value: r.value != null ? Number(r.value) : null })),
      });
    }
    res.json({ regions });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/scatter", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const year = Number.parseInt(String(req.query.year || ""), 10);
  const x = String(req.query.x || "").trim().toLowerCase();
  const y = String(req.query.y || "").trim().toLowerCase();
  if (!typ || !Number.isFinite(year) || !WAHlen_NUM_COLS.has(x) || !WAHlen_NUM_COLS.has(y)) {
    res.status(400).json({ error: "typ, year, x und y (numerische Spalten) erforderlich" });
    return;
  }
  const sql =
    "SELECT ags, ags_name, state, `" +
    x +
    "` AS x, `" +
    y +
    "` AS y FROM wahlen WHERE typ = ? AND election_year = ?";
  try {
    const [rows] = await getPool().query(sql, [typ, year]);
    const out = rows
      .filter((r) => r.x != null && r.y != null)
      .map((r) => ({
        ags: r.ags,
        name: r.ags_name,
        state: r.state,
        x: Number(r.x),
        y: Number(r.y),
      }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/ranking", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const year = Number.parseInt(String(req.query.year || ""), 10);
  const party = String(req.query.party || "").trim().toLowerCase();
  const limit = Math.min(500, Math.max(1, Number.parseInt(String(req.query.limit || "20"), 10) || 20));
  const order = String(req.query.order || "desc").toLowerCase() === "asc" ? "ASC" : "DESC";
  if (!typ || !Number.isFinite(year) || !WAHlen_NUM_COLS.has(party)) {
    res.status(400).json({ error: "typ, year und party erforderlich" });
    return;
  }
  const sql =
    "SELECT ags, ags_name, state_name, `" +
    party +
    "` AS value FROM wahlen WHERE typ = ? AND election_year = ? AND `" +
    party +
    "` IS NOT NULL ORDER BY `" +
    party +
    "` " +
    order +
    " LIMIT ?";
  try {
    const [rows] = await getPool().query(sql, [typ, year, limit]);
    let rank = 1;
    const out = rows.map((r, i) => ({
      ags: r.ags,
      name: r.ags_name,
      state_name: r.state_name,
      value: Number(r.value),
      rank: order === "DESC" ? i + 1 : i + 1,
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/change", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const party = String(req.query.party || "").trim().toLowerCase();
  const fromY = Number.parseInt(String(req.query.from || ""), 10);
  const toY = Number.parseInt(String(req.query.to || ""), 10);
  if (!typ || !WAHlen_NUM_COLS.has(party) || !Number.isFinite(fromY) || !Number.isFinite(toY)) {
    res.status(400).json({ error: "typ, from, to und party erforderlich" });
    return;
  }
  const col = party;
  const sql = `
    SELECT a.ags, a.ags_name, a.state_name,
           a.\`${col}\` AS v_from, b.\`${col}\` AS v_to
    FROM wahlen a
    INNER JOIN wahlen b ON a.typ = b.typ AND a.ags = b.ags
        AND IFNULL(a.election_type,'') = IFNULL(b.election_type,'')
        AND IFNULL(a.round,0) = IFNULL(b.round,0)
    WHERE a.typ = ? AND a.election_year = ? AND b.election_year = ?
      AND a.\`${col}\` IS NOT NULL AND b.\`${col}\` IS NOT NULL
  `;
  try {
    const [rows] = await getPool().query(sql, [typ, fromY, toY]);
    const out = rows.map((r) => {
      const vf = Number(r.v_from);
      const vt = Number(r.v_to);
      return {
        ags: r.ags,
        name: r.ags_name,
        change: vt - vf,
        value_from: vf,
        value_to: vt,
        state_name: r.state_name,
      };
    });
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

/** Bundesweiter Durchschnitt pro Wahljahr (alle Kreise mit Wert) */
app.get("/api/wahlen/national-average", async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const party = String(req.query.party || "").trim().toLowerCase();
  if (!typ || !WAHlen_NUM_COLS.has(party)) {
    res.status(400).json({ error: "typ und party erforderlich" });
    return;
  }
  const sql = `
    SELECT election_year AS year, AVG(\`${party}\`) AS value
    FROM wahlen
    WHERE typ = ? AND \`${party}\` IS NOT NULL
    GROUP BY election_year
    ORDER BY election_year ASC
  `;
  try {
    const [rows] = await getPool().query(sql, [typ]);
    const out = rows.map((r) => ({
      year: r.year,
      value: r.value != null ? Number(r.value) : null,
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/stats", async (_req, res) => {
  try {
    const pool = getPool();
    const [[{ total_records }]] = await pool.query("SELECT COUNT(*) AS total_records FROM wahlen");
    const [byTyp] = await pool.query(
      "SELECT typ, COUNT(*) AS c FROM wahlen GROUP BY typ",
    );
    const [[yr]] = await pool.query(
      "SELECT MIN(election_year) AS y_min, MAX(election_year) AS y_max FROM wahlen",
    );
    const types = {};
    for (const row of byTyp) {
      types[row.typ] = Number(row.c) || 0;
    }
    res.json({
      total_records: Number(total_records) || 0,
      types,
      years_range:
        yr.y_min != null && yr.y_max != null
          ? { min: yr.y_min, max: yr.y_max }
          : null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/wahlen/region/:ags", async (req, res) => {
  const clause = wahlenAgsClause(req.params.ags);
  if (!clause) {
    res.status(400).json({ error: "ags ungültig" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT * FROM wahlen WHERE ${clause.sql} ORDER BY election_year DESC, typ ASC`,
      clause.params,
    );
    if (!rows.length) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const first = rows[0];
    res.json({
      ags: first.ags,
      ags_name: first.ags_name,
      state_name: first.state_name,
      elections: rows.map(wahlenRowToElection),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/categories", async (_req, res) => {
  try {
    const [rows] = await getPool().query(
      `SELECT indicator_code, indicator_name, category, unit,
              description_de, description_en
       FROM world_indicator_meta
       ORDER BY category, indicator_code`,
    );
    const byCat = new Map();
    for (const r of rows) {
      const cat = r.category || "other";
      if (!byCat.has(cat)) byCat.set(cat, []);
      byCat.get(cat).push({
        code: r.indicator_code,
        name: r.indicator_name,
        unit: r.unit,
        description_de: r.description_de,
        description_en: r.description_en,
      });
    }
    const order = Object.keys(WORLD_CATEGORY_LABELS);
    const out = [];
    for (const id of order) {
      const indicators = byCat.get(id);
      if (!indicators?.length) continue;
      const lab = WORLD_CATEGORY_LABELS[id];
      out.push({
        id,
        label_de: lab.label_de,
        label_en: lab.label_en,
        indicators,
      });
    }
    for (const [id, indicators] of byCat) {
      if (order.includes(id)) continue;
      out.push({
        id,
        label_de: id,
        label_en: id,
        indicators,
      });
    }
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/indicators", async (_req, res) => {
  try {
    const [rows] = await getPool().query(
      `SELECT indicator_code AS code, indicator_name AS name, category, unit,
              description_de, description_en, source, source_url
       FROM world_indicator_meta
       ORDER BY category, indicator_code`,
    );
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/map", async (req, res) => {
  const indicator = String(req.query.indicator || "").trim();
  let year = Number.parseInt(String(req.query.year || ""), 10);
  if (!indicator) {
    res.status(400).json({ error: "indicator erforderlich" });
    return;
  }
  if (!Number.isFinite(year)) {
    res.status(400).json({ error: "year ungültig" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT country_code, country_name, value, region, income_level
       FROM world_indicators
       WHERE indicator_code = ? AND year = ? AND value IS NOT NULL`,
      [indicator, year],
    );
    res.json(
      rows.map((r) => ({
        country_code: r.country_code,
        country_name: r.country_name,
        value: worldNum(r.value),
        region: r.region,
        income_level: r.income_level,
      })),
    );
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/country/:code", async (req, res) => {
  const code = String(req.params.code || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  if (!code || code.length !== 3) {
    res.status(400).json({ error: "Ungültiger Ländercode" });
    return;
  }
  try {
    const [[meta]] = await getPool().query(
      `SELECT DISTINCT country_code, country_name, region, income_level
       FROM world_indicators
       WHERE country_code = ?
       LIMIT 1`,
      [code],
    );
    if (!meta) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const [dataRows] = await getPool().query(
      `SELECT indicator_code, year, value
       FROM world_indicators
       WHERE country_code = ?
       ORDER BY indicator_code, year ASC`,
      [code],
    );
    const [metaRows] = await getPool().query(
      `SELECT indicator_code, indicator_name, category
       FROM world_indicator_meta`,
    );
    const metaByCode = new Map(
      metaRows.map((m) => [m.indicator_code, m]),
    );
    const byInd = new Map();
    for (const r of dataRows) {
      const ic = r.indicator_code;
      if (!byInd.has(ic)) byInd.set(ic, []);
      byInd.get(ic).push({
        year: r.year,
        value: worldNum(r.value),
      });
    }
    const indicators = [];
    for (const [indicator_code, values] of byInd) {
      const m = metaByCode.get(indicator_code);
      indicators.push({
        indicator_code,
        name: m?.indicator_name ?? indicator_code,
        category: m?.category ?? null,
        values,
      });
    }
    indicators.sort((a, b) =>
      a.indicator_code.localeCompare(b.indicator_code),
    );
    res.json({
      country_code: meta.country_code,
      country_name: meta.country_name,
      region: meta.region,
      income_level: meta.income_level,
      indicators,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/timeseries", async (req, res) => {
  const country = String(req.query.country || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  const indicator = String(req.query.indicator || "").trim();
  if (!country || country.length !== 3 || !indicator) {
    res.status(400).json({ error: "country und indicator erforderlich" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT year, value
       FROM world_indicators
       WHERE country_code = ? AND indicator_code = ?
       ORDER BY year ASC`,
      [country, indicator],
    );
    res.json(
      rows.map((r) => ({
        year: r.year,
        value: worldNum(r.value),
      })),
    );
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/compare", async (req, res) => {
  const countriesRaw = String(req.query.countries || "").trim();
  const indicator = String(req.query.indicator || "").trim();
  if (!countriesRaw || !indicator) {
    res.status(400).json({ error: "countries und indicator erforderlich" });
    return;
  }
  const codes = countriesRaw
    .split(",")
    .map((s) => s.trim().toUpperCase().slice(0, 3))
    .filter((c) => c.length === 3);
  if (!codes.length) {
    res.status(400).json({ error: "Keine gültigen Ländercodes" });
    return;
  }
  const uniq = [...new Set(codes)];
  try {
    const ph = uniq.map(() => "?").join(",");
    const [rows] = await getPool().query(
      `SELECT country_code, country_name, year, value
       FROM world_indicators
       WHERE indicator_code = ? AND country_code IN (${ph})
       ORDER BY country_code, year ASC`,
      [indicator, ...uniq],
    );
    const byCountry = new Map();
    for (const r of rows) {
      if (!byCountry.has(r.country_code)) {
        byCountry.set(r.country_code, {
          code: r.country_code,
          name: r.country_name,
          data: [],
        });
      }
      byCountry.get(r.country_code).data.push({
        year: r.year,
        value: worldNum(r.value),
      });
    }
    const countries = uniq
      .map((c) => byCountry.get(c))
      .filter(Boolean);
    res.json({ countries });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/ranking", async (req, res) => {
  const indicator = String(req.query.indicator || "").trim();
  let year = Number.parseInt(String(req.query.year || ""), 10);
  let limit = Number.parseInt(String(req.query.limit || "20"), 10);
  const order = String(req.query.order || "desc").toLowerCase() === "asc"
    ? "ASC"
    : "DESC";
  if (!indicator || !Number.isFinite(year)) {
    res.status(400).json({ error: "indicator und year erforderlich" });
    return;
  }
  if (!Number.isFinite(limit) || limit < 1) limit = 20;
  if (limit > 500) limit = 500;
  try {
    const [rows] = await getPool().query(
      `SELECT country_code, country_name, value
       FROM world_indicators
       WHERE indicator_code = ? AND year = ? AND value IS NOT NULL
       ORDER BY value ${order}, country_code ASC
       LIMIT ?`,
      [indicator, year, limit],
    );
    const out = rows.map((r, i) => ({
      country_code: r.country_code,
      country_name: r.country_name,
      value: worldNum(r.value),
      rank: i + 1,
    }));
    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/scatter", async (req, res) => {
  const xCode = String(req.query.x || "").trim();
  const yCode = String(req.query.y || "").trim();
  let year = Number.parseInt(String(req.query.year || ""), 10);
  if (!xCode || !yCode || !Number.isFinite(year)) {
    res.status(400).json({ error: "x, y und year erforderlich" });
    return;
  }
  try {
    const [rows] = await getPool().query(
      `SELECT a.country_code, a.country_name, a.region,
              a.value AS x, b.value AS y
       FROM world_indicators a
       INNER JOIN world_indicators b
         ON a.country_code = b.country_code AND a.year = b.year
       WHERE a.indicator_code = ? AND b.indicator_code = ?
         AND a.year = ?
         AND a.value IS NOT NULL AND b.value IS NOT NULL`,
      [xCode, yCode, year],
    );
    res.json(
      rows.map((r) => ({
        country_code: r.country_code,
        country_name: r.country_name,
        region: r.region,
        x: worldNum(r.x),
        y: worldNum(r.y),
      })),
    );
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.get("/api/world/stats", async (_req, res) => {
  try {
    const pool = getPool();
    const [[{ total_records }]] = await pool.query(
      "SELECT COUNT(*) AS total_records FROM world_indicators",
    );
    const [[{ countries }]] = await pool.query(
      "SELECT COUNT(DISTINCT country_code) AS countries FROM world_indicators",
    );
    const [[{ indicators }]] = await pool.query(
      "SELECT COUNT(*) AS indicators FROM world_indicator_meta",
    );
    const [[yr]] = await pool.query(
      "SELECT MIN(year) AS y_min, MAX(year) AS y_max FROM world_indicators",
    );
    res.json({
      total_records: Number(total_records) || 0,
      countries: Number(countries) || 0,
      indicators: Number(indicators) || 0,
      years_range:
        yr.y_min != null && yr.y_max != null
          ? { min: yr.y_min, max: yr.y_max }
          : null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Datenbankfehler" });
  }
});

app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});
