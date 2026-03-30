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
      "SELECT COUNT(*) AS total FROM eu_urteile"
    );
    const [byGericht] = await pool.query(
      "SELECT gericht, COUNT(*) AS c FROM eu_urteile GROUP BY gericht ORDER BY gericht"
    );
    const [byRg] = await pool.query(
      `SELECT rechtsgebiet, COUNT(*) AS c FROM eu_urteile
       WHERE rechtsgebiet IS NOT NULL AND rechtsgebiet != ''
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

app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});
