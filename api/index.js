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
         a.bgbl_referenz
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
    }));
    res.json(out);
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
      diff: r.diff,
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

app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
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
