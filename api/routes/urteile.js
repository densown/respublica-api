"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

/** Urteile: Liste */
router.get("/urteile", asyncHandler(async (req, res) => {
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
}));

const ROEMISCH = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII", "XIII", "XIV"];

/**
 * Schreibweisen, unter denen ein Gesetz in urteil_gesetze stehen kann.
 *
 * urteil_gesetze.gesetz_kuerzel ist freier Text aus der Auswertung der
 * Urteile ("GG", "SGB V", "AO (Abgabenordnung)"). gesetze.kuerzel traegt
 * dagegen meist den Schluessel von gesetze-im-internet.de
 * ("GG-BJNR000010949"), die uebliche Abkuerzung steht in
 * amtliche_abkuerzung. Ein Vergleich nur ueber kuerzel fand deshalb fast
 * nichts, das Grundgesetz zum Beispiel gar nicht.
 */
function kuerzelVarianten(gesetz) {
  const varianten = new Set();
  for (const roh of [gesetz.amtliche_abkuerzung, gesetz.kuerzel]) {
    const k = String(roh ?? "").trim();
    // Schluessel wie "GG-BJNR000010949" kommen in Urteilen nicht vor
    if (!k || /-BJNR/i.test(k)) continue;
    varianten.add(k);
    // "SGB 5" in der Datenbank, "SGB V" in den Urteilen
    const m = /^SGB\s+(\d{1,2})$/i.exec(k);
    if (m && ROEMISCH[Number(m[1])]) varianten.add(`SGB ${ROEMISCH[Number(m[1])]}`);
    const r = /^SGB\s+([IVX]+)$/i.exec(k);
    if (r) {
      const zahl = ROEMISCH.indexOf(r[1].toUpperCase());
      if (zahl > 0) varianten.add(`SGB ${zahl}`);
    }
  }
  return [...varianten];
}

/** LIKE-Sonderzeichen woertlich nehmen. */
function likeWoertlich(s) {
  return s.replace(/[\\%_]/g, (z) => `\\${z}`);
}

/**
 * Urteile zu einem Gesetz (gesetze.id). Findet auch Eintraege mit
 * Klammerzusatz: "AO (Abgabenordnung)" und "Abgabenordnung (AO)".
 */
router.get("/urteile/by-gesetz-id", asyncHandler(async (req, res) => {
  const gesetzId = Number.parseInt(String(req.query.gesetz_id ?? ""), 10);
  if (!Number.isFinite(gesetzId) || gesetzId < 1) {
    res.status(400).json({ error: "Ungültige gesetz_id" });
    return;
  }
  const pool = getPool();
  const [gesetze] = await pool.query(
    `SELECT kuerzel, amtliche_abkuerzung FROM gesetze WHERE id = ? LIMIT 1`,
    [gesetzId]
  );
  if (!gesetze.length) {
    res.status(404).json({ error: "Nicht gefunden" });
    return;
  }
  const varianten = kuerzelVarianten(gesetze[0]);
  if (!varianten.length) {
    res.json({ kuerzel: [], urteile: [] });
    return;
  }
  const bedingungen = [];
  const params = [];
  for (const v of varianten) {
    const w = likeWoertlich(v);
    bedingungen.push("ug.gesetz_kuerzel = ?", "ug.gesetz_kuerzel LIKE ?", "ug.gesetz_kuerzel LIKE ?");
    params.push(v, `${w} (%`, `%(${w})`);
  }
  const [rows] = await pool.query(
    `SELECT DISTINCT u.id, u.doc_id, u.gericht, u.senat, u.typ, u.datum,
            u.aktenzeichen, u.leitsatz, u.zusammenfassung, u.auswirkung, u.rechtsgebiet
       FROM urteil_gesetze ug
       INNER JOIN urteile u ON u.id = ug.urteil_id
      WHERE ${bedingungen.join(" OR ")}
      ORDER BY u.datum DESC, u.id DESC
      LIMIT 100`,
    params
  );
  res.json({
    kuerzel: varianten,
    urteile: rows.map((r) => ({ ...r, datum: formatDate(r.datum) })),
  });
}));

/** Urteile: Einzeln */
router.get("/urteile/:id", asyncHandler(async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) { res.status(400).json({ error: "Ungültige id" }); return; }
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
}));

module.exports = router;
module.exports.kuerzelVarianten = kuerzelVarianten;
