"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

/**
 * Urteile: Liste. Mit ?gesetz=KUERZEL nur Urteile, die dieses Gesetz
 * zitieren, dann zusaetzlich gesetze[] je Urteil (fuer die Gesetz-Detailseite).
 */
router.get("/urteile", asyncHandler(async (req, res) => {
  const rechtsgebiet = req.query.rechtsgebiet || null;
  const gericht = req.query.gericht || null;
  const gesetz = String(req.query.gesetz ?? "").trim() || null;
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
  if (gesetz) {
    query += ` AND id IN (SELECT urteil_id FROM urteil_gesetze WHERE gesetz_kuerzel = ?)`;
    params.push(gesetz);
  }
  query += ` ORDER BY datum DESC, id DESC`;
  const [rows] = await getPool().query(query, params);
  const out = rows.map(r => ({ ...r, datum: formatDate(r.datum) }));
  if (gesetz && out.length) {
    const [links] = await getPool().query(
      `SELECT urteil_id, gesetz_kuerzel FROM urteil_gesetze
       WHERE urteil_id IN (${out.map(() => "?").join(",")})`,
      out.map(r => r.id)
    );
    const byUrteil = new Map(out.map(r => [r.id, []]));
    for (const l of links) byUrteil.get(l.urteil_id).push(l.gesetz_kuerzel);
    for (const r of out) r.gesetze = byUrteil.get(r.id);
  }
  res.json(out);
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
