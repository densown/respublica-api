"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

/** EU-Recht: Statistiken (vor :id registrieren) */
router.get("/eu-recht/stats", asyncHandler(async (_req, res) => {
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
}));

/** EU-Recht: Liste mit Paginierung */
router.get("/eu-recht", asyncHandler(async (req, res) => {
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
            zusammenfassung_de, zusammenfassung_en, rechtsgebiet, eurlex_url, created_at
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
    zusammenfassung_de: r.zusammenfassung_de, zusammenfassung_en: r.zusammenfassung_en,
    rechtsgebiet: r.rechtsgebiet,
    eurlex_url: r.eurlex_url,
    linked_gesetze: linked[r.id] || [],
  }));

  res.json({ total, limit, offset, items });
}));

/** EU-Recht: Einzelansicht */
router.get("/eu-recht/:id", asyncHandler(async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT id, celex, titel_de, titel_en, typ, typ_label, datum, in_kraft,
            eurovoc_tags, zusammenfassung_de, zusammenfassung_en, rechtsgebiet, eurlex_url, created_at
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
    zusammenfassung_de: r.zusammenfassung_de, zusammenfassung_en: r.zusammenfassung_en,
    rechtsgebiet: r.rechtsgebiet,
    eurlex_url: r.eurlex_url,
    linked_gesetze: linkRows.map((x) => ({
      id: x.gesetz_id,
      kuerzel: x.kuerzel,
    })),
  });
}));

/** EU-Urteile: Statistiken (vor :id registrieren) */
router.get("/eu-urteile/stats", asyncHandler(async (_req, res) => {
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
}));

/** EU-Urteile: Liste mit Paginierung */
router.get("/eu-urteile", asyncHandler(async (req, res) => {
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
}));

/** EU-Urteile: Einzelansicht */
router.get("/eu-urteile/:id", asyncHandler(async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
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
}));


module.exports = router;
