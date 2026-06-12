"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

/** Liste: kein diff (kann sehr groß sein) */
router.get("/gesetze", asyncHandler(async (req, res) => {
  const [rows] = await getPool().query(
    `SELECT
       a.id,
       g.kuerzel AS kuerzel,
       COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel) AS name,
       COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel) AS titel,
       g.amtliche_abkuerzung AS amtliche_abkuerzung,
       DATE_FORMAT(g.ausfertigung_datum, '%Y-%m-%d') AS ausfertigung_datum,
       g.fundstelle_periodikum AS fundstelle_periodikum,
       g.fundstelle_zitstelle AS fundstelle_zitstelle,
       g.gii_slug AS gii_slug,
       g.status AS gesetz_status,
       (CASE WHEN EXISTS(SELECT 1 FROM lobby_gesetze lg WHERE lg.gesetz_id = g.id) THEN 1 ELSE 0 END) AS has_lobby,
       g.titel_offiziell AS titel_offiziell,
       a.datum,
       a.zusammenfassung,
       a.kontext,
       a.bgbl_referenz,
       a.poll_id
     FROM aenderungen a
     INNER JOIN gesetze g ON g.id = a.gesetz_id
     ORDER BY (g.titel_offiziell IS NULL) ASC, a.datum DESC, a.id DESC`
  );
  const out = rows.map((r) => ({
    id: r.id,
    kuerzel: r.kuerzel,
    name: r.name,
    titel: r.titel,
    amtliche_abkuerzung: r.amtliche_abkuerzung,
    ausfertigung_datum: r.ausfertigung_datum,
    fundstelle_periodikum: r.fundstelle_periodikum,
    fundstelle_zitstelle: r.fundstelle_zitstelle,
    gii_slug: r.gii_slug,
    gesetz_status: r.gesetz_status,
    has_lobby: Number(r.has_lobby) === 1,
    titel_offiziell: r.titel_offiziell,
    datum: formatDate(r.datum),
    zusammenfassung: r.zusammenfassung,
    kontext: r.kontext,
    bgbl_referenz: r.bgbl_referenz,
    poll_id: r.poll_id,
  }));
  res.json(out);
}));

/** Statistik Gesetze / Änderungen (vor :id registrieren) */
router.get("/gesetze/stats", asyncHandler(async (_req, res) => {
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
}));

/** Einzeln inkl. vollem diff */
router.get("/gesetze/:id", asyncHandler(async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT
       a.id,
       a.gesetz_id AS gesetz_id,
       g.kuerzel AS kuerzel,
       COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel) AS name,
       COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel) AS titel,
       g.amtliche_abkuerzung AS amtliche_abkuerzung,
       DATE_FORMAT(g.ausfertigung_datum, '%Y-%m-%d') AS ausfertigung_datum,
       g.fundstelle_periodikum AS fundstelle_periodikum,
       g.fundstelle_zitstelle AS fundstelle_zitstelle,
       g.letzter_stand AS letzter_stand,
       g.gii_slug AS gii_slug,
       g.status AS gesetz_status,
       a.datum,
       a.zusammenfassung,
       a.kontext,
       a.bgbl_referenz,
       a.poll_id,
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
    gesetz_id: r.gesetz_id,
    kuerzel: r.kuerzel,
    name: r.name,
    titel: r.titel,
    amtliche_abkuerzung: r.amtliche_abkuerzung,
    ausfertigung_datum: r.ausfertigung_datum,
    fundstelle_periodikum: r.fundstelle_periodikum,
    fundstelle_zitstelle: r.fundstelle_zitstelle,
    letzter_stand: r.letzter_stand,
    gii_slug: r.gii_slug,
    gesetz_status: r.gesetz_status,
    datum: formatDate(r.datum),
    zusammenfassung: r.zusammenfassung,
    kontext: r.kontext,
    bgbl_referenz: r.bgbl_referenz,
    poll_id: r.poll_id,
    diff: r.diff,
  });
}));

module.exports = router;
