"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");
const { parsePagination } = require("../lib/validate");

/**
 * Rechtsgebiet aus dem Kuerzel, gleiche Regel wie rechtGebietFromKuerzel im
 * Dashboard (src/components/gesetze/utils.ts): Kuerzel in Grossbuchstaben,
 * nur Buchstaben und Ziffern.
 */
const KUERZEL_NORM = "UPPER(REGEXP_REPLACE(COALESCE(g.kuerzel, ''), '[^A-Za-z0-9]', ''))";
const GEBIET_SQL = `CASE
  WHEN ${KUERZEL_NORM} IN ('BGB','ZPO','HGB','INSO','FAMFG','WEG') THEN 'zivil'
  WHEN ${KUERZEL_NORM} IN ('STGB','STPO','JGG','BTMG') THEN 'straf'
  WHEN ${KUERZEL_NORM} LIKE 'SGB%' THEN 'sozial'
  WHEN ${KUERZEL_NORM} IN ('GG','BVERFGG') THEN 'verfassung'
  WHEN ${KUERZEL_NORM} IN ('ESTG','AO','ARBGG','BETRVG') THEN 'steuer_arbeit'
  ELSE 'bundes' END`;
const GEBIETE = new Set(["zivil", "straf", "sozial", "verfassung", "steuer_arbeit", "bundes"]);
const TITEL_SQL =
  "COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel)";
const SORTIERUNG = {
  new: "a.datum DESC, a.id DESC",
  old: "a.datum ASC, a.id ASC",
  az: `${TITEL_SQL} ASC, a.datum DESC, a.id DESC`,
};

/** LIKE-Sonderzeichen woertlich nehmen. */
function likeWoertlich(s) {
  return s.replace(/[\\%_]/g, (z) => `\\${z}`);
}

/**
 * Bedingungen fuer die seitenweise Liste aus den Query-Parametern.
 * Exportiert fuer Tests (ohne Datenbank).
 */
function seitenFilter(query) {
  const where = [];
  const params = [];
  if (query.zusammenfassung === "1") {
    where.push("a.zusammenfassung IS NOT NULL AND TRIM(a.zusammenfassung) <> ''");
  }
  const gebiet = String(query.gebiet ?? "");
  if (GEBIETE.has(gebiet)) {
    where.push(`(${GEBIET_SQL}) = ?`);
    params.push(gebiet);
  }
  if (query.lobby === "1") {
    where.push("EXISTS(SELECT 1 FROM lobby_gesetze lg WHERE lg.gesetz_id = g.id)");
  }
  if (query.klartitel === "1") {
    where.push("g.titel_offiziell IS NOT NULL AND TRIM(g.titel_offiziell) <> ''");
  }
  const q = String(query.q ?? "").trim().slice(0, 200);
  if (q) {
    const muster = `%${likeWoertlich(q.toLowerCase())}%`;
    where.push(
      `(LOWER(COALESCE(g.kuerzel, '')) LIKE ? OR LOWER(${TITEL_SQL}) LIKE ?
        OR LOWER(COALESCE(a.zusammenfassung, '')) LIKE ?
        OR LOWER(COALESCE(g.amtliche_abkuerzung, '')) LIKE ?)`
    );
    params.push(muster, muster, muster, muster);
  }
  const sort = SORTIERUNG[String(query.sort ?? "")] ? String(query.sort) : "new";
  return { where, params, orderBy: SORTIERUNG[sort] };
}

/**
 * Seitenweise Liste mit Gesamtzahl: { gesamt, eintraege }.
 * Fuer die Gesetzgebungsseite. Vorher lud sie alle rund 8.600 Aenderungen
 * (roh ~10 MB, gzip ~2 MB) und filterte im Browser; jetzt filtert und
 * blaettert die Datenbank, eine Seite ist wenige KB gross.
 * Die Zusammenfassung ist in der Liste gekuerzt (die Liste zeigt 80 Zeichen),
 * kontext fehlt; beides liefert /gesetze/:id vollstaendig.
 */
async function gesetzeSeite(query) {
  const { limit, offset } = parsePagination(query, { defLimit: 20, maxLimit: 100 });
  const { where, params, orderBy } = seitenFilter(query);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const pool = getPool();
  const [[zaehler]] = await pool.query(
    `SELECT COUNT(*) AS n
     FROM aenderungen a
     INNER JOIN gesetze g ON g.id = a.gesetz_id
     ${whereSql}`,
    params
  );
  const [rows] = await pool.query(
    `SELECT
       a.id,
       a.gesetz_id AS gesetz_id,
       g.kuerzel AS kuerzel,
       ${TITEL_SQL} AS name,
       ${TITEL_SQL} AS titel,
       g.amtliche_abkuerzung AS amtliche_abkuerzung,
       g.titel_offiziell AS titel_offiziell,
       (CASE WHEN EXISTS(SELECT 1 FROM lobby_gesetze lg WHERE lg.gesetz_id = g.id) THEN 1 ELSE 0 END) AS has_lobby,
       a.datum,
       LEFT(a.zusammenfassung, 160) AS zusammenfassung,
       a.poll_id
     FROM aenderungen a
     INNER JOIN gesetze g ON g.id = a.gesetz_id
     ${whereSql}
     ORDER BY ${orderBy}
     LIMIT ? OFFSET ?`,
    [...params, limit, offset]
  );
  return {
    gesamt: Number(zaehler.n) || 0,
    limit,
    offset,
    eintraege: rows.map((r) => ({
      id: r.id,
      gesetz_id: r.gesetz_id,
      kuerzel: r.kuerzel,
      name: r.name,
      titel: r.titel,
      amtliche_abkuerzung: r.amtliche_abkuerzung,
      titel_offiziell: r.titel_offiziell,
      has_lobby: Number(r.has_lobby) === 1,
      datum: formatDate(r.datum),
      zusammenfassung: r.zusammenfassung,
      poll_id: r.poll_id,
    })),
  };
}

/**
 * Liste: kein diff (kann sehr groß sein).
 *
 * Ohne `limit` kommt wie bisher die ganze Liste (rund 8.600 Zeilen, die
 * Gesetzgebungsseite filtert selbst). Mit `limit` (1 bis 500) und `offset`
 * nur ein Ausschnitt; die Startseite holte sonst fuer drei Eintraege 10 MB.
 * `zusammenfassung=1` laesst Aenderungen ohne Zusammenfassung weg.
 */
router.get("/gesetze", asyncHandler(async (req, res) => {
  if (req.query.mit_gesamt === "1") {
    // Daten aendern sich hoechstens taeglich; Browser darf 5 Minuten puffern.
    res.set("Cache-Control", "public, max-age=300");
    res.json(await gesetzeSeite(req.query));
    return;
  }
  const where = [];
  if (req.query.zusammenfassung === "1") {
    where.push("a.zusammenfassung IS NOT NULL AND TRIM(a.zusammenfassung) <> ''");
  }
  const seite = req.query.limit === undefined
    ? null
    : parsePagination(req.query, { defLimit: 50, maxLimit: 500 });

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
     ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
     ORDER BY (g.titel_offiziell IS NULL) ASC, a.datum DESC, a.id DESC
     ${seite ? "LIMIT ? OFFSET ?" : ""}`,
    seite ? [seite.limit, seite.offset] : []
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
module.exports.seitenFilter = seitenFilter;
