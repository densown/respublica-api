"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");
const { parsePagination } = require("../lib/validate");

const TITEL_SQL =
  "COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel)";
const HAS_LOBBY_SQL =
  "EXISTS(SELECT 1 FROM lobby_gesetze lg WHERE lg.gesetz_id = g.id)";

/** Spalten der Listenansicht: kein diff (kann sehr groß sein) */
const LIST_SELECT = `SELECT
       a.id,
       g.kuerzel AS kuerzel,
       ${TITEL_SQL} AS name,
       ${TITEL_SQL} AS titel,
       g.amtliche_abkuerzung AS amtliche_abkuerzung,
       DATE_FORMAT(g.ausfertigung_datum, '%Y-%m-%d') AS ausfertigung_datum,
       g.fundstelle_periodikum AS fundstelle_periodikum,
       g.fundstelle_zitstelle AS fundstelle_zitstelle,
       g.gii_slug AS gii_slug,
       g.status AS gesetz_status,
       (CASE WHEN ${HAS_LOBBY_SQL} THEN 1 ELSE 0 END) AS has_lobby,
       g.titel_offiziell AS titel_offiziell,
       a.datum,
       a.zusammenfassung,
       a.kontext,
       a.bgbl_referenz,
       a.poll_id
     FROM aenderungen a
     INNER JOIN gesetze g ON g.id = a.gesetz_id`;

function mapListRow(r) {
  return {
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
  };
}

/**
 * Rechtsgebiet aus dem Kuerzel (Heuristik, frueher im Dashboard): Kuerzel
 * normalisiert auf A-Z/0-9, feste Listen plus SGB-Praefix, alles andere ist
 * "bundes". Die Werte entsprechen dem Filter-Dropdown der Gesetze-Seite.
 */
const KUERZEL_NORM_SQL = "REGEXP_REPLACE(UPPER(g.kuerzel), '[^A-Z0-9]', '')";
const BEREICH_KUERZEL = {
  zivil: ["BGB", "ZPO", "HGB", "INSO", "FAMFG", "WEG"],
  straf: ["STGB", "STPO", "JGG", "BTMG"],
  verfassung: ["GG", "BVERFGG"],
  steuer_arbeit: ["ESTG", "AO", "ARBGG", "BETRVG"],
};

function bereichWhere(bereich) {
  if (bereich === "sozial") {
    return { sql: `${KUERZEL_NORM_SQL} LIKE 'SGB%'`, params: [] };
  }
  if (bereich === "bundes") {
    const alle = Object.values(BEREICH_KUERZEL).flat();
    return {
      sql: `NOT (${KUERZEL_NORM_SQL} LIKE 'SGB%' OR ${KUERZEL_NORM_SQL} IN (${alle.map(() => "?").join(",")}))`,
      params: alle,
    };
  }
  if (!Object.hasOwn(BEREICH_KUERZEL, bereich)) return null;
  const liste = BEREICH_KUERZEL[bereich];
  return {
    sql: `${KUERZEL_NORM_SQL} IN (${liste.map(() => "?").join(",")})`,
    params: liste,
  };
}

const FILTER_SQL = {
  mit_lobby: HAS_LOBBY_SQL,
  klartitel: "TRIM(g.titel_offiziell) <> ''",
  mit_zusammenfassung: "TRIM(a.zusammenfassung) <> ''",
};

// Nachrangig immer die Reihenfolge der Gesamtliste, damit Gleichstaende
// stabil bleiben wie bisher bei der Sortierung im Browser.
const SORT_SQL = {
  new: "a.datum DESC, (g.titel_offiziell IS NULL) ASC, a.id DESC",
  old: "a.datum ASC, (g.titel_offiziell IS NULL) ASC, a.id DESC",
  az: `${TITEL_SQL} ASC, (g.titel_offiziell IS NULL) ASC, a.datum DESC, a.id DESC`,
};

/** LIKE-Platzhalter im Suchbegriff wörtlich nehmen */
function escapeLike(s) {
  return s.replace(/[\\%_]/g, (m) => `\\${m}`);
}

/** Liste: kein diff (kann sehr groß sein) */
router.get("/gesetze", asyncHandler(async (req, res) => {
  const [rows] = await getPool().query(
    `${LIST_SELECT}
     ORDER BY (g.titel_offiziell IS NULL) ASC, a.datum DESC, a.id DESC`
  );
  res.json(rows.map(mapListRow));
}));

/**
 * Seitenweise Liste mit Suche, Filter und Sortierung in SQL. Die Gesamtliste
 * oben ist rund 10 MB groß; das Dashboard lädt hierüber nur die sichtbare Seite.
 * Query: limit, offset, search, bereich, filter, sort (new|old|az).
 */
router.get("/gesetze/liste", asyncHandler(async (req, res) => {
  const { limit, offset } = parsePagination(req.query, { defLimit: 20, maxLimit: 100 });
  const search = String(req.query.search ?? "").trim();
  const bereich = String(req.query.bereich ?? "");
  const filter = String(req.query.filter ?? "");
  const sort = String(req.query.sort ?? "");
  const orderSql = Object.hasOwn(SORT_SQL, sort) ? SORT_SQL[sort] : SORT_SQL.new;

  const where = [];
  const params = [];
  const b = bereichWhere(bereich);
  if (b) {
    where.push(b.sql);
    params.push(...b.params);
  }
  if (Object.hasOwn(FILTER_SQL, filter)) where.push(FILTER_SQL[filter]);
  if (search) {
    const s = `%${escapeLike(search)}%`;
    where.push(
      `(g.kuerzel LIKE ? OR ${TITEL_SQL} LIKE ? OR a.zusammenfassung LIKE ? OR g.amtliche_abkuerzung LIKE ?)`
    );
    params.push(s, s, s, s);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const pool = getPool();
  const [[[{ total }]], [rows]] = await Promise.all([
    pool.query(
      `SELECT COUNT(*) AS total
       FROM aenderungen a
       INNER JOIN gesetze g ON g.id = a.gesetz_id
       ${whereSql}`,
      params
    ),
    pool.query(
      `${LIST_SELECT}
       ${whereSql}
       ORDER BY ${orderSql}
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    ),
  ]);
  res.json({ total: Number(total) || 0, limit, offset, items: rows.map(mapListRow) });
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
