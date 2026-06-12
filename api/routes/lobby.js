"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

function safeJsonParse(raw, fallback) {
  if (raw == null) return fallback;
  if (typeof raw === "object") return raw;
  if (typeof raw !== "string") return fallback;
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}


router.get("/lobbyregister", asyncHandler(async (req, res) => {
  let page = Number.parseInt(String(req.query.page ?? "0"), 10);
  let limit = Number.parseInt(String(req.query.limit ?? "50"), 10);
  const q = String(req.query.q ?? "").trim();
  const sortRaw = String(req.query.sort ?? "financial_expenses_euro DESC").trim();
  const foi = String(req.query.foi ?? "").trim();
  const city = String(req.query.city ?? "").trim();
  const active = String(req.query.active ?? "").trim().toLowerCase();
  let minExpense = Number.parseInt(String(req.query.min_expense ?? "0"), 10);

  if (!Number.isFinite(page) || page < 0) page = 0;
  if (!Number.isFinite(limit) || limit < 1) limit = 50;
  if (limit > 100) limit = 100;
  if (!Number.isFinite(minExpense) || minExpense < 0) minExpense = 0;

  const allowedSort = new Set([
    "financial_expenses_euro DESC",
    "financial_expenses_euro ASC",
    "name ASC",
    "name DESC",
    "employee_fte DESC",
    "employee_fte ASC",
    "regulatory_projects_count DESC",
    "regulatory_projects_count ASC",
    "statements_count DESC",
    "statements_count ASC",
    "updated_at DESC",
    "updated_at ASC",
  ]);
  const sort = allowedSort.has(sortRaw) ? sortRaw : "financial_expenses_euro DESC";

  let where = "WHERE 1=1";
  const params = [];
  if (q) {
    where += " AND name LIKE ?";
    params.push(`%${q}%`);
  }
  if (foi) {
    where += " AND JSON_CONTAINS(fields_of_interest, JSON_OBJECT('code', ?))";
    params.push(foi);
  }
  if (city) {
    where += " AND city = ?";
    params.push(city);
  }
  if (active === "true") {
    where += " AND active = 1";
  }
  if (minExpense > 0) {
    where += " AND financial_expenses_euro >= ?";
    params.push(minExpense);
  }

  const pool = getPool();
  const [[{ total }]] = await pool.query(
    `SELECT COUNT(*) AS total FROM lobbyregister ${where}`,
    params,
  );

  const offset = page * limit;
  const [rows] = await pool.query(
    `SELECT
       id,
       register_number,
       name,
       legal_form,
       city,
       active,
       employee_fte,
       financial_expenses_euro,
       financial_year_start,
       financial_year_end,
       fields_of_interest,
       regulatory_projects_count,
       statements_count,
       details_url
     FROM lobbyregister
     ${where}
     ORDER BY ${sort}, id DESC
     LIMIT ? OFFSET ?`,
    [...params, limit, offset],
  );

  const items = rows.map((r) => ({
    id: r.id,
    register_number: r.register_number,
    name: r.name,
    legal_form: r.legal_form,
    city: r.city,
    active: r.active === null ? null : Boolean(r.active),
    employee_fte: r.employee_fte != null ? Number(r.employee_fte) : null,
    financial_expenses_euro:
      r.financial_expenses_euro != null ? Number(r.financial_expenses_euro) : null,
    financial_year_start: formatDate(r.financial_year_start),
    financial_year_end: formatDate(r.financial_year_end),
    fields_of_interest: safeJsonParse(r.fields_of_interest, []),
    regulatory_projects_count:
      r.regulatory_projects_count != null ? Number(r.regulatory_projects_count) : null,
    statements_count: r.statements_count != null ? Number(r.statements_count) : null,
    details_url: r.details_url,
  }));

  res.json({ total: Number(total) || 0, page, limit, items });
}));

router.get("/lobbyregister/stats", asyncHandler(async (_req, res) => {
  const pool = getPool();
  const [[agg]] = await pool.query(
    `SELECT
       COUNT(*) AS total,
       SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active,
       SUM(CASE WHEN financial_expenses_euro IS NOT NULL THEN 1 ELSE 0 END) AS mit_finanzdaten,
       MAX(financial_expenses_euro) AS max_ausgaben,
       AVG(financial_expenses_euro) AS avg_ausgaben
     FROM lobbyregister`,
  );
  const [top10Rows] = await pool.query(
    `SELECT
       register_number,
       name,
       city,
       financial_expenses_euro,
       employee_fte,
       details_url
     FROM lobbyregister
     WHERE financial_expenses_euro IS NOT NULL
     ORDER BY financial_expenses_euro DESC
     LIMIT 10`,
  );
  const top10 = top10Rows.map((r) => ({
    register_number: r.register_number,
    name: r.name,
    city: r.city,
    financial_expenses_euro:
      r.financial_expenses_euro != null ? Number(r.financial_expenses_euro) : null,
    employee_fte: r.employee_fte != null ? Number(r.employee_fte) : null,
    details_url: r.details_url,
  }));

  res.json({
    total: Number(agg?.total) || 0,
    active: Number(agg?.active) || 0,
    mit_finanzdaten: Number(agg?.mit_finanzdaten) || 0,
    max_ausgaben: agg?.max_ausgaben != null ? Number(agg.max_ausgaben) : null,
    avg_ausgaben: agg?.avg_ausgaben != null ? Number(agg.avg_ausgaben) : null,
    top10,
  });
}));

router.get("/lobbyregister/by-field", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT
       foi.code AS code,
       foi.de AS de,
       foi.en AS en,
       COUNT(*) AS count,
       SUM(financial_expenses_euro) AS total_expenses,
       AVG(financial_expenses_euro) AS avg_expenses
     FROM lobbyregister,
     JSON_TABLE(fields_of_interest, '$[*]' COLUMNS (
       code VARCHAR(100) PATH '$.code',
       de VARCHAR(200) PATH '$.de',
       en VARCHAR(200) PATH '$.en'
     )) AS foi
     WHERE financial_expenses_euro IS NOT NULL
       AND active = 1
     GROUP BY foi.code, foi.de, foi.en
     ORDER BY total_expenses DESC
     LIMIT 15`,
  );

  res.json({
    items: rows.map((r) => ({
      code: r.code,
      de: r.de,
      en: r.en,
      count: Number(r.count) || 0,
      total_expenses:
        r.total_expenses != null ? Number(r.total_expenses) : null,
      avg_expenses: r.avg_expenses != null ? Number(r.avg_expenses) : null,
    })),
  });
}));

router.get("/lobbyregister/by-city", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT
       city,
       country,
       COUNT(*) AS count,
       SUM(financial_expenses_euro) AS total_expenses,
       AVG(financial_expenses_euro) AS avg_expenses
     FROM lobbyregister
     WHERE active = 1
       AND city IS NOT NULL
       AND city != ''
     GROUP BY city, country
     ORDER BY total_expenses DESC
     LIMIT 50`,
  );

  res.json({
    items: rows.map((r) => ({
      city: r.city,
      country: r.country,
      count: Number(r.count) || 0,
      total_expenses:
        r.total_expenses != null ? Number(r.total_expenses) : null,
      avg_expenses: r.avg_expenses != null ? Number(r.avg_expenses) : null,
    })),
  });
}));

router.get("/lobbyregister/by-time", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT
       DATE_FORMAT(first_publication, '%Y-%m') AS month,
       COUNT(*) AS count,
       SUM(COUNT(*)) OVER (
         ORDER BY DATE_FORMAT(first_publication, '%Y-%m')
       ) AS cumulative
     FROM lobbyregister
     WHERE first_publication IS NOT NULL
     GROUP BY DATE_FORMAT(first_publication, '%Y-%m')
     ORDER BY month ASC`,
  );

  res.json({
    items: rows.map((r) => ({
      month: r.month,
      count: Number(r.count) || 0,
      cumulative: Number(r.cumulative) || 0,
    })),
  });
}));

router.get("/lobbyregister/:register_number/projects", asyncHandler(async (req, res) => {
  const registerNumber = String(req.params.register_number || "").trim();
  if (!registerNumber) {
    res.status(400).json({ error: "Ungültige register_number" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT *
     FROM lobby_regulatory_projects
     WHERE lobby_register_number = ?
     ORDER BY updated_at DESC`,
    [registerNumber],
  );
  res.json({ items: rows });
}));

router.get("/lobby-projects/by-gesetz-id", asyncHandler(async (req, res) => {
  const gesetzId = Number.parseInt(String(req.query.gesetz_id ?? ""), 10);
  if (!Number.isFinite(gesetzId) || gesetzId < 1) {
    res.status(400).json({ error: "Ungültige gesetz_id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT
       lrp.id, lrp.title, lrp.project_number, lrp.description,
       lrp.affected_laws, lrp.project_url, lrp.document_url,
       lrp.leading_ministries,
       l.register_number, l.name AS lobbyist_name,
       l.financial_expenses_euro, l.city, l.legal_form
     FROM lobby_gesetze lg
     INNER JOIN lobby_regulatory_projects lrp ON lrp.id = lg.project_id
     INNER JOIN lobbyregister l ON l.register_number = lrp.lobby_register_number
     WHERE lg.gesetz_id = ?
     ORDER BY l.financial_expenses_euro DESC
     LIMIT 50`,
    [gesetzId],
  );
  res.json({ exact: rows, related: [] });
}));

router.get("/lobbyregister/:register_number/gesetze", asyncHandler(async (req, res) => {
  const registerNumber = String(req.params.register_number || "").trim();
  if (!registerNumber) {
    res.status(400).json({ error: "register_number fehlt" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT
       g.id AS gesetz_id,
       g.kuerzel,
       g.titel_offiziell,
       g.name,
       g.gii_slug,
       COUNT(DISTINCT lg.project_id) AS projekt_count,
       (
         SELECT a.id 
         FROM aenderungen a 
         WHERE a.gesetz_id = g.id 
         ORDER BY a.datum DESC, a.id DESC 
         LIMIT 1
       ) AS aenderung_id
     FROM lobby_gesetze lg
     INNER JOIN lobby_regulatory_projects lrp ON lrp.id = lg.project_id
     INNER JOIN gesetze g ON g.id = lg.gesetz_id
     WHERE lrp.lobby_register_number = ?
     GROUP BY g.id, g.kuerzel, g.titel_offiziell, g.name, g.gii_slug
     ORDER BY projekt_count DESC, g.kuerzel ASC`,
    [registerNumber],
  );

  // Stats fuer methodische Transparenz
  const [[stats]] = await getPool().query(
    `SELECT 
       COUNT(DISTINCT lrp.id) AS projekte_gesamt,
       COUNT(DISTINCT lg.project_id) AS projekte_mit_mapping
     FROM lobby_regulatory_projects lrp
     LEFT JOIN lobby_gesetze lg ON lg.project_id = lrp.id
     WHERE lrp.lobby_register_number = ?`,
    [registerNumber],
  );

  res.json({
    items: rows,
    stats: {
      projekte_gesamt: Number(stats.projekte_gesamt) || 0,
      projekte_mit_mapping: Number(stats.projekte_mit_mapping) || 0,
      unique_gesetze: rows.length,
    },
  });
}));

router.get("/lobby-projects/by-law", asyncHandler(async (req, res) => {
  const q = String(req.query.q ?? "").trim();
  if (!q || q.length < 2) {
    res.json({ exact: [], related: [] });
    return;
  }
  const [exactRows] = await getPool().query(
    `SELECT
       lrp.id, lrp.title, lrp.project_number, lrp.description,
       lrp.affected_laws, lrp.project_url, lrp.document_url,
       lrp.leading_ministries,
       l.register_number, l.name AS lobbyist_name,
       l.financial_expenses_euro, l.city, l.legal_form
     FROM lobby_regulatory_projects lrp
     JOIN lobbyregister l ON l.register_number = lrp.lobby_register_number
     WHERE JSON_SEARCH(lrp.affected_laws, 'one', ?) IS NOT NULL
     ORDER BY l.financial_expenses_euro DESC
     LIMIT 20`,
    [q],
  );

  const [titleRows] = await getPool().query(
    `SELECT
       lrp.id, lrp.title, lrp.project_number, lrp.description,
       lrp.affected_laws, lrp.project_url, lrp.document_url,
       lrp.leading_ministries,
       l.register_number, l.name AS lobbyist_name,
       l.financial_expenses_euro, l.city, l.legal_form
     FROM lobby_regulatory_projects lrp
     JOIN lobbyregister l ON l.register_number = lrp.lobby_register_number
     WHERE lrp.title LIKE ?
       AND JSON_SEARCH(lrp.affected_laws, 'one', ?) IS NULL
     ORDER BY l.financial_expenses_euro DESC
     LIMIT 20`,
    [`%${q}%`, q],
  );

  res.json({ exact: exactRows, related: titleRows });
}));

router.get("/lobby-projects/stats", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT title, project_number, COUNT(*) as lobby_count,
            SUM(l.financial_expenses_euro) as total_lobby_budget
     FROM lobby_regulatory_projects lrp
     JOIN lobbyregister l ON l.register_number = lrp.lobby_register_number
     WHERE lrp.project_number IS NOT NULL
     GROUP BY title, project_number
     ORDER BY lobby_count DESC
     LIMIT 10`,
  );
  res.json({ items: rows });
}));

router.get("/lobbyregister/:register_number", asyncHandler(async (req, res) => {
  const registerNumber = String(req.params.register_number || "").trim();
  if (!registerNumber) {
    res.status(400).json({ error: "Ungültige register_number" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT
       id,
       register_number,
       name,
       legal_form,
       city,
       country,
       active,
       members_count,
       employee_fte,
       financial_expenses_euro,
       financial_year_start,
       financial_year_end,
       fields_of_interest,
       activity_description,
       regulatory_projects_count,
       statements_count,
       details_url,
       first_publication,
       last_update
     FROM lobbyregister
     WHERE register_number = ?
     LIMIT 1`,
    [registerNumber],
  );
  if (!rows.length) {
    res.status(404).json({ error: "Nicht gefunden" });
    return;
  }
  const r = rows[0];
  res.json({
    id: r.id,
    register_number: r.register_number,
    name: r.name,
    legal_form: r.legal_form,
    city: r.city,
    country: r.country,
    active: r.active === null ? null : Boolean(r.active),
    members_count: r.members_count != null ? Number(r.members_count) : null,
    employee_fte: r.employee_fte != null ? Number(r.employee_fte) : null,
    financial_expenses_euro:
      r.financial_expenses_euro != null ? Number(r.financial_expenses_euro) : null,
    financial_year_start: formatDate(r.financial_year_start),
    financial_year_end: formatDate(r.financial_year_end),
    fields_of_interest: safeJsonParse(r.fields_of_interest, []),
    activity_description: r.activity_description,
    regulatory_projects_count:
      r.regulatory_projects_count != null ? Number(r.regulatory_projects_count) : null,
    statements_count: r.statements_count != null ? Number(r.statements_count) : null,
    details_url: r.details_url,
    first_publication: formatDate(r.first_publication),
    last_update: formatDate(r.last_update),
  });
}));


module.exports = router;
