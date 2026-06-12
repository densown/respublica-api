"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

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
router.get("/wahlen/types", (_req, res) => {
  res.json(WAHlen_TYPS);
});

router.get("/wahlen/years", asyncHandler(async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  if (!typ) {
    res.status(400).json({ error: "typ erforderlich oder ungültig" });
    return;
  }
  const [rows] = await getPool().query(
    "SELECT DISTINCT election_year FROM wahlen WHERE typ = ? ORDER BY election_year ASC",
    [typ],
  );
  res.json(rows.map((r) => r.election_year));
}));

router.get("/wahlen/states", (_req, res) => {
  res.json(DE_STATES);
});

router.get("/wahlen/map", asyncHandler(async (req, res) => {
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
}));

router.get("/wahlen/timeseries", asyncHandler(async (req, res) => {
  const typ = wahlenParseTyp(req.query.typ);
  const party = String(req.query.party || "").trim().toLowerCase();
  const agsParam = String(req.query.ags || "").trim();
  const clause = wahlenAgsClause(agsParam);
  if (!typ || !party || !clause || !WAHlen_NUM_COLS.has(party)) {
    res.status(400).json({ error: "ags, typ und party erforderlich" });
    return;
  }
  const sql = `SELECT election_year AS year, \`${party}\` AS value FROM wahlen WHERE typ = ? AND ${clause.sql} ORDER BY election_year ASC`;
  const [rows] = await getPool().query(sql, [typ, ...clause.params]);
  const out = rows.map((r) => ({
    year: r.year,
    value: r.value != null ? Number(r.value) : null,
  }));
  res.json(out);
}));

router.get("/wahlen/compare", asyncHandler(async (req, res) => {
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
}));

router.get("/wahlen/scatter", asyncHandler(async (req, res) => {
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
}));

router.get("/wahlen/ranking", asyncHandler(async (req, res) => {
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
  const [rows] = await getPool().query(sql, [typ, year, limit]);
  const out = rows.map((r, i) => ({
    ags: r.ags,
    name: r.ags_name,
    state_name: r.state_name,
    value: Number(r.value),
    rank: i + 1,
  }));
  res.json(out);
}));

router.get("/wahlen/change", asyncHandler(async (req, res) => {
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
}));

/** Bundesweiter Durchschnitt pro Wahljahr (alle Kreise mit Wert) */
router.get("/wahlen/national-average", asyncHandler(async (req, res) => {
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
  const [rows] = await getPool().query(sql, [typ]);
  const out = rows.map((r) => ({
    year: r.year,
    value: r.value != null ? Number(r.value) : null,
  }));
  res.json(out);
}));

router.get("/wahlen/stats", asyncHandler(async (_req, res) => {
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
}));

router.get("/wahlen/region/:ags", asyncHandler(async (req, res) => {
  const clause = wahlenAgsClause(req.params.ags);
  if (!clause) {
    res.status(400).json({ error: "ags ungültig" });
    return;
  }
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
}));

module.exports = router;
