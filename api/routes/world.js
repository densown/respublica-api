"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");

/** World helper: parse DB decimal to number */
function worldNum(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** World API: `lang=de`|`lang=en`, default `en` (legacy parity). */
function worldLang(req) {
  const v = String(req.query.lang || "").trim().toLowerCase();
  return v === "de" ? "de" : "en";
}
function worldNameCol(lang) {
  return lang === "de" ? "name_de" : "name_en";
}
function worldRegionCol(lang) {
  return lang === "de" ? "region_de" : "region_en";
}
function worldUnitCol(lang) {
  return lang === "de" ? "unit_de" : "unit_en";
}
/** Maps data_countries.income_level enum to legacy world_indicators strings. */
function worldIncomeCaseSql(alias = "dc") {
  return `CASE ${alias}.income_level
    WHEN 'high' THEN 'High income'
    WHEN 'upper_middle' THEN 'Upper middle income'
    WHEN 'lower_middle' THEN 'Lower middle income'
    WHEN 'low' THEN 'Low income'
    WHEN 'aggregate' THEN 'Aggregates'
    ELSE NULL
  END`;
}

/** Join world_indicators row matching data_values (same indicator + year). */
function worldWiDimNameSql(lang, dv = "dv", dc = "dc", wi = "wi") {
  const dvRef = `${dv}.country_code`;
  if (lang === "de") {
    return `COALESCE(${dc}.\`name_de\`, ${wi}.country_name, ${dvRef})`;
  }
  return `COALESCE(${wi}.country_name, ${dc}.\`name_en\`, ${dvRef})`;
}

function worldWiDimRegionSql(lang, dc = "dc", wi = "wi") {
  if (lang === "de") {
    return `COALESCE(${dc}.\`region_de\`, ${wi}.region)`;
  }
  return `COALESCE(${wi}.region, ${dc}.\`region_en\`)`;
}

const WORLD_CATEGORY_LABELS = {
  economy: { label_de: "Wirtschaft", label_en: "Economy" },
  population: { label_de: "Bevölkerung", label_en: "Population" },
  education: { label_de: "Bildung", label_en: "Education" },
  health: { label_de: "Gesundheit", label_en: "Health" },
  environment: { label_de: "Umwelt", label_en: "Environment" },
  governance: { label_de: "Governance", label_en: "Governance" },
  military: { label_de: "Militär", label_en: "Military" },
  inequality: { label_de: "Ungleichheit", label_en: "Inequality" },
  technology: { label_de: "Technologie", label_en: "Technology" },
  trade: { label_de: "Handel", label_en: "Trade" },
  security: { label_de: "Sicherheit", label_en: "Security" },
  democracy: { label_de: "Demokratie", label_en: "Democracy" },
};

router.get("/world/categories", asyncHandler(async (req, res) => {
  const lang = worldLang(req);
  const nameCol = worldNameCol(lang);
  const unitCol = worldUnitCol(lang);
  const [rows] = await getPool().query(
    `SELECT di.code AS indicator_code,
            COALESCE(wm.indicator_name, di.\`${nameCol}\`) AS indicator_name,
            COALESCE(wm.category, di.category) AS category,
            COALESCE(wm.unit, di.\`${unitCol}\`) AS unit,
            COALESCE(wm.description_de, di.description_de) AS description_de,
            COALESCE(wm.description_en, di.description_en) AS description_en
     FROM data_indicators di
     LEFT JOIN world_indicator_meta wm ON wm.indicator_code = di.code
     WHERE di.is_active = 1
       -- Köppen-Geiger-Klimaklassen (kategorial 1..30) aus dem Karten-Dropdown
       -- ausblenden: die kontinuierliche Choropleth-Farbskala würde sie falsch
       -- als Gradient einfärben. Sie bleiben über /api/world/country/:code und
       -- /api/world/climate/:iso3 verfügbar.
       AND di.code NOT LIKE 'climate.koppen.dominant_%'
     ORDER BY di.category, di.code`,
  );
  const byCat = new Map();
  for (const r of rows) {
    const cat = r.category || "other";
    if (!byCat.has(cat)) byCat.set(cat, []);
    byCat.get(cat).push({
      code: r.indicator_code,
      name: r.indicator_name,
      unit: r.unit,
      description_de: r.description_de,
      description_en: r.description_en,
    });
  }
  const order = Object.keys(WORLD_CATEGORY_LABELS);
  const out = [];
  for (const id of order) {
    const indicators = byCat.get(id);
    if (!indicators?.length) continue;
    const lab = WORLD_CATEGORY_LABELS[id];
    out.push({
      id,
      label_de: lab.label_de,
      label_en: lab.label_en,
      indicators,
    });
  }
  for (const [id, indicators] of byCat) {
    if (order.includes(id)) continue;
    out.push({
      id,
      label_de: id,
      label_en: id,
      indicators,
    });
  }
  res.json(out);
}));

router.get("/world/indicators", asyncHandler(async (req, res) => {
  const lang = worldLang(req);
  const nameCol = worldNameCol(lang);
  const unitCol = worldUnitCol(lang);
  const [rows] = await getPool().query(
    `SELECT di.code,
            COALESCE(wm.indicator_name, di.\`${nameCol}\`) AS name,
            COALESCE(wm.category, di.category) AS category,
            COALESCE(wm.unit, di.\`${unitCol}\`) AS unit,
            COALESCE(wm.description_de, di.description_de) AS description_de,
            COALESCE(wm.description_en, di.description_en) AS description_en,
            COALESCE(wm.source, ds.name) AS source,
            COALESCE(wm.source_url, ds.url) AS source_url
     FROM data_indicators di
     LEFT JOIN world_indicator_meta wm ON wm.indicator_code = di.code
     LEFT JOIN data_sources ds ON ds.id = di.source_id
     WHERE di.is_active = 1
     ORDER BY di.category, di.code`,
  );
  res.json(rows);
}));

router.get("/world/map", asyncHandler(async (req, res) => {
  const indicator = String(req.query.indicator || "").trim();
  let year = Number.parseInt(String(req.query.year || ""), 10);
  if (!indicator) {
    res.status(400).json({ error: "indicator erforderlich" });
    return;
  }
  // Kategoriale Köppen-Geiger-Indikatoren (Werte 1..30) können nicht als
  // kontinuierliche Choropleth gerendert werden und sind hier deshalb gesperrt.
  // Sie sind aus dem Karten-Dropdown gefiltert (/api/world/categories); dieser
  // Block fängt direkt konstruierte Anfragen ab. Wenn später ein echter
  // Klimakarten-Modus mit kategorialer Färbung dazukommt, wird /api/world/map
  // entsprechend erweitert und dieser Block fällt.
  if (indicator.startsWith("climate.koppen.dominant_")) {
    res.status(400).json({
      error:
        "Kategoriale Indikatoren sind über diesen Endpoint nicht abrufbar. " +
        "Nutze /api/world/climate/:iso3 für Klimadaten.",
    });
    return;
  }
  if (!Number.isFinite(year)) {
    res.status(400).json({ error: "year ungültig" });
    return;
  }
  const lang = worldLang(req);
  const incomeSql = worldIncomeCaseSql("dc");
  const nameSql = worldWiDimNameSql(lang, "dv", "dc", "wi");
  const regionSql = worldWiDimRegionSql(lang, "dc", "wi");
  const [rows] = await getPool().query(
    `SELECT dv.country_code,
            ${nameSql} AS country_name,
            dv.value,
            ${regionSql} AS region,
            COALESCE(wi.income_level, ${incomeSql}) AS income_level
     FROM data_values dv
     INNER JOIN data_indicators di ON di.id = dv.indicator_id AND di.code = ?
     LEFT JOIN data_countries dc ON dc.iso3 = dv.country_code
     LEFT JOIN world_indicators wi
       ON wi.country_code = dv.country_code
      AND wi.indicator_code = di.code
      AND wi.year = dv.year
     WHERE dv.year = ? AND dv.value IS NOT NULL`,
    [indicator, year],
  );
  res.json(
    rows.map((r) => ({
      country_code: r.country_code,
      country_name: r.country_name,
      value: worldNum(r.value),
      region: r.region,
      income_level: r.income_level,
    })),
  );
}));

router.get("/world/country/:code", asyncHandler(async (req, res) => {
  const code = String(req.params.code || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  if (!code || code.length !== 3) {
    res.status(400).json({ error: "Ungültiger Ländercode" });
    return;
  }
  const lang = worldLang(req);
  const nameCol = worldNameCol(lang);
  const regionCol = worldRegionCol(lang);
  const incomeSql = worldIncomeCaseSql("dc");
  const pool = getPool();

  const [[hasValues]] = await pool.query(
    `SELECT 1 AS ok FROM data_values WHERE country_code = ? LIMIT 1`,
    [code],
  );
  if (!hasValues?.ok) {
    res.status(404).json({ error: "Nicht gefunden" });
    return;
  }

  const [[wiMeta]] = await pool.query(
    `SELECT country_code, country_name, region, income_level
     FROM world_indicators
     WHERE country_code = ?
     LIMIT 1`,
    [code],
  );
  let resolvedMeta;
  if (wiMeta?.country_code) {
    resolvedMeta = {
      country_code: wiMeta.country_code,
      country_name: wiMeta.country_name,
      region: wiMeta.region,
      income_level: wiMeta.income_level,
    };
  } else {
    const [[dcMeta]] = await pool.query(
      `SELECT iso3 AS country_code,
              dc.\`${nameCol}\` AS country_name,
              dc.\`${regionCol}\` AS region,
              ${incomeSql} AS income_level
       FROM data_countries dc
       WHERE dc.iso3 = ?
       LIMIT 1`,
      [code],
    );
    resolvedMeta =
      dcMeta && dcMeta.country_code
        ? {
            country_code: dcMeta.country_code,
            country_name: dcMeta.country_name,
            region: dcMeta.region,
            income_level: dcMeta.income_level,
          }
        : {
            country_code: code,
            country_name: code,
            region: null,
            income_level: null,
          };
  }

  const [dataRows] = await pool.query(
    `SELECT di.code AS indicator_code,
            dv.year,
            dv.value
     FROM data_values dv
     INNER JOIN data_indicators di ON di.id = dv.indicator_id
     WHERE dv.country_code = ?
     ORDER BY di.code, dv.year ASC`,
    [code],
  );

  const [metaRows] = await pool.query(
    `SELECT di.code AS indicator_code,
            COALESCE(wm.indicator_name, di.\`${nameCol}\`) AS indicator_name,
            COALESCE(wm.category, di.category) AS category
     FROM data_indicators di
     LEFT JOIN world_indicator_meta wm ON wm.indicator_code = di.code
     WHERE di.is_active = 1`,
  );
  const metaByCode = new Map(
    metaRows.map((m) => [m.indicator_code, m]),
  );
  const byInd = new Map();
  for (const r of dataRows) {
    const ic = r.indicator_code;
    if (!byInd.has(ic)) byInd.set(ic, []);
    byInd.get(ic).push({
      year: r.year,
      value: worldNum(r.value),
    });
  }
  const indicators = [];
  for (const [indicator_code, values] of byInd) {
    const m = metaByCode.get(indicator_code);
    indicators.push({
      indicator_code,
      name: m?.indicator_name ?? indicator_code,
      category: m?.category ?? null,
      values,
    });
  }
  indicators.sort((a, b) =>
    a.indicator_code.localeCompare(b.indicator_code),
  );
  res.json({
    country_code: resolvedMeta.country_code,
    country_name: resolvedMeta.country_name,
    region: resolvedMeta.region,
    income_level: resolvedMeta.income_level,
    indicators,
  });
}));

// Köppen-Geiger-Klimaklassen pro Land: dominante Klasse + volle Verteilung je
// Szenario. Vorlage: GET /api/world/trade/:iso3. Joint data_values (die fünf
// climate.koppen.dominant_*-Indikatoren) mit climate_koeppen_classes (Symbol/
// Name/Farbe) und climate_koeppen_distribution (Anteil je vorkommender Klasse).
const CLIMATE_SCENARIOS = [
  { scenario: "historical", code: "climate.koppen.dominant_historical", period: "1991_2020", year: 2020 },
  { scenario: "ssp126", code: "climate.koppen.dominant_ssp126", period: "2071_2099", year: 2099 },
  { scenario: "ssp245", code: "climate.koppen.dominant_ssp245", period: "2071_2099", year: 2099 },
  { scenario: "ssp370", code: "climate.koppen.dominant_ssp370", period: "2071_2099", year: 2099 },
  { scenario: "ssp585", code: "climate.koppen.dominant_ssp585", period: "2071_2099", year: 2099 },
];

router.get("/world/climate/:iso3", asyncHandler(async (req, res) => {
  const iso3 = String(req.params.iso3 || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  if (!iso3 || iso3.length !== 3) {
    res.status(400).json({ error: "Ungültiger ISO3-Code" });
    return;
  }
  const lang = worldLang(req);
  const nameCol = worldNameCol(lang);
  const pool = getPool();

  // Dominante Klasse je Szenario (data_values -> Indikator -> Klassen-Lookup).
  const [dominantRows] = await pool.query(
    `SELECT di.code AS indicator_code,
            dv.year,
            ROUND(dv.value) AS class_code,
            k.symbol, k.name_de, k.name_en,
            k.short_name_de, k.short_name_en,
            k.color_rgb, k.major_group
     FROM data_values dv
     INNER JOIN data_indicators di ON di.id = dv.indicator_id
     LEFT JOIN climate_koeppen_classes k ON k.class_code = ROUND(dv.value)
     WHERE dv.country_code = ?
       AND di.code IN (?, ?, ?, ?, ?)`,
    [iso3, ...CLIMATE_SCENARIOS.map((s) => s.code)],
  );

  // Kein Klima-Datensatz für dieses Land -> wie unbekannt behandeln.
  if (!dominantRows.length) {
    res.status(404).json({ error: "Nicht gefunden" });
    return;
  }

  // Volle Verteilung je Szenario/Periode (eine Zeile pro vorkommender Klasse).
  const [distRows] = await pool.query(
    `SELECT d.scenario,
            d.period,
            d.class_code,
            d.share,
            d.pixel_count,
            k.symbol,
            k.color_rgb
     FROM climate_koeppen_distribution d
     LEFT JOIN climate_koeppen_classes k ON k.class_code = d.class_code
     WHERE d.country_code = ?
     ORDER BY d.scenario ASC, d.share DESC`,
    [iso3],
  );

  const [[nameRow]] = await pool.query(
    `SELECT dc.\`${nameCol}\` AS country_name
     FROM data_countries dc
     WHERE dc.iso3 = ?
     LIMIT 1`,
    [iso3],
  );

  // Indikator-Code + Jahr -> dominante Klasse. Das Jahr unterscheidet später
  // evtl. ergänzte Mid-Century-Rows (2041_2070) am selben SSP-Indikator.
  const dominantByKey = new Map(
    dominantRows.map((r) => [`${r.indicator_code}|${r.year}`, r]),
  );
  // Szenario + Periode -> Verteilungszeilen (Reihenfolge bereits share DESC).
  const distByKey = new Map();
  for (const r of distRows) {
    const key = `${r.scenario}|${r.period}`;
    if (!distByKey.has(key)) distByKey.set(key, []);
    distByKey.get(key).push({
      class_code: worldNum(r.class_code),
      symbol: r.symbol,
      color_rgb: r.color_rgb,
      share: worldNum(r.share),
      pixel_count: worldNum(r.pixel_count),
    });
  }

  const scenarios = CLIMATE_SCENARIOS.map((s) => {
    const dom = dominantByKey.get(`${s.code}|${s.year}`);
    const distribution = distByKey.get(`${s.scenario}|${s.period}`) || [];
    let dominant = null;
    if (dom && dom.class_code != null) {
      const classCode = worldNum(dom.class_code);
      // Anteil der dominanten Klasse aus der Verteilung übernehmen.
      const domShare = distribution.find((d) => d.class_code === classCode);
      dominant = {
        class_code: classCode,
        symbol: dom.symbol,
        name_de: dom.name_de,
        name_en: dom.name_en,
        short_name: lang === "de" ? dom.short_name_de : dom.short_name_en,
        color_rgb: dom.color_rgb,
        major_group: dom.major_group,
        share: domShare ? domShare.share : null,
      };
    }
    return {
      scenario: s.scenario,
      period: s.period,
      year: s.year,
      dominant,
      distribution,
    };
  });

  res.json({
    iso3,
    country_name: nameRow?.country_name || iso3,
    scenarios,
  });
}));

router.get("/world/timeseries", asyncHandler(async (req, res) => {
  const country = String(req.query.country || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  const indicator = String(req.query.indicator || "").trim();
  if (!country || country.length !== 3 || !indicator) {
    res.status(400).json({ error: "country und indicator erforderlich" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT dv.year, dv.value
     FROM data_values dv
     INNER JOIN data_indicators di ON di.id = dv.indicator_id AND di.code = ?
     WHERE dv.country_code = ?
     ORDER BY dv.year ASC`,
    [indicator, country],
  );
  res.json(
    rows.map((r) => ({
      year: r.year,
      value: worldNum(r.value),
    })),
  );
}));

router.get("/world/compare", asyncHandler(async (req, res) => {
  const countriesRaw = String(req.query.countries || "").trim();
  const indicator = String(req.query.indicator || "").trim();
  if (!countriesRaw || !indicator) {
    res.status(400).json({ error: "countries und indicator erforderlich" });
    return;
  }
  const codes = countriesRaw
    .split(",")
    .map((s) => s.trim().toUpperCase().slice(0, 3))
    .filter((c) => c.length === 3);
  if (!codes.length) {
    res.status(400).json({ error: "Keine gültigen Ländercodes" });
    return;
  }
  const uniq = [...new Set(codes)];
  const lang = worldLang(req);
  const nameSql = worldWiDimNameSql(lang, "dv", "dc", "wi");
  const ph = uniq.map(() => "?").join(",");
  const [rows] = await getPool().query(
    `SELECT dv.country_code,
            ${nameSql} AS country_name,
            dv.year,
            dv.value
     FROM data_values dv
     INNER JOIN data_indicators di ON di.id = dv.indicator_id AND di.code = ?
     LEFT JOIN data_countries dc ON dc.iso3 = dv.country_code
     LEFT JOIN world_indicators wi
       ON wi.country_code = dv.country_code
      AND wi.indicator_code = di.code
      AND wi.year = dv.year
     WHERE dv.country_code IN (${ph})
     ORDER BY dv.country_code, dv.year ASC`,
    [indicator, ...uniq],
  );
  const byCountry = new Map();
  for (const r of rows) {
    if (!byCountry.has(r.country_code)) {
      byCountry.set(r.country_code, {
        code: r.country_code,
        name: r.country_name,
        data: [],
      });
    }
    byCountry.get(r.country_code).data.push({
      year: r.year,
      value: worldNum(r.value),
    });
  }
  const countries = uniq
    .map((c) => byCountry.get(c))
    .filter(Boolean);
  res.json({ countries });
}));

router.get("/world/ranking", asyncHandler(async (req, res) => {
  const indicator = String(req.query.indicator || "").trim();
  let year = Number.parseInt(String(req.query.year || ""), 10);
  let limit = Number.parseInt(String(req.query.limit || "2500"), 10);
  const order = String(req.query.order || "desc").toLowerCase() === "asc"
    ? "ASC"
    : "DESC";
  if (!indicator || !Number.isFinite(year)) {
    res.status(400).json({ error: "indicator und year erforderlich" });
    return;
  }
  if (!Number.isFinite(limit) || limit < 1) limit = 2500;
  if (limit > 5000) limit = 5000;
  const lang = worldLang(req);
  const nameSql = worldWiDimNameSql(lang, "dv", "dc", "wi");
  const [rows] = await getPool().query(
    `SELECT dv.country_code,
            ${nameSql} AS country_name,
            dv.value
     FROM data_values dv
     INNER JOIN data_indicators di ON di.id = dv.indicator_id AND di.code = ?
     LEFT JOIN data_countries dc ON dc.iso3 = dv.country_code
     LEFT JOIN world_indicators wi
       ON wi.country_code = dv.country_code
      AND wi.indicator_code = di.code
      AND wi.year = dv.year
     WHERE dv.year = ? AND dv.value IS NOT NULL
     ORDER BY dv.value ${order}, dv.country_code ASC
     LIMIT ?`,
    [indicator, year, limit],
  );
  const out = rows.map((r, i) => ({
    country_code: r.country_code,
    country_name: r.country_name,
    value: worldNum(r.value),
    rank: i + 1,
  }));
  res.json(out);
}));

router.get("/world/scatter", asyncHandler(async (req, res) => {
  const xCode = String(req.query.x || "").trim();
  const yCode = String(req.query.y || "").trim();
  let year = Number.parseInt(String(req.query.year || ""), 10);
  if (!xCode || !yCode || !Number.isFinite(year)) {
    res.status(400).json({ error: "x, y und year erforderlich" });
    return;
  }
  const lang = worldLang(req);
  const nameSql = worldWiDimNameSql(lang, "a", "dc", "wi");
  const regionSql = worldWiDimRegionSql(lang, "dc", "wi");
  const [rows] = await getPool().query(
    `SELECT a.country_code,
            ${nameSql} AS country_name,
            ${regionSql} AS region,
            a.value AS x, b.value AS y
     FROM data_values a
     INNER JOIN data_indicators dix
       ON dix.id = a.indicator_id AND dix.code = ?
     INNER JOIN data_values b
       ON b.country_code = a.country_code AND b.year = a.year
     INNER JOIN data_indicators diy
       ON diy.id = b.indicator_id AND diy.code = ?
     LEFT JOIN data_countries dc ON dc.iso3 = a.country_code
     LEFT JOIN world_indicators wi
       ON wi.country_code = a.country_code
      AND wi.indicator_code = dix.code
      AND wi.year = a.year
     WHERE a.year = ?
       AND a.value IS NOT NULL AND b.value IS NOT NULL`,
    [xCode, yCode, year],
  );
  res.json(
    rows.map((r) => ({
      country_code: r.country_code,
      country_name: r.country_name,
      region: r.region,
      x: worldNum(r.x),
      y: worldNum(r.y),
    })),
  );
}));

const WORLDMAP_SOURCE_SLUGS = new Set([
  "worldbank_wdi",
  "vdem",
  "sipri",
  "rsf",
  "oecd",
  "imf_weo",
  "freedomhouse",
  "un_hdr",
  "un_comtrade",
  "yale_epi",
  "cepii_baci_hs17",
]);

function dataSourceDomain(slug) {
  return WORLDMAP_SOURCE_SLUGS.has(String(slug || "").trim())
    ? "worldmap"
    : "other";
}

function isoOrNull(v) {
  if (v == null) return null;
  if (v instanceof Date && !Number.isNaN(v.getTime())) {
    return v.toISOString();
  }
  const s = String(v).trim();
  if (!s) return null;
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d.toISOString();
  return s;
}

router.get("/world/sources", asyncHandler(async (_req, res) => {
  const pool = getPool();
  const [rows] = await pool.query(
    `SELECT s.id,
            s.slug,
            s.name,
            s.provider,
            s.url,
            s.license,
            s.update_freq,
            s.last_fetched,
            (SELECT COUNT(DISTINCT i.id)
             FROM data_indicators i
             WHERE i.source_id = s.id) AS indicator_count,
            COALESCE(
              (SELECT COUNT(*)
               FROM data_values v
               INNER JOIN data_indicators i ON i.id = v.indicator_id
               WHERE i.source_id = s.id),
              0
            ) + COALESCE(
              (SELECT COUNT(*) FROM trade_flows_v2 t WHERE t.source_id = s.id),
              0
            ) AS value_count
     FROM data_sources s
     ORDER BY s.id`,
  );
  const sources = rows.map((r) => ({
    slug: r.slug,
    name: r.name,
    provider: r.provider ?? null,
    url: r.url ?? null,
    license: r.license ?? null,
    update_freq: r.update_freq ?? null,
    last_fetched: isoOrNull(r.last_fetched),
    domain: dataSourceDomain(r.slug),
    indicator_count: Number(r.indicator_count) || 0,
    value_count: Number(r.value_count) || 0,
  }));
  res.json({ sources });
}));

router.get("/world/stats", asyncHandler(async (_req, res) => {
  const pool = getPool();
  const [[{ total_records }]] = await pool.query(
    "SELECT COUNT(*) AS total_records FROM data_values",
  );
  const [[{ countries }]] = await pool.query(
    "SELECT COUNT(DISTINCT country_code) AS countries FROM data_values",
  );
  const [[{ indicators }]] = await pool.query(
    "SELECT COUNT(*) AS indicators FROM data_indicators WHERE is_active = 1",
  );
  const [[yr]] = await pool.query(
    "SELECT MIN(year) AS y_min, MAX(year) AS y_max FROM data_values",
  );
  res.json({
    total_records: Number(total_records) || 0,
    countries: Number(countries) || 0,
    indicators: Number(indicators) || 0,
    years_range:
      yr.y_min != null && yr.y_max != null
        ? { min: yr.y_min, max: yr.y_max }
        : null,
  });
}));

router.get("/world/trade/:iso3", asyncHandler(async (req, res) => {
  const iso3 = String(req.params.iso3 || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  const lang = worldLang(req);
  const partnerNameCol = worldNameCol(lang);
  const year = Number.parseInt(String(req.query.year || ""), 10) || 2023;
  const includeSections = String(req.query.breakdown || "").trim() === "sections";
  const partnerRaw = String(req.query.partner || "")
    .trim()
    .toUpperCase();
  const partner = partnerRaw ? partnerRaw.slice(0, 3) : null;
  if (!iso3 || iso3.length !== 3) {
    res.status(400).json({ error: "Ungültiger ISO3-Code" });
    return;
  }
  if (partnerRaw && partnerRaw.length !== 3) {
    res.status(400).json({ error: "Ungültiger Partner-ISO3-Code" });
    return;
  }
  const pool = getPool();
  const [exportsRows] = await pool.query(
    `SELECT t.partner_iso3 AS partner_code,
            COALESCE(dc.\`${partnerNameCol}\`, t.partner_iso3) AS partner_name,
            t.value_usd
     FROM trade_flows_v2 t
     LEFT JOIN data_countries dc ON dc.iso3 = t.partner_iso3
     WHERE t.reporter_iso3 = ? AND t.flow = 'export' AND t.year = ?
       AND t.hs_section = 'TOTAL'
     ORDER BY t.value_usd DESC
     LIMIT 10`,
    [iso3, year],
  );
  const [importsRows] = await pool.query(
    `SELECT t.partner_iso3 AS partner_code,
            COALESCE(dc.\`${partnerNameCol}\`, t.partner_iso3) AS partner_name,
            t.value_usd
     FROM trade_flows_v2 t
     LEFT JOIN data_countries dc ON dc.iso3 = t.partner_iso3
     WHERE t.reporter_iso3 = ? AND t.flow = 'import' AND t.year = ?
       AND t.hs_section = 'TOTAL'
     ORDER BY t.value_usd DESC
     LIMIT 10`,
    [iso3, year],
  );
  const [[totals]] = await pool.query(
    `SELECT
      SUM(CASE WHEN flow='export' THEN value_usd ELSE 0 END) AS total_export,
      SUM(CASE WHEN flow='import' THEN value_usd ELSE 0 END) AS total_import
     FROM trade_flows_v2
     WHERE reporter_iso3 = ? AND year = ? AND hs_section = 'TOTAL'`,
    [iso3, year],
  );
  const payload = {
    iso3,
    year,
    total_export_usd: totals.total_export,
    total_import_usd: totals.total_import,
    top_exports: exportsRows,
    top_imports: importsRows,
  };

  if (includeSections) {
    const [sectionRows] = await pool.query(
      `SELECT flow, hs_section, SUM(value_usd) AS value_usd
       FROM trade_flows_v2
       WHERE reporter_iso3 = ? AND year = ? AND hs_section <> 'TOTAL'
         AND (? IS NULL OR partner_iso3 = ?)
       GROUP BY flow, hs_section
       ORDER BY flow ASC, value_usd DESC`,
      [iso3, year, partner, partner],
    );
    payload.sections_export = sectionRows.filter((r) => r.flow === "export");
    payload.sections_import = sectionRows.filter((r) => r.flow === "import");
  }

  res.json(payload);
}));

router.get("/world/trade/:iso3/timeseries", asyncHandler(async (req, res) => {
  const iso3 = String(req.params.iso3 || "")
    .trim()
    .toUpperCase()
    .slice(0, 3);
  const rawMin = Number.parseInt(String(req.query.yearMin || ""), 10);
  const rawMax = Number.parseInt(String(req.query.yearMax || ""), 10);
  const yearMin = Number.isFinite(rawMin) ? Math.max(1990, Math.min(2100, rawMin)) : 2017;
  const yearMax = Number.isFinite(rawMax) ? Math.max(1990, Math.min(2100, rawMax)) : 2024;
  const fromYear = Math.min(yearMin, yearMax);
  const toYear = Math.max(yearMin, yearMax);

  if (!iso3 || iso3.length !== 3) {
    res.status(400).json({ error: "Ungültiger ISO3-Code" });
    return;
  }

  const pool = getPool();
  const [rows] = await pool.query(
    `SELECT
       year,
       SUM(CASE WHEN flow='export' THEN value_usd ELSE 0 END) AS total_export_usd,
       SUM(CASE WHEN flow='import' THEN value_usd ELSE 0 END) AS total_import_usd
     FROM trade_flows_v2
     WHERE reporter_iso3 = ?
       AND hs_section = 'TOTAL'
       AND year BETWEEN ? AND ?
     GROUP BY year
     ORDER BY year ASC`,
    [iso3, fromYear, toYear],
  );

  res.json({
    iso3,
    years: rows.map((r) => ({
      year: Number(r.year),
      total_export_usd: Number(r.total_export_usd || 0),
      total_import_usd: Number(r.total_import_usd || 0),
    })),
  });
}));

module.exports = router;
