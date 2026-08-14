"use strict";

/**
 * Regierungsführung (Worldwide Governance Indicators der Weltbank).
 *
 * Sechs Dimensionen, 205 Länder, 2000 bis 2023. Bewusst NICHT zu einer Zahl
 * verrechnet: der interessante Befund liegt gerade darin, dass Abbau die
 * Dimensionen ungleich trifft. Hongkong verliert die Mitsprache und behaelt
 * eine saubere Verwaltung; El Salvador wird rechtsfoermiger und zugleich
 * unfreier. Ein Mittelwert loescht genau das aus.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");

/** Reihenfolge ist redaktionell: von der Freiheit zur Verwaltung. */
const DIMENSIONEN = [
  "VA.EST",
  "RL.EST",
  "CC.EST",
  "PV.EST",
  "GE.EST",
  "RQ.EST",
];
const DIM_SET = new Set(DIMENSIONEN);

const ISO_RE = /^[A-Z]{3}$/;
const JAHR_MIN = 2000;
const JAHR_MAX = 2023;

function parseJahr(wert, standard) {
  const n = Number.parseInt(String(wert ?? ""), 10);
  if (!Number.isFinite(n) || n < JAHR_MIN || n > JAHR_MAX) return standard;
  return n;
}

function parseLaender(wert) {
  if (!wert) return null;
  const liste = String(wert)
    .toUpperCase()
    .split(",")
    .map((s) => s.trim())
    .filter((s) => ISO_RE.test(s));
  return liste.length ? liste.slice(0, 30) : null;
}

/** Die sechs Dimensionen mit Namen und Abdeckung. */
router.get(
  "/governance/dimensionen",
  asyncHandler(async (_req, res) => {
    const [rows] = await getPool().query(
      `SELECT i.code, i.name_de, i.name_en, i.description_de, i.description_en,
              MIN(v.year) AS von, MAX(v.year) AS bis,
              COUNT(DISTINCT v.country_code) AS laender
         FROM data_indicators i
         LEFT JOIN data_values v ON v.indicator_id = i.id
        WHERE i.category = 'governance' AND i.is_active = 1
        GROUP BY i.id`,
    );
    // Redaktionelle Reihenfolge statt alphabetisch
    const nachCode = new Map(rows.map((r) => [r.code, r]));
    res.json({
      dimensionen: DIMENSIONEN.filter((c) => nachCode.has(c)).map((c) => {
        const r = nachCode.get(c);
        return {
          code: r.code,
          name_de: r.name_de,
          name_en: r.name_en,
          von: Number(r.von),
          bis: Number(r.bis),
          laender: Number(r.laender),
        };
      }),
      skala: { min: -2.5, max: 2.5 },
      quelle: {
        name: "Worldwide Governance Indicators",
        herausgeber: "Weltbank",
        url: "https://www.worldbank.org/en/publication/worldwide-governance-indicators",
      },
    });
  }),
);

/**
 * Zeitreihen für die Streifengrafik.
 *
 * Ohne ?laender= werden die staerksten Rueckgaenge der gewaehlten Dimension
 * ausgegeben, Deutschland immer angehaengt — die Seite soll auch dann etwas
 * zeigen, wenn niemand eine Auswahl trifft, und der deutsche Bezug ist der
 * Anker.
 */
router.get(
  "/governance/verlauf",
  asyncHandler(async (req, res) => {
    const code = String(req.query.dimension || "VA.EST").toUpperCase();
    if (!DIM_SET.has(code)) {
      res.status(400).json({ error: "dimension ungültig" });
      return;
    }
    const gewaehlt = parseLaender(req.query.laender);
    const anzahl = Math.min(
      20,
      Math.max(3, Number.parseInt(String(req.query.anzahl || "7"), 10) || 7),
    );
    // Bezugsjahr fuer Rangfolge UND ausgewiesene Veraenderung. Beides muss
    // dasselbe Fenster benutzen: eine Liste der groessten Rueckgaenge, in der
    // ein Land mit positiver Veraenderung steht, widerlegt sich selbst.
    // Afghanistan etwa faellt seit 2013 stark, liegt ueber 2000 bis 2023 aber
    // im Plus, weil es nach 2001 zunaechst stieg.
    const seit = parseJahr(req.query.seit, 2013);

    let laender = gewaehlt;
    if (!laender) {
      // Groesste Rueckgaenge zwischen erstem und letztem Jahr der Reihe
      // Der Join auf data_countries gehoert schon in die Rangfolge, nicht
      // erst in die Datenabfrage. Sonst waehlt die Rangfolge Gebiete aus, die
      // spaeter still herausfallen — die Weltbank fuehrt Kaimaninseln und
      // Amerikanische Jungferninseln, unsere Laenderliste nicht. Die Seite
      // verspraeche dann sieben Zeilen und lieferte fuenf.
      const [top] = await getPool().query(
        `SELECT a.country_code
           FROM data_values a
           INNER JOIN data_values b
                   ON b.country_code = a.country_code
                  AND b.indicator_id = a.indicator_id
                  AND b.year = ?
           INNER JOIN data_indicators i ON i.id = a.indicator_id AND i.code = ?
           INNER JOIN data_countries c ON c.iso3 = a.country_code
          WHERE a.year = ?
          ORDER BY (b.value - a.value) ASC
          LIMIT ?`,
        [JAHR_MAX, code, seit, anzahl - 1],
      );
      laender = top.map((r) => r.country_code);
      if (!laender.includes("DEU")) laender.push("DEU");
    }

    const platzhalter = laender.map(() => "?").join(",");
    const [rows] = await getPool().query(
      `SELECT v.country_code, c.name_de, c.name_en, v.year, v.value
         FROM data_values v
         INNER JOIN data_indicators i ON i.id = v.indicator_id AND i.code = ?
         INNER JOIN data_countries c ON c.iso3 = v.country_code
        WHERE v.country_code IN (${platzhalter})
        ORDER BY v.country_code, v.year`,
      [code, ...laender],
    );

    const proLand = new Map();
    for (const r of rows) {
      let e = proLand.get(r.country_code);
      if (!e) {
        e = {
          iso3: r.country_code,
          name_de: r.name_de,
          name_en: r.name_en,
          jahre: [],
          werte: [],
        };
        proLand.set(r.country_code, e);
      }
      e.jahre.push(Number(r.year));
      e.werte.push(Number(r.value));
    }

    // Reihenfolge der Anfrage beibehalten — sie traegt die Aussage
    const reihen = laender.map((iso) => proLand.get(iso)).filter(Boolean);
    for (const r of reihen) {
      const iVon = r.jahre.indexOf(seit);
      const letzter = r.werte[r.werte.length - 1];
      // Veraenderung im Bezugsfenster — dasselbe, nach dem sortiert wurde
      r.veraenderung =
        iVon >= 0 ? Number((letzter - r.werte[iVon]).toFixed(2)) : null;
      // Die Streifen zeigen weiterhin die volle Reihe; der Langzeitwert
      // steht daneben, damit beide Lesarten sichtbar bleiben.
      r.veraenderung_gesamt =
        r.werte.length > 1
          ? Number((letzter - r.werte[0]).toFixed(2))
          : null;
    }

    res.json({ dimension: code, seit, laender: reihen });
  }),
);

/**
 * Veränderung aller sechs Dimensionen zwischen zwei Jahren.
 *
 * Das ist die Grundlage der "Handschriften": zwei Laender koennen denselben
 * Rueckgang bei der Mitsprache haben und sich in allem anderen gegenlaeufig
 * entwickeln.
 */
router.get(
  "/governance/veraenderung",
  asyncHandler(async (req, res) => {
    const von = parseJahr(req.query.von, 2013);
    const bis = parseJahr(req.query.bis, JAHR_MAX);
    if (von >= bis) {
      res.status(400).json({ error: "von muss vor bis liegen" });
      return;
    }
    const gewaehlt = parseLaender(req.query.laender);

    const filter = gewaehlt
      ? ` AND a.country_code IN (${gewaehlt.map(() => "?").join(",")})`
      : "";
    // Reihenfolge der Platzhalter: b.year, a.year, dann der Länderfilter
    const params = [bis, von, ...(gewaehlt ?? [])];

    const [rows] = await getPool().query(
      `SELECT a.country_code, c.name_de, c.name_en, i.code,
              a.value AS wert_von, b.value AS wert_bis
         FROM data_values a
         INNER JOIN data_values b
                 ON b.country_code = a.country_code
                AND b.indicator_id = a.indicator_id
                AND b.year = ?
         INNER JOIN data_indicators i
                 ON i.id = a.indicator_id AND i.category = 'governance'
         INNER JOIN data_countries c ON c.iso3 = a.country_code
        WHERE a.year = ?${filter}`,
      params,
    );

    const proLand = new Map();
    for (const r of rows) {
      let e = proLand.get(r.country_code);
      if (!e) {
        e = {
          iso3: r.country_code,
          name_de: r.name_de,
          name_en: r.name_en,
          dimensionen: {},
        };
        proLand.set(r.country_code, e);
      }
      e.dimensionen[r.code] = {
        von: Number(r.wert_von),
        bis: Number(r.wert_bis),
        delta: Number((r.wert_bis - r.wert_von).toFixed(2)),
      };
    }

    const laender = [...proLand.values()].filter(
      (l) => Object.keys(l.dimensionen).length === DIMENSIONEN.length,
    );
    // Nach Rueckgang bei Mitsprache sortiert: das ist die Leitdimension
    laender.sort(
      (a, b) => a.dimensionen["VA.EST"].delta - b.dimensionen["VA.EST"].delta,
    );

    res.json({ von, bis, reihenfolge: DIMENSIONEN, laender });
  }),
);

/**
 * Handelsspiegel: Wie verteilt sich der deutsche Handel auf Laender nach
 * ihrer Regierungsfuehrung?
 *
 * Verbindet zwei Bestaende, die hier ohnehin liegen — Handelsstroeme und
 * Governance-Werte, verknuepfbar ueber iso3.
 *
 * Bewusst in Baendern statt mit einer Schwelle. Eine Aussage wie "ein Fuenftel
 * der Importe stammt aus Laendern unter dem Weltmittel" haengt sonst an
 * Grenzfaellen: China liegt bei der Rechtsstaatlichkeit auf -0,04, also
 * praktisch auf dem Mittel, macht aber allein ueber die Haelfte dieses
 * Fuenftels aus. Die Baender zeigen stattdessen die tatsaechliche Verteilung —
 * und dass die deutsche Abhaengigkeit sich auf ein einziges Land konzentriert.
 */
const BAENDER = [
  { id: "sehr_niedrig", bis: -1 },
  { id: "niedrig", bis: 0 },
  { id: "mittel", bis: 1 },
  { id: "hoch", bis: Infinity },
];

function bandFuer(wert) {
  return BAENDER.find((b) => wert < b.bis)?.id ?? "hoch";
}

router.get(
  "/governance/handel",
  asyncHandler(async (req, res) => {
    const code = String(req.query.dimension || "RL.EST").toUpperCase();
    if (!DIM_SET.has(code)) {
      res.status(400).json({ error: "dimension ungültig" });
      return;
    }
    const land = String(req.query.land || "DEU").toUpperCase();
    if (!ISO_RE.test(land)) {
      res.status(400).json({ error: "land ungültig" });
      return;
    }
    const richtung = req.query.richtung === "export" ? "export" : "import";
    // Bewertungsjahr konstant halten: sonst mischte sich die Veraenderung der
    // Handelsstroeme mit der Veraenderung der Bewertung.
    const bewertungsjahr = JAHR_MAX;

    const [rows] = await getPool().query(
      `SELECT t.year, v.value AS bewertung, SUM(t.value_usd) AS wert
         FROM trade_flows_v2 t
         INNER JOIN data_values v
                 ON v.country_code = t.partner_iso3 AND v.year = ?
         INNER JOIN data_indicators i ON i.id = v.indicator_id AND i.code = ?
        WHERE t.reporter_iso3 = ? AND t.flow = ? AND t.hs_section = 'TOTAL'
        GROUP BY t.year, t.partner_iso3, v.value
        ORDER BY t.year`,
      [bewertungsjahr, code, land, richtung],
    );

    const proJahr = new Map();
    for (const r of rows) {
      const jahr = Number(r.year);
      let e = proJahr.get(jahr);
      if (!e) {
        e = { jahr, gesamt_usd: 0, baender: {} };
        for (const b of BAENDER) e.baender[b.id] = 0;
        proJahr.set(jahr, e);
      }
      const wert = Number(r.wert);
      e.gesamt_usd += wert;
      e.baender[bandFuer(Number(r.bewertung))] += wert;
    }

    const jahre = [...proJahr.values()]
      .sort((a, b) => a.jahr - b.jahr)
      .map((e) => ({
        jahr: e.jahr,
        gesamt_usd: e.gesamt_usd,
        baender: Object.fromEntries(
          BAENDER.map((b) => [
            b.id,
            {
              usd: e.baender[b.id],
              prozent: Number(((e.baender[b.id] / e.gesamt_usd) * 100).toFixed(1)),
            },
          ]),
        ),
      }));

    // Groesste Partner unterhalb des Weltmittels im juengsten Handelsjahr.
    // Zeigt, worauf sich das Band stuetzt — meist auf sehr wenige Laender.
    const [partner] = await getPool().query(
      `SELECT t.partner_iso3 AS iso3, c.name_de, c.name_en,
              SUM(t.value_usd) AS wert, ROUND(v.value, 2) AS bewertung
         FROM trade_flows_v2 t
         INNER JOIN data_values v
                 ON v.country_code = t.partner_iso3 AND v.year = ?
         INNER JOIN data_indicators i ON i.id = v.indicator_id AND i.code = ?
         INNER JOIN data_countries c ON c.iso3 = t.partner_iso3
        WHERE t.reporter_iso3 = ? AND t.flow = ? AND t.hs_section = 'TOTAL'
          AND t.year = (SELECT MAX(year) FROM trade_flows_v2 WHERE reporter_iso3 = ?)
          AND v.value < 0
        GROUP BY t.partner_iso3, v.value
        ORDER BY wert DESC
        LIMIT 8`,
      [bewertungsjahr, code, land, richtung, land],
    );

    res.json({
      land,
      richtung,
      dimension: code,
      bewertungsjahr,
      baender: BAENDER.map((b) => b.id),
      jahre,
      groesste_partner_unter_mittel: partner.map((p) => ({
        iso3: p.iso3,
        name_de: p.name_de,
        name_en: p.name_en,
        wert_usd: Number(p.wert),
        bewertung: Number(p.bewertung),
        band: bandFuer(Number(p.bewertung)),
      })),
    });
  }),
);

module.exports = router;
