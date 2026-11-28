"use strict";

/**
 * Strompreise Europa: Day-Ahead-Preise je Gebotszone.
 *
 * Daten von energy-charts.info (Fraunhofer ISE, CC BY 4.0, Daten der
 * Bundesnetzagentur | SMARD.de), geladen von scripts/fetch_strom.py. Die
 * Namensnennung geht in jeder Antwort mit (Feld `quelle`).
 *
 * Preise sind Boersenpreise in EUR/MWh ohne Steuern, Abgaben und Netzentgelte.
 * `einheit` steht in jeder Antwort, damit keine Zahl ohne Einheit angezeigt
 * wird. ct/kWh = EUR/MWh geteilt durch 10.
 *
 * Tage und Stunden sind Marktzeit (Europe/Berlin), so wie die Auktion laeuft.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

const QUELLE = Object.freeze({
  slug: "energy-charts",
  name: "Energy-Charts (Fraunhofer ISE)",
  daten: "Bundesnetzagentur | SMARD.de",
  url: "https://www.energy-charts.info/",
  lizenz: "CC BY 4.0",
  lizenz_url: "https://creativecommons.org/licenses/by/4.0/",
});
const EINHEIT = "EUR/MWh";
const DATUM_RE = /^\d{4}-\d{2}-\d{2}$/;
const ZONE_RE = /^[A-Z]{2}(-?[A-Z0-9]{1,3})?$/;

function lang(req) {
  return String(req.query.lang || "").trim().toLowerCase() === "en" ? "en" : "de";
}
const r2 = (v) => (v == null ? null : Math.round(Number(v) * 100) / 100);

async function stand(pool) {
  const [rows] = await pool.query("SELECT last_success FROM data_sources WHERE slug = ? LIMIT 1", [QUELLE.slug]);
  const v = rows[0]?.last_success;
  return v ? new Date(v).toISOString() : null;
}

function lokalDatum(d) {
  // DATE kommt als Date in lokaler Zeit des Servers: nicht ueber UTC schieben
  if (d instanceof Date) {
    const p = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
  }
  return formatDate(d);
}

function plusTage(datum, n) {
  const d = new Date(`${datum}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

router.get("/strom/tag", asyncHandler(async (req, res) => {
  const pool = getPool();
  const [[bereich]] = await pool.query("SELECT MIN(liefertag) AS von, MAX(liefertag) AS bis FROM strom_preise");
  const von = lokalDatum(bereich.von);
  const bis = lokalDatum(bereich.bis);
  let datum = String(req.query.datum || "").trim();
  if (datum && !DATUM_RE.test(datum)) {
    res.status(400).json({ error: "datum ungültig" });
    return;
  }
  const l = lang(req);
  const [zonen] = await pool.query(
    `SELECT code, land, ${l === "de" ? "name_de" : "name_en"} AS name, spalte, zeile
     FROM strom_zonen WHERE aktiv = 1 ORDER BY zeile, spalte`,
  );
  if (!bis) {
    res.json({ datum: null, verfuegbar: { von: null, bis: null }, einheit: EINHEIT, zonen: [], stand: await stand(pool), quelle: QUELLE });
    return;
  }
  // Ohne Angabe: der juengste Liefertag (ab mittags ist das morgen)
  if (!datum) datum = bis;
  const vortag = plusTage(datum, -1);
  const [rows] = await pool.query(
    `SELECT liefertag, zone, stunde, SUM(eur_mwh * minuten) / SUM(minuten) AS m, SUM(minuten) AS minuten
     FROM strom_preise WHERE liefertag IN (?, ?) GROUP BY liefertag, zone, stunde`,
    [datum, vortag],
  );
  const heute = new Map();
  const gestern = new Map();
  for (const r of rows) {
    const ziel = lokalDatum(r.liefertag) === datum ? heute : gestern;
    if (!ziel.has(r.zone)) ziel.set(r.zone, []);
    ziel.get(r.zone).push([Number(r.stunde), r2(r.m)]);
  }
  const mittel = (paare) => (paare && paare.length ? r2(paare.reduce((s, p) => s + p[1], 0) / paare.length) : null);
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    datum,
    verfuegbar: { von, bis },
    einheit: EINHEIT,
    zonen: zonen.map((z) => {
      const paare = (heute.get(z.code) || []).sort((a, b) => a[0] - b[0]);
      const stunden = Array.from({ length: 24 }, () => null);
      for (const [h, v] of paare) stunden[h] = v;
      const werte = paare.map((p) => p[1]);
      return {
        code: z.code,
        land: z.land,
        name: z.name,
        spalte: z.spalte,
        zeile: z.zeile,
        stunden,
        mittel: mittel(paare),
        min: werte.length ? Math.min(...werte) : null,
        max: werte.length ? Math.max(...werte) : null,
        vortag_mittel: mittel(gestern.get(z.code)),
      };
    }),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

// Tagesmittel mehrerer Zonen in einem Abruf, fuer den Laendervergleich.
// Hoechstens vier Zonen, hoechstens 400 Tage: bleibt unter 30 KB.
router.get("/strom/verlauf", asyncHandler(async (req, res) => {
  const zonen = [...new Set(String(req.query.zonen || "").toUpperCase().split(",").map((z) => z.trim()).filter(Boolean))];
  if (!zonen.length || zonen.length > 4 || zonen.some((z) => !ZONE_RE.test(z))) {
    res.status(400).json({ error: "zonen ungültig: eine bis vier Zonen, durch Komma getrennt" });
    return;
  }
  const tage = Math.min(400, Math.max(7, Number.parseInt(String(req.query.tage || "90"), 10) || 90));
  const pool = getPool();
  const l = lang(req);
  const [bekannt] = await pool.query(
    `SELECT code, land, ${l === "de" ? "name_de" : "name_en"} AS name FROM strom_zonen WHERE code IN (?) AND aktiv = 1`,
    [zonen],
  );
  if (!bekannt.length) {
    res.status(404).json({ error: "Zone nicht gefunden" });
    return;
  }
  const codes = bekannt.map((z) => z.code);
  const [rows] = await pool.query(
    `SELECT zone, liefertag, SUM(eur_mwh * minuten) / SUM(minuten) AS mittel
     FROM strom_preise
     WHERE zone IN (?) AND liefertag > (SELECT MAX(liefertag) FROM strom_preise WHERE zone IN (?)) - INTERVAL ? DAY
     GROUP BY zone, liefertag ORDER BY liefertag`,
    [codes, codes, tage],
  );
  const tageListe = [...new Set(rows.map((r) => lokalDatum(r.liefertag)))];
  const stelle = new Map(tageListe.map((d, i) => [d, i]));
  const reihen = new Map(codes.map((c) => [c, new Array(tageListe.length).fill(null)]));
  for (const r of rows) reihen.get(r.zone)[stelle.get(lokalDatum(r.liefertag))] = r2(r.mittel);
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    einheit: EINHEIT,
    tage: tageListe,
    // Reihenfolge wie angefragt
    zonen: zonen.filter((z) => reihen.has(z)).map((z) => ({ ...bekannt.find((b) => b.code === z), werte: reihen.get(z) })),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

router.get("/strom/zone/:code", asyncHandler(async (req, res) => {
  const code = String(req.params.code || "").toUpperCase();
  if (!ZONE_RE.test(code)) {
    res.status(400).json({ error: "zone ungültig" });
    return;
  }
  const tage = Math.min(400, Math.max(7, Number.parseInt(String(req.query.tage || "90"), 10) || 90));
  const pool = getPool();
  const l = lang(req);
  const [zone] = await pool.query(
    `SELECT code, land, ${l === "de" ? "name_de" : "name_en"} AS name FROM strom_zonen WHERE code = ? AND aktiv = 1 LIMIT 1`,
    [code],
  );
  if (!zone.length) {
    res.status(404).json({ error: "Zone nicht gefunden" });
    return;
  }
  const [rows] = await pool.query(
    `SELECT liefertag, SUM(eur_mwh * minuten) / SUM(minuten) AS mittel, MIN(eur_mwh) AS min, MAX(eur_mwh) AS max,
            SUM(IF(eur_mwh < 0, minuten, 0)) / 60 AS negativ_stunden
     FROM strom_preise
     WHERE zone = ? AND liefertag > (SELECT MAX(liefertag) FROM strom_preise WHERE zone = ?) - INTERVAL ? DAY
     GROUP BY liefertag ORDER BY liefertag`,
    [code, code, tage],
  );
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    zone: zone[0],
    einheit: EINHEIT,
    tage: rows.map((r) => ({
      datum: lokalDatum(r.liefertag),
      mittel: r2(r.mittel),
      min: r2(r.min),
      max: r2(r.max),
      negativ_stunden: Math.round(Number(r.negativ_stunden) * 10) / 10,
    })),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

router.get("/strom/kennzahlen", asyncHandler(async (req, res) => {
  const pool = getPool();
  const [rows] = await pool.query(
    "SELECT inhalt, erzeugt_am FROM schnappschuesse WHERE schluessel = 'strom_kennzahlen' LIMIT 1",
  );
  let inhalt = null;
  if (rows.length) {
    try {
      inhalt = typeof rows[0].inhalt === "string" ? JSON.parse(rows[0].inhalt) : rows[0].inhalt;
    } catch {
      inhalt = null;
    }
  }
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    einheit: EINHEIT,
    kennzahlen: inhalt,
    berechnet: rows.length ? new Date(rows[0].erzeugt_am).toISOString() : null,
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

module.exports = router;
