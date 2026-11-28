"use strict";

/**
 * Quellenregister und Datenstand.
 *
 *   /api/quellen  alle Datenquellen mit Herausgeber, Lizenz, Link zum Original
 *                 und Stand. Die App baut daraus Quellenzeile, Fundstelle und
 *                 die Seite Quellen, damit keine Angabe von Hand gepflegt wird.
 *   /api/stand    kurz: welche Quelle ist aktuell, welche veraltet. Fuer den
 *                 Stand-Stempel in der App und den Morgen-Check im Brain.
 *
 * Grundlage ist data_sources. scripts/lib/quelle.py schreibt dort nach jedem
 * Abruf last_fetched, last_success und last_error. Der Fehlertext bleibt
 * intern (kann Adressen oder Schluesselnamen enthalten), nach aussen geht nur,
 * ob der letzte Abruf scheiterte.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { frische } = require("../lib/quellen");

const BEREICH_RE = /^[a-z0-9-]{1,30}$/;
const SLUG_RE = /^[a-z0-9_-]{1,50}$/;

const SPALTEN = `id, slug, name, name_de, provider, url, license, license_url, area,
                 note_de, note_en, update_freq, stale_after_hours,
                 last_fetched, last_success, (last_error IS NOT NULL) AS fehler`;

function iso(v) {
  if (v == null) return null;
  const d = v instanceof Date ? v : new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function mapQuelle(r, lang, jetzt) {
  const f = frische(r, jetzt);
  return {
    slug: r.slug,
    name: (lang === "de" && r.name_de) || r.name,
    herausgeber: r.provider,
    url: r.url ?? null,
    lizenz: r.license ?? null,
    lizenz_url: r.license_url ?? null,
    bereich: r.area ?? null,
    hinweis: (lang === "de" ? r.note_de : r.note_en) ?? null,
    takt: r.update_freq ?? null,
    letzter_abruf: iso(r.last_fetched),
    letzter_erfolg: f.letzter_erfolg,
    zustand: f.zustand,
    alter_stunden: f.alter_stunden,
    grenze_stunden: f.grenze_stunden,
    letzter_abruf_fehlgeschlagen: Boolean(Number(r.fehler)),
  };
}

function lang(req) {
  return String(req.query.lang || "").trim().toLowerCase() === "en" ? "en" : "de";
}

// Fuenf Minuten im Browser und in nginx: der Stand aendert sich nur nach
// einem Abruf, und die Stempel sollen keine eigene Last erzeugen.
function puffern(res) {
  res.set("Cache-Control", "public, max-age=300");
}

router.get("/quellen", asyncHandler(async (req, res) => {
  const bereich = String(req.query.bereich || "").trim().toLowerCase();
  if (bereich && !BEREICH_RE.test(bereich)) {
    res.status(400).json({ error: "bereich ungültig" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT ${SPALTEN} FROM data_sources
     WHERE is_active = 1 ${bereich ? "AND area = ?" : ""}
     ORDER BY area, name`,
    bereich ? [bereich] : [],
  );
  const jetzt = new Date();
  const l = lang(req);
  puffern(res);
  res.json({ stand: jetzt.toISOString(), quellen: rows.map((r) => mapQuelle(r, l, jetzt)) });
}));

router.get("/quellen/:slug", asyncHandler(async (req, res) => {
  const slug = String(req.params.slug || "").toLowerCase();
  if (!SLUG_RE.test(slug)) {
    res.status(400).json({ error: "slug ungültig" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT ${SPALTEN} FROM data_sources WHERE slug = ? AND is_active = 1 LIMIT 1`,
    [slug],
  );
  if (!rows.length) {
    res.status(404).json({ error: "Quelle nicht gefunden" });
    return;
  }
  puffern(res);
  res.json(mapQuelle(rows[0], lang(req), new Date()));
}));

router.get("/stand", asyncHandler(async (req, res) => {
  const [rows] = await getPool().query(
    `SELECT slug, area, update_freq, stale_after_hours, last_success,
            (last_error IS NOT NULL) AS fehler
     FROM data_sources WHERE is_active = 1 ORDER BY slug`,
  );
  const jetzt = new Date();
  const quellen = rows.map((r) => {
    const f = frische(r, jetzt);
    return {
      slug: r.slug,
      bereich: r.area ?? null,
      zustand: f.zustand,
      letzter_erfolg: f.letzter_erfolg,
      alter_stunden: f.alter_stunden,
      grenze_stunden: f.grenze_stunden,
      letzter_abruf_fehlgeschlagen: Boolean(Number(r.fehler)),
    };
  });
  puffern(res);
  res.json({
    stand: jetzt.toISOString(),
    veraltet: quellen.filter((q) => q.zustand === "veraltet").map((q) => q.slug),
    quellen,
  });
}));

module.exports = router;
