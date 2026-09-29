"use strict";

/**
 * Versammlungen (Demonstrations-Tracker).
 *
 * Phase 1: nur Berlin (Polizei Berlin, § 12 VersFG BE). Die Daten sind
 * *angezeigte* Versammlungen, keine gezaehlten. `status` sagt, was wir
 * wissen, und nicht mehr: ob eine Versammlung stattgefunden hat, steht in
 * keiner Quelle. Deshalb wird `hinweis` bei jeder Antwort mitgeliefert,
 * damit das Frontend die Einschraenkung nicht vergessen kann.
 *
 * Personenbezogene Daten gibt es in den Tabellen nicht (siehe Migration 016).
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler, ValidationError } = require("../lib/errors");
const { parsePagination, parseIntParam } = require("../lib/validate");

const QUELLEN = Object.freeze({
  polizei_berlin: {
    name: "Polizei Berlin, Versammlungsbehörde",
    url: "https://www.berlin.de/polizei/service/versammlungsbehoerde/versammlungen-aufzuege/",
    rechtsgrundlage: "§ 12 VersFG BE",
  },
});

const HINWEIS =
  "Erfasst sind angezeigte Versammlungen. Spontanversammlungen fehlen, " +
  "und ob eine Versammlung stattgefunden hat, geht aus den Quellen nicht hervor.";

const TYPEN = new Set(["kundgebung", "aufzug"]);
const STATUS = new Set(["angezeigt", "vergangen", "vor_termin_entfernt"]);
const KATEGORIE_RE = /^[a-z_]{1,32}$/;
const DATUM_RE = /^\d{4}-\d{2}-\d{2}$/;

// Datum/Zeit als String aus der DB holen: vermeidet Zeitzonen-Verschiebung
// beim Umweg ueber JS-Date.
const SELECT_FELDER = `
  v.id, v.quelle, v.land, v.stadt,
  DATE_FORMAT(v.datum, '%Y-%m-%d') AS datum,
  TIME_FORMAT(v.von, '%H:%i') AS von,
  TIME_FORMAT(v.bis, '%H:%i') AS bis,
  v.thema, v.plz, v.ort, v.aufzugsstrecke, v.typ, v.kategorie, v.status`;

function mapVersammlung(r) {
  return {
    id: r.id,
    quelle: r.quelle,
    land: r.land,
    stadt: r.stadt,
    datum: r.datum,
    von: r.von,
    bis: r.bis,
    thema: r.thema,
    plz: r.plz,
    ort: r.ort,
    aufzugsstrecke: r.aufzugsstrecke,
    typ: r.typ,
    kategorie: r.kategorie,
    status: r.status,
  };
}

function parseDatum(value, name) {
  const s = String(value ?? "").trim();
  if (!s) return null;
  // Rundreise ueber Date faengt Werte wie 2026-13-01 oder 2026-02-30 ab
  const d = new Date(`${s}T00:00:00Z`);
  if (!DATUM_RE.test(s) || Number.isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== s) {
    throw new ValidationError(`${name} muss ein gültiges Datum JJJJ-MM-TT sein`);
  }
  return s;
}

function parseSet(value, allowed, name) {
  const s = String(value ?? "").trim().toLowerCase();
  if (!s) return null;
  if (!allowed.has(s)) throw new ValidationError(`${name} ungültig`);
  return s;
}

/** Liste mit Filtern. Standard: ab heute aufsteigend (kommende zuerst). */
router.get(
  "/versammlungen",
  asyncHandler(async (req, res) => {
    const { limit, offset } = parsePagination(req.query, { defLimit: 50, maxLimit: 500 });
    const where = [];
    const params = [];

    const von = parseDatum(req.query.von, "von");
    const bis = parseDatum(req.query.bis, "bis");
    if (von) {
      where.push("v.datum >= ?");
      params.push(von);
    }
    if (bis) {
      where.push("v.datum <= ?");
      params.push(bis);
    }
    if (!von && !bis) where.push("v.datum >= CURDATE()");

    const typ = parseSet(req.query.typ, TYPEN, "typ");
    if (typ) {
      where.push("v.typ = ?");
      params.push(typ);
    }
    const status = parseSet(req.query.status, STATUS, "status");
    if (status) {
      where.push("v.status = ?");
      params.push(status);
    }
    const kategorie = String(req.query.kategorie ?? "").trim().toLowerCase();
    if (kategorie) {
      if (!KATEGORIE_RE.test(kategorie)) throw new ValidationError("kategorie ungültig");
      where.push("v.kategorie = ?");
      params.push(kategorie);
    }
    const land = String(req.query.land ?? "").trim().toUpperCase();
    if (land) {
      if (!/^[A-Z]{2}$/.test(land)) throw new ValidationError("land ungültig");
      where.push("v.land = ?");
      params.push(land);
    }
    const q = String(req.query.q ?? "").trim();
    if (q) {
      where.push("(v.thema LIKE ? OR v.ort LIKE ?)");
      params.push(`%${q}%`, `%${q}%`);
    }

    // Vergangenheit absteigend, Zukunft aufsteigend lesen sich jeweils natuerlich
    const absteigend = String(req.query.sort ?? "").toLowerCase() === "desc";
    const order = absteigend ? "v.datum DESC, v.von DESC" : "v.datum ASC, v.von ASC";
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const pool = getPool();
    const [[{ total }]] = await pool.query(
      `SELECT COUNT(*) AS total FROM versammlungen v ${whereSql}`,
      params,
    );
    const [rows] = await pool.query(
      `SELECT ${SELECT_FELDER} FROM versammlungen v ${whereSql}
        ORDER BY ${order}, v.id LIMIT ? OFFSET ?`,
      [...params, limit, offset],
    );

    res.json({
      total: Number(total) || 0,
      limit,
      offset,
      items: rows.map(mapVersammlung),
      quellen: QUELLEN,
      hinweis: HINWEIS,
    });
  }),
);

/** Kennzahlen fuer Kopfzeile, Themenverteilung und Zeitreihe. */
router.get(
  "/versammlungen/stats",
  asyncHandler(async (req, res) => {
    const pool = getPool();

    const [[kopf]] = await pool.query(
      `SELECT COUNT(*) AS gesamt,
              SUM(datum >= CURDATE() AND status = 'angezeigt') AS kommend,
              SUM(status = 'vor_termin_entfernt') AS vor_termin_entfernt,
              SUM(typ = 'aufzug') AS aufzuege,
              DATE_FORMAT(MIN(erstmals_gesehen), '%Y-%m-%d') AS erfasst_seit
         FROM versammlungen`,
    );
    const [[abruf]] = await pool.query(
      `SELECT DATE_FORMAT(MAX(abgerufen), '%Y-%m-%dT%H:%i:%sZ') AS letzter_abruf,
              COUNT(*) AS snapshots
         FROM versammlungen_rohdaten`,
    );
    const [kategorien] = await pool.query(
      `SELECT COALESCE(kategorie, 'unklassifiziert') AS kategorie, COUNT(*) AS anzahl
         FROM versammlungen
        WHERE status <> 'vor_termin_entfernt'
        GROUP BY kategorie
        ORDER BY anzahl DESC`,
    );
    // Abgesagte zaehlen nicht mit: die Zeitreihe soll zeigen, was angezeigt
    // blieb, nicht was je auf der Liste stand.
    const [monate] = await pool.query(
      `SELECT DATE_FORMAT(datum, '%Y-%m') AS monat, COUNT(*) AS anzahl
         FROM versammlungen
        WHERE status <> 'vor_termin_entfernt'
          AND datum >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)
        GROUP BY monat
        ORDER BY monat`,
    );
    const [wochentage] = await pool.query(
      `SELECT WEEKDAY(datum) AS wochentag, COUNT(*) AS anzahl
         FROM versammlungen
        WHERE status <> 'vor_termin_entfernt'
        GROUP BY wochentag
        ORDER BY wochentag`,
    );

    res.json({
      gesamt: Number(kopf.gesamt) || 0,
      kommend: Number(kopf.kommend) || 0,
      vor_termin_entfernt: Number(kopf.vor_termin_entfernt) || 0,
      aufzuege: Number(kopf.aufzuege) || 0,
      erfasst_seit: kopf.erfasst_seit ?? null,
      letzter_abruf: abruf.letzter_abruf ?? null,
      snapshots: Number(abruf.snapshots) || 0,
      kategorien: kategorien.map((r) => ({ kategorie: r.kategorie, anzahl: Number(r.anzahl) })),
      pro_monat: monate.map((r) => ({ monat: r.monat, anzahl: Number(r.anzahl) })),
      // 0 = Montag (MariaDB WEEKDAY)
      pro_wochentag: wochentage.map((r) => ({
        wochentag: Number(r.wochentag),
        anzahl: Number(r.anzahl),
      })),
      quellen: QUELLEN,
      hinweis: HINWEIS,
    });
  }),
);

/** Einzelne Versammlung mit allen genannten Teilnehmerzahlen. */
router.get(
  "/versammlungen/:id",
  asyncHandler(async (req, res) => {
    const id = parseIntParam(req.params.id, "id");
    const pool = getPool();
    const [[row]] = await pool.query(
      `SELECT ${SELECT_FELDER} FROM versammlungen v WHERE v.id = ?`,
      [id],
    );
    if (!row) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    const [zahlen] = await pool.query(
      `SELECT quelle_typ, wert, wert_bis, quelle_name, quelle_url,
              DATE_FORMAT(genannt_am, '%Y-%m-%dT%H:%i:%s') AS genannt_am
         FROM versammlung_zahlen
        WHERE versammlung_id = ?
        ORDER BY FIELD(quelle_typ, 'veranstalter', 'polizei', 'presse', 'schaetzung'), genannt_am`,
      [id],
    );
    res.json({
      ...mapVersammlung(row),
      teilnehmer: zahlen.map((z) => ({
        quelle_typ: z.quelle_typ,
        wert: Number(z.wert),
        wert_bis: z.wert_bis == null ? null : Number(z.wert_bis),
        quelle_name: z.quelle_name,
        quelle_url: z.quelle_url,
        genannt_am: z.genannt_am,
      })),
      quelle_info: QUELLEN[row.quelle] ?? null,
      hinweis: HINWEIS,
    });
  }),
);

module.exports = router;
