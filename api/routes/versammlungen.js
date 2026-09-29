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
 * Zaehlweise (Migration 017): eine Zeile ist ein Versammlungstag. Serien
 * (taegliche Mahnwache, woechentliche Kundgebung) teilen eine `serie_id`.
 *   versammlungstage  = Zeilen
 *   versammlungen     = verschiedene Serien + Einzeltermine
 *
 * `erfassung`: 'liste' stammt aus einer vollstaendigen amtlichen Liste,
 * 'bericht' nur aus einer Meldung. Beides wird nie zusammengezaehlt; ohne
 * Filter zaehlt /stats nur 'liste'.
 *
 * Personenbezogene Daten gibt es in den Tabellen nicht (siehe Migration 016).
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler, ValidationError } = require("../lib/errors");
const { parsePagination, parseIntParam } = require("../lib/validate");

// laufend: taeglich abgerufene Liste (Archiv). Die anderen sind einmalig
// importierte historische Datensaetze (scripts/import_versammlungen_altdaten.py);
// Zeitraeume, Lizenz und Hinweise liefert `abdeckung` in /stats.
const QUELLEN = Object.freeze({
  polizei_berlin: {
    name: "Polizei Berlin, Versammlungsbehörde",
    url: "https://www.berlin.de/polizei/service/versammlungsbehoerde/versammlungen-aufzuege/",
    rechtsgrundlage: "§ 12 VersFG BE",
    laufend: true,
  },
  pomerenke_berlin: {
    name: "The German Protest Registrations Dataset (David Pomerenke, 2023)",
    url: "https://doi.org/10.5281/zenodo.10094245",
    lizenz: "CC BY-SA 4.0",
    laufend: false,
  },
  fds_berlin_2023: {
    name: "Polizei Berlin, IFG-Auskunft via FragDenStaat",
    url: "https://fragdenstaat.de/anfrage/versammlungen-2023-und-2024/",
    laufend: false,
  },
});
const LAUFENDE_QUELLEN = Object.keys(QUELLEN).filter((q) => QUELLEN[q].laufend);

const HINWEIS =
  "Erfasst sind angezeigte Versammlungen. Spontanversammlungen fehlen, " +
  "und ob eine Versammlung stattgefunden hat, geht aus der laufenden Liste nicht hervor. " +
  "Historische Daten (2018 bis April 2024) stammen aus IFG-Auskünften; wo die Polizei " +
  "eine Teilnehmendenzahl festgestellt hat, gilt die Versammlung als durchgeführt.";

const ZAEHLWEISE =
  "Eine Serie (etwa eine tägliche Mahnwache) zählt als eine Versammlung, " +
  "jeder ihrer Termine als ein Versammlungstag.";

// 'unbekannt' = Quellen ohne Ort/Strecke (typ IS NULL)
const TYPEN = new Set(["kundgebung", "aufzug", "unbekannt"]);
const STATUS = new Set(["angezeigt", "vergangen", "vor_termin_entfernt", "stattgefunden"]);
const ERFASSUNG = new Set(["liste", "bericht"]);
const KATEGORIE_RE = /^[a-z_]{1,32}$/;
const DATUM_RE = /^\d{4}-\d{2}-\d{2}$/;
const SERIE_RE = /^[0-9a-f]{40}$/;

// Schluessel einer Versammlung: Serie oder Einzeltermin
const VERSAMMLUNG_KEY = "COALESCE(v.serie_id, CONCAT('e', v.id))";

// Datum/Zeit als String aus der DB holen: vermeidet Zeitzonen-Verschiebung
// beim Umweg ueber JS-Date.
const SELECT_FELDER = `
  v.id, v.quelle, v.land, v.stadt,
  DATE_FORMAT(v.datum, '%Y-%m-%d') AS datum,
  TIME_FORMAT(v.von, '%H:%i') AS von,
  TIME_FORMAT(v.bis, '%H:%i') AS bis,
  v.ganztaegig, v.thema, v.plz, v.ort, v.aufzugsstrecke, v.typ, v.kategorie,
  v.status, v.erfassung, v.serie_id,
  DATE_FORMAT(v.serie_von, '%Y-%m-%d') AS serie_von,
  DATE_FORMAT(v.serie_bis, '%Y-%m-%d') AS serie_bis,
  v.serie_rhythmus`;

function mapVersammlung(r) {
  return {
    id: r.id,
    quelle: r.quelle,
    land: r.land,
    stadt: r.stadt,
    datum: r.datum,
    von: r.von,
    bis: r.bis,
    ganztaegig: Number(r.ganztaegig) === 1,
    thema: r.thema,
    plz: r.plz,
    ort: r.ort,
    aufzugsstrecke: r.aufzugsstrecke,
    typ: r.typ,
    kategorie: r.kategorie,
    status: r.status,
    erfassung: r.erfassung,
    serie: r.serie_id
      ? {
          id: r.serie_id,
          von: r.serie_von,
          bis: r.serie_bis,
          rhythmus: r.serie_rhythmus,
          // nur bei gebuendelter Liste bzw. Detailansicht gesetzt
          ...(r.serie_termine != null && { termine: Number(r.serie_termine) }),
        }
      : null,
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

/**
 * Gemeinsame Filter fuer Liste und Stats. `status` wird nur geparst, nicht
 * angewendet, weil Liste und Stats unterschiedliche Standards haben.
 */
function buildFilter(query) {
  const where = [];
  const params = [];

  const von = parseDatum(query.von, "von");
  const bis = parseDatum(query.bis, "bis");
  if (von && bis && von > bis) throw new ValidationError("von liegt nach bis");
  if (von) {
    where.push("v.datum >= ?");
    params.push(von);
  }
  if (bis) {
    where.push("v.datum <= ?");
    params.push(bis);
  }

  const typ = parseSet(query.typ, TYPEN, "typ");
  if (typ === "unbekannt") {
    where.push("v.typ IS NULL");
  } else if (typ) {
    where.push("v.typ = ?");
    params.push(typ);
  }
  const erfassung = parseSet(query.erfassung, ERFASSUNG, "erfassung");
  if (erfassung) {
    where.push("v.erfassung = ?");
    params.push(erfassung);
  }
  const kategorie = String(query.kategorie ?? "").trim().toLowerCase();
  if (kategorie) {
    if (!KATEGORIE_RE.test(kategorie)) throw new ValidationError("kategorie ungültig");
    if (kategorie === "unklassifiziert") {
      where.push("v.kategorie IS NULL");
    } else {
      where.push("v.kategorie = ?");
      params.push(kategorie);
    }
  }
  const land = String(query.land ?? "").trim().toUpperCase();
  if (land) {
    if (!/^[A-Z]{2}$/.test(land)) throw new ValidationError("land ungültig");
    where.push("v.land = ?");
    params.push(land);
  }
  const serie = String(query.serie ?? "").trim().toLowerCase();
  if (serie) {
    if (!SERIE_RE.test(serie)) throw new ValidationError("serie ungültig");
    where.push("v.serie_id = ?");
    params.push(serie);
  }
  const q = String(query.q ?? "").trim();
  if (q) {
    where.push("(v.thema LIKE ? OR v.ort LIKE ?)");
    params.push(`%${q}%`, `%${q}%`);
  }

  const status = parseSet(query.status, STATUS, "status");
  return { where, params, von, bis, erfassung, status };
}

function whereSql(where) {
  return where.length ? `WHERE ${where.join(" AND ")}` : "";
}

/**
 * Liste mit Filtern. Standard: ab heute aufsteigend (kommende zuerst).
 *
 * `buendeln=1`: je Serie nur der erste Termin im Filterzeitraum, mit
 * `serie.termine` = Anzahl der Termine im Zeitraum. Einzeltermine unveraendert.
 */
router.get(
  "/versammlungen",
  asyncHandler(async (req, res) => {
    const { limit, offset } = parsePagination(req.query, { defLimit: 50, maxLimit: 500 });
    const f = buildFilter(req.query);
    if (!f.von && !f.bis) f.where.push("v.datum >= CURDATE()");
    if (f.status) {
      f.where.push("v.status = ?");
      f.params.push(f.status);
    }
    const buendeln = ["1", "true"].includes(String(req.query.buendeln ?? "").toLowerCase());

    // Vergangenheit absteigend, Zukunft aufsteigend lesen sich jeweils natuerlich
    const absteigend = String(req.query.sort ?? "").toLowerCase() === "desc";
    const richtung = absteigend ? "DESC" : "ASC";
    const order = `datum ${richtung}, von ${richtung}, id`;

    const pool = getPool();
    let total;
    let rows;
    if (buendeln) {
      const inner = `
        SELECT ${SELECT_FELDER},
               ROW_NUMBER() OVER (PARTITION BY ${VERSAMMLUNG_KEY}
                                  ORDER BY v.datum ${richtung}, v.von ${richtung}) AS rn,
               COUNT(*) OVER (PARTITION BY ${VERSAMMLUNG_KEY}) AS serie_termine
          FROM versammlungen v ${whereSql(f.where)}`;
      [[{ total }]] = await pool.query(
        `SELECT COUNT(DISTINCT ${VERSAMMLUNG_KEY}) AS total FROM versammlungen v ${whereSql(f.where)}`,
        f.params,
      );
      [rows] = await pool.query(
        `SELECT * FROM (${inner}) x WHERE x.rn = 1 ORDER BY ${order} LIMIT ? OFFSET ?`,
        [...f.params, limit, offset],
      );
    } else {
      [[{ total }]] = await pool.query(
        `SELECT COUNT(*) AS total FROM versammlungen v ${whereSql(f.where)}`,
        f.params,
      );
      [rows] = await pool.query(
        `SELECT ${SELECT_FELDER} FROM versammlungen v ${whereSql(f.where)}
          ORDER BY v.${order.replace(/, /g, ", v.")} LIMIT ? OFFSET ?`,
        [...f.params, limit, offset],
      );
    }

    res.json({
      total: Number(total) || 0,
      limit,
      offset,
      gebuendelt: buendeln,
      items: rows.map(mapVersammlung),
      quellen: QUELLEN,
      hinweis: HINWEIS,
    });
  }),
);

/**
 * Kennzahlen fuer Vorspann, Zeitverlauf, Themen und Wochentag x Stunde.
 * Nimmt dieselben Filter wie die Liste. Ohne `status` zaehlen vor dem
 * Termin entfernte Versammlungen nicht mit (Ausnahme: der Zaehler
 * `vor_termin_entfernt` im Kopf), ohne `erfassung` nur vollstaendige Listen.
 */
router.get(
  "/versammlungen/stats",
  asyncHandler(async (req, res) => {
    const f = buildFilter(req.query);
    if (!f.erfassung) {
      f.where.push("v.erfassung = 'liste'");
    }
    const kopfWhere = [...f.where];
    const kopfParams = [...f.params];
    if (f.status) {
      kopfWhere.push("v.status = ?");
      kopfParams.push(f.status);
      f.where.push("v.status = ?");
      f.params.push(f.status);
    } else {
      f.where.push("v.status <> 'vor_termin_entfernt'");
    }
    const w = whereSql(f.where);
    const p = f.params;
    const pool = getPool();

    const [
      [[kopf]],
      [[abruf]],
      [kategorien],
      [monate],
      [wochen],
      [kategorieMonat],
      [wochentagStunde],
      [abdeckung],
    ] = await Promise.all([
      pool.query(
        `SELECT SUM(v.status <> 'vor_termin_entfernt') AS versammlungstage,
                COUNT(DISTINCT IF(v.status <> 'vor_termin_entfernt', ${VERSAMMLUNG_KEY}, NULL))
                  AS versammlungen,
                COUNT(DISTINCT IF(v.status <> 'vor_termin_entfernt', v.serie_id, NULL)) AS serien,
                SUM(v.datum >= CURDATE() AND v.status = 'angezeigt') AS kommend,
                SUM(v.status = 'vor_termin_entfernt') AS vor_termin_entfernt,
                SUM(v.typ = 'aufzug' AND v.status <> 'vor_termin_entfernt') AS aufzuege,
                SUM(v.ganztaegig = 1 AND v.status <> 'vor_termin_entfernt') AS ganztaegig,
                DATE_FORMAT(MIN(v.erstmals_gesehen), '%Y-%m-%d') AS erfasst_seit,
                DATE_FORMAT(MIN(v.datum), '%Y-%m-%d') AS erster_termin,
                DATE_FORMAT(MAX(v.datum), '%Y-%m-%d') AS letzter_termin
           FROM versammlungen v ${whereSql(kopfWhere)}`,
        kopfParams,
      ),
      pool.query(
        `SELECT DATE_FORMAT(MAX(abgerufen), '%Y-%m-%dT%H:%i:%sZ') AS letzter_abruf,
                COUNT(*) AS snapshots
           FROM versammlungen_rohdaten
          WHERE quelle IN (?)`,
        [LAUFENDE_QUELLEN],
      ),
      pool.query(
        `SELECT COALESCE(v.kategorie, 'unklassifiziert') AS kategorie,
                COUNT(*) AS versammlungstage,
                COUNT(DISTINCT ${VERSAMMLUNG_KEY}) AS versammlungen
           FROM versammlungen v ${w}
          GROUP BY 1
          ORDER BY versammlungstage DESC`,
        p,
      ),
      pool.query(
        `SELECT DATE_FORMAT(v.datum, '%Y-%m') AS monat,
                COUNT(*) AS versammlungstage,
                COUNT(DISTINCT ${VERSAMMLUNG_KEY}) AS versammlungen
           FROM versammlungen v ${w}
          GROUP BY 1
          ORDER BY 1`,
        p,
      ),
      // Woche = Montag der ISO-Woche
      pool.query(
        `SELECT DATE_FORMAT(DATE_SUB(v.datum, INTERVAL WEEKDAY(v.datum) DAY), '%Y-%m-%d') AS woche,
                COUNT(*) AS versammlungstage,
                COUNT(DISTINCT ${VERSAMMLUNG_KEY}) AS versammlungen
           FROM versammlungen v ${w}
          GROUP BY 1
          ORDER BY 1`,
        p,
      ),
      pool.query(
        `SELECT DATE_FORMAT(v.datum, '%Y-%m') AS monat,
                COALESCE(v.kategorie, 'unklassifiziert') AS kategorie,
                COUNT(*) AS versammlungstage,
                COUNT(DISTINCT ${VERSAMMLUNG_KEY}) AS versammlungen
           FROM versammlungen v ${w}
          GROUP BY 1, 2
          ORDER BY 1, 3 DESC`,
        p,
      ),
      // Ganztaegige und Termine ohne Beginn haben keine sinnvolle Stunde
      pool.query(
        `SELECT WEEKDAY(v.datum) AS wochentag, HOUR(v.von) AS stunde, COUNT(*) AS anzahl
           FROM versammlungen v ${w}${w ? " AND" : " WHERE"} v.ganztaegig = 0 AND v.von IS NOT NULL
          GROUP BY 1, 2
          ORDER BY 1, 2`,
        p,
      ),
      pool.query(
        `SELECT quelle, land, stadt, DATE_FORMAT(von, '%Y-%m-%d') AS von,
                DATE_FORMAT(bis, '%Y-%m-%d') AS bis, name, url, lizenz, hinweis
           FROM versammlungen_abdeckung
          ORDER BY stadt, von`,
      ),
    ]);

    const zaehler = (r) => ({
      versammlungstage: Number(r.versammlungstage),
      versammlungen: Number(r.versammlungen),
    });
    const proWochentag = new Map();
    const proStunde = new Map();
    for (const r of wochentagStunde) {
      const wt = Number(r.wochentag);
      const h = Number(r.stunde);
      const n = Number(r.anzahl);
      proWochentag.set(wt, (proWochentag.get(wt) ?? 0) + n);
      proStunde.set(h, (proStunde.get(h) ?? 0) + n);
    }

    res.json({
      zeitraum: { von: f.von, bis: f.bis },
      versammlungen: Number(kopf.versammlungen) || 0,
      versammlungstage: Number(kopf.versammlungstage) || 0,
      serien: Number(kopf.serien) || 0,
      kommend: Number(kopf.kommend) || 0,
      vor_termin_entfernt: Number(kopf.vor_termin_entfernt) || 0,
      aufzuege: Number(kopf.aufzuege) || 0,
      ganztaegig: Number(kopf.ganztaegig) || 0,
      erfasst_seit: kopf.erfasst_seit ?? null,
      erster_termin: kopf.erster_termin ?? null,
      letzter_termin: kopf.letzter_termin ?? null,
      letzter_abruf: abruf.letzter_abruf ?? null,
      snapshots: Number(abruf.snapshots) || 0,
      kategorien: kategorien.map((r) => ({ kategorie: r.kategorie, ...zaehler(r) })),
      pro_monat: monate.map((r) => ({ monat: r.monat, ...zaehler(r) })),
      pro_woche: wochen.map((r) => ({ woche: r.woche, ...zaehler(r) })),
      kategorie_pro_monat: kategorieMonat.map((r) => ({
        monat: r.monat,
        kategorie: r.kategorie,
        ...zaehler(r),
      })),
      // Die folgenden drei zaehlen Versammlungstage mit festem Beginn
      // 0 = Montag (MariaDB WEEKDAY)
      pro_wochentag: [...proWochentag].sort((a, b) => a[0] - b[0])
        .map(([wochentag, anzahl]) => ({ wochentag, anzahl })),
      pro_stunde: [...proStunde].sort((a, b) => a[0] - b[0])
        .map(([stunde, anzahl]) => ({ stunde, anzahl })),
      wochentag_stunde: wochentagStunde.map((r) => ({
        wochentag: Number(r.wochentag),
        stunde: Number(r.stunde),
        anzahl: Number(r.anzahl),
      })),
      // Zeitraeume mit Daten; alles dazwischen ist Luecke, nicht "null Versammlungen"
      abdeckung,
      quellen: QUELLEN,
      hinweis: HINWEIS,
      zaehlweise: ZAEHLWEISE,
    });
  }),
);

/** Einzelne Versammlung mit Serie und allen genannten Teilnehmerzahlen. */
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
        ORDER BY FIELD(quelle_typ, 'angemeldet', 'veranstalter', 'polizei', 'presse', 'schaetzung'), genannt_am`,
      [id],
    );
    let termine = null;
    if (row.serie_id) {
      const [t] = await pool.query(
        `SELECT DATE_FORMAT(datum, '%Y-%m-%d') AS datum, status
           FROM versammlungen WHERE serie_id = ? ORDER BY datum`,
        [row.serie_id],
      );
      row.serie_termine = t.length;
      termine = t.map((x) => ({ datum: x.datum, status: x.status }));
    }
    res.json({
      ...mapVersammlung(row),
      serie_termine: termine,
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
