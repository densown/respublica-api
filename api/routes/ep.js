"use strict";

/**
 * EU-Parlament: namentliche Hauptabstimmungen der 10. Wahlperiode.
 *
 * Daten von HowTheyVote.eu (ODbL 1.0), geladen von scripts/fetch_ep.py. Die
 * Namensnennung ist Lizenzbedingung und geht deshalb in jeder Antwort mit
 * (Feld `quelle`), wie bei dawum in routes/umfragen.js.
 *
 * Titel: der Export ist englisch. `lang=de` liefert die deutsche Uebersetzung
 * aus titel_de, wenn es sie gibt, und sagt mit `titel_uebersetzt`, ob sie
 * maschinell entstanden ist. Fehlt sie, kommt der englische Titel.
 *
 * `art` sagt, worueber abgestimmt wurde. Bei `ablehnungsantrag` bedeutet
 * `angenommen`, dass der Text gescheitert ist: das Frontend muss das zeigen.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");

const QUELLE = Object.freeze({
  slug: "howtheyvote",
  name: "HowTheyVote.eu",
  url: "https://howtheyvote.eu/",
  lizenz: "ODbL 1.0",
  lizenz_url: "https://opendatacommons.org/licenses/odbl/1-0/",
});

const LAND_RE = /^[A-Z]{3}$/;
const ERGEBNISSE = new Set(["angenommen", "abgelehnt"]);
const ARTEN = new Set(["text", "einigung", "ablehnungsantrag", "verhandlungsmandat", "dringlichkeit"]);
const POSITIONEN = ["ja", "nein", "enthaltung", "nicht"];

function lang(req) {
  return String(req.query.lang || "").trim().toLowerCase() === "en" ? "en" : "de";
}

function ganzzahl(v, vorgabe, min, max) {
  const n = Number.parseInt(String(v ?? ""), 10);
  if (!Number.isFinite(n)) return vorgabe;
  return Math.min(max, Math.max(min, n));
}

function zeitText(v) {
  if (v == null) return null;
  // DATETIME ohne Zeitzone (Ortszeit der Sitzung): als Text durchreichen,
  // nicht ueber Date nach UTC schieben.
  if (v instanceof Date) {
    const p = (n) => String(n).padStart(2, "0");
    return `${v.getFullYear()}-${p(v.getMonth() + 1)}-${p(v.getDate())}T${p(v.getHours())}:${p(v.getMinutes())}:${p(v.getSeconds())}`;
  }
  return String(v).replace(" ", "T").slice(0, 19);
}

async function stand(pool) {
  const [rows] = await pool.query(
    "SELECT last_success FROM data_sources WHERE slug = ? LIMIT 1",
    [QUELLE.slug],
  );
  const v = rows[0]?.last_success;
  return v ? new Date(v).toISOString() : null;
}

function mapAbstimmung(r, l) {
  const de = l === "de" && r.titel_de;
  return {
    id: r.id,
    zeit: zeitText(r.zeit),
    datum: zeitText(r.zeit)?.slice(0, 10) ?? null,
    titel: de ? r.titel_de : r.titel_en,
    titel_uebersetzt: Boolean(de),
    titel_original: r.titel_en,
    art: r.art,
    ergebnis: r.ergebnis,
    referenz: r.referenz,
    verfahren: r.verfahren,
    verfahren_typ: r.verfahren_typ,
    lesung: r.lesung,
    stimmen: { ja: r.ja, nein: r.nein, enthaltung: r.enthaltung, nicht: r.nicht_abgestimmt },
    deutschland: { ja: r.de_ja, nein: r.de_nein, enthaltung: r.de_enthaltung, nicht: r.de_nicht },
    url: `https://howtheyvote.eu/votes/${r.id}`,
  };
}

function leer() {
  return { ja: 0, nein: 0, enthaltung: 0, nicht: 0 };
}

router.get("/ep/stand", asyncHandler(async (_req, res) => {
  const pool = getPool();
  const [[z]] = await pool.query(
    `SELECT COUNT(*) AS abstimmungen, MAX(zeit) AS letzte,
            (SELECT COUNT(*) FROM ep_abgeordnete WHERE im_amt = 1) AS abgeordnete,
            (SELECT COUNT(*) FROM ep_abgeordnete WHERE im_amt = 1 AND land = 'DEU') AS deutsche
     FROM ep_abstimmungen`,
  );
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    abstimmungen: Number(z.abstimmungen),
    letzte_abstimmung: zeitText(z.letzte)?.slice(0, 10) ?? null,
    abgeordnete: Number(z.abgeordnete),
    deutsche_abgeordnete: Number(z.deutsche),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

router.get("/ep/abstimmungen", asyncHandler(async (req, res) => {
  const limit = ganzzahl(req.query.limit, 20, 1, 50);
  const offset = ganzzahl(req.query.offset, 0, 0, 100000);
  const q = String(req.query.q || "").trim().slice(0, 80);
  const ergebnis = String(req.query.ergebnis || "").trim();
  const art = String(req.query.art || "").trim();
  if (ergebnis && !ERGEBNISSE.has(ergebnis)) {
    res.status(400).json({ error: "ergebnis ungültig" });
    return;
  }
  if (art && !ARTEN.has(art)) {
    res.status(400).json({ error: "art ungültig" });
    return;
  }
  const wo = [];
  const p = [];
  if (q) {
    const like = `%${q.replace(/[\\%_]/g, "\\$&")}%`;
    wo.push("(titel_en LIKE ? OR titel_de LIKE ? OR referenz LIKE ? OR verfahren LIKE ?)");
    p.push(like, like, like, like);
  }
  if (ergebnis) {
    wo.push("ergebnis = ?");
    p.push(ergebnis);
  }
  if (art) {
    wo.push("art = ?");
    p.push(art);
  }
  // knapp=1: Abstimmungen, bei denen Ja und Nein hoechstens 50 Stimmen trennen
  if (String(req.query.knapp || "") === "1") wo.push("ABS(CAST(ja AS SIGNED) - CAST(nein AS SIGNED)) <= 50");
  const where = wo.length ? `WHERE ${wo.join(" AND ")}` : "";
  const pool = getPool();
  const [[{ gesamt }]] = await pool.query(`SELECT COUNT(*) AS gesamt FROM ep_abstimmungen ${where}`, p);
  const [rows] = await pool.query(
    `SELECT * FROM ep_abstimmungen ${where} ORDER BY zeit DESC, id DESC LIMIT ? OFFSET ?`,
    [...p, limit, offset],
  );
  const l = lang(req);
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    gesamt: Number(gesamt),
    limit,
    offset,
    abstimmungen: rows.map((r) => mapAbstimmung(r, l)),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

router.get("/ep/abstimmungen/:id", asyncHandler(async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id) || id <= 0) {
    res.status(400).json({ error: "id ungültig" });
    return;
  }
  const pool = getPool();
  const [abst] = await pool.query("SELECT * FROM ep_abstimmungen WHERE id = ? LIMIT 1", [id]);
  if (!abst.length) {
    res.status(404).json({ error: "Abstimmung nicht gefunden" });
    return;
  }
  const [nachFraktion] = await pool.query(
    `SELECT s.fraktion AS code, f.kurz, f.name, s.position, COUNT(*) AS n
     FROM ep_stimmen s LEFT JOIN ep_fraktionen f ON f.code = s.fraktion
     WHERE s.abstimmung_id = ? GROUP BY s.fraktion, f.kurz, f.name, s.position`,
    [id],
  );
  const [nachLand] = await pool.query(
    "SELECT land, position, COUNT(*) AS n FROM ep_stimmen WHERE abstimmung_id = ? GROUP BY land, position",
    [id],
  );
  const [deutsche] = await pool.query(
    `SELECT s.mitglied_id AS id, a.vorname, a.nachname, s.partei, s.fraktion, f.kurz AS fraktion_kurz, s.position
     FROM ep_stimmen s
     LEFT JOIN ep_abgeordnete a ON a.id = s.mitglied_id
     LEFT JOIN ep_fraktionen f ON f.code = s.fraktion
     WHERE s.abstimmung_id = ? AND s.land = 'DEU'
     ORDER BY a.nachname, a.vorname`,
    [id],
  );
  // Weitere Abstimmungen zum selben Verfahren (etwa Ablehnungsantrag und Einigung am selben Tag)
  const verfahren = abst[0].verfahren;
  const [verwandt] = verfahren
    ? await pool.query(
        "SELECT * FROM ep_abstimmungen WHERE verfahren = ? AND id <> ? ORDER BY zeit DESC LIMIT 10",
        [verfahren, id],
      )
    : [[]];

  const fraktionen = new Map();
  for (const r of nachFraktion) {
    const code = r.code || "NI";
    if (!fraktionen.has(code)) fraktionen.set(code, { code, kurz: r.kurz || code, name: r.name || code, ...leer() });
    fraktionen.get(code)[r.position] += Number(r.n);
  }
  const laender = new Map();
  for (const r of nachLand) {
    if (!laender.has(r.land)) laender.set(r.land, { land: r.land, ...leer() });
    laender.get(r.land)[r.position] += Number(r.n);
  }
  const groesse = (x) => POSITIONEN.reduce((s, k) => s + x[k], 0);
  const l = lang(req);
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    ...mapAbstimmung(abst[0], l),
    beschreibung: abst[0].beschreibung,
    text_referenz: abst[0].text_referenz,
    fraktionen: [...fraktionen.values()].sort((a, b) => groesse(b) - groesse(a)),
    laender: [...laender.values()].sort((a, b) => a.land.localeCompare(b.land)),
    deutsche: deutsche.map((d) => ({
      id: d.id,
      name: [d.vorname, d.nachname].filter(Boolean).join(" ") || String(d.id),
      partei: d.partei,
      fraktion: d.fraktion,
      fraktion_kurz: d.fraktion_kurz || d.fraktion,
      position: d.position,
    })),
    verwandt: verwandt.map((r) => mapAbstimmung(r, l)),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

router.get("/ep/abgeordnete", asyncHandler(async (req, res) => {
  const land = String(req.query.land || "DEU").trim().toUpperCase();
  if (!LAND_RE.test(land)) {
    res.status(400).json({ error: "land ungültig" });
    return;
  }
  const pool = getPool();
  const [rows] = await pool.query(
    `SELECT a.id, a.vorname, a.nachname, a.partei, a.fraktion, f.kurz AS fraktion_kurz,
            SUM(s.position = 'ja') AS ja, SUM(s.position = 'nein') AS nein,
            SUM(s.position = 'enthaltung') AS enthaltung, SUM(s.position = 'nicht') AS nicht
     FROM ep_abgeordnete a
     LEFT JOIN ep_fraktionen f ON f.code = a.fraktion
     LEFT JOIN ep_stimmen s ON s.mitglied_id = a.id
     WHERE a.land = ? AND a.im_amt = 1
     GROUP BY a.id, a.vorname, a.nachname, a.partei, a.fraktion, f.kurz
     ORDER BY a.nachname, a.vorname`,
    [land],
  );
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    land,
    abgeordnete: rows.map((r) => {
      const st = { ja: Number(r.ja || 0), nein: Number(r.nein || 0), enthaltung: Number(r.enthaltung || 0), nicht: Number(r.nicht || 0) };
      const alle = st.ja + st.nein + st.enthaltung + st.nicht;
      return {
        id: r.id,
        name: `${r.vorname} ${r.nachname}`,
        nachname: r.nachname,
        partei: r.partei,
        fraktion: r.fraktion,
        fraktion_kurz: r.fraktion_kurz || r.fraktion,
        stimmen: st,
        // Anteil der Hauptabstimmungen seit Mandatsbeginn, an denen teilgenommen wurde
        teilnahme: alle ? Math.round(((alle - st.nicht) / alle) * 1000) / 10 : null,
      };
    }),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

router.get("/ep/abgeordnete/:id", asyncHandler(async (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(id) || id <= 0) {
    res.status(400).json({ error: "id ungültig" });
    return;
  }
  const limit = ganzzahl(req.query.limit, 20, 1, 50);
  const offset = ganzzahl(req.query.offset, 0, 0, 100000);
  const pool = getPool();
  const [abg] = await pool.query(
    `SELECT a.*, f.kurz AS fraktion_kurz, f.name AS fraktion_name
     FROM ep_abgeordnete a LEFT JOIN ep_fraktionen f ON f.code = a.fraktion WHERE a.id = ? LIMIT 1`,
    [id],
  );
  if (!abg.length) {
    res.status(404).json({ error: "Abgeordnete nicht gefunden" });
    return;
  }
  const [bilanz] = await pool.query(
    "SELECT position, COUNT(*) AS n FROM ep_stimmen WHERE mitglied_id = ? GROUP BY position",
    [id],
  );
  // Wie oft mit der Mehrheit der eigenen Fraktion gestimmt (nur wenn teilgenommen)
  const [[linie]] = await pool.query(
    `SELECT COUNT(*) AS teilgenommen, SUM(x.position = x.mehrheit) AS mit_fraktion
     FROM (
       SELECT s.position,
              CASE WHEN SUM(g.position = 'ja') >= SUM(g.position = 'nein') AND SUM(g.position = 'ja') >= SUM(g.position = 'enthaltung') THEN 'ja'
                   WHEN SUM(g.position = 'nein') >= SUM(g.position = 'enthaltung') THEN 'nein'
                   ELSE 'enthaltung' END AS mehrheit
       FROM ep_stimmen s
       JOIN ep_stimmen g ON g.abstimmung_id = s.abstimmung_id AND g.fraktion = s.fraktion AND g.position <> 'nicht'
       WHERE s.mitglied_id = ? AND s.position <> 'nicht' AND s.fraktion <> 'NI'
       GROUP BY s.abstimmung_id, s.position
     ) x`,
    [id],
  );
  const [stimmen] = await pool.query(
    `SELECT v.*, s.position FROM ep_stimmen s JOIN ep_abstimmungen v ON v.id = s.abstimmung_id
     WHERE s.mitglied_id = ? ORDER BY v.zeit DESC, v.id DESC LIMIT ? OFFSET ?`,
    [id, limit, offset],
  );
  const st = leer();
  for (const r of bilanz) st[r.position] = Number(r.n);
  const alle = POSITIONEN.reduce((s, k) => s + st[k], 0);
  const a = abg[0];
  const l = lang(req);
  const teil = Number(linie.teilgenommen || 0);
  res.set("Cache-Control", "public, max-age=300");
  res.json({
    id: a.id,
    name: `${a.vorname} ${a.nachname}`,
    land: a.land,
    partei: a.partei,
    fraktion: a.fraktion,
    fraktion_kurz: a.fraktion_kurz || a.fraktion,
    fraktion_name: a.fraktion_name,
    im_amt: Boolean(a.im_amt),
    stimmen: st,
    abstimmungen: alle,
    teilnahme: alle ? Math.round(((alle - st.nicht) / alle) * 1000) / 10 : null,
    mit_fraktion: teil ? Math.round((Number(linie.mit_fraktion || 0) / teil) * 1000) / 10 : null,
    profil_url: `https://www.europarl.europa.eu/meps/de/${a.id}`,
    limit,
    offset,
    letzte: stimmen.map((r) => ({ ...mapAbstimmung(r, l), position: r.position })),
    stand: await stand(pool),
    quelle: QUELLE,
  });
}));

module.exports = router;
