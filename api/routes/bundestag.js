"use strict";

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

/** Aktuelle Sitzverteilung Bundestag, 21. Wahlperiode (fest codiert) */
const BUNDESTAG_SITZVERTEILUNG_WP21 = [
  { partei: "Linke", farbe: "#BE3075", sitze: 64, position: 0 },
  { partei: "SSW", farbe: "#003F8E", sitze: 1, position: 1 },
  { partei: "Grüne", farbe: "#46962B", sitze: 85, position: 2 },
  { partei: "SPD", farbe: "#E3000F", sitze: 119, position: 3 },
  { partei: "CDU/CSU", farbe: "#000000", sitze: 208, position: 4 },
  { partei: "AfD", farbe: "#009EE0", sitze: 150, position: 5 },
  { partei: "Fraktionslos", farbe: "#808080", sitze: 2, position: 6 },
];

/** Neueste Abstimmungen (vor :poll_id registrieren) */
router.get("/abstimmungen/latest", asyncHandler(async (req, res) => {
  let limit = Number.parseInt(String(req.query.limit ?? "3"), 10);
  if (!Number.isFinite(limit) || limit < 1) limit = 3;
  if (limit > 10) limit = 10;
  const [rows] = await getPool().query(
    `SELECT DISTINCT poll_id, poll_titel, poll_datum
     FROM abstimmungen
     ORDER BY poll_datum DESC
     LIMIT ?`,
    [limit]
  );
  const out = rows.map((r) => ({
    poll_id: r.poll_id,
    poll_titel: r.poll_titel,
    poll_datum: formatDate(r.poll_datum),
  }));
  res.json(out);
}));

router.get("/bundestag/sitzverteilung", (_req, res) => {
  res.json(BUNDESTAG_SITZVERTEILUNG_WP21);
});

/** Bundestag: alle Abgeordneten (DB), sortiert nach Fraktion, Nachname */
router.get("/bundestag/abgeordnete", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT * FROM abgeordnete ORDER BY fraktion, nachname`
  );
  res.json(rows);
}));

/** Bundestag: ein Abgeordneter nach Abgeordnetenwatch-Mandats-ID (aw_id) */
router.get("/bundestag/abgeordnete/:id", asyncHandler(async (req, res) => {
  const awId = Number.parseInt(req.params.id, 10);
  if (!Number.isFinite(awId)) {
    res.status(400).json({ error: "Ungültige id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT * FROM abgeordnete WHERE aw_id = ? LIMIT 1`,
    [awId]
  );
  if (!rows.length) {
    res.status(404).json({ error: "Nicht gefunden" });
    return;
  }
  res.json(rows[0]);
}));

/** Alle Abgeordneten für Frontend-Hemicycle-Mapping */
router.get("/abgeordnete", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT id, aw_id, name, fraktion, wahlkreis, foto_url, profil_url
     FROM abgeordnete
     ORDER BY fraktion, nachname`
  );
  res.json(rows);
}));

/**
 * Abstimmungsprofil eines Abgeordneten nach Themenfeld.
 *
 * Liefert je Thema die Stimmenbilanz plus die Zahl der Abweichungen von der
 * Mehrheit der eigenen Fraktion. Das ist die eigentliche Aussage: nicht wie
 * jemand abstimmt, sondern wo er oder sie von der Fraktion abweicht.
 *
 * Zur Definition von "Abweichung": verglichen wird nur, wenn die oder der
 * Abgeordnete tatsaechlich abgestimmt hat (ja, nein, enthalten). Nichtteilnahme
 * ist keine inhaltliche Abweichung und bleibt draussen — sonst zaehlten
 * Krankheit und Dienstreise als Aufmuepfigkeit.
 */
router.get("/abgeordnete/:aw_id/themen", asyncHandler(async (req, res) => {
  const awId = Number.parseInt(req.params.aw_id, 10);
  if (!Number.isFinite(awId)) {
    res.status(400).json({ error: "Ungültige aw_id" });
    return;
  }

  const [person] = await getPool().query(
    `SELECT aw_id, name, fraktion, wahlkreis, foto_url, profil_url
       FROM abgeordnete WHERE aw_id = ?`,
    [awId],
  );
  if (!person.length) {
    res.status(404).json({ error: "Abgeordnete:r nicht gefunden" });
    return;
  }

  // Mehrheitsvotum je Fraktion und Abstimmung. Bei Gleichstand entscheidet
  // die Stimmenzahl, danach alphabetisch — deterministisch, damit dieselbe
  // Anfrage nicht mal so und mal so antwortet.
  const [rows] = await getPool().query(
    `WITH fraktion_votes AS (
       SELECT poll_id, fraction_label, vote, COUNT(*) AS n
         FROM votes
        WHERE vote IS NOT NULL AND vote <> 'no_show'
        GROUP BY poll_id, fraction_label, vote
     ),
     fraktion_mehrheit AS (
       SELECT poll_id, fraction_label, vote,
              ROW_NUMBER() OVER (
                PARTITION BY poll_id, fraction_label ORDER BY n DESC, vote ASC
              ) AS rang
         FROM fraktion_votes
     )
     SELECT t.slug, t.name_de, t.name_en, t.sortierung,
            v.poll_id, v.vote,
            fm.vote AS fraktion_vote
       FROM votes v
       INNER JOIN poll_themenfelder pt ON pt.poll_id = v.poll_id
       INNER JOIN themenfelder t ON t.id = pt.themenfeld_id
       LEFT JOIN fraktion_mehrheit fm
              ON fm.poll_id = v.poll_id
             AND fm.fraction_label = v.fraction_label
             AND fm.rang = 1
      WHERE v.mandate_id = ?
      ORDER BY t.sortierung ASC`,
    [awId],
  );

  const themen = new Map();
  const abwPolls = new Set();

  for (const r of rows) {
    let th = themen.get(r.slug);
    if (!th) {
      th = {
        slug: r.slug,
        name_de: r.name_de,
        name_en: r.name_en,
        abstimmungen: 0,
        ja: 0, nein: 0, enthalten: 0, abwesend: 0,
        abweichungen: 0,
      };
      themen.set(r.slug, th);
    }
    th.abstimmungen += 1;
    if (r.vote === "yes") th.ja += 1;
    else if (r.vote === "no") th.nein += 1;
    else if (r.vote === "abstain") th.enthalten += 1;
    else if (r.vote === "no_show") th.abwesend += 1;

    const abweichung =
      r.vote !== "no_show" && r.fraktion_vote != null && r.vote !== r.fraktion_vote;
    if (abweichung) {
      th.abweichungen += 1;
      abwPolls.add(r.poll_id);
    }
  }

  // Eine Abstimmung kann an mehreren Themen haengen — die Gesamtbilanz zaehlt
  // deshalb ueber Abstimmungen, nicht ueber die Themenzeilen.
  const [bilanz] = await getPool().query(
    `SELECT vote, COUNT(*) AS n FROM votes WHERE mandate_id = ? GROUP BY vote`,
    [awId],
  );
  const gesamt = {
    abstimmungen: 0, ja: 0, nein: 0, enthalten: 0, abwesend: 0,
    abweichungen: abwPolls.size,
  };
  for (const b of bilanz) {
    const n = Number(b.n);
    gesamt.abstimmungen += n;
    if (b.vote === "yes") gesamt.ja = n;
    else if (b.vote === "no") gesamt.nein = n;
    else if (b.vote === "abstain") gesamt.enthalten = n;
    else if (b.vote === "no_show") gesamt.abwesend = n;
  }

  res.json({
    abgeordnete: {
      aw_id: person[0].aw_id,
      name: person[0].name,
      fraktion: person[0].fraktion,
      wahlkreis: person[0].wahlkreis,
      foto_url: person[0].foto_url,
      profil_url: person[0].profil_url,
    },
    gesamt,
    themen: [...themen.values()].filter((t) => t.abstimmungen > 0),
  });
}));

/** Abstimmungshistorie eines Abgeordneten (über votes + abstimmungen) */
router.get("/abgeordnete/:aw_id/votes", asyncHandler(async (req, res) => {
  const awId = Number.parseInt(req.params.aw_id, 10);
  if (!Number.isFinite(awId)) {
    res.status(400).json({ error: "Ungültige aw_id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT v.poll_id, v.vote, a.poll_titel, a.poll_datum
     FROM votes v
     JOIN abstimmungen a ON a.poll_id = v.poll_id
     WHERE v.mandate_id = ?
     GROUP BY v.poll_id, v.vote, a.poll_titel, a.poll_datum
     ORDER BY a.poll_datum DESC`,
    [awId]
  );
  res.json(
    rows.map((r) => ({
      poll_id: r.poll_id,
      vote: r.vote,
      poll_titel: r.poll_titel,
      poll_datum: formatDate(r.poll_datum),
    }))
  );
}));

router.get("/bundestag/abstimmungen", asyncHandler(async (_req, res) => {
  const [rows] = await getPool().query(
    `SELECT DISTINCT poll_id, poll_titel, poll_datum
     FROM abstimmungen
     ORDER BY poll_datum DESC`
  );
  const out = rows.map((r) => ({
    poll_id: r.poll_id,
    poll_titel: r.poll_titel,
    poll_datum: formatDate(r.poll_datum),
  }));
  res.json(out);
}));

router.get("/bundestag/abstimmungen/:pollId", asyncHandler(async (req, res) => {
  const pollId = Number.parseInt(req.params.pollId, 10);
  if (!Number.isFinite(pollId)) {
    res.status(400).json({ error: "Ungültige poll_id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT partei, ja, nein, enthalten, abwesend, poll_titel, poll_datum
     FROM abstimmungen
     WHERE poll_id = ?
     ORDER BY partei`,
    [pollId]
  );
  if (!rows.length) {
    res.status(404).json({ error: "Nicht gefunden" });
    return;
  }
  const r0 = rows[0];
  let ja_gesamt = 0;
  let nein_gesamt = 0;
  let enthalten_gesamt = 0;
  let abwesend_gesamt = 0;
  const fraktionen = rows.map((r) => {
    const ja = Number(r.ja) || 0;
    const nein = Number(r.nein) || 0;
    const enthalten = Number(r.enthalten) || 0;
    const abwesend = Number(r.abwesend) || 0;
    ja_gesamt += ja;
    nein_gesamt += nein;
    enthalten_gesamt += enthalten;
    abwesend_gesamt += abwesend;
    return { partei: r.partei, ja, nein, enthalten, abwesend };
  });
  res.json({
    poll_id: pollId,
    poll_titel: r0.poll_titel,
    poll_datum: formatDate(r0.poll_datum),
    ergebnis: {
      ja_gesamt,
      nein_gesamt,
      enthalten_gesamt,
      abwesend_gesamt,
    },
    fraktionen,
  });
}));

router.get("/bundestag/poll-votes/:poll_id", asyncHandler(async (req, res) => {
  const pollId = Number.parseInt(req.params.poll_id, 10);
  if (!Number.isFinite(pollId)) {
    res.status(400).json({ error: "Ungültige poll_id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT v.mandate_id, v.vote, v.abgeordneter_name
     FROM votes v
     WHERE v.poll_id = ?`,
    [pollId]
  );
  res.json({
    votes: rows.map((r) => ({
      mandate_id: Number(r.mandate_id),
      vote: r.vote,
      abgeordneter_name: r.abgeordneter_name,
    })),
  });
}));

router.get("/abstimmungen/:poll_id", asyncHandler(async (req, res) => {
  const pollId = Number.parseInt(req.params.poll_id, 10);
  if (!Number.isFinite(pollId)) {
    res.status(400).json({ error: "Ungültige poll_id" });
    return;
  }
  const [rows] = await getPool().query(
    `SELECT
       partei,
       ja,
       nein,
       enthalten,
       abwesend,
       poll_titel,
       poll_datum
     FROM abstimmungen
     WHERE poll_id = ?
     ORDER BY partei`,
    [pollId]
  );
  const out = rows.map((r) => ({
    partei: r.partei,
    ja: r.ja,
    nein: r.nein,
    enthalten: r.enthalten,
    abwesend: r.abwesend,
    poll_titel: r.poll_titel,
    poll_datum: formatDate(r.poll_datum),
  }));
  res.json(out);
}));


module.exports = router;
