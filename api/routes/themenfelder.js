"use strict";

/**
 * Themenfeld-Taxonomie (Wahlen-Modul Phase 1).
 *
 * Die gemeinsame Achse fuer Wahlprogramm-Positionen, Koalitionsvertrags-
 * Zusagen und Abstimmungsverhalten. Grundlage sind die DIP21-Sachgebiete des
 * Bundestags; Herkunft und Pflege siehe scripts/import_themenfelder.py.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

const SLUG_RE = /^[a-z0-9-]{1,64}$/;

/**
 * Taxonomie als Baum, je Thema die Zahl verknuepfter Abstimmungen.
 *
 * ?fuer_positionen=1 blendet die Felder aus, die nur parlamentarisches
 * Verfahren abbilden (Geschaeftsordnung, Immunitaet ...) — die sind als Ziel
 * fuer Wahlprogramm-Positionen untauglich.
 */
router.get(
  "/themenfelder",
  asyncHandler(async (req, res) => {
    const nurPositionen = String(req.query.fuer_positionen || "") === "1";

    const [rows] = await getPool().query(
      `SELECT t.id, t.slug, t.name_de, t.name_en, t.parent_id,
              t.fuer_positionen, t.sortierung
         FROM themenfelder t
        ${nurPositionen ? "WHERE t.fuer_positionen = 1" : ""}
        ORDER BY t.sortierung ASC`,
    );

    // Nur Abstimmungen zaehlen, die wir auch wirklich haben. In
    // poll_themenfelder stehen alle 1758 thematisierten Abstimmungen von
    // abgeordnetenwatch; vorliegen tut uns davon nur ein Bruchteil. Ohne
    // diesen Join meldet der Baum Zahlen, die der Detail-Endpoint nicht
    // einloesen kann.
    const [paare] = await getPool().query(
      `SELECT DISTINCT pt.themenfeld_id, pt.poll_id
         FROM poll_themenfelder pt
         INNER JOIN abstimmungen a ON a.poll_id = pt.poll_id`,
    );
    const eigene = new Map();
    for (const p of paare) {
      let s = eigene.get(p.themenfeld_id);
      if (!s) eigene.set(p.themenfeld_id, (s = new Set()));
      s.add(p.poll_id);
    }

    const byId = new Map();
    for (const r of rows) {
      byId.set(r.id, {
        id: r.id,
        slug: r.slug,
        name_de: r.name_de,
        name_en: r.name_en,
        fuer_positionen: Number(r.fuer_positionen) === 1,
        abstimmungen: 0,
        unterthemen: [],
      });
    }

    // Kinder einhaengen. Faellt das Elternthema durch den Filter, rutscht das
    // Kind auf die oberste Ebene statt zu verschwinden.
    const wurzeln = [];
    for (const r of rows) {
      const knoten = byId.get(r.id);
      const eltern = r.parent_id != null ? byId.get(r.parent_id) : null;
      if (eltern) eltern.unterthemen.push(knoten);
      else wurzeln.push(knoten);
    }

    // Unterthemen ins Oberthema hochrollen, als Vereinigung statt Summe:
    // eine Abstimmung kann an mehreren Themen desselben Astes haengen und
    // wuerde sonst doppelt gezaehlt. Deckt sich damit exakt mit dem, was
    // /themenfelder/:slug/abstimmungen ausliefert.
    const rollup = (knoten) => {
      const menge = new Set(eigene.get(knoten.id) ?? []);
      for (const kind of knoten.unterthemen) {
        for (const poll of rollup(kind)) menge.add(poll);
      }
      knoten.abstimmungen = menge.size;
      delete knoten.id;
      return menge;
    };
    wurzeln.forEach(rollup);

    res.json({ themenfelder: wurzeln });
  }),
);

/** Abstimmungen zu einem Themenfeld, Unterthemen eingeschlossen. */
router.get(
  "/themenfelder/:slug/abstimmungen",
  asyncHandler(async (req, res) => {
    const slug = String(req.params.slug || "").trim().toLowerCase();
    if (!SLUG_RE.test(slug)) {
      res.status(400).json({ error: "slug ungültig" });
      return;
    }

    const [themen] = await getPool().query(
      "SELECT id, slug, name_de, name_en FROM themenfelder WHERE slug = ?",
      [slug],
    );
    if (!themen.length) {
      res.status(404).json({ error: "Themenfeld nicht gefunden" });
      return;
    }
    const thema = themen[0];

    // Ein Oberthema soll die Abstimmungen seiner Unterthemen mit ausweisen —
    // sonst wirkt "Umwelt" leer, obwohl unter "Klima" Abstimmungen haengen.
    const [rows] = await getPool().query(
      `SELECT DISTINCT a.poll_id, a.poll_titel, a.poll_datum,
              SUM(a.ja) AS ja, SUM(a.nein) AS nein,
              SUM(a.enthalten) AS enthalten, SUM(a.abwesend) AS abwesend
         FROM poll_themenfelder pt
         INNER JOIN themenfelder t ON t.id = pt.themenfeld_id
         INNER JOIN abstimmungen a ON a.poll_id = pt.poll_id
        WHERE t.id = ? OR t.parent_id = ?
        GROUP BY a.poll_id
        ORDER BY a.poll_datum DESC`,
      [thema.id, thema.id],
    );

    res.json({
      themenfeld: {
        slug: thema.slug,
        name_de: thema.name_de,
        name_en: thema.name_en,
      },
      abstimmungen: rows.map((r) => ({
        poll_id: r.poll_id,
        titel: r.poll_titel,
        datum: formatDate(r.poll_datum),
        ja: Number(r.ja),
        nein: Number(r.nein),
        enthalten: Number(r.enthalten),
        abwesend: Number(r.abwesend),
      })),
    });
  }),
);

module.exports = router;
