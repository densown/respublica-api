"use strict";

/**
 * Wahltermine und Wahlumfragen (Wahlen-Modul Phase 0).
 *
 * Eigener Namespace `/api/wahltermine/*` — `/api/wahlen/*` gehoert den
 * historischen GERDA-Ergebnissen in routes/wahlen.js und bleibt unberuehrt.
 *
 * Datenquelle der Umfragen ist dawum.de unter ODC-ODbL. Die Attribution wird
 * bei jeder Antwort mitgeliefert (Feld `quelle`), damit das Frontend sie nicht
 * hartkodieren muss und sie nicht versehentlich verloren geht.
 */

const express = require("express");
const router = express.Router();
const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

const EBENEN = new Set(["bund", "land", "eu"]);
const STATUS = new Set(["kommend", "laufend", "abgeschlossen"]);
const SLUG_RE = /^[a-z0-9-]{1,64}$/;

// ODbL verlangt Namensnennung von Quelle und Autor.
const QUELLE = Object.freeze({
  name: "dawum.de",
  url: "https://dawum.de/",
  autor: "Dipl.-Jur. Philipp Guttmann",
  lizenz: "ODC-ODbL",
  lizenz_url: "https://opendatacommons.org/licenses/odbl/1-0/",
});

function mapWahltermin(r) {
  return {
    slug: r.slug,
    ebene: r.ebene,
    land: r.land,
    name_de: r.name_de,
    name_en: r.name_en,
    datum: formatDate(r.datum),
    status: r.status,
    umfragen: Number(r.umfragen ?? 0),
    letzte_umfrage: formatDate(r.letzte_umfrage),
  };
}

/** Liste aller Wahltermine, optional gefiltert. */
router.get(
  "/wahltermine",
  asyncHandler(async (req, res) => {
    const where = [];
    const params = [];

    const status = String(req.query.status || "").trim().toLowerCase();
    if (status) {
      if (!STATUS.has(status)) {
        res.status(400).json({ error: "status ungültig" });
        return;
      }
      where.push("w.status = ?");
      params.push(status);
    }

    const ebene = String(req.query.ebene || "").trim().toLowerCase();
    if (ebene) {
      if (!EBENEN.has(ebene)) {
        res.status(400).json({ error: "ebene ungültig" });
        return;
      }
      where.push("w.ebene = ?");
      params.push(ebene);
    }

    const [rows] = await getPool().query(
      `SELECT w.slug, w.ebene, w.land, w.name_de, w.name_en, w.datum, w.status,
              COUNT(u.id) AS umfragen, MAX(u.veroeffentlicht) AS letzte_umfrage
         FROM wahltermine w
         LEFT JOIN umfragen u ON u.wahltermin_id = w.id
        ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
        GROUP BY w.id
        ORDER BY w.datum IS NULL DESC, w.datum DESC`,
      params,
    );

    res.json({ wahltermine: rows.map(mapWahltermin), quelle: QUELLE });
  }),
);

/** Ein Wahltermin mit den Parteien, die dort tatsaechlich abgefragt werden. */
router.get(
  "/wahltermine/:slug",
  asyncHandler(async (req, res) => {
    const slug = String(req.params.slug || "").trim().toLowerCase();
    if (!SLUG_RE.test(slug)) {
      res.status(400).json({ error: "slug ungültig" });
      return;
    }

    const [rows] = await getPool().query(
      `SELECT w.id, w.slug, w.ebene, w.land, w.name_de, w.name_en, w.datum, w.status,
              COUNT(u.id) AS umfragen, MAX(u.veroeffentlicht) AS letzte_umfrage
         FROM wahltermine w
         LEFT JOIN umfragen u ON u.wahltermin_id = w.id
        WHERE w.slug = ?
        GROUP BY w.id`,
      [slug],
    );
    if (!rows.length) {
      res.status(404).json({ error: "Wahltermin nicht gefunden" });
      return;
    }

    const [parteien] = await getPool().query(
      `SELECT DISTINCT p.kuerzel, p.name, p.farbe_hex, p.sortierung
         FROM umfrage_werte v
         INNER JOIN umfragen u ON u.id = v.umfrage_id
         INNER JOIN parteien p ON p.id = v.partei_id
        WHERE u.wahltermin_id = ?
        ORDER BY p.sortierung ASC`,
      [rows[0].id],
    );

    res.json({
      wahl: mapWahltermin(rows[0]),
      parteien: parteien.map((p) => ({
        kuerzel: p.kuerzel,
        name: p.name,
        farbe_hex: p.farbe_hex,
      })),
      quelle: QUELLE,
    });
  }),
);

/**
 * Zeitreihe der Umfragen. Die Werte liegen flach auf der Zeile (ein Feld je
 * Partei-Kuerzel), damit das Frontend sie ohne Umformung an den Chart geben
 * kann. Partei-Kuerzel kollidieren nicht mit den Metadaten-Feldern.
 */
router.get(
  "/wahltermine/:slug/umfragen",
  asyncHandler(async (req, res) => {
    const slug = String(req.params.slug || "").trim().toLowerCase();
    if (!SLUG_RE.test(slug)) {
      res.status(400).json({ error: "slug ungültig" });
      return;
    }

    const [wahl] = await getPool().query(
      `SELECT id, slug, ebene, land, name_de, name_en, datum, status
         FROM wahltermine WHERE slug = ?`,
      [slug],
    );
    if (!wahl.length) {
      res.status(404).json({ error: "Wahltermin nicht gefunden" });
      return;
    }
    const wahlterminId = wahl[0].id;

    const params = [wahlterminId];
    let institutFilter = "";
    const institut = String(req.query.institut || "").trim();
    if (institut) {
      institutFilter = " AND u.institut = ?";
      params.push(institut);
    }

    const [rows] = await getPool().query(
      `SELECT u.id, u.dawum_survey_id, u.institut, u.auftraggeber,
              u.erhebung_start, u.erhebung_ende, u.veroeffentlicht,
              u.befragte, u.methode,
              p.kuerzel, p.sortierung, v.prozent
         FROM umfragen u
         INNER JOIN umfrage_werte v ON v.umfrage_id = u.id
         INNER JOIN parteien p ON p.id = v.partei_id
        WHERE u.wahltermin_id = ?${institutFilter}
        ORDER BY u.veroeffentlicht ASC, u.id ASC, p.sortierung ASC`,
      params,
    );

    // Zeilen zu je einer Umfrage zusammenfassen; Reihenfolge bleibt erhalten.
    const byId = new Map();
    const parteienSeen = new Map();
    for (const r of rows) {
      let row = byId.get(r.id);
      if (!row) {
        row = {
          dawum_survey_id: r.dawum_survey_id,
          institut: r.institut,
          auftraggeber: r.auftraggeber,
          erhebung_start: formatDate(r.erhebung_start),
          erhebung_ende: formatDate(r.erhebung_ende),
          veroeffentlicht: formatDate(r.veroeffentlicht),
          befragte: r.befragte == null ? null : Number(r.befragte),
          methode: r.methode,
        };
        byId.set(r.id, row);
      }
      row[r.kuerzel] = Number(r.prozent);
      if (!parteienSeen.has(r.kuerzel)) {
        parteienSeen.set(r.kuerzel, r.sortierung);
      }
    }

    const [institute] = await getPool().query(
      `SELECT DISTINCT institut FROM umfragen
        WHERE wahltermin_id = ? ORDER BY institut ASC`,
      [wahlterminId],
    );

    res.json({
      wahl: {
        slug: wahl[0].slug,
        ebene: wahl[0].ebene,
        land: wahl[0].land,
        name_de: wahl[0].name_de,
        name_en: wahl[0].name_en,
        datum: formatDate(wahl[0].datum),
        status: wahl[0].status,
      },
      parteien: [...parteienSeen.entries()]
        .sort((a, b) => a[1] - b[1])
        .map(([kuerzel]) => kuerzel),
      institute: institute.map((r) => r.institut),
      umfragen: [...byId.values()],
      quelle: QUELLE,
    });
  }),
);

/**
 * Kandidaturen zu einer Wahl, nach Partei gruppiert.
 *
 * Verknuepft ueber wahltermine.aw_parliament_period_id — dawum (Umfragen) und
 * abgeordnetenwatch (Personen) nummerieren ihre Perioden unabhaengig
 * voneinander, deshalb traegt `wahltermine` beide Schluessel.
 */
router.get(
  "/wahltermine/:slug/kandidaturen",
  asyncHandler(async (req, res) => {
    const slug = String(req.params.slug || "").trim().toLowerCase();
    if (!SLUG_RE.test(slug)) {
      res.status(400).json({ error: "slug ungültig" });
      return;
    }

    const [wahl] = await getPool().query(
      `SELECT slug, name_de, name_en, datum, land, aw_parliament_period_id
         FROM wahltermine WHERE slug = ?`,
      [slug],
    );
    if (!wahl.length) {
      res.status(404).json({ error: "Wahltermin nicht gefunden" });
      return;
    }
    const periode = wahl[0].aw_parliament_period_id;
    if (periode == null) {
      // Kein Fehler: fuer die meisten Wahlen fuehrt abgeordnetenwatch (noch)
      // keine Kandidaturen. Leere Liste statt 404, damit das Frontend die
      // Wahl trotzdem anzeigen kann.
      res.json({ wahl: wahl[0], gesamt: 0, parteien: [] });
      return;
    }

    const params = [periode];
    let parteiFilter = "";
    const partei = String(req.query.partei || "").trim();
    if (partei) {
      parteiFilter = " AND partei = ?";
      params.push(partei);
    }

    const [rows] = await getPool().query(
      `SELECT aw_id, name, partei, wahlkreis, wahlkreis_nr, listenplatz, profil_url, foto_url
         FROM abgeordnete
        WHERE parliament_period = ? AND typ = 'kandidatur'${parteiFilter}
        ORDER BY partei ASC, wahlkreis_nr ASC, listenplatz ASC, nachname ASC`,
      params,
    );

    // abgeordnetenwatch haengt an jedes Wahlkreis-Label die Wahl an
    // ("13 - Magdeburg IV (Sachsen-Anhalt Wahl 2026)"). Auf einer Seite, die
    // ohnehin genau diese Wahl zeigt, ist das Rauschen.
    const kurzerWahlkreis = (label) =>
      label == null ? null : String(label).replace(/\s*\([^)]*\)\s*$/, "").trim();

    const nachPartei = new Map();
    for (const r of rows) {
      const key = r.partei || "—";
      let gruppe = nachPartei.get(key);
      if (!gruppe) {
        gruppe = { partei: key, anzahl: 0, kandidaturen: [] };
        nachPartei.set(key, gruppe);
      }
      gruppe.anzahl += 1;
      gruppe.kandidaturen.push({
        aw_id: r.aw_id,
        name: r.name,
        wahlkreis: kurzerWahlkreis(r.wahlkreis),
        wahlkreis_nr: r.wahlkreis_nr == null ? null : Number(r.wahlkreis_nr),
        listenplatz: r.listenplatz == null ? null : Number(r.listenplatz),
        profil_url: r.profil_url,
        foto_url: r.foto_url,
      });
    }

    /*
     * Spitzenkandidaturen.
     *
     * abgeordnetenwatch fuehrt keine Kennzeichnung dafuer — abgeleitet wird
     * ueber Listenplatz 1 der Landesliste. Das ist der uebliche und
     * nachpruefbare Stellvertreter: wer eine Landesliste anfuehrt, ist das
     * Gesicht der Partei im Wahlkampf. Parteien ohne Landesliste tauchen
     * folglich nicht auf, was korrekt ist — sie treten nur mit
     * Direktbewerbungen an.
     *
     * Bewusst nicht redaktionell gesetzt: eine gepflegte Liste waere eine
     * weitere Stelle, die vor jeder Wahl veralten kann.
     */
    const spitzen = rows
      .filter((r) => Number(r.listenplatz) === 1)
      .map((r) => ({
        aw_id: r.aw_id,
        name: r.name,
        partei: r.partei,
        wahlkreis: kurzerWahlkreis(r.wahlkreis),
        foto_url: r.foto_url,
        profil_url: r.profil_url,
      }));

    res.json({
      wahl: {
        slug: wahl[0].slug,
        name_de: wahl[0].name_de,
        name_en: wahl[0].name_en,
        land: wahl[0].land,
        datum: formatDate(wahl[0].datum),
      },
      gesamt: rows.length,
      spitzenkandidaturen: spitzen,
      // Groesste Parteien zuerst — das entspricht der Relevanz und deckt sich
      // mit der Reihenfolge auf der Umfrageseite.
      parteien: [...nachPartei.values()].sort((a, b) => b.anzahl - a.anzahl),
    });
  }),
);

module.exports = router;
