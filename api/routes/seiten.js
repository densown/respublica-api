"use strict";

/**
 * Seiten der App fuer Abrufe ohne JavaScript.
 *
 * Die App (app.respublica.media) liefert fuer jede Adresse dieselbe leere
 * index.html, den Inhalt holt erst der Browser. Suchmaschinen, Reddit,
 * WhatsApp und KI-Suchen sehen deshalb fuer jede Abstimmung denselben Titel
 * und keinen Text. Diese Routen setzen Titel, Beschreibung, Vorschau-Angaben
 * und einen Kerntext direkt in die index.html der App:
 *
 *   GET /api/seiten/html?pfad=/bundestag/6773
 *   GET /api/seiten/html?pfad=/bundestag/abgeordnete/123
 *   GET /api/seiten/html?pfad=/gesetze/456
 *   GET /api/seiten/sitemap.xml
 *
 * Der Kerntext steht in <div id="root">. React ersetzt den Inhalt beim Start
 * (createRoot), Menschen sehen also weiter die App. nginx fragt diese Routen
 * nur fuer Einzelseiten ab und faellt bei jedem Fehler auf die statische
 * index.html zurueck, siehe deploy/nginx im Dashboard-Repo.
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const router = express.Router();

const { getPool } = require("../lib/db");
const { asyncHandler } = require("../lib/errors");
const { formatDate } = require("../lib/helpers");

const BASIS = "https://app.respublica.media";
const MARKE = "Res.Publica";
const BUNDESTAG_PERIOD = 161;
const SITEMAP_HOECHSTENS = 45000; // Grenze des Formats: 50.000 Adressen je Datei
const SITEMAP_HALTBARKEIT_MS = 60 * 60 * 1000;

/** index.html der gebauten App. Wechselt bei jedem Deploy, daher nach mtime gepuffert. */
function indexPfad() {
  return (
    process.env.DASHBOARD_INDEX ||
    path.join(__dirname, "..", "..", "..", "dashboard", "dist", "index.html")
  );
}

let indexPuffer = { mtimeMs: 0, pfad: "", html: null };

function indexHtml() {
  const pfad = indexPfad();
  try {
    const { mtimeMs } = fs.statSync(pfad);
    if (indexPuffer.html && indexPuffer.pfad === pfad && indexPuffer.mtimeMs === mtimeMs) {
      return indexPuffer.html;
    }
    indexPuffer = { mtimeMs, pfad, html: fs.readFileSync(pfad, "utf8") };
    return indexPuffer.html;
  } catch {
    return null;
  }
}

function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Auf ganze Woerter kuerzen, fuer Beschreibungen in Suchergebnissen. */
function kuerzen(text, max) {
  const t = String(text ?? "").replace(/\s+/g, " ").trim();
  if (t.length <= max) return t;
  const schnitt = t.slice(0, max - 1);
  const ende = schnitt.lastIndexOf(" ");
  return `${schnitt.slice(0, ende > max * 0.6 ? ende : schnitt.length).replace(/[,;:.\s]+$/, "")}…`;
}

function datumLang(iso) {
  if (!iso) return "";
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00Z`);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString("de-DE", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
}

const STIMME = { yes: "Ja", no: "Nein", abstain: "Enthalten", no_show: "Nicht abgestimmt" };

/**
 * Setzt die Angaben einer Seite in die index.html der App.
 * seite: { titel, beschreibung, pfad, inhalt (HTML), daten (JSON-LD) }
 */
function seiteEinsetzen(html, seite) {
  const voll = seite.titel.includes(MARKE) ? seite.titel : `${seite.titel} · ${MARKE}`;
  const url = `${BASIS}${seite.pfad}`;
  const ersetze = (h, muster, neu) => (muster.test(h) ? h.replace(muster, () => neu) : h);

  let h = html;
  h = ersetze(h, /<title>[\s\S]*?<\/title>/, `<title>${esc(voll)}</title>`);
  h = ersetze(
    h,
    /<meta\s+name="description"[\s\S]*?\/>/,
    `<meta name="description" content="${esc(seite.beschreibung)}" />`
  );
  h = ersetze(h, /<link\s+rel="canonical"[\s\S]*?\/>/, `<link rel="canonical" href="${esc(url)}" />`);
  h = ersetze(
    h,
    /<meta\s+property="og:title"[\s\S]*?\/>/,
    `<meta property="og:title" content="${esc(voll)}" />`
  );
  h = ersetze(
    h,
    /<meta\s+property="og:description"[\s\S]*?\/>/,
    `<meta property="og:description" content="${esc(seite.beschreibung)}" />`
  );
  h = ersetze(h, /<meta\s+property="og:url"[\s\S]*?\/>/, `<meta property="og:url" content="${esc(url)}" />`);
  h = ersetze(
    h,
    /<meta\s+property="og:type"[\s\S]*?\/>/,
    `<meta property="og:type" content="article" />`
  );

  if (seite.daten) {
    // "<" im JSON maskieren, damit kein Text den script-Block schliessen kann.
    const json = JSON.stringify(seite.daten).replace(/</g, "\\u003c");
    h = h.replace("</head>", () => `    <script type="application/ld+json">${json}</script>\n  </head>`);
  }

  // Kerntext in #root. Nur wenn #root leer ist, sonst bleibt die Datei, wie sie ist.
  h = h.replace(/<div id="root">\s*<\/div>/, () => `<div id="root">${seite.inhalt}</div>`);
  return h;
}

const RAHMEN_ANFANG =
  '<main style="max-width:40rem;margin:2rem auto;padding:0 1rem;font-family:Georgia,serif;line-height:1.6">';
const RAHMEN_ENDE =
  '<p><a href="/bundestag">Alle Abstimmungen</a> · <a href="/gesetze">Gesetze</a> · <a href="/lobbyregister">Lobbyregister</a> · <a href="/">Start</a></p></main>';

async function seiteAbstimmung(pollId) {
  const pool = getPool();
  const [rows] = await pool.query(
    `SELECT partei, ja, nein, enthalten, abwesend, poll_titel, poll_datum
       FROM abstimmungen WHERE poll_id = ? ORDER BY partei`,
    [pollId]
  );
  if (!rows.length) return null;
  const [details] = await pool.query(
    `SELECT intro, angenommen,
            CASE WHEN kurztext_hash = SHA1(intro) THEN kurztext END AS kurztext
       FROM abstimmung_details WHERE poll_id = ?`,
    [pollId]
  );
  const d = details[0] || {};
  const titel = String(rows[0].poll_titel || `Abstimmung ${pollId}`).trim();
  const datum = formatDate(rows[0].poll_datum);
  const sum = (k) => rows.reduce((n, r) => n + (Number(r[k]) || 0), 0);
  const ja = sum("ja");
  const nein = sum("nein");
  const enth = sum("enthalten");
  const ergebnis = d.angenommen == null ? "" : d.angenommen ? "Angenommen" : "Abgelehnt";
  const stimmen = `${ja} Ja, ${nein} Nein, ${enth} ${enth === 1 ? "Enthaltung" : "Enthaltungen"}`;
  const kern = [
    `Namentliche Abstimmung im Bundestag${datum ? ` am ${datumLang(datum)}` : ""}: ${stimmen}.`,
    ergebnis ? `${ergebnis}.` : "",
  ]
    .filter(Boolean)
    .join(" ");
  const beschreibung = kuerzen(
    `${kern} ${d.kurztext || "Alle Einzelstimmen nach Fraktion und Abgeordneten."}`,
    300
  );

  const zeilen = rows
    .map(
      (r) =>
        `<tr><th scope="row" style="text-align:left">${esc(r.partei)}</th><td>${Number(r.ja) || 0}</td><td>${Number(r.nein) || 0}</td><td>${Number(r.enthalten) || 0}</td><td>${Number(r.abwesend) || 0}</td></tr>`
    )
    .join("");
  const inhalt = [
    RAHMEN_ANFANG,
    `<h1>${esc(titel)}</h1>`,
    `<p>${esc(kern)}</p>`,
    d.kurztext ? `<p>${esc(d.kurztext)}</p>` : "",
    `<table><caption>Stimmen nach Fraktion</caption><thead><tr><th></th><th>Ja</th><th>Nein</th><th>Enthalten</th><th>Nicht abgestimmt</th></tr></thead><tbody>${zeilen}</tbody></table>`,
    d.intro ? `<p>${esc(kuerzen(d.intro, 1500))}</p>` : "",
    RAHMEN_ENDE,
  ].join("");

  return {
    titel: kuerzen(titel, 110),
    beschreibung,
    pfad: `/bundestag/${pollId}`,
    inhalt,
    daten: {
      "@context": "https://schema.org",
      "@type": "Article",
      headline: kuerzen(titel, 110),
      description: beschreibung,
      datePublished: datum || undefined,
      mainEntityOfPage: `${BASIS}/bundestag/${pollId}`,
      publisher: { "@id": "https://respublica.media/#organization" },
      isAccessibleForFree: true,
    },
  };
}

async function seiteAbgeordnete(awId) {
  const pool = getPool();
  const [rows] = await pool.query(
    `SELECT name, fraktion, wahlkreis FROM abgeordnete WHERE aw_id = ? LIMIT 1`,
    [awId]
  );
  if (!rows.length) return null;
  const p = rows[0];
  const [stimmen] = await pool.query(
    `SELECT v.poll_id, v.vote, a.poll_titel, a.poll_datum
       FROM votes v
       JOIN (SELECT poll_id, MAX(poll_titel) AS poll_titel, MAX(poll_datum) AS poll_datum
               FROM abstimmungen GROUP BY poll_id) a ON a.poll_id = v.poll_id
      WHERE v.mandate_id = ?
      ORDER BY a.poll_datum DESC, v.poll_id DESC
      LIMIT 40`,
    [awId]
  );
  const name = String(p.name || "").trim() || `Abgeordnete ${awId}`;
  const zusatz = [p.fraktion, p.wahlkreis ? `Wahlkreis ${p.wahlkreis}` : ""].filter(Boolean).join(", ");
  const beschreibung = kuerzen(
    `Wie ${name}${p.fraktion ? ` (${p.fraktion})` : ""} im Bundestag abstimmt: alle namentlichen Abstimmungen und wo die Stimme von der eigenen Fraktion abweicht.${p.wahlkreis ? ` Wahlkreis ${p.wahlkreis}.` : ""}`,
    300
  );
  const liste = stimmen
    .map(
      (s) =>
        `<li><a href="/bundestag/${Number(s.poll_id)}">${esc(s.poll_titel)}</a>${s.poll_datum ? `, ${esc(datumLang(formatDate(s.poll_datum)))}` : ""}: ${esc(STIMME[s.vote] || "")}</li>`
    )
    .join("");
  const inhalt = [
    RAHMEN_ANFANG,
    `<h1>${esc(name)}: Abstimmungen im Bundestag</h1>`,
    zusatz ? `<p>${esc(zusatz)}</p>` : "",
    liste ? `<h2>Letzte namentliche Abstimmungen</h2><ul>${liste}</ul>` : "",
    '<p><a href="/bundestag?ansicht=abgeordnete">Alle Abgeordneten</a></p>',
    RAHMEN_ENDE,
  ].join("");

  return {
    titel: `${name}: Abstimmungen im Bundestag`,
    beschreibung,
    pfad: `/bundestag/abgeordnete/${awId}`,
    inhalt,
    daten: {
      "@context": "https://schema.org",
      "@type": "ProfilePage",
      mainEntity: {
        "@type": "Person",
        name,
        ...(p.fraktion ? { memberOf: { "@type": "Organization", name: p.fraktion } } : {}),
      },
      mainEntityOfPage: `${BASIS}/bundestag/abgeordnete/${awId}`,
    },
  };
}

async function seiteGesetz(id) {
  const [rows] = await getPool().query(
    `SELECT a.id, a.datum, a.zusammenfassung, a.kontext, a.bgbl_referenz, a.poll_id,
            COALESCE(NULLIF(TRIM(g.titel_offiziell), ''), NULLIF(TRIM(g.name), ''), g.kuerzel) AS name,
            g.kuerzel
       FROM aenderungen a
       INNER JOIN gesetze g ON g.id = a.gesetz_id
      WHERE a.id = ? LIMIT 1`,
    [id]
  );
  if (!rows.length) return null;
  const r = rows[0];
  const datum = formatDate(r.datum);
  const name = String(r.name || r.kuerzel || `Gesetz ${id}`).trim();
  const titel = `${kuerzen(name, 80)}: Änderung${datum ? ` vom ${datumLang(datum)}` : ""}`;
  const beschreibung = kuerzen(
    r.zusammenfassung ||
      `Was sich am Gesetz ${name} geändert hat${datum ? `, Stand ${datumLang(datum)}` : ""}, mit Fundstelle und Wortlaut im Vergleich.`,
    300
  );
  const inhalt = [
    RAHMEN_ANFANG,
    `<h1>${esc(name)}</h1>`,
    `<p>Änderung${datum ? ` vom ${esc(datumLang(datum))}` : ""}${r.bgbl_referenz ? `, ${esc(r.bgbl_referenz)}` : ""}</p>`,
    r.zusammenfassung ? `<p>${esc(r.zusammenfassung)}</p>` : "",
    r.kontext ? `<p>${esc(kuerzen(r.kontext, 1500))}</p>` : "",
    r.poll_id ? `<p><a href="/bundestag/${Number(r.poll_id)}">Namentliche Abstimmung dazu</a></p>` : "",
    RAHMEN_ENDE,
  ].join("");

  return {
    titel,
    beschreibung,
    pfad: `/gesetze/${id}`,
    inhalt,
    daten: {
      "@context": "https://schema.org",
      "@type": "Article",
      headline: kuerzen(titel, 110),
      description: beschreibung,
      datePublished: datum || undefined,
      mainEntityOfPage: `${BASIS}/gesetze/${id}`,
      publisher: { "@id": "https://respublica.media/#organization" },
      isAccessibleForFree: true,
    },
  };
}

/** Pfad der App auf die passende Seite abbilden. null: keine Einzelseite. */
async function seiteFuerPfad(pfad) {
  let m = /^\/bundestag\/(\d{1,9})\/?$/.exec(pfad);
  if (m) return seiteAbstimmung(Number(m[1]));
  m = /^\/bundestag\/abgeordnete\/(\d{1,9})\/?$/.exec(pfad);
  if (m) return seiteAbgeordnete(Number(m[1]));
  m = /^\/(?:gesetze|gesetzgebung)\/(\d{1,9})\/?$/.exec(pfad);
  if (m) return seiteGesetz(Number(m[1]));
  return null;
}

router.get(
  "/seiten/html",
  asyncHandler(async (req, res) => {
    const pfad = typeof req.query.pfad === "string" ? req.query.pfad : "";
    const html = indexHtml();
    if (!html) {
      // Ohne index.html der App gibt es nichts einzusetzen. nginx liefert
      // dann die statische Datei selbst aus.
      res.status(503).json({ error: "index.html der App nicht lesbar" });
      return;
    }
    const seite = await seiteFuerPfad(pfad);
    if (!seite) {
      res.status(404).json({ error: "Nicht gefunden" });
      return;
    }
    res.set("Content-Type", "text/html; charset=utf-8");
    // Wie die index.html der App: nie zwischenspeichern, sonst zeigt sie nach
    // einem Deploy auf Skripte, die es nicht mehr gibt.
    res.set("Cache-Control", "no-store, no-cache, must-revalidate");
    res.send(seiteEinsetzen(html, seite));
  })
);

const FESTE_SEITEN = [
  ["/", "daily", "1.0"],
  ["/bundestag", "daily", "0.9"],
  ["/wahlen", "weekly", "0.9"],
  ["/wahlen/umfragen", "daily", "0.9"],
  ["/wahlen/kandidaturen", "weekly", "0.8"],
  ["/gesetze", "daily", "0.9"],
  ["/kleine-anfragen", "daily", "0.8"],
  ["/lobbyregister", "weekly", "0.8"],
  ["/eu-recht", "weekly", "0.7"],
  ["/europa", "daily", "0.7"],
  ["/europa/strom", "daily", "0.8"],
  ["/europa/vergleich", "daily", "0.6"],
  ["/europa/parlament", "daily", "0.8"],
  ["/weltkarte", "monthly", "0.6"],
  ["/demokratie", "monthly", "0.6"],
  ["/versammlungen", "weekly", "0.6"],
  ["/quellen", "monthly", "0.4"],
];

let sitemapPuffer = { zeit: 0, xml: null };

async function sitemapXml() {
  const pool = getPool();
  const heute = new Date().toISOString().slice(0, 10);
  const eintraege = FESTE_SEITEN.map(([pfad, freq, prio]) => ({ pfad, lastmod: heute, freq, prio }));

  const [polls] = await pool.query(
    `SELECT poll_id, MAX(poll_datum) AS datum FROM abstimmungen
      WHERE poll_id IS NOT NULL GROUP BY poll_id ORDER BY datum DESC`
  );
  for (const p of polls) {
    eintraege.push({ pfad: `/bundestag/${p.poll_id}`, lastmod: formatDate(p.datum), freq: "monthly", prio: "0.7" });
  }

  const [personen] = await pool.query(
    `SELECT aw_id, updated_at FROM abgeordnete
      WHERE aw_id IS NOT NULL AND parliament_period = ? AND typ = 'mandat'
      ORDER BY aw_id`,
    [BUNDESTAG_PERIOD]
  );
  for (const p of personen) {
    eintraege.push({
      pfad: `/bundestag/abgeordnete/${p.aw_id}`,
      lastmod: formatDate(p.updated_at),
      freq: "weekly",
      prio: "0.6",
    });
  }

  // Nur Aenderungen mit Zusammenfassung: ohne Text waere die Seite fuer
  // Suchende leer und zieht den Rest herunter.
  const [gesetze] = await pool.query(
    `SELECT id, datum FROM aenderungen
      WHERE zusammenfassung IS NOT NULL AND TRIM(zusammenfassung) <> ''
      ORDER BY datum DESC, id DESC LIMIT ?`,
    [Math.max(0, SITEMAP_HOECHSTENS - eintraege.length)]
  );
  for (const g of gesetze) {
    eintraege.push({ pfad: `/gesetze/${g.id}`, lastmod: formatDate(g.datum), freq: "yearly", prio: "0.5" });
  }

  const urls = eintraege
    .slice(0, SITEMAP_HOECHSTENS)
    .map(
      (e) =>
        `  <url><loc>${esc(BASIS + e.pfad)}</loc>${e.lastmod ? `<lastmod>${e.lastmod}</lastmod>` : ""}<changefreq>${e.freq}</changefreq><priority>${e.prio}</priority></url>`
    )
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls}\n</urlset>\n`;
}

router.get(
  "/seiten/sitemap.xml",
  asyncHandler(async (_req, res) => {
    const jetzt = Date.now();
    if (!sitemapPuffer.xml || jetzt - sitemapPuffer.zeit > SITEMAP_HALTBARKEIT_MS) {
      sitemapPuffer = { zeit: jetzt, xml: await sitemapXml() };
    }
    res.set("Content-Type", "application/xml; charset=utf-8");
    res.set("Cache-Control", "public, max-age=3600");
    res.send(sitemapPuffer.xml);
  })
);

module.exports = router;
module.exports.seiteEinsetzen = seiteEinsetzen;
module.exports.kuerzen = kuerzen;
module.exports.seiteFuerPfad = seiteFuerPfad;
