"use strict";

// Einsetzen von Titel, Beschreibung und Kerntext in die index.html der App.
// Ohne Datenbank: geprueft wird nur die Textarbeit.

const test = require("node:test");
const assert = require("node:assert");

const { seiteEinsetzen, kuerzen } = require("../routes/seiten");

const INDEX = `<!doctype html>
<html lang="de">
  <head>
    <title>Res.Publica · Politische Daten nachprüfbar</title>
    <meta
      name="description"
      content="Allgemein."
    />
    <link rel="canonical" href="https://app.respublica.media/" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="Res.Publica · Politische Daten nachprüfbar" />
    <meta
      property="og:description"
      content="Allgemein."
    />
    <meta property="og:url" content="https://app.respublica.media/" />
    <meta property="og:image" content="https://app.respublica.media/share-og-v3.png" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/assets/index-abc.js"></script>
  </body>
</html>`;

const SEITE = {
  titel: 'Wohngeld "Plus" <2027>',
  beschreibung: "310 Ja, 250 Nein & 3 Enthaltungen.",
  pfad: "/bundestag/6773",
  inhalt: "<main><h1>Wohngeld</h1></main>",
  daten: { "@type": "Article", headline: "</script><script>alert(1)" },
};

test("setzt Titel, Beschreibung, Canonical und Vorschau-Angaben", () => {
  const h = seiteEinsetzen(INDEX, SEITE);
  assert.match(h, /<title>Wohngeld &quot;Plus&quot; &lt;2027&gt; · Res\.Publica<\/title>/);
  assert.match(h, /<meta name="description" content="310 Ja, 250 Nein &amp; 3 Enthaltungen\." \/>/);
  assert.match(h, /<link rel="canonical" href="https:\/\/app\.respublica\.media\/bundestag\/6773" \/>/);
  assert.match(h, /<meta property="og:url" content="https:\/\/app\.respublica\.media\/bundestag\/6773" \/>/);
  assert.match(h, /<meta property="og:type" content="article" \/>/);
  assert.ok(!h.includes("Allgemein."));
  // Bild und Skript der App bleiben unangetastet
  assert.ok(h.includes("share-og-v3.png"));
  assert.ok(h.includes('<script type="module" src="/assets/index-abc.js"></script>'));
});

test("Kerntext steht in #root, JSON-LD kann den script-Block nicht schliessen", () => {
  const h = seiteEinsetzen(INDEX, SEITE);
  assert.ok(h.includes('<div id="root"><main><h1>Wohngeld</h1></main></div>'));
  assert.ok(h.includes('<script type="application/ld+json">'));
  assert.ok(!h.includes("</script><script>alert(1)"));
  assert.ok(h.includes("\\u003c/script>"));
});

test("Dollarzeichen im Text bleiben woertlich", () => {
  const h = seiteEinsetzen(INDEX, { ...SEITE, titel: "Kosten $1 und $&", beschreibung: "$' $`" });
  assert.ok(h.includes("<title>Kosten $1 und $&amp; · Res.Publica</title>"));
});

test("kuerzen schneidet an Wortgrenzen", () => {
  assert.strictEqual(kuerzen("kurz", 10), "kurz");
  const k = kuerzen("Der Bundestag hat über das Gesetz namentlich abgestimmt", 30);
  assert.ok(k.length <= 30);
  assert.ok(k.endsWith("…"));
  assert.ok(!/\s…$/.test(k));
});
