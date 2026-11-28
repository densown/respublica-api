"use strict";

// EU-Parlament mit Daten: Liste, Detail mit Fraktionen und deutschen Stimmen,
// Profil mit Fraktionslinie, Titel nach Sprache, Namensnennung der Quelle.
//
// Schreibt Testzeilen und laeuft deshalb nur in der Wegwerf-Datenbank der CI
// oder mit API_TEST_FIXTURES=1. Nie gegen die Live-Datenbank.

const test = require("node:test");
const assert = require("node:assert");
const request = require("supertest");

const app = require("../index.js");
const { getPool } = require("../lib/db");
const agent = request(app);

const MIT_DATEN = process.env.GITHUB_ACTIONS === "true" || process.env.API_TEST_FIXTURES === "1";
const skip = MIT_DATEN ? false : "schreibt Testzeilen, nur in der CI-Datenbank";

// Abgeordnete 9001..9005: drei Deutsche (zwei EPP, eine SD), ein Franzose (EPP), eine Fraktionslose
const ABG = [
  [9001, "Anna", "Alt", "DEU", "EPP", "CDU", 1],
  [9002, "Bert", "Berg", "DEU", "EPP", "CSU", 1],
  [9003, "Cara", "Cron", "DEU", "SD", "SPD", 1],
  [9004, "Dan", "Dupont", "FRA", "EPP", "LR", 1],
  [9005, "Eva", "Ehemals", "DEU", "NI", "parteilos", 0],
];

test.before(async () => {
  if (!MIT_DATEN) return;
  const db = getPool();
  await db.query("DELETE FROM ep_stimmen WHERE abstimmung_id >= 900000");
  await db.query("DELETE FROM ep_abstimmungen WHERE id >= 900000");
  await db.query("DELETE FROM ep_abgeordnete WHERE id BETWEEN 9001 AND 9005");
  await db.query(
    `INSERT INTO ep_fraktionen (code, name, kurz) VALUES ('EPP', 'European People’s Party', 'EPP'),
     ('SD', 'Socialists and Democrats', 'S&D'), ('NI', 'Non-attached Members', 'Non-attached')
     ON DUPLICATE KEY UPDATE code = code`,
  );
  await db.query(
    "INSERT INTO ep_abgeordnete (id, vorname, nachname, land, fraktion, partei, im_amt, abgerufen_am) VALUES ?",
    [ABG.map((a) => [...a, new Date()])],
  );
  await db.query(
    `INSERT INTO ep_abstimmungen
       (id, zeit, titel_en, titel_de, referenz, art, verfahren, verfahren_typ, ja, nein, enthaltung, nicht_abgestimmt,
        ergebnis, de_ja, de_nein, de_enthaltung, de_nicht, abgerufen_am)
     VALUES
       (900001, '2099-11-26 12:08:29', 'Testlaw on methane', 'Testgesetz zu Methan', 'C10-0001/2099', 'einigung',
        '2099/0001(COD)', 'COD', 3, 1, 0, 1, 'angenommen', 2, 1, 0, 1, NOW()),
       (900002, '2099-11-26 12:07:00', 'Testlaw on methane', NULL, 'C10-0001/2099', 'ablehnungsantrag',
        '2099/0001(COD)', 'COD', 1, 3, 1, 0, 'abgelehnt', 1, 2, 1, 0, NOW()),
       (900003, '2099-11-25 12:00:00', 'Other test resolution', NULL, NULL, 'text',
        NULL, 'RSP', 400, 100, 5, 5, 'angenommen', 0, 0, 0, 0, NOW())`,
  );
  const s = (v, m, pos) => {
    const a = ABG.find((x) => x[0] === m);
    return [v, m, pos, a[3], a[4], a[5]];
  };
  await db.query("INSERT INTO ep_stimmen (abstimmung_id, mitglied_id, position, land, fraktion, partei) VALUES ?", [[
    s(900001, 9001, "ja"), s(900001, 9002, "ja"), s(900001, 9003, "nein"), s(900001, 9004, "ja"), s(900001, 9005, "nicht"),
    // 900002: Anna weicht von der EPP-Mehrheit (nein) ab
    s(900002, 9001, "ja"), s(900002, 9002, "nein"), s(900002, 9003, "enthaltung"), s(900002, 9004, "nein"), s(900002, 9005, "nein"),
  ]]);
});

test.after(async () => {
  if (MIT_DATEN) {
    const db = getPool();
    await db.query("DELETE FROM ep_stimmen WHERE abstimmung_id >= 900000");
    await db.query("DELETE FROM ep_abstimmungen WHERE id >= 900000");
    await db.query("DELETE FROM ep_abgeordnete WHERE id BETWEEN 9001 AND 9005");
  }
  await getPool().end();
});

test("Liste: neueste zuerst, Titel nach Sprache, Quelle mit Lizenz", { skip }, async () => {
  const de = await agent.get("/api/ep/abstimmungen?limit=2");
  assert.strictEqual(de.status, 200);
  assert.deepStrictEqual(de.body.abstimmungen.map((a) => a.id), [900001, 900002]);
  assert.strictEqual(de.body.abstimmungen[0].titel, "Testgesetz zu Methan");
  assert.strictEqual(de.body.abstimmungen[0].titel_uebersetzt, true);
  assert.strictEqual(de.body.abstimmungen[0].titel_original, "Testlaw on methane");
  // ohne Uebersetzung bleibt der englische Titel, und das wird gesagt
  assert.strictEqual(de.body.abstimmungen[1].titel, "Testlaw on methane");
  assert.strictEqual(de.body.abstimmungen[1].titel_uebersetzt, false);
  assert.deepStrictEqual(de.body.abstimmungen[0].deutschland, { ja: 2, nein: 1, enthaltung: 0, nicht: 1 });
  assert.strictEqual(de.body.abstimmungen[0].datum, "2099-11-26");
  assert.strictEqual(de.body.abstimmungen[0].zeit, "2099-11-26T12:08:29");
  assert.strictEqual(de.body.quelle.lizenz, "ODbL 1.0");
  assert.match(de.body.quelle.url, /howtheyvote\.eu/);

  const en = await agent.get("/api/ep/abstimmungen?limit=1&lang=en");
  assert.strictEqual(en.body.abstimmungen[0].titel, "Testlaw on methane");
  assert.strictEqual(en.body.abstimmungen[0].titel_uebersetzt, false);
});

test("Liste: Suche in deutschem und englischem Titel, Filter, knappe Abstimmungen", { skip }, async () => {
  const q = await agent.get("/api/ep/abstimmungen?q=Testgesetz");
  assert.deepStrictEqual(q.body.abstimmungen.map((a) => a.id), [900001]);
  const art = await agent.get("/api/ep/abstimmungen?q=Testlaw&art=ablehnungsantrag");
  assert.deepStrictEqual(art.body.abstimmungen.map((a) => a.id), [900002]);
  const knapp = await agent.get("/api/ep/abstimmungen?q=test&knapp=1");
  assert.ok(!knapp.body.abstimmungen.some((a) => a.id === 900003), "400 zu 100 ist nicht knapp");
  assert.strictEqual((await agent.get("/api/ep/abstimmungen?ergebnis=vielleicht")).status, 400);
  assert.strictEqual((await agent.get("/api/ep/abstimmungen?art=x")).status, 400);
  // Platzhalter in der Suche sind Text, kein Muster
  assert.strictEqual((await agent.get("/api/ep/abstimmungen?q=%25%25%25zzz")).body.gesamt, 0);
});

test("Detail: Fraktionen, Laender, deutsche Einzelstimmen, verwandte Abstimmung", { skip }, async () => {
  const r = await agent.get("/api/ep/abstimmungen/900001");
  assert.strictEqual(r.status, 200);
  const epp = r.body.fraktionen.find((f) => f.code === "EPP");
  assert.deepStrictEqual({ ja: epp.ja, nein: epp.nein, enthaltung: epp.enthaltung, nicht: epp.nicht }, { ja: 3, nein: 0, enthaltung: 0, nicht: 0 });
  assert.strictEqual(epp.kurz, "EPP");
  assert.strictEqual(r.body.fraktionen[0].code, "EPP", "groesste Fraktion zuerst");
  assert.deepStrictEqual(r.body.laender.find((l) => l.land === "FRA"), { land: "FRA", ja: 1, nein: 0, enthaltung: 0, nicht: 0 });
  assert.deepStrictEqual(r.body.deutsche.map((d) => [d.name, d.position]), [
    ["Anna Alt", "ja"], ["Bert Berg", "ja"], ["Cara Cron", "nein"], ["Eva Ehemals", "nicht"],
  ]);
  assert.strictEqual(r.body.deutsche[2].fraktion_kurz, "S&D");
  assert.deepStrictEqual(r.body.verwandt.map((v) => [v.id, v.art]), [[900002, "ablehnungsantrag"]]);
  assert.strictEqual(r.body.url, "https://howtheyvote.eu/votes/900001");
  assert.strictEqual((await agent.get("/api/ep/abstimmungen/899999999")).status, 404);
  assert.strictEqual((await agent.get("/api/ep/abstimmungen/abc")).status, 400);
});

test("Abgeordnete: nur im Amt, Teilnahme, Profil mit Fraktionslinie", { skip }, async () => {
  const liste = await agent.get("/api/ep/abgeordnete?land=DEU");
  const unsere = liste.body.abgeordnete.filter((a) => a.id >= 9001 && a.id <= 9005);
  assert.deepStrictEqual(unsere.map((a) => a.name), ["Anna Alt", "Bert Berg", "Cara Cron"]);
  assert.strictEqual(unsere[0].teilnahme, 100);
  assert.strictEqual((await agent.get("/api/ep/abgeordnete?land=deutschland")).status, 400);

  // Laenderseite: Sitze, Fraktionen, juengste Abstimmungen mit den Stimmen des Landes
  const landDe = await agent.get("/api/ep/land/deu?limit=3");
  assert.strictEqual(landDe.status, 200);
  assert.ok(landDe.body.sitze >= 3);
  assert.strictEqual(landDe.body.fraktionen.reduce((n, f) => n + f.sitze, 0), landDe.body.sitze);
  assert.ok(landDe.body.letzte.length <= 3);
  for (const a of landDe.body.letzte) {
    assert.deepStrictEqual(Object.keys(a.land).sort(), ["enthaltung", "ja", "nein", "nicht"]);
  }
  assert.strictEqual(landDe.body.quelle.slug, "howtheyvote");
  assert.strictEqual((await agent.get("/api/ep/land/deutschland")).status, 400);
  assert.strictEqual((await agent.get("/api/ep/land/XXX")).status, 404);

  const anna = await agent.get("/api/ep/abgeordnete/9001");
  assert.strictEqual(anna.status, 200);
  assert.deepStrictEqual(anna.body.stimmen, { ja: 2, nein: 0, enthaltung: 0, nicht: 0 });
  // 900001 mit der EPP (ja), 900002 gegen die EPP-Mehrheit (nein): 50 %
  assert.strictEqual(anna.body.mit_fraktion, 50);
  assert.deepStrictEqual(anna.body.letzte.map((v) => [v.id, v.position]), [[900001, "ja"], [900002, "ja"]]);
  assert.match(anna.body.profil_url, /europarl\.europa\.eu\/meps\/de\/9001$/);

  // Fraktionslose haben keine Fraktionslinie, wer nie teilnahm keine Teilnahme ueber 0
  const eva = await agent.get("/api/ep/abgeordnete/9005");
  assert.strictEqual(eva.body.mit_fraktion, null);
  assert.strictEqual(eva.body.teilnahme, 50);
  assert.strictEqual(eva.body.im_amt, false);
  assert.strictEqual((await agent.get("/api/ep/abgeordnete/899999999")).status, 404);
});

test("Stand nennt Quelle und zaehlt", { skip }, async () => {
  const r = await agent.get("/api/ep/stand");
  assert.strictEqual(r.status, 200);
  assert.ok(r.body.abstimmungen >= 3);
  assert.strictEqual(r.body.quelle.slug, "howtheyvote");
});
