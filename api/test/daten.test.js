"use strict";

// Prueft Verhalten mit Daten: Ausschnitte der Gesetze-Liste, Status von
// Wahlterminen nach dem Wahltag, Umfragen nur bis zum Wahltag, Datum in der
// Zukunft bei EU-Recht und Weltdaten, Gesamtsumme im Lobbyregister.
//
// Schreibt Testzeilen in die Datenbank und laeuft deshalb nur in der
// Wegwerf-Datenbank der CI (GitHub Actions) oder mit API_TEST_FIXTURES=1
// gegen eine eigene Testdatenbank. Nie gegen die Live-Datenbank.

const test = require("node:test");
const assert = require("node:assert");
const request = require("supertest");

const app = require("../index.js");
const { getPool } = require("../lib/db");
const agent = request(app);

const MIT_DATEN = process.env.GITHUB_ACTIONS === "true" || process.env.API_TEST_FIXTURES === "1";
const skip = MIT_DATEN ? false : "schreibt Testzeilen, nur in der CI-Datenbank";

test.before(async () => {
  if (!MIT_DATEN) return;
  const db = getPool();
  await db.query(
    `INSERT INTO gesetze (id, kuerzel, name, titel_offiziell, pfad)
     VALUES (901, 'ta', 'Test A', 'Testgesetz A', 'ta'), (902, 'tb', 'Test B', NULL, 'tb')`,
  );
  // Aenderung i gehoert zu A (ungerade) oder B (gerade); jede dritte ohne Zusammenfassung
  const zeilen = [];
  for (let i = 1; i <= 10; i++) {
    zeilen.push([9000 + i, i % 2 ? 901 : 902, `2026-10-${String(i).padStart(2, "0")}`, i % 3 ? `Zusammenfassung ${i}` : null]);
  }
  await db.query("INSERT INTO aenderungen (id, gesetz_id, datum, zusammenfassung) VALUES ?", [zeilen]);
  await db.query(
    `INSERT INTO wahltermine (id, slug, ebene, land, name_de, name_en, datum, dawum_parliament_id, status) VALUES
     (901, 'test-mv-2021', 'land', 'MV', 'MV 2021', 'MV 2021', '2021-09-26', 8, 'abgeschlossen'),
     (902, 'test-mv-2026', 'land', 'MV', 'MV 2026', 'MV 2026', '2026-09-20', 8, 'kommend'),
     (903, 'test-sh-2099', 'land', 'SH', 'SH 2099', 'SH 2099', '2099-04-18', 15, 'kommend'),
     (904, 'test-btw', 'bund', NULL, 'BTW', 'BTW', NULL, 0, 'kommend')`,
  );
  await db.query(`INSERT INTO parteien (id, kuerzel, name, sortierung) VALUES (901, 'tcdu', 'CDU', 1), (902, 'tspd', 'SPD', 2)`);
  await db.query(
    `INSERT INTO umfragen (id, dawum_survey_id, wahltermin_id, institut, veroeffentlicht) VALUES
     (901, 90101, 902, 'Infratest', '2026-09-01'),
     (902, 90102, 902, 'Forsa', '2026-09-20'),
     (903, 90103, 902, 'INSA', '2026-10-05')`,
  );
  await db.query(
    `INSERT INTO umfrage_werte (umfrage_id, partei_id, prozent) VALUES
     (901, 901, 30), (901, 902, 20), (902, 901, 31), (902, 902, 21), (903, 901, 25), (903, 902, 25)`,
  );
  // EU-Rechtsakt mit Tippfehler im Datum (wie 32026R2053: 2029 statt 2026)
  await db.query(
    `INSERT INTO eu_rechtsakte (id, celex, titel_de, typ, datum) VALUES
     (90901, 'TEST2026R0001', 'Test vom 1. September 2026', 'REG', '2026-09-01'),
     (90902, 'TEST2026R0002', 'Test vom 10. September 2026', 'REG', '2099-09-10')`,
  );
  // Messwerte bis 2020 und eine Projektion fuer 2099
  await db.query(
    `INSERT INTO data_sources (id, slug, name, provider) VALUES (901, 'test', 'Test', 'Test')`,
  );
  await db.query(
    `INSERT INTO data_indicators (id, code, source_id, category, name_de, name_en) VALUES
     (901, 'test.mess', 901, 'test', 'Messung', 'Measurement'),
     (902, 'test.projektion', 901, 'test', 'Projektion', 'Projection')`,
  );
  await db.query(
    `INSERT INTO data_values (indicator_id, country_code, year, value) VALUES
     (901, 'DEU', 2019, 1), (901, 'DEU', 2020, 2), (902, 'DEU', 2099, 3)`,
  );
  // A ist in zwei Gebieten taetig, C nicht mehr aktiv
  const foi = (...codes) => JSON.stringify(codes.map((code) => ({ code, de: code, en: code })));
  await db.query(
    `INSERT INTO lobbyregister (id, register_number, name, active, financial_expenses_euro, fields_of_interest) VALUES
     (901, 'TEST001', 'A', 1, 100000, ?),
     (902, 'TEST002', 'B', 1, 50000, ?),
     (903, 'TEST003', 'C', 0, 1000000, ?)`,
    [foi("FOI_X", "FOI_Y"), foi("FOI_X"), foi("FOI_X")],
  );
});

test.after(async () => {
  await getPool().end();
});

const testIds = (body) => body.map((x) => x.id).filter((id) => id > 9000);

test("Gesetze ohne limit: ganze Liste wie bisher", { skip }, async () => {
  const res = await agent.get("/api/gesetze");
  assert.strictEqual(res.status, 200);
  assert.strictEqual(testIds(res.body).length, 10);
});

test("Gesetze mit limit und offset", { skip }, async () => {
  const a = await agent.get("/api/gesetze?limit=3");
  assert.strictEqual(a.body.length, 3);
  // Gesetze mit amtlichem Titel zuerst, dann nach Datum absteigend
  assert.deepStrictEqual(testIds(a.body), [9009, 9007, 9005]);
  const b = await agent.get("/api/gesetze?limit=3&offset=3");
  assert.deepStrictEqual(testIds(b.body), [9003, 9001, 9010]);
});

test("Gesetze mit zusammenfassung=1 lassen leere weg", { skip }, async () => {
  const res = await agent.get("/api/gesetze?limit=3&zusammenfassung=1");
  assert.deepStrictEqual(testIds(res.body), [9007, 9005, 9001]);
});

test("Wahltermin nach dem Wahltag gilt als abgeschlossen", { skip }, async () => {
  const kommend = await agent.get("/api/wahltermine?status=kommend");
  const slugs = kommend.body.wahltermine.map((w) => w.slug).filter((s) => s.startsWith("test-"));
  assert.deepStrictEqual(slugs.sort(), ["test-btw", "test-sh-2099"]);

  const alle = await agent.get("/api/wahltermine");
  const mv = alle.body.wahltermine.find((w) => w.slug === "test-mv-2026");
  assert.strictEqual(mv.status, "abgeschlossen");
  // Die Umfrage vom 05.10. gehoert nicht mehr zu dieser Wahl
  assert.strictEqual(mv.umfragen, 2);
  assert.strictEqual(mv.letzte_umfrage, "2026-09-20");
});

test("Detail und Zeitreihe ohne Umfragen nach dem Wahltag", { skip }, async () => {
  const detail = await agent.get("/api/wahltermine/test-mv-2026");
  assert.strictEqual(detail.status, 200);
  assert.strictEqual(detail.body.wahl.status, "abgeschlossen");

  const reihe = await agent.get("/api/wahltermine/test-mv-2026/umfragen");
  assert.strictEqual(reihe.status, 200);
  assert.strictEqual(reihe.body.wahl.status, "abgeschlossen");
  assert.deepStrictEqual(reihe.body.umfragen.map((u) => u.institut), ["Infratest", "Forsa"]);
  assert.deepStrictEqual(reihe.body.institute, ["Forsa", "Infratest"]);

  const sh = await agent.get("/api/wahltermine/test-sh-2099");
  assert.strictEqual(sh.body.wahl.status, "kommend");
});

test("EU-Recht: neuestes Datum nicht in der Zukunft", { skip }, async () => {
  const res = await agent.get("/api/eu-recht/stats");
  assert.strictEqual(res.status, 200);
  assert.strictEqual(res.body.latest_datum, "2026-09-01");
});

test("Weltdaten: Jahresspanne ohne Projektionen", { skip }, async () => {
  const res = await agent.get("/api/world/stats");
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual(res.body.years_range, { min: 2019, max: 2020 });
});

test("Lobbyregister: Gesamtsumme zaehlt jede aktive Organisation einmal", { skip }, async () => {
  const res = await agent.get("/api/lobbyregister/by-field");
  assert.strictEqual(res.status, 200);
  assert.strictEqual(res.body.total_expenses, 150000);
  const x = res.body.items.find((i) => i.code === "FOI_X");
  const y = res.body.items.find((i) => i.code === "FOI_Y");
  assert.strictEqual(x.total_expenses, 150000);
  assert.strictEqual(y.total_expenses, 100000);
});
