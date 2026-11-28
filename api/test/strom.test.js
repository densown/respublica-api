"use strict";

// Strompreise mit Daten: Tagesansicht mit Stundenmitteln aus Viertelstunden,
// Vortag, Zonenverlauf, Kennzahlen aus dem Schnappschuss, Einheit und Quelle.
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

async function aufraeumen(db) {
  await db.query("DELETE FROM strom_preise WHERE zone IN ('TA', 'TB')");
  await db.query("DELETE FROM strom_zonen WHERE code IN ('TA', 'TB')");
  await db.query("DELETE FROM schnappschuesse WHERE schluessel = 'strom_kennzahlen'");
}

test.before(async () => {
  if (!MIT_DATEN) return;
  const db = getPool();
  await aufraeumen(db);
  await db.query(
    `INSERT INTO strom_zonen (code, land, name_de, name_en, quelle_code, spalte, zeile) VALUES
     ('TA', 'DE', 'Testzone A', 'Test zone A', 'TA', 1, 9), ('TB', 'FR', 'Testzone B', 'Test zone B', 'TB', 2, 9)`,
  );
  const zeilen = [];
  // Liefertag 2099-06-02 in Zone TA: Stunde 0 aus vier Viertelstunden (10, 20, 30, 40), Stunde 1 mit -8
  [10, 20, 30, 40].forEach((p, i) => zeilen.push(["TA", `2099-06-01 22:${String(i * 15).padStart(2, "0")}:00`, 15, "2099-06-02", 0, p]));
  zeilen.push(["TA", "2099-06-01 23:00:00", 60, "2099-06-02", 1, -8]);
  // Vortag TA: 50, Zone TB am Liefertag: 100
  zeilen.push(["TA", "2099-05-31 22:00:00", 60, "2099-06-01", 0, 50]);
  zeilen.push(["TB", "2099-06-01 22:00:00", 60, "2099-06-02", 0, 100]);
  await db.query(
    "INSERT INTO strom_preise (zone, zeit_utc, minuten, liefertag, stunde, eur_mwh, abgerufen_am) VALUES ?",
    [zeilen.map((z) => [...z, new Date()])],
  );
  await db.query(
    "INSERT INTO schnappschuesse (schluessel, inhalt, erzeugt_am) VALUES ('strom_kennzahlen', ?, NOW())",
    [JSON.stringify({ jahr: 2099, negativ: { TA: { stunden: 1, tage: 1 } } })],
  );
});

test.after(async () => {
  if (MIT_DATEN) await aufraeumen(getPool());
  await getPool().end();
});

test("Tag: juengster Liefertag ohne Angabe, Stundenmittel, Vortag, Einheit, Quelle", { skip }, async () => {
  const r = await agent.get("/api/strom/tag");
  assert.strictEqual(r.status, 200);
  assert.strictEqual(r.body.datum, "2099-06-02");
  assert.strictEqual(r.body.verfuegbar.bis, "2099-06-02");
  assert.strictEqual(r.body.einheit, "EUR/MWh");
  assert.strictEqual(r.body.quelle.lizenz, "CC BY 4.0");
  const ta = r.body.zonen.find((z) => z.code === "TA");
  assert.strictEqual(ta.name, "Testzone A");
  assert.strictEqual(ta.stunden.length, 24);
  assert.strictEqual(ta.stunden[0], 25, "Mittel der vier Viertelstunden");
  assert.strictEqual(ta.stunden[1], -8);
  assert.strictEqual(ta.stunden[2], null, "fehlende Stunde bleibt leer, nicht 0");
  assert.strictEqual(ta.mittel, 8.5);
  assert.strictEqual(ta.min, -8);
  assert.strictEqual(ta.max, 25);
  assert.strictEqual(ta.vortag_mittel, 50);
  assert.strictEqual(r.body.zonen.find((z) => z.code === "TB").vortag_mittel, null);
});

test("Tag: bestimmtes Datum, englische Namen, Zone ohne Daten bleibt leer", { skip }, async () => {
  const r = await agent.get("/api/strom/tag?datum=2099-06-01&lang=en");
  assert.strictEqual(r.body.datum, "2099-06-01");
  const ta = r.body.zonen.find((z) => z.code === "TA");
  assert.strictEqual(ta.name, "Test zone A");
  assert.strictEqual(ta.mittel, 50);
  const tb = r.body.zonen.find((z) => z.code === "TB");
  assert.strictEqual(tb.mittel, null);
  assert.strictEqual(tb.min, null);
  assert.strictEqual((await agent.get("/api/strom/tag?datum=gestern")).status, 400);
});

test("Zone: Tageswerte mit negativen Stunden", { skip }, async () => {
  const r = await agent.get("/api/strom/zone/TA?tage=30");
  assert.strictEqual(r.status, 200);
  assert.deepStrictEqual(r.body.tage.map((t) => t.datum), ["2099-06-01", "2099-06-02"]);
  const t = r.body.tage[1];
  assert.strictEqual(t.mittel, 8.5);
  assert.strictEqual(t.min, -8);
  assert.strictEqual(t.max, 40);
  assert.strictEqual(t.negativ_stunden, 1);
  assert.strictEqual(r.body.einheit, "EUR/MWh");
  assert.strictEqual((await agent.get("/api/strom/zone/ZZ")).status, 404);

  // Vergleich: mehrere Zonen, gleiche Tagesachse, Reihenfolge wie angefragt
  const v = await agent.get("/api/strom/verlauf?zonen=TA&tage=30");
  assert.strictEqual(v.status, 200);
  assert.strictEqual(v.body.einheit, "EUR/MWh");
  assert.strictEqual(v.body.zonen[0].code, "TA");
  assert.strictEqual(v.body.zonen[0].werte.length, v.body.tage.length);
  assert.strictEqual((await agent.get("/api/strom/verlauf")).status, 400);
  assert.strictEqual((await agent.get("/api/strom/verlauf?zonen=A1,B2,C3,D4,E5")).status, 400);
  assert.strictEqual((await agent.get("/api/strom/verlauf?zonen=ZZ")).status, 404);
  assert.strictEqual((await agent.get("/api/strom/zone/zu-lang-und-falsch")).status, 400);
});

test("Kennzahlen kommen fertig aus dem Schnappschuss", { skip }, async () => {
  const r = await agent.get("/api/strom/kennzahlen");
  assert.strictEqual(r.status, 200);
  assert.strictEqual(r.body.kennzahlen.jahr, 2099);
  assert.deepStrictEqual(r.body.kennzahlen.negativ.TA, { stunden: 1, tage: 1 });
  assert.ok(r.body.berechnet);
});
