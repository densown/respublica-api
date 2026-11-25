"use strict";

// Filter der seitenweisen Gesetzesliste (/api/gesetze?mit_gesamt=1). Ohne Datenbank.

const test = require("node:test");
const assert = require("node:assert");

const { seitenFilter } = require("../routes/gesetze");

test("ohne Parameter: kein Filter, neueste zuerst", () => {
  const f = seitenFilter({});
  assert.deepStrictEqual(f.where, []);
  assert.deepStrictEqual(f.params, []);
  assert.match(f.orderBy, /^a\.datum DESC/);
});

test("unbekannte Sortierung und unbekanntes Gebiet werden ignoriert", () => {
  const f = seitenFilter({ sort: "a.id; DROP TABLE x", gebiet: "quatsch" });
  assert.match(f.orderBy, /^a\.datum DESC/);
  assert.deepStrictEqual(f.where, []);
});

test("Gebiet, Lobby und Klartitel", () => {
  const f = seitenFilter({ gebiet: "sozial", lobby: "1", klartitel: "1", sort: "old" });
  assert.strictEqual(f.where.length, 3);
  assert.deepStrictEqual(f.params, ["sozial"]);
  assert.match(f.orderBy, /^a\.datum ASC/);
});

test("Suche: klein geschrieben, LIKE-Zeichen woertlich, viermal", () => {
  const f = seitenFilter({ q: "  SGB_5%  " });
  assert.strictEqual(f.where.length, 1);
  assert.deepStrictEqual(f.params, Array(4).fill("%sgb\\_5\\%%"));
});
