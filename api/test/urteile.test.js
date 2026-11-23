"use strict";

// Schreibweisen eines Gesetzes in urteil_gesetze. Ohne Datenbank.

const test = require("node:test");
const assert = require("node:assert");

const { kuerzelVarianten } = require("../routes/urteile");

test("Schluessel von gesetze-im-internet.de zaehlt nicht, die Abkuerzung schon", () => {
  assert.deepStrictEqual(
    kuerzelVarianten({ kuerzel: "GG-BJNR000010949", amtliche_abkuerzung: "GG" }),
    ["GG"]
  );
});

test("altes Kuerzel ohne amtliche Abkuerzung bleibt nutzbar", () => {
  assert.deepStrictEqual(kuerzelVarianten({ kuerzel: "BGB", amtliche_abkuerzung: null }), ["BGB"]);
});

test("SGB mit arabischer und roemischer Ziffer", () => {
  assert.deepStrictEqual(
    kuerzelVarianten({ kuerzel: "SGB_5-BJNR024820988", amtliche_abkuerzung: "SGB 5" }).sort(),
    ["SGB 5", "SGB V"]
  );
  assert.deepStrictEqual(
    kuerzelVarianten({ kuerzel: "x-BJNR1", amtliche_abkuerzung: "SGB IX" }).sort(),
    ["SGB 9", "SGB IX"]
  );
});

test("ohne brauchbares Kuerzel keine Varianten", () => {
  assert.deepStrictEqual(kuerzelVarianten({ kuerzel: "X-BJNR1", amtliche_abkuerzung: null }), []);
  assert.deepStrictEqual(kuerzelVarianten({ kuerzel: "", amtliche_abkuerzung: "  " }), []);
});
