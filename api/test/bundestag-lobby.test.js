"use strict";

// Drucksachennummern einer Abstimmung fuer den Abgleich mit dem Lobbyregister.

const test = require("node:test");
const assert = require("node:assert");

const { drucksachenVarianten } = require("../routes/bundestag");

test("liest die Nummer aus dem Dokument und baut Schreibweisen mit Nullen", () => {
  const { nummern, varianten } = drucksachenVarianten([
    { titel: "Gesetzentwurf", url: "x", dokument: "Drucksache 21/229" },
    { titel: "Beschlussempfehlung", url: "y", dokument: "Drucksache 21/00229" },
    { titel: "Ohne Dokument", url: "z" },
  ]);
  assert.deepStrictEqual(nummern, ["21/229"]);
  assert.deepStrictEqual(varianten, ["21/229", "21/0229", "21/00229", "21/000229"]);
});

test("kaputte Eingaben ergeben nichts", () => {
  assert.deepStrictEqual(drucksachenVarianten(null), { nummern: [], varianten: [] });
  assert.deepStrictEqual(drucksachenVarianten([{ dokument: "PDF" }, null]), { nummern: [], varianten: [] });
});
