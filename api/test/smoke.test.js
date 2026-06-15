"use strict";

// Smoke-Tests fuer die Res.Publica-API (M-012).
// Die API ist read-only (nur GET) -> diese Tests koennen gefahrlos gegen die
// Live-DB laufen (kein Seed noetig). Ziel: jeder Endpoint ist verdrahtet und
// kein Handler crasht (5xx). List-/Stats-Endpoints muessen 200 liefern;
// parametrisierte/Query-Endpoints duerfen 400/404 liefern (Handler lief), aber
// niemals 5xx.

const test = require("node:test");
const assert = require("node:assert");
const request = require("supertest");

const app = require("../index.js");
const { getPool } = require("../lib/db");
const agent = request(app);

// Pool nach allen Tests schliessen, sonst haelt der offene DB-Pool den Prozess
// am Leben (Node 18 hat kein --test-force-exit).
test.after(async () => {
  await getPool().end();
});

// Endpoints ohne Pflicht-Parameter, die 200 liefern muessen.
const EXPECT_200 = [
  "/api/health",
  "/api/gesetze?limit=1",
  "/api/gesetze/stats",
  "/api/abstimmungen/latest",
  "/api/bundestag/sitzverteilung",
  "/api/bundestag/abgeordnete",
  "/api/abgeordnete",
  "/api/bundestag/abstimmungen",
  "/api/eu-recht/stats",
  "/api/eu-recht?limit=1",
  "/api/eu-urteile/stats",
  "/api/eu-urteile?limit=1",
  "/api/urteile?limit=1",
  "/api/lobbyregister?limit=1",
  "/api/lobbyregister/stats",
  "/api/lobbyregister/by-field",
  "/api/lobbyregister/by-city",
  "/api/lobbyregister/by-time",
  "/api/lobby-projects/stats",
  "/api/wahlen/types",
  "/api/wahlen/years?typ=federal",
  "/api/wahlen/states",
  "/api/wahlen/stats",
  "/api/world/categories",
  "/api/world/indicators",
  "/api/world/sources",
  "/api/world/stats",
];

// Endpoints, die je nach Query 200/400/404 liefern duerfen, aber kein 5xx.
// Parametrisierte Routen mit plausiblen Werten (DEU existiert sicher); nicht
// existierende IDs muessen 404 liefern (kein Crash) -> ebenfalls < 500.
const NO_5XX = [
  ...EXPECT_200,
  "/api/world/map",
  "/api/world/country/DEU",
  "/api/world/climate/DEU",
  "/api/world/timeseries",
  "/api/world/compare",
  "/api/world/ranking",
  "/api/world/scatter",
  "/api/world/trade/DEU",
  "/api/world/trade/DEU/timeseries",
  "/api/wahlen/map",
  "/api/wahlen/timeseries",
  "/api/wahlen/compare",
  "/api/wahlen/scatter",
  "/api/wahlen/ranking",
  "/api/wahlen/change",
  "/api/wahlen/national-average",
  "/api/wahlen/region/11000000",
  "/api/lobby-projects/by-gesetz-id",
  "/api/lobby-projects/by-law",
  // parametrisierte Detail-Routen mit Dummy-IDs -> erwartet 404, nie 5xx
  "/api/gesetze/999999999",
  "/api/eu-recht/999999999",
  "/api/eu-urteile/999999999",
  "/api/urteile/999999999",
  "/api/bundestag/abgeordnete/999999999",
  "/api/abgeordnete/999999999/votes",
  "/api/bundestag/abstimmungen/999999999",
  "/api/bundestag/poll-votes/999999999",
  "/api/abstimmungen/999999999",
  "/api/lobbyregister/R0000000000",
  "/api/lobbyregister/R0000000000/projects",
  "/api/lobbyregister/R0000000000/gesetze",
];

test("List-/Stats-Endpoints liefern 200", async (t) => {
  for (const ep of EXPECT_200) {
    await t.test(ep, async () => {
      const res = await agent.get(ep);
      assert.strictEqual(res.status, 200, `${ep} -> ${res.status}`);
    });
  }
});

test("Kein Endpoint liefert 5xx (Handler crasht nicht)", async (t) => {
  for (const ep of NO_5XX) {
    await t.test(ep, async () => {
      const res = await agent.get(ep);
      assert.ok(res.status < 500, `${ep} -> ${res.status}`);
    });
  }
});

test("Detail-Route mit echter ID liefert 200 (happy path)", async (t) => {
  // echte gesetz-id holen und den Detail-Endpoint exerzieren
  const list = await agent.get("/api/gesetze?limit=1");
  const id = Array.isArray(list.body) ? list.body[0]?.id : list.body?.data?.[0]?.id;
  if (!id) {
    // CI laeuft gegen ein leeres Schema -> kein Happy-Path moeglich, ueberspringen
    t.skip("keine Daten in DB (z.B. CI gegen leeres Schema)");
    return;
  }
  const res = await agent.get(`/api/gesetze/${id}`);
  assert.strictEqual(res.status, 200, `status ${res.status}`);
});

test("404 fuer unbekannte Route", async () => {
  const res = await agent.get("/api/gibts-nicht");
  assert.strictEqual(res.status, 404);
});
