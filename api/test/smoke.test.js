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

// /api/seiten/html liest die index.html der gebauten App. Die gibt es in den
// Tests nicht, daher eine kleine Vorlage.
process.env.DASHBOARD_INDEX =
  process.env.DASHBOARD_INDEX || require("path").join(__dirname, "fixtures", "index.html");

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
  "/api/quellen",
  "/api/quellen?bereich=europa&lang=en",
  "/api/stand",
  "/api/strom/tag",
  "/api/strom/tag?datum=2026-11-28&lang=en",
  "/api/strom/kennzahlen",
  "/api/ep/stand",
  "/api/ep/abstimmungen",
  "/api/ep/abstimmungen?q=methan&ergebnis=angenommen&art=einigung&knapp=1&lang=en&limit=5&offset=10",
  "/api/ep/abgeordnete",
  "/api/ep/abgeordnete?land=FRA",
  "/api/gesetze?limit=1",
  "/api/gesetze?mit_gesamt=1&limit=5",
  "/api/gesetze?mit_gesamt=1&gebiet=sozial&lobby=1&klartitel=1&q=sgb_5&sort=az&offset=20",
  "/api/gesetze/stats",
  "/api/abstimmungen/latest",
  "/api/bundestag/sitzverteilung",
  "/api/bundestag/abgeordnete",
  "/api/abgeordnete",
  "/api/abgeordnete?alle=1",
  "/api/bundestag/abstimmungen",
  "/api/eu-recht/stats",
  "/api/eu-recht?limit=1",
  "/api/eu-urteile/stats",
  "/api/eu-urteile?limit=1",
  "/api/urteile?limit=1",
  "/api/urteile?mit_gesamt=1&limit=5",
  "/api/urteile?mit_gesamt=1&gericht=BGH&rechtsgebiet=Zivilrecht&zeitraum=1y&q=a_1%25&sort=old&offset=20",
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
  "/api/wahlen/national-average?typ=federal&party=spd",
  "/api/world/categories",
  "/api/world/indicators",
  "/api/world/sources",
  "/api/world/stats",
  "/api/versammlungen",
  "/api/versammlungen?von=2026-01-01&bis=2026-12-31&sort=desc",
  "/api/versammlungen/stats",
  "/api/versammlungen/stats?von=2026-09-01&bis=2026-12-31&kategorie=unklassifiziert",
  "/api/versammlungen?buendeln=1&limit=5",
  "/api/versammlungen?typ=unbekannt&von=2018-01-01&limit=5",
  "/api/versammlungen?buendeln=1&von=2026-01-01&bis=2026-12-31&sort=desc",
  "/api/kleine-anfragen",
  "/api/kleine-anfragen?q=Abschiebungen&fraktion=linke&sort=relevanz",
  "/api/kleine-anfragen?q=ab&sachgebiet=Innere%20Sicherheit&min_wert=5&nur_zahlen=1",
  "/api/kleine-anfragen?sort=wert&von=2026-01-01&bis=2026-12-31&wp=21&nicht_erhoben=1",
  "/api/kleine-anfragen/facetten",
  "/api/kleine-anfragen/woche",
  "/api/kleine-anfragen/woche?tage=30&top=5",
  "/api/seiten/sitemap.xml",
];

// Endpoints, die je nach Query 200/400/404 liefern duerfen, aber kein 5xx.
// Parametrisierte Routen mit plausiblen Werten (DEU existiert sicher); nicht
// existierende IDs muessen 404 liefern (kein Crash) -> ebenfalls < 500.
const NO_5XX = [
  ...EXPECT_200,
  "/api/world/map",
  "/api/world/country/DEU",
  "/api/world/climate/DEU",
  "/api/world/steckbrief/DEU",
  "/api/strom/verlauf?zonen=DE-LU,FR&tage=30",
  "/api/ep/land/DEU",
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
  "/api/wahlen/gesamt",
  "/api/wahlen/gesamt?typ=federal&year=1900",
  "/api/wahlen/gesamt?typ=federal&year=2025&state=99",
  "/api/wahlen/region/11000000",
  "/api/lobby-projects/by-gesetz-id",
  "/api/lobby-projects/by-law",
  // parametrisierte Detail-Routen mit Dummy-IDs -> erwartet 404, nie 5xx
  "/api/gesetze/999999999",
  "/api/eu-recht/999999999",
  "/api/eu-urteile/999999999",
  "/api/urteile/999999999",
  "/api/urteile/by-gesetz-id",
  "/api/urteile/by-gesetz-id?gesetz_id=999999999",
  "/api/urteile/by-gesetz-id?gesetz_id=1",
  "/api/bundestag/abgeordnete/999999999",
  "/api/abgeordnete/999999999/votes",
  "/api/bundestag/abstimmungen/999999999",
  "/api/bundestag/abstimmungen/999999999/lobby",
  "/api/bundestag/abstimmungen/abc/lobby",
  "/api/bundestag/poll-votes/999999999",
  "/api/abstimmungen/999999999",
  "/api/lobbyregister/R0000000000",
  "/api/lobbyregister/R0000000000/projects",
  "/api/lobbyregister/R0000000000/gesetze",
  "/api/versammlungen/999999999",
  "/api/versammlungen?von=kaputt",
  "/api/versammlungen?typ=unbekannt",
  "/api/versammlungen?serie=kaputt",
  "/api/versammlungen/stats?von=2026-12-31&bis=2026-01-01",
  "/api/versammlungen/stats?erfassung=unbekannt",
  "/api/kleine-anfragen/21/999999",
  "/api/kleine-anfragen?fraktion=kaputt",
  "/api/kleine-anfragen?sort=kaputt",
  "/api/kleine-anfragen?min_wert=11",
  "/api/kleine-anfragen/woche?tage=0",
  "/api/seiten/html",
  "/api/seiten/html?pfad=/bundestag/999999999",
  "/api/seiten/html?pfad=/bundestag/abgeordnete/999999999",
  "/api/seiten/html?pfad=/gesetze/999999999",
  "/api/seiten/html?pfad=/../etc/passwd",
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
