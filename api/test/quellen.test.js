"use strict";

// Frische einer Quelle: reine Rechnung, ohne Datenbank.

const test = require("node:test");
const assert = require("node:assert");
const { frische, grenzeStunden } = require("../lib/quellen");

const JETZT = new Date("2026-11-28T12:00:00Z");
const vor = (stunden) => new Date(JETZT.getTime() - stunden * 3600000);

test("taegliche Quelle: nach einem Tag aktuell, nach drei Tagen veraltet", () => {
  assert.strictEqual(frische({ update_freq: "daily", last_success: vor(24) }, JETZT).zustand, "aktuell");
  const alt = frische({ update_freq: "daily", last_success: vor(72) }, JETZT);
  assert.strictEqual(alt.zustand, "veraltet");
  assert.strictEqual(alt.alter_stunden, 72);
  assert.strictEqual(alt.grenze_stunden, 48);
});

test("eigene Grenze schlaegt die Vorgabe nach Takt", () => {
  const q = { update_freq: "daily", stale_after_hours: 30, last_success: vor(31) };
  assert.strictEqual(grenzeStunden(q), 30);
  assert.strictEqual(frische(q, JETZT).zustand, "veraltet");
});

test("stuendliche Quelle veraltet nach sechs Stunden", () => {
  assert.strictEqual(frische({ update_freq: "hourly", last_success: vor(5) }, JETZT).zustand, "aktuell");
  assert.strictEqual(frische({ update_freq: "hourly", last_success: vor(7) }, JETZT).zustand, "veraltet");
});

test("ohne protokollierten Erfolg: unbekannt, nicht veraltet", () => {
  const f = frische({ update_freq: "weekly", last_success: null }, JETZT);
  assert.strictEqual(f.zustand, "unbekannt");
  assert.strictEqual(f.alter_stunden, null);
});

test("von Hand gepflegte Quelle hat keine Grenze", () => {
  assert.strictEqual(frische({ update_freq: "manual", last_success: vor(9999) }, JETZT).zustand, "ohne_takt");
});

test("Zeit als Text aus der Datenbank wird gelesen, Unsinn nicht", () => {
  assert.strictEqual(frische({ update_freq: "daily", last_success: "2026-11-28T06:00:00Z" }, JETZT).zustand, "aktuell");
  assert.strictEqual(frische({ update_freq: "daily", last_success: "kaputt" }, JETZT).zustand, "unbekannt");
});
