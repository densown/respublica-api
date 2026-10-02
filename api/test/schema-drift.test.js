"use strict";

// Prueft, ob api/test/schema.sql alle Tabellen und Spalten aus migrations/
// enthaelt. Die Smoke-Tests laufen gegen dieses Schema; fehlt dort eine neue
// Spalte, liefern Endpoints 500 und main wird rot, obwohl der Code stimmt
// (so geschehen mit 014, 017 und 018). Dieser Test sagt dann direkt, welche
// Migration nachzutragen ist.

const test = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");

const ROOT = path.join(__dirname, "..", "..");
const MIGRATIONS = path.join(ROOT, "migrations");
const SCHEMA = path.join(__dirname, "schema.sql");

const NON_COLUMN = /^(PRIMARY|UNIQUE|KEY|INDEX|CONSTRAINT|FOREIGN|FULLTEXT|SPATIAL|CHECK)\b/i;

function stripComments(sql) {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/^\s*--.*$/gm, " ")
    .replace(/^\s*#.*$/gm, " ");
}

// Trennt an Kommas auf oberster Klammerebene (nicht in DECIMAL(5,2), ENUM(...)).
function splitTopLevel(s) {
  const parts = [];
  let depth = 0;
  let quote = null;
  let cur = "";
  for (const ch of s) {
    if (quote) {
      if (ch === quote) quote = null;
    } else if (ch === "'" || ch === '"') {
      quote = ch;
    } else if (ch === "(") depth++;
    else if (ch === ")") depth--;
    else if (ch === "," && depth === 0) {
      parts.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) parts.push(cur);
  return parts.map((p) => p.trim()).filter(Boolean);
}

const ident = (s) => s.replace(/`/g, "").toLowerCase();

// Liefert Map tabelle -> Set(spalten) fuer eine Folge von SQL-Anweisungen.
function applySql(sql, tables, origin) {
  for (const raw of stripComments(sql).split(";")) {
    const stmt = raw.trim();
    let m = stmt.match(/^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(([\s\S]*)\)[^)]*$/i);
    if (m) {
      const cols = new Map();
      for (const def of splitTopLevel(m[2])) {
        if (NON_COLUMN.test(def)) continue;
        const c = def.match(/^`?(\w+)`?/);
        if (c) cols.set(ident(c[1]), origin);
      }
      tables.set(ident(m[1]), cols);
      continue;
    }
    m = stmt.match(/^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?/i);
    if (m) {
      tables.delete(ident(m[1]));
      continue;
    }
    m = stmt.match(/^ALTER\s+TABLE\s+`?(\w+)`?\s+([\s\S]*)$/i);
    if (!m) continue;
    const table = ident(m[1]);
    if (!tables.has(table)) tables.set(table, new Map());
    const cols = tables.get(table);
    for (const clause of splitTopLevel(m[2])) {
      let c;
      if ((c = clause.match(/^ADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?/i))) {
        if (!NON_COLUMN.test(c[1])) cols.set(ident(c[1]), origin);
      } else if ((c = clause.match(/^CHANGE\s+(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?`?(\w+)`?\s+`?(\w+)`?/i))) {
        cols.delete(ident(c[1]));
        cols.set(ident(c[2]), origin);
      } else if ((c = clause.match(/^RENAME\s+COLUMN\s+`?(\w+)`?\s+TO\s+`?(\w+)`?/i))) {
        cols.delete(ident(c[1]));
        cols.set(ident(c[2]), origin);
      } else if ((c = clause.match(/^DROP\s+(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?`?(\w+)`?/i))) {
        if (!NON_COLUMN.test(c[1])) cols.delete(ident(c[1]));
      }
    }
  }
  return tables;
}

test("test/schema.sql enthaelt alle Tabellen und Spalten aus migrations/", () => {
  const expected = new Map();
  const files = fs
    .readdirSync(MIGRATIONS)
    .filter((f) => /^\d+.*\.sql$/.test(f))
    .sort();
  for (const f of files) {
    applySql(fs.readFileSync(path.join(MIGRATIONS, f), "utf8"), expected, f);
  }

  const actual = applySql(fs.readFileSync(SCHEMA, "utf8"), new Map(), "schema.sql");

  const missing = [];
  for (const [table, cols] of expected) {
    const have = actual.get(table);
    if (!have) {
      const from = [...new Set(cols.values())].join(", ") || "?";
      missing.push(`Tabelle ${table} (aus ${from})`);
      continue;
    }
    for (const [col, from] of cols) {
      if (!have.has(col)) missing.push(`${table}.${col} (aus ${from})`);
    }
  }

  assert.deepStrictEqual(
    missing,
    [],
    "In api/test/schema.sql fehlt, was die Migrationen anlegen:\n  " +
      missing.join("\n  ") +
      "\nMigration in schema.sql nachziehen (oder schema.sql neu per mysqldump --no-data erzeugen).",
  );
});
