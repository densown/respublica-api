"use strict";

const path = require("path");

function dep(name) {
  try {
    return require(name);
  } catch {
    return require(path.join(__dirname, "..", "api", "node_modules", name));
  }
}

const mysql = dep("mysql2/promise");
dep("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const DB_NAME = process.env.DB_NAME || "respublica_gesetze";
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions";
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let pool;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST || "localhost",
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD || "",
      database: DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      charset: "utf8mb4",
    });
  }
  return pool;
}

async function summarizeText(title, description, language) {
  const apiKey = process.env.GROQ_API_KEY;
  if (!apiKey) throw new Error("GROQ_API_KEY missing");

  const prompt =
    "Fasse diesen Nachrichtenartikel in 2-3 Sätzen zusammen. Nur die Zusammenfassung, kein Präambel. " +
    "Wenn der Artikel auf Englisch ist, antworte auf Englisch. Wenn auf Deutsch, antworte auf Deutsch. " +
    `Titel: ${title}. Inhalt: ${description || ""}`;

  const response = await fetch(GROQ_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: GROQ_MODEL,
      temperature: 0.2,
      messages: [
        { role: "system", content: "Du fasst Nachrichten präzise und knapp zusammen." },
        { role: "user", content: prompt },
      ],
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Groq request failed ${response.status}: ${body}`);
  }

  const json = await response.json();
  const out = json?.choices?.[0]?.message?.content?.trim();
  if (!out) throw new Error("Empty Groq summary response");
  return out;
}

async function runNewsSummarizer(limit = 50) {
  const db = getPool();
  const [rows] = await db.query(
    `SELECT id, title, description, language
     FROM news_items
     WHERE groq_summary IS NULL
       AND published_at >= DATE_SUB(NOW(), INTERVAL 48 HOUR)
     ORDER BY published_at DESC
     LIMIT ?`,
    [limit]
  );

  console.log(`[newsSummarizer] queued=${rows.length}`);
  let ok = 0;
  let failed = 0;

  for (const row of rows) {
    try {
      const summary = await summarizeText(row.title, row.description, row.language);
      await db.query(
        `UPDATE news_items
         SET groq_summary = ?, summarized_at = NOW()
         WHERE id = ?`,
        [summary, row.id]
      );
      ok += 1;
      console.log(`[newsSummarizer] summarized id=${row.id}`);
    } catch (err) {
      failed += 1;
      console.error(`[newsSummarizer] failed id=${row.id}:`, err?.message || err);
    }
    await sleep(10000);
  }

  console.log(`[newsSummarizer] done ok=${ok} failed=${failed}`);
  return { ok, failed };
}

if (require.main === module) {
  runNewsSummarizer()
    .then(async () => {
      if (pool) await pool.end();
      process.exit(0);
    })
    .catch(async (err) => {
      console.error("[newsSummarizer] fatal:", err);
      if (pool) await pool.end();
      process.exit(1);
    });
}

module.exports = { runNewsSummarizer, summarizeText };
