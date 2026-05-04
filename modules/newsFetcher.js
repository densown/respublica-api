"use strict";

const path = require("path");
const fs = require("fs/promises");
const { execFile } = require("child_process");
const { promisify } = require("util");
const crypto = require("crypto");

function dep(name) {
  try {
    return require(name);
  } catch {
    return require(path.join(__dirname, "..", "api", "node_modules", name));
  }
}

const Parser = dep("rss-parser");
const mysql = dep("mysql2/promise");
const execFileAsync = promisify(execFile);
const parser = new Parser();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

dep("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const DB_NAME = process.env.DB_NAME || "respublica_gesetze";
const FEED_CACHE_TTL_SECONDS = 15 * 60;

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

async function redisGet(key) {
  try {
    const { stdout } = await execFileAsync("redis-cli", ["GET", key], { timeout: 2500 });
    const out = String(stdout || "").trim();
    return out || null;
  } catch {
    return null;
  }
}

async function redisSetEx(key, ttlSec, value) {
  try {
    await execFileAsync("redis-cli", ["SETEX", key, String(ttlSec), value], { timeout: 2500 });
  } catch {
    // Redis is optional for fetch robustness.
  }
}

function toDateOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.valueOf()) ? null : d;
}

function makeGuid(item, sourceKey) {
  const raw = item.guid || item.id || item.link || item.title;
  if (raw) return String(raw).slice(0, 512);
  const hash = crypto
    .createHash("sha256")
    .update(`${sourceKey}:${item.title || ""}:${item.pubDate || ""}:${item.contentSnippet || ""}`)
    .digest("hex");
  return `hash:${hash}`;
}

function flattenSources(sourcesConfig) {
  const flat = [];
  for (const [category, items] of Object.entries(sourcesConfig)) {
    for (const source of items) {
      const sourceKey = crypto
        .createHash("md5")
        .update(`${category}:${source.label}:${source.url}`)
        .digest("hex");
      flat.push({ ...source, category, source_key: sourceKey });
    }
  }
  return flat;
}

async function fetchAllNews() {
  const cfgPath = path.join(__dirname, "..", "config", "news-sources.json");
  const raw = await fs.readFile(cfgPath, "utf8");
  const sources = flattenSources(JSON.parse(raw));
  const db = getPool();

  let totalInserted = 0;
  let totalErrors = 0;

  for (const source of sources) {
    const cacheKey = `feed:${source.source_key}`;
    try {
      const lastFetch = await redisGet(cacheKey);
      if (lastFetch) {
        console.log(`[newsFetcher] skip cached ${source.label} (${source.category})`);
        continue;
      }

      const feed = await parser.parseURL(source.url);
      let insertedForFeed = 0;

      for (const item of feed.items || []) {
        const guid = makeGuid(item, source.source_key);
        const title = (item.title || "").trim();
        if (!guid || !title) continue;

        const description = item.contentSnippet || item.summary || item.description || null;
        const content = item["content:encoded"] || item.content || null;
        const publishedAt = toDateOrNull(item.isoDate || item.pubDate || null);
        const url = item.link || null;

        const [result] = await db.query(
          `INSERT IGNORE INTO news_items
            (guid, title, description, content, url, published_at, source_key, source_name, category, language)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            guid,
            title,
            description,
            content,
            url,
            publishedAt,
            source.source_key,
            source.label,
            source.category,
            source.lang || "de",
          ]
        );
        if (result.affectedRows > 0) insertedForFeed += 1;
      }

      await redisSetEx(cacheKey, FEED_CACHE_TTL_SECONDS, new Date().toISOString());
      totalInserted += insertedForFeed;
      console.log(`[newsFetcher] ${source.label}: +${insertedForFeed} items`);
    } catch (err) {
      totalErrors += 1;
      console.error(`[newsFetcher] ${source.label} failed:`, err?.message || err);
    }
    await sleep(500);
  }

  console.log(`[newsFetcher] done inserted=${totalInserted} feedErrors=${totalErrors}`);
  return { inserted: totalInserted, errors: totalErrors };
}

if (require.main === module) {
  fetchAllNews()
    .then(async () => {
      if (pool) await pool.end();
      process.exit(0);
    })
    .catch(async (err) => {
      console.error("[newsFetcher] fatal:", err);
      if (pool) await pool.end();
      process.exit(1);
    });
}

module.exports = { fetchAllNews };
