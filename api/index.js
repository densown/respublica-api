"use strict";

const path = require("path");
const express = require("express");
const cors = require("cors");

const { getPool, DB_NAME } = require("./lib/db");
const { notFoundHandler, errorHandler } = require("./lib/errors");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const PORT = Number.parseInt(process.env.PORT || "3002", 10);

async function ensureAbstimmungenPollIdIndex() {
  const sql = `
    SELECT COUNT(*) AS c
    FROM information_schema.statistics
    WHERE table_schema = ?
      AND table_name = 'abstimmungen'
      AND index_name = 'idx_poll_id'
  `;
  const [[row]] = await getPool().query(sql, [DB_NAME]);
  if (Number(row?.c) > 0) return;
  await getPool().query(
    "ALTER TABLE abstimmungen ADD INDEX idx_poll_id (poll_id)"
  );
}

const app = express();
app.use(cors());
app.use(express.json());

app.use("/api", require("./routes/gesetze"));
app.use("/api", require("./routes/bundestag"));
app.use("/api", require("./routes/urteile"));
app.use("/api", require("./routes/eu"));
app.use("/api", require("./routes/lobby"));
app.use("/api", require("./routes/wahlen"));
app.use("/api", require("./routes/world"));

app.use(notFoundHandler);
app.use(errorHandler);

async function start() {
  try {
    await ensureAbstimmungenPollIdIndex();
    console.log("Ensured index idx_poll_id on abstimmungen.poll_id");
  } catch (err) {
    console.error("Could not ensure idx_poll_id:", err);
  }
  app.listen(PORT, () => {
    console.log(`API listening on port ${PORT}`);
  });
}

void start();
