"use strict";

const path = require("path");
const express = require("express");
const cors = require("cors");

const { notFoundHandler, errorHandler } = require("./lib/errors");
const { getPool } = require("./lib/db");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

// Prozessweite Sicherheitsnetze: ohne diese kann eine Rejection/Exception
// ausserhalb des Request-Zyklus den Prozess unprotokolliert beenden.
process.on("unhandledRejection", (err) => {
  console.error("unhandledRejection", err);
});
process.on("uncaughtException", (err) => {
  console.error("uncaughtException", err);
  process.exit(1); // sauberer Neustart durch PM2 statt undefinierter Zustand
});

const PORT = Number.parseInt(process.env.PORT || "3002", 10);

const app = express();

// Erlaubte Browser-Origins. Das Dashboard (app.respublica.media) ruft die API
// cross-origin unter api.respublica.media auf (VITE_API_BASE), daher ist CORS
// hier wirklich noetig. Requests ohne Origin (curl, Uptime-Checks, Server-zu-
// Server) werden durchgelassen. Die API ist read-only/public -> CORS ist Hygiene.
const ALLOWED_ORIGINS = new Set([
  "https://app.respublica.media",
  "https://staging.respublica.media",
  "https://respublica.media",
  "https://www.respublica.media",
  "http://localhost:5173", // vite dev
  "http://localhost:4173", // vite preview
]);
app.use(
  cors({
    origin(origin, cb) {
      if (!origin || ALLOWED_ORIGINS.has(origin)) return cb(null, true);
      return cb(null, false); // kein ACAO-Header -> Browser blockt, kein Fehler
    },
    methods: ["GET"],
  })
);
app.use(express.json());

// Health-Check fuer PM2/Monitoring (pruegt DB-Erreichbarkeit).
app.get("/api/health", async (req, res) => {
  try {
    await getPool().query("SELECT 1");
    res.json({ status: "ok" });
  } catch (err) {
    res.status(503).json({ status: "db_down" });
  }
});

app.use("/api", require("./routes/gesetze"));
app.use("/api", require("./routes/bundestag"));
app.use("/api", require("./routes/urteile"));
app.use("/api", require("./routes/eu"));
app.use("/api", require("./routes/lobby"));
app.use("/api", require("./routes/wahlen"));
app.use("/api", require("./routes/world"));

app.use(notFoundHandler);
app.use(errorHandler);

app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});
