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
app.use(cors());
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
