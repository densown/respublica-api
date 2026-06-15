"use strict";

// Zentraler strukturierter Logger (M-010). Level via LOG_LEVEL (Default: info).
const pino = require("pino");

const logger = pino({
  level: process.env.LOG_LEVEL || "info",
});

module.exports = logger;
