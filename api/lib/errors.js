"use strict";

/** Leitet Rejections aus async Route-Handlern an die Error-Middleware weiter. */
const asyncHandler = (fn) => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(next);

/** 404-Fallback für unbekannte Routen (nach allen Routen registrieren). */
function notFoundHandler(req, res) {
  res.status(404).json({ error: "Nicht gefunden" });
}

/** Zentrale Fehlerbehandlung: loggt strukturiert mit Request-Kontext. */
function errorHandler(err, req, res, next) {
  const log = req.log || console;
  log.error({ err, method: req.method, url: req.originalUrl }, "request failed");
  res.status(500).json({ error: "Datenbankfehler" });
}

module.exports = { asyncHandler, notFoundHandler, errorHandler };
