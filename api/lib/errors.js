"use strict";

/** Leitet Rejections aus async Route-Handlern an die Error-Middleware weiter. */
const asyncHandler = (fn) => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(next);

/** 404-Fallback für unbekannte Routen (nach allen Routen registrieren). */
function notFoundHandler(req, res) {
  res.status(404).json({ error: "Nicht gefunden" });
}

/** Zentrale Fehlerbehandlung: loggt mit Request-Kontext, antwortet generisch. */
function errorHandler(err, req, res, next) {
  console.error(`[${req.method} ${req.originalUrl}]`, err);
  res.status(500).json({ error: "Datenbankfehler" });
}

module.exports = { asyncHandler, notFoundHandler, errorHandler };
