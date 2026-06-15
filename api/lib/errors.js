"use strict";

/** Erwartbarer Client-Fehler (ungueltige Eingabe) -> HTTP 400 mit Klartext. */
class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = "ValidationError";
    this.statusCode = 400;
    this.isOperational = true;
  }
}

/** Leitet Rejections aus async Route-Handlern an die Error-Middleware weiter. */
const asyncHandler = (fn) => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(next);

/** 404-Fallback für unbekannte Routen (nach allen Routen registrieren). */
function notFoundHandler(req, res) {
  res.status(404).json({ error: "Nicht gefunden" });
}

/** Zentrale Fehlerbehandlung: loggt strukturiert mit Request-Kontext. */
function errorHandler(err, req, res, next) {
  const status = err.statusCode || 500;
  const log = req.log || console;
  if (status >= 500) {
    log.error({ err, method: req.method, url: req.originalUrl }, "request failed");
  } else {
    // 4xx sind erwartbare Client-Fehler -> als warn, ohne Stacktrace-Rauschen
    log.warn({ err: err.message, method: req.method, url: req.originalUrl }, "request rejected");
  }
  // Serverfehler generisch halten; operationale 4xx-Meldungen durchreichen.
  const message = status >= 500 ? "Datenbankfehler" : err.message;
  res.status(status).json({ error: message });
}

module.exports = { ValidationError, asyncHandler, notFoundHandler, errorHandler };
