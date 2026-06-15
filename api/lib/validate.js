"use strict";

// Wiederverwendbare Eingabe-Validierung (M-011). Ersetzt copy-paste-Clamping
// in den Routern. Bewusst schlank (kein Zod) fuer ein read-only GET-API.

const { ValidationError } = require("./errors");

/**
 * Liest limit/offset aus den Query-Params und klemmt sie auf sichere Grenzen.
 * @param {object} query - req.query
 * @param {{defLimit?: number, maxLimit?: number}} opts
 * @returns {{limit: number, offset: number}}
 */
function parsePagination(query, { defLimit = 50, maxLimit = 200 } = {}) {
  let limit = Number.parseInt(String(query.limit ?? defLimit), 10);
  let offset = Number.parseInt(String(query.offset ?? "0"), 10);
  if (!Number.isFinite(limit) || limit < 1) limit = defLimit;
  if (limit > maxLimit) limit = maxLimit;
  if (!Number.isFinite(offset) || offset < 0) offset = 0;
  return { limit, offset };
}

/**
 * Parst einen Pflicht-Integer (z.B. :id). Wirft ValidationError(400) bei Unfug.
 * @param {unknown} value
 * @param {string} name - fuer die Fehlermeldung
 * @returns {number}
 */
function parseIntParam(value, name) {
  const n = Number.parseInt(String(value), 10);
  if (!Number.isInteger(n)) {
    throw new ValidationError(`${name} muss eine ganze Zahl sein`);
  }
  return n;
}

module.exports = { parsePagination, parseIntParam };
