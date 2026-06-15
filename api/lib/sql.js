"use strict";

// Sichere Behandlung von SQL-IDENTIFIERN (Spalten-/Sortiernamen), die — anders
// als Werte — nicht per `?` parametrisiert werden koennen und daher interpoliert
// werden muessen (M-024). Zentraler, getesteter Chokepoint statt verstreuter
// `Set.has()`-Checks: verhindert, dass kuenftiger Code eine Spalte ohne
// Allowlist interpoliert.

const { ValidationError } = require("./errors");

/**
 * Gibt `value` zurueck, wenn es in der Allowlist ist, sonst ValidationError(400).
 * Nur fuer Identifier verwenden (Spaltennamen etc.) — niemals fuer Werte (dort `?`).
 * @param {unknown} value
 * @param {Set<string>|string[]} allowed
 * @param {string} label - fuer die Fehlermeldung
 * @returns {string}
 */
function requireIdent(value, allowed, label = "Spalte") {
  const v = String(value);
  const ok = allowed instanceof Set ? allowed.has(v) : Array.isArray(allowed) && allowed.includes(v);
  if (!ok) throw new ValidationError(`Ungültige ${label}: ${v}`);
  return v;
}

module.exports = { requireIdent };
