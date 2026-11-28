"use strict";

/**
 * Frische einer Datenquelle: ab wann gilt sie als veraltet?
 *
 * Grenze ist data_sources.stale_after_hours. Fehlt sie, gilt die Vorgabe
 * nach dem Takt der Quelle. Die Vorgaben lassen Luft fuer einen ausgefallenen
 * Lauf und fuer Wochenenden, damit die App nicht bei jeder Verspaetung warnt.
 */

const GRENZE_STUNDEN = Object.freeze({
  hourly: 6,
  daily: 48,
  weekly: 10 * 24,
  monthly: 40 * 24,
  quarterly: 110 * 24,
  yearly: 400 * 24,
  manual: null,
});

function grenzeStunden(quelle) {
  const eigene = Number(quelle.stale_after_hours);
  if (Number.isFinite(eigene) && eigene > 0) return eigene;
  return GRENZE_STUNDEN[quelle.update_freq] ?? null;
}

function zeit(v) {
  if (v == null) return null;
  const d = v instanceof Date ? v : new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * Zustand einer Quelle zum Zeitpunkt `jetzt`:
 *   aktuell    letzter erfolgreicher Abruf innerhalb der Grenze
 *   veraltet   letzter Erfolg aelter als die Grenze
 *   unbekannt  noch nie ein Erfolg protokolliert (alte Importe ohne Protokoll)
 *   ohne_takt  von Hand gepflegte Quelle, keine Grenze
 */
function frische(quelle, jetzt = new Date()) {
  const erfolg = zeit(quelle.last_success);
  const grenze = grenzeStunden(quelle);
  const alter = erfolg ? Math.max(0, (jetzt.getTime() - erfolg.getTime()) / 3600000) : null;
  let zustand;
  if (!erfolg) zustand = "unbekannt";
  else if (grenze == null) zustand = "ohne_takt";
  else zustand = alter > grenze ? "veraltet" : "aktuell";
  return {
    zustand,
    letzter_erfolg: erfolg ? erfolg.toISOString() : null,
    alter_stunden: alter == null ? null : Math.round(alter * 10) / 10,
    grenze_stunden: grenze,
  };
}

module.exports = { GRENZE_STUNDEN, grenzeStunden, frische };
