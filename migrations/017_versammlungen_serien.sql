-- 017: Versammlungen, Parser v2 (Serien, Themen-Cache, Mehrquellenfaehigkeit)
--
-- Befund aus dem ersten echten Abruf (29.09.2026): Berlin listet Serien
-- (Dauermahnwachen, woechentliche Kundgebungen) als eine Zeile PRO TERMIN,
-- das Thema endet dann auf "(vom 01.08. bis 01.10.2026 - taeglich)". Rund
-- drei Viertel der Zeilen gehoeren zu Serien. Ohne Serien-Modell zaehlt eine
-- einzige taegliche Mahnwache 365-mal.
--
-- Loesung: jede Zeile bleibt ein Versammlungstag, `serie_id` fasst die Tage
-- einer Serie zusammen. Gezaehlt werden getrennt
--   versammlungstage  Zeilen
--   versammlungen     verschiedene Serien + Einzeltermine
--
-- `serie_id` haengt bewusst NICHT vom Serienende ab: wird eine Serie
-- verlaengert, aendert Berlin das Suffix, die Serie bleibt aber dieselbe.
--
-- Kategorien haengen am Thema, nicht an der Zeile (`versammlung_themen`):
-- eine taegliche Mahnwache wird einmal klassifiziert, und ein --rebuild
-- verliert keine Kategorien mehr.
--
-- `thema` enthaelt ab jetzt das bereinigte Thema ohne Serien-Suffix
-- (Silbentrennung, ",," usw. korrigiert). Der Originaltext bleibt im
-- Rohdaten-Archiv.
--
-- Nach dem Anwenden: fetch_versammlungen_berlin.py --rebuild (die quelle_id
-- wird jetzt aus dem Thema OHNE Serien-Suffix gebildet).

ALTER TABLE versammlungen
  ADD COLUMN thema_hash      CHAR(40)     NULL AFTER thema,
  ADD COLUMN ganztaegig      TINYINT(1)   NOT NULL DEFAULT 0 AFTER bis,
  ADD COLUMN serie_id        CHAR(40)     NULL AFTER typ,
  ADD COLUMN serie_von       DATE         NULL AFTER serie_id,
  ADD COLUMN serie_bis       DATE         NULL AFTER serie_von,
  ADD COLUMN serie_rhythmus  VARCHAR(100) NULL AFTER serie_bis,
  -- liste:   aus einer vollstaendigen amtlichen Liste (Berlin, Dresden, ...)
  -- bericht: nur aus einer Polizei-/Pressemeldung bekannt (selektiv)
  -- Beides wird nie zusammengezaehlt.
  ADD COLUMN erfassung       ENUM('liste','bericht') NOT NULL DEFAULT 'liste' AFTER serie_rhythmus,
  -- stattgefunden: nur wenn ein Bericht es belegt (ab M3)
  MODIFY COLUMN status ENUM('angezeigt','vergangen','vor_termin_entfernt','stattgefunden')
                       NOT NULL DEFAULT 'angezeigt',
  ADD INDEX idx_thema_hash (thema_hash),
  ADD INDEX idx_serie (serie_id),
  ADD INDEX idx_erfassung_datum (erfassung, datum);

-- Kategorie-Cache: ein Eintrag pro verschiedenem (bereinigtem) Thema.
-- `thema_hash` = sha1 des normalisierten Themas ohne Serien-Suffix.
CREATE TABLE IF NOT EXISTS versammlung_themen (
  thema_hash        CHAR(40)    NOT NULL,
  thema             TEXT        NOT NULL,
  kategorie         VARCHAR(32) NOT NULL,
  klassifiziert_am  DATETIME    NOT NULL,
  PRIMARY KEY (thema_hash),
  INDEX idx_kategorie (kategorie)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
