-- 014: Abgeordnete um Wahlperiode, Typ und Partei erweitern
--
-- Bisher hielt `abgeordnete` ausschliesslich die 629 Mandate des 21.
-- Bundestags (abgeordnetenwatch parliament_period 161). Fuer Landtagswahlen
-- kommen Kandidaturen dazu — ohne Unterscheidung wuerden sie sich mit den
-- MdB mischen und /api/abgeordnete, das die Sitzverteilung im Frontend
-- speist, mit Landeskandidaten fluten.
--
-- Das Modell folgt der Quelle: abgeordnetenwatch fuehrt beides als
-- "candidacies-mandates" — dieselbe Entitaet, unterschieden durch `type`.

ALTER TABLE abgeordnete
  ADD COLUMN parliament_period INT NULL AFTER politiker_id,
  ADD COLUMN typ ENUM('mandat','kandidatur') NOT NULL DEFAULT 'mandat' AFTER parliament_period,
  -- Kandidaturen haben keine Fraktion, sondern eine Partei. Das bestehende
  -- Feld `fraktion` bleibt den Mandaten vorbehalten.
  ADD COLUMN partei VARCHAR(96) NULL AFTER fraktion,
  ADD INDEX idx_periode_typ (parliament_period, typ);

-- Bestand ist der 21. Bundestag. Ohne dieses Backfill wuerden die 629 MdB
-- aus jeder periodengefilterten Abfrage fallen.
UPDATE abgeordnete SET parliament_period = 161 WHERE parliament_period IS NULL;

-- Bruecke zwischen den beiden Fremdsystemen: `wahltermine` haengt bisher nur
-- an dawum (Umfragen). Kandidaturen kommen von abgeordnetenwatch, das eine
-- eigene Periodennummerierung hat.
ALTER TABLE wahltermine
  ADD COLUMN aw_parliament_period_id INT NULL AFTER dawum_parliament_id,
  ADD INDEX idx_aw_periode (aw_parliament_period_id);

UPDATE wahltermine SET aw_parliament_period_id = 168 WHERE slug = 'ltw-sachsen-anhalt-2026';
UPDATE wahltermine SET aw_parliament_period_id = 161 WHERE slug = 'btw-2025';
