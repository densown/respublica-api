-- 009_climate_koeppen_short_names.sql
-- Kurznamen (Klartext) für die 30 Köppen-Geiger-Klassen.
--
-- Ergänzt climate_koeppen_classes (Migration 008) um zwei Spalten short_name_de
-- und short_name_en. Diese kompakten Labels (≤ 40 Zeichen) werden im Klima-Tab
-- der Country Console unter dem Symbol des dominanten Szenario-Klotzes angezeigt
-- (z.B. Cfb -> "Gemäßigt warm"). Die ausführlichen name_de/name_en bleiben
-- unverändert.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS (MariaDB) ist ein No-Op, wenn die Spalten
-- schon existieren; die UPDATEs setzen die Werte deterministisch je class_code.

ALTER TABLE climate_koeppen_classes
  ADD COLUMN IF NOT EXISTS short_name_de VARCHAR(40) NOT NULL DEFAULT '' AFTER name_en,
  ADD COLUMN IF NOT EXISTS short_name_en VARCHAR(40) NOT NULL DEFAULT '' AFTER short_name_de;

UPDATE climate_koeppen_classes SET short_name_de = 'Tropischer Regenwald',          short_name_en = 'Tropical rainforest'      WHERE class_code = 1;
UPDATE climate_koeppen_classes SET short_name_de = 'Tropischer Monsun',             short_name_en = 'Tropical monsoon'         WHERE class_code = 2;
UPDATE climate_koeppen_classes SET short_name_de = 'Tropische Savanne',             short_name_en = 'Tropical savanna'         WHERE class_code = 3;
UPDATE climate_koeppen_classes SET short_name_de = 'Heiße Wüste',                   short_name_en = 'Hot desert'               WHERE class_code = 4;
UPDATE climate_koeppen_classes SET short_name_de = 'Kalte Wüste',                   short_name_en = 'Cold desert'              WHERE class_code = 5;
UPDATE climate_koeppen_classes SET short_name_de = 'Heiße Steppe',                  short_name_en = 'Hot steppe'               WHERE class_code = 6;
UPDATE climate_koeppen_classes SET short_name_de = 'Kalte Steppe',                  short_name_en = 'Cold steppe'              WHERE class_code = 7;
UPDATE climate_koeppen_classes SET short_name_de = 'Mediterran heiß',              short_name_en = 'Mediterranean hot'        WHERE class_code = 8;
UPDATE climate_koeppen_classes SET short_name_de = 'Mediterran mild',              short_name_en = 'Mediterranean mild'       WHERE class_code = 9;
UPDATE climate_koeppen_classes SET short_name_de = 'Mediterran kühl',              short_name_en = 'Mediterranean cool'       WHERE class_code = 10;
UPDATE climate_koeppen_classes SET short_name_de = 'Subtropisch monsunal',         short_name_en = 'Subtropical monsoon'      WHERE class_code = 11;
UPDATE climate_koeppen_classes SET short_name_de = 'Hochland subtropisch',         short_name_en = 'Subtropical highland'     WHERE class_code = 12;
UPDATE climate_koeppen_classes SET short_name_de = 'Hochland kühl',                short_name_en = 'Cool highland'            WHERE class_code = 13;
UPDATE climate_koeppen_classes SET short_name_de = 'Gemäßigt heiß',               short_name_en = 'Temperate hot'            WHERE class_code = 14;
UPDATE climate_koeppen_classes SET short_name_de = 'Gemäßigt warm',               short_name_en = 'Temperate warm'           WHERE class_code = 15;
UPDATE climate_koeppen_classes SET short_name_de = 'Subpolar ozeanisch',           short_name_en = 'Subpolar oceanic'         WHERE class_code = 16;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental trocken-heiß',     short_name_en = 'Continental dry-hot'      WHERE class_code = 17;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental trocken-mild',     short_name_en = 'Continental dry-mild'     WHERE class_code = 18;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental trocken-kühl',     short_name_en = 'Continental dry-cool'     WHERE class_code = 19;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental trocken-sehr kalt', short_name_en = 'Continental dry-frigid'  WHERE class_code = 20;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental monsunal-heiß',    short_name_en = 'Continental monsoon-hot'  WHERE class_code = 21;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental monsunal-mild',    short_name_en = 'Continental monsoon-mild' WHERE class_code = 22;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental monsunal-kühl',    short_name_en = 'Continental monsoon-cool' WHERE class_code = 23;
UPDATE climate_koeppen_classes SET short_name_de = 'Kontinental monsunal-frostig', short_name_en = 'Continental monsoon-frigid' WHERE class_code = 24;
UPDATE climate_koeppen_classes SET short_name_de = 'Feucht kontinental heiß',      short_name_en = 'Humid continental hot'    WHERE class_code = 25;
UPDATE climate_koeppen_classes SET short_name_de = 'Feucht kontinental mild',      short_name_en = 'Humid continental mild'   WHERE class_code = 26;
UPDATE climate_koeppen_classes SET short_name_de = 'Subarktisch',                  short_name_en = 'Subarctic'                WHERE class_code = 27;
UPDATE climate_koeppen_classes SET short_name_de = 'Subarktisch frostig',          short_name_en = 'Subarctic frigid'         WHERE class_code = 28;
UPDATE climate_koeppen_classes SET short_name_de = 'Tundra',                       short_name_en = 'Tundra'                   WHERE class_code = 29;
UPDATE climate_koeppen_classes SET short_name_de = 'Eis',                          short_name_en = 'Ice cap'                  WHERE class_code = 30;
