-- Stammdaten fuer das Wahlen-Modul (Phase 0). Idempotent — beliebig oft ausfuehrbar.
--
--   mysql respublica_gesetze < scripts/seed_wahlen_stammdaten.sql
--
-- Bewusst KEINE Migration: Parteiliste und Wahltermine wachsen redaktionell
-- weiter, das gehoert nicht in die nummerierte Schema-Historie.

-- ---------------------------------------------------------------------------
-- Parteien
-- ---------------------------------------------------------------------------
-- `kuerzel` und `farbe_hex` sind aus src/pages/elections/partyColors.ts
-- gespiegelt (PARTY_COLORS_LIGHT). Aendert sich dort etwas, hier nachziehen.
INSERT INTO parteien (kuerzel, name, farbe_hex, sortierung) VALUES
  ('cdu_csu',       'CDU/CSU',           '#1A1A1A',  10),
  ('spd',           'SPD',               '#E3000F',  20),
  ('gruene',        'Bündnis 90/Die Grünen', '#46962B', 30),
  ('fdp',           'FDP',               '#FFED00',  40),
  ('linke_pds',     'Die Linke',         '#BE3075',  50),
  ('afd',           'AfD',               '#009EE0',  60),
  ('bsw',           'BSW',               '#572887',  70),
  ('freie_waehler', 'Freie Wähler',      '#F29400',  80),
  ('piraten',       'Piraten',           '#FF820A',  90),
  ('die_partei',    'Die PARTEI',        '#B92837', 100),
  ('npd',           'NPD / Die Heimat',  '#8B4513', 110),
  ('other',         'Sonstige',          '#999999', 999)
ON DUPLICATE KEY UPDATE
  name = VALUES(name), farbe_hex = VALUES(farbe_hex), sortierung = VALUES(sortierung);

-- ---------------------------------------------------------------------------
-- dawum-Party-IDs -> interne Parteien (m:1)
-- ---------------------------------------------------------------------------
-- 1 = CDU/CSU (Bundesebene), 101 = CDU, 102 = CSU (Landesebene). Alle drei
-- muessen auf cdu_csu zeigen, sonst fehlt die Union in Landes-Zeitreihen.
-- Kleinparteien ohne eigene Farbe landen auf `other`; der Importer summiert
-- mehrere `other`-Treffer derselben Umfrage auf.
INSERT INTO parteien_dawum (dawum_id, partei_id, dawum_name)
SELECT m.dawum_id, p.id, m.dawum_name
FROM (
            SELECT   1 AS dawum_id, 'cdu_csu'       AS kuerzel, 'CDU/CSU'          AS dawum_name
  UNION ALL SELECT 101, 'cdu_csu',       'CDU'
  UNION ALL SELECT 102, 'cdu_csu',       'CSU'
  UNION ALL SELECT   2, 'spd',           'SPD'
  UNION ALL SELECT   4, 'gruene',        'Grüne'
  UNION ALL SELECT   3, 'fdp',           'FDP'
  UNION ALL SELECT   5, 'linke_pds',     'Linke'
  UNION ALL SELECT   7, 'afd',           'AfD'
  UNION ALL SELECT  23, 'bsw',           'BSW'
  UNION ALL SELECT   8, 'freie_waehler', 'Freie Wähler'
  UNION ALL SELECT   6, 'piraten',       'Piraten'
  UNION ALL SELECT  13, 'die_partei',    'Die PARTEI'
  UNION ALL SELECT   9, 'npd',           'NPD'
  UNION ALL SELECT   0, 'other',         'Sonstige'
  -- Kleinparteien -> Sonstige (bei Bedarf spaeter eigene Slugs + Farben)
  UNION ALL SELECT  10, 'other',         'SSW'
  UNION ALL SELECT  11, 'other',         'Bayernpartei'
  UNION ALL SELECT  12, 'other',         'ÖDP'
  UNION ALL SELECT  14, 'other',         'BVB/FW'
  UNION ALL SELECT  15, 'other',         'Tierschutzpartei'
  UNION ALL SELECT  16, 'other',         'BD'
  UNION ALL SELECT  17, 'other',         'Familie'
  UNION ALL SELECT  18, 'other',         'Volt'
  UNION ALL SELECT  21, 'other',         'bunt.saar'
  UNION ALL SELECT  22, 'other',         'BfTh'
  UNION ALL SELECT  24, 'other',         'Plus Brandenburg'
  UNION ALL SELECT  25, 'other',         'WerteUnion'
) m
JOIN parteien p ON p.kuerzel = m.kuerzel
ON DUPLICATE KEY UPDATE partei_id = VALUES(partei_id), dawum_name = VALUES(dawum_name);

-- ---------------------------------------------------------------------------
-- Wahltermine
-- ---------------------------------------------------------------------------
-- Abgeschlossene Termine sind KEIN Beiwerk: der Importer ordnet jede Umfrage
-- dem naechsten Wahltermin desselben Parlaments zu. Ohne die Vorgaenger-Zeilen
-- landeten alle Umfragen seit 2017 faelschlich bei der Wahl 2026.
INSERT INTO wahltermine (slug, ebene, land, name_de, name_en, datum, dawum_parliament_id, status) VALUES
  -- Sachsen-Anhalt (dawum 14)
  ('ltw-sachsen-anhalt-2021', 'land', 'Sachsen-Anhalt',
   'Landtagswahl Sachsen-Anhalt 2021', 'Saxony-Anhalt state election 2021',
   '2021-06-06', 14, 'abgeschlossen'),
  ('ltw-sachsen-anhalt-2026', 'land', 'Sachsen-Anhalt',
   'Landtagswahl Sachsen-Anhalt 2026', 'Saxony-Anhalt state election 2026',
   '2026-09-06', 14, 'kommend'),

  -- Mecklenburg-Vorpommern (dawum 8)
  ('ltw-mecklenburg-vorpommern-2021', 'land', 'Mecklenburg-Vorpommern',
   'Landtagswahl Mecklenburg-Vorpommern 2021', 'Mecklenburg-Vorpommern state election 2021',
   '2021-09-26', 8, 'abgeschlossen'),
  ('ltw-mecklenburg-vorpommern-2026', 'land', 'Mecklenburg-Vorpommern',
   'Landtagswahl Mecklenburg-Vorpommern 2026', 'Mecklenburg-Vorpommern state election 2026',
   '2026-09-20', 8, 'kommend'),

  -- Berlin (dawum 3) — 2023 war die vom Verfassungsgerichtshof angeordnete
  -- vollstaendige Wiederholung der Wahl vom 26.09.2021.
  ('awh-berlin-2021', 'land', 'Berlin',
   'Wahl zum Abgeordnetenhaus von Berlin 2021', 'Berlin state election 2021',
   '2021-09-26', 3, 'abgeschlossen'),
  ('awh-berlin-2023', 'land', 'Berlin',
   'Wiederholungswahl zum Abgeordnetenhaus von Berlin 2023', 'Berlin repeat state election 2023',
   '2023-02-12', 3, 'abgeschlossen'),
  ('awh-berlin-2026', 'land', 'Berlin',
   'Wahl zum Abgeordnetenhaus von Berlin 2026', 'Berlin state election 2026',
   '2026-09-20', 3, 'kommend'),

  -- Bundestag (dawum 0) — dawum-Bestand beginnt am 18.01.2017, deshalb ist
  -- die BTW 2017 als aeltester Bezugspunkt noetig. Der Termin der naechsten
  -- regulaeren Wahl steht noch nicht fest -> datum NULL.
  ('btw-2017', 'bund', NULL, 'Bundestagswahl 2017', 'German federal election 2017',
   '2017-09-24', 0, 'abgeschlossen'),
  ('btw-2021', 'bund', NULL, 'Bundestagswahl 2021', 'German federal election 2021',
   '2021-09-26', 0, 'abgeschlossen'),
  ('btw-2025', 'bund', NULL, 'Bundestagswahl 2025', 'German federal election 2025',
   '2025-02-23', 0, 'abgeschlossen'),
  ('btw-next', 'bund', NULL, 'Nächste Bundestagswahl', 'Next German federal election',
   NULL, 0, 'kommend')
ON DUPLICATE KEY UPDATE
  ebene = VALUES(ebene), land = VALUES(land),
  name_de = VALUES(name_de), name_en = VALUES(name_en),
  datum = VALUES(datum), dawum_parliament_id = VALUES(dawum_parliament_id),
  status = VALUES(status);
