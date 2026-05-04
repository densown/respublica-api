-- Worldmap API prep: 59. Indikator (war nur in world_indicator_meta, keine Werte in world_indicators)
ALTER TABLE data_update_log
  ADD COLUMN context JSON NULL AFTER error_message;

INSERT INTO data_sources
  (slug, name, provider, url, license, update_freq, is_active)
SELECT
  'cepii_baci_hs17',
  'BACI HS17 Bilateral Trade Flows',
  'CEPII',
  'http://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37',
  'Etalab 2.0 (open license)',
  'yearly',
  1
WHERE NOT EXISTS (
  SELECT 1 FROM data_sources WHERE slug = 'cepii_baci_hs17'
);

INSERT INTO data_indicators
  (code, source_id, category, name_de, name_en, description_de, description_en,
   unit_de, unit_en, value_type, lower_is_better, priority, year_min, year_max,
   country_count, is_active)
SELECT
  'EN.ATM.CO2E.PC',
  (SELECT id FROM data_sources WHERE slug = 'worldbank_wdi' LIMIT 1),
  'environment',
  'CO2-Emissionen pro Kopf',
  'CO2 emissions (metric tons per capita)',
  'CO2-Emissionen in Tonnen pro Kopf.',
  'Carbon dioxide emissions in metric tons per capita.',
  't pro Kopf',
  'metric tons per capita',
  'absolute',
  1,
  70,
  2000,
  2024,
  0,
  1
WHERE NOT EXISTS (SELECT 1 FROM data_indicators WHERE code = 'EN.ATM.CO2E.PC');
