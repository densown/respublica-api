-- Worldmap API prep: 59. Indikator (war nur in world_indicator_meta, keine Werte in world_indicators)
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
