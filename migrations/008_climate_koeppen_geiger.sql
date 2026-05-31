-- 008_climate_koeppen_geiger.sql
-- Köppen-Geiger climate classification — Hybrid-Schema.
--
-- Datenquelle: Beck et al. 2023, "High-resolution (1 km) Köppen-Geiger maps
-- for 1901–2099 based on constrained CMIP6 projections", Scientific Data
-- 10:724. doi:10.1038/s41597-023-02549-6. GeoTIFFs unter
-- https://www.gloh2o.org/koppen/ (Figshare 10.6084/m9.figshare.21789074).
--
-- Pixel-Kodierung im Raster (uint8, NoData=0, Klassen 1..30) ist gegen die
-- archiv-eigene `legend.txt` verifiziert, Reihenfolge nach Peel et al. 2007.
--
-- Was diese Migration anlegt:
--   (1) value_type-Enum von data_indicators um 'categorical' erweitern, weil
--       die dominante Köppen-Klasse 1..30 ein Lookup-Code ist, keine Skala.
--   (2) Quelle in data_sources registrieren (slug 'koppen_geiger_v3').
--   (3) Fünf Indikatoren in data_indicators eintragen:
--          climate.koppen.dominant_historical (year=2020, Periode 1991-2020)
--          climate.koppen.dominant_ssp126     (year=2099, Periode 2071-2099)
--          climate.koppen.dominant_ssp245     (year=2099, Periode 2071-2099)
--          climate.koppen.dominant_ssp370     (year=2099, Periode 2071-2099)
--          climate.koppen.dominant_ssp585     (year=2099, Periode 2071-2099)
--       (Mid-Century 2041-2070 → bei Bedarf zusätzliche Rows mit year=2070
--        an dieselben SSP-Indikatoren, ohne Schemaänderung.)
--   (4) Lookup-Tabelle climate_koeppen_classes (30 Zeilen, statisch).
--   (5) Begleittabelle climate_koeppen_distribution (volle Verteilung pro
--       Land × Szenario × Periode × Klasse).
--
-- Der Import-Lauf schreibt anschließend:
--   * je 1 Zeile in data_values pro (country, indicator) — dominante Klasse
--   * je n Zeilen in climate_koeppen_distribution pro (country, scenario,
--     period) — eine je in dem Land tatsächlich vorkommenden Köppen-Klasse.
--
-- Idempotent: ALTER MODIFY auf identischen Typ ist ein No-Op; INSERT IGNORE
-- bzw. CREATE TABLE IF NOT EXISTS für den Rest.

-- ---------------------------------------------------------------------------
-- (1) value_type-Enum erweitern
-- ---------------------------------------------------------------------------
ALTER TABLE data_indicators
  MODIFY COLUMN value_type
    ENUM('absolute','percent','index','rate','count','currency','ratio','categorical')
    DEFAULT 'absolute';

-- ---------------------------------------------------------------------------
-- (2) Quelle registrieren
-- ---------------------------------------------------------------------------
INSERT IGNORE INTO data_sources
  (slug, name, provider, url, license, update_freq, is_active)
VALUES (
  'koppen_geiger_v3',
  'Köppen-Geiger Climate Classification (1901–2099, V3)',
  'GloH2O / Beck et al.',
  'https://www.gloh2o.org/koppen/',
  'CC BY 4.0',
  'manual',
  1
);

-- Source-ID für die folgenden Indikator-Inserts merken.
SET @koppen_source_id := (SELECT id FROM data_sources WHERE slug = 'koppen_geiger_v3');

-- ---------------------------------------------------------------------------
-- (3) Indikatoren: 1× historical + 4× SSP (Tier-1)
-- ---------------------------------------------------------------------------
INSERT IGNORE INTO data_indicators
  (code, source_id, category, subcategory,
   name_de, name_en, description_de, description_en,
   unit_de, unit_en, value_type, lower_is_better, display_decimals,
   year_min, year_max, priority)
VALUES
('climate.koppen.dominant_historical', @koppen_source_id, 'environment', 'climate',
 'Dominante Köppen-Geiger-Klimaklasse (1991–2020)',
 'Dominant Köppen-Geiger climate class (1991–2020)',
 'Häufigste Köppen-Geiger-Klimaklasse innerhalb der Landesfläche, kodiert als Integer 1..30 (Lookup in climate_koeppen_classes). Beobachtungsbasierte Klimatologie 1991–2020 (Beck et al. 2023).',
 'Most frequent Köppen-Geiger climate class within the country area, encoded as integer 1..30 (lookup in climate_koeppen_classes). Observation-based climatology 1991–2020 (Beck et al. 2023).',
 'Klasse 1..30', 'class 1..30', 'categorical', 0, 0, 2020, 2020, 70),

('climate.koppen.dominant_ssp126', @koppen_source_id, 'environment', 'climate',
 'Dominante Köppen-Geiger-Klimaklasse (2071–2099, SSP1-2.6)',
 'Dominant Köppen-Geiger climate class (2071–2099, SSP1-2.6)',
 'Projizierte häufigste Köppen-Geiger-Klimaklasse innerhalb der Landesfläche für 2071–2099 unter SSP1-2.6 (niedrige Emissionen, +1.5 °C-Pfad). Integer 1..30 (Lookup in climate_koeppen_classes).',
 'Projected most frequent Köppen-Geiger climate class within the country area for 2071–2099 under SSP1-2.6 (low emissions, +1.5 °C pathway). Integer 1..30 (lookup in climate_koeppen_classes).',
 'Klasse 1..30', 'class 1..30', 'categorical', 0, 0, 2099, 2099, 65),

('climate.koppen.dominant_ssp245', @koppen_source_id, 'environment', 'climate',
 'Dominante Köppen-Geiger-Klimaklasse (2071–2099, SSP2-4.5)',
 'Dominant Köppen-Geiger climate class (2071–2099, SSP2-4.5)',
 'Projizierte häufigste Köppen-Geiger-Klimaklasse innerhalb der Landesfläche für 2071–2099 unter SSP2-4.5 (mittlerer Pfad). Integer 1..30 (Lookup in climate_koeppen_classes).',
 'Projected most frequent Köppen-Geiger climate class within the country area for 2071–2099 under SSP2-4.5 (middle-of-the-road pathway). Integer 1..30 (lookup in climate_koeppen_classes).',
 'Klasse 1..30', 'class 1..30', 'categorical', 0, 0, 2099, 2099, 65),

('climate.koppen.dominant_ssp370', @koppen_source_id, 'environment', 'climate',
 'Dominante Köppen-Geiger-Klimaklasse (2071–2099, SSP3-7.0)',
 'Dominant Köppen-Geiger climate class (2071–2099, SSP3-7.0)',
 'Projizierte häufigste Köppen-Geiger-Klimaklasse innerhalb der Landesfläche für 2071–2099 unter SSP3-7.0 (regionale Rivalität, hohe Emissionen). Integer 1..30 (Lookup in climate_koeppen_classes).',
 'Projected most frequent Köppen-Geiger climate class within the country area for 2071–2099 under SSP3-7.0 (regional rivalry, high emissions). Integer 1..30 (lookup in climate_koeppen_classes).',
 'Klasse 1..30', 'class 1..30', 'categorical', 0, 0, 2099, 2099, 65),

('climate.koppen.dominant_ssp585', @koppen_source_id, 'environment', 'climate',
 'Dominante Köppen-Geiger-Klimaklasse (2071–2099, SSP5-8.5)',
 'Dominant Köppen-Geiger climate class (2071–2099, SSP5-8.5)',
 'Projizierte häufigste Köppen-Geiger-Klimaklasse innerhalb der Landesfläche für 2071–2099 unter SSP5-8.5 (fossil-getriebener Wachstumspfad, höchste Emissionen). Integer 1..30 (Lookup in climate_koeppen_classes).',
 'Projected most frequent Köppen-Geiger climate class within the country area for 2071–2099 under SSP5-8.5 (fossil-fueled development, highest emissions). Integer 1..30 (lookup in climate_koeppen_classes).',
 'Klasse 1..30', 'class 1..30', 'categorical', 0, 0, 2099, 2099, 65);

-- ---------------------------------------------------------------------------
-- (4) Klassen-Lookup (statisch, 30 Zeilen)
--
-- Reihenfolge & Symbole: Peel et al. 2007 / Beck et al. 2023 legend.txt
-- Farben: Peel et al. 2007 ("Updated world map of the Köppen-Geiger climate
-- classification", HESS 11:1633), gespiegelt im Beck-Archiv legend.txt.
-- Wenn das legend.txt im Daten-ZIP für eine einzelne Zeile abweichende RGBs
-- zeigt, diese Tabelle entsprechend per UPDATE korrigieren — die Symbole
-- selbst sind verifiziert.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climate_koeppen_classes (
  class_code     TINYINT UNSIGNED NOT NULL,
  symbol         VARCHAR(4)   NOT NULL,
  major_group    CHAR(1)      NOT NULL COMMENT 'A,B,C,D,E nach Köppen-Hauptklassen',
  name_de        VARCHAR(120) NOT NULL,
  name_en        VARCHAR(120) NOT NULL,
  color_rgb      CHAR(7)      NOT NULL COMMENT '#RRGGBB nach Peel et al. 2007',
  description_de TEXT         DEFAULT NULL,
  description_en TEXT         DEFAULT NULL,
  PRIMARY KEY (class_code),
  UNIQUE KEY uk_symbol (symbol),
  KEY idx_major (major_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO climate_koeppen_classes
  (class_code, symbol, major_group, name_de, name_en, color_rgb)
VALUES
 (1,  'Af',  'A', 'Tropisch, Regenwald',                                'Tropical rainforest',                       '#0000FF'),
 (2,  'Am',  'A', 'Tropisch, Monsun',                                   'Tropical monsoon',                          '#0078FF'),
 (3,  'Aw',  'A', 'Tropisch, Savanne',                                  'Tropical savannah',                         '#46AAFA'),
 (4,  'BWh', 'B', 'Arid, Wüste, heiß',                                  'Arid, desert, hot',                         '#FF0000'),
 (5,  'BWk', 'B', 'Arid, Wüste, kalt',                                  'Arid, desert, cold',                        '#FF9696'),
 (6,  'BSh', 'B', 'Arid, Steppe, heiß',                                 'Arid, steppe, hot',                         '#F5A500'),
 (7,  'BSk', 'B', 'Arid, Steppe, kalt',                                 'Arid, steppe, cold',                        '#FFDC64'),
 (8,  'Csa', 'C', 'Gemäßigt, trockener heißer Sommer',                  'Temperate, dry summer, hot summer',         '#FFFF00'),
 (9,  'Csb', 'C', 'Gemäßigt, trockener warmer Sommer',                  'Temperate, dry summer, warm summer',        '#C6C700'),
 (10, 'Csc', 'C', 'Gemäßigt, trockener kühler Sommer',                  'Temperate, dry summer, cold summer',        '#969600'),
 (11, 'Cwa', 'C', 'Gemäßigt, trockener Winter, heißer Sommer',          'Temperate, dry winter, hot summer',         '#96FF96'),
 (12, 'Cwb', 'C', 'Gemäßigt, trockener Winter, warmer Sommer',          'Temperate, dry winter, warm summer',        '#63C764'),
 (13, 'Cwc', 'C', 'Gemäßigt, trockener Winter, kühler Sommer',          'Temperate, dry winter, cold summer',        '#329633'),
 (14, 'Cfa', 'C', 'Gemäßigt, feucht, heißer Sommer',                    'Temperate, no dry season, hot summer',      '#C6FF4E'),
 (15, 'Cfb', 'C', 'Gemäßigt, feucht, warmer Sommer',                    'Temperate, no dry season, warm summer',     '#66FF33'),
 (16, 'Cfc', 'C', 'Gemäßigt, feucht, kühler Sommer',                    'Temperate, no dry season, cold summer',     '#33C700'),
 (17, 'Dsa', 'D', 'Kontinental, trockener heißer Sommer',               'Cold, dry summer, hot summer',              '#FF00FE'),
 (18, 'Dsb', 'D', 'Kontinental, trockener warmer Sommer',               'Cold, dry summer, warm summer',             '#C600C7'),
 (19, 'Dsc', 'D', 'Kontinental, trockener kühler Sommer',               'Cold, dry summer, cold summer',             '#963295'),
 (20, 'Dsd', 'D', 'Kontinental, trockener Sommer, sehr kalter Winter',  'Cold, dry summer, very cold winter',        '#966495'),
 (21, 'Dwa', 'D', 'Kontinental, trockener Winter, heißer Sommer',       'Cold, dry winter, hot summer',              '#ABB1FF'),
 (22, 'Dwb', 'D', 'Kontinental, trockener Winter, warmer Sommer',       'Cold, dry winter, warm summer',             '#5A77DB'),
 (23, 'Dwc', 'D', 'Kontinental, trockener Winter, kühler Sommer',       'Cold, dry winter, cold summer',             '#4C51B5'),
 (24, 'Dwd', 'D', 'Kontinental, trockener Winter, sehr kalter Winter',  'Cold, dry winter, very cold winter',        '#320087'),
 (25, 'Dfa', 'D', 'Kontinental, feucht, heißer Sommer',                 'Cold, no dry season, hot summer',           '#00FFFF'),
 (26, 'Dfb', 'D', 'Kontinental, feucht, warmer Sommer',                 'Cold, no dry season, warm summer',          '#37C7FF'),
 (27, 'Dfc', 'D', 'Kontinental, feucht, kühler Sommer',                 'Cold, no dry season, cold summer',          '#007E7D'),
 (28, 'Dfd', 'D', 'Kontinental, feucht, sehr kalter Winter',            'Cold, no dry season, very cold winter',     '#00455E'),
 (29, 'ET',  'E', 'Polar, Tundra',                                      'Polar, tundra',                             '#B2B2B2'),
 (30, 'EF',  'E', 'Polar, Eis',                                         'Polar, frost',                              '#666666');

-- ---------------------------------------------------------------------------
-- (5) Volle Verteilung pro Land × Szenario × Periode × Klasse
--
-- Eine Zeile pro tatsächlich vorkommender Klasse (typisch 3..15 Zeilen pro
-- Land), nicht für Klassen mit 0 Pixeln. Beim Re-Import per UPSERT auf den
-- Unique-Key überschrieben.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS climate_koeppen_distribution (
  id             INT AUTO_INCREMENT PRIMARY KEY,
  country_code   VARCHAR(3)       NOT NULL,
  scenario       VARCHAR(16)      NOT NULL COMMENT 'historical / ssp119 / ssp126 / ssp245 / ssp370 / ssp434 / ssp460 / ssp585',
  period         VARCHAR(9)       NOT NULL COMMENT 'z.B. 1991_2020, 2041_2070, 2071_2099',
  class_code     TINYINT UNSIGNED NOT NULL,
  share          DECIMAL(7,4)     NOT NULL COMMENT 'Anteil 0..100 (%) der Landfläche dieser Klasse',
  pixel_count    INT UNSIGNED     NOT NULL COMMENT 'Pixel dieser Klasse im Landausschnitt',
  raster_file    VARCHAR(200)     DEFAULT NULL COMMENT 'Quelldatei aus dem Beck-Archiv',
  fetched_at     TIMESTAMP        NULL DEFAULT current_timestamp(),
  UNIQUE KEY uk_dist (country_code, scenario, period, class_code),
  KEY idx_country_period   (country_code, period),
  KEY idx_scenario_period  (scenario, period),
  CONSTRAINT fk_dist_class FOREIGN KEY (class_code)
    REFERENCES climate_koeppen_classes (class_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
