-- 024: Strompreise Europa (Europa-Daten, Stufe 2)
--
-- Day-Ahead-Preise je Gebotszone, Quelle energy-charts.info (Fraunhofer ISE).
-- Dort stehen die Preise von 15 Zonen unter CC BY 4.0 (Daten der
-- Bundesnetzagentur | SMARD.de): AT, BE, CH, CZ, DE-LU, DK1, DK2, FR, HU,
-- IT-North, NL, NO2, PL, SE4, SI. Nur diese Zonen werden geladen. Weitere
-- Zonen kommen spaeter ueber ENTSO-E in dieselben Tabellen (Spalte quelle_id).
--
-- strom_preise: ein Wert je Zone und Lieferzeitraum, so fein wie die Quelle
-- liefert (seit 01.10.2025 Viertelstunden). zeit_utc ist der Beginn des
-- Zeitraums in UTC. liefertag und stunde sind derselbe Zeitpunkt in Marktzeit
-- (Europe/Berlin), vorberechnet, damit Tagesansichten ohne Zeitzonenrechnung
-- auskommen. Am Tag der Zeitumstellung im Herbst kommt Stunde 2 zweimal vor.
-- Preis in EUR/MWh wie an der Boerse, die App rechnet in ct/kWh um (geteilt
-- durch 10).
--
-- schnappschuesse: fertig gerechnete Antworten (Kennzahlen, Satz des Tages),
-- die ein Abruf-Skript nach jedem Lauf schreibt und die API nur ausliefert.
--
-- Anwenden: nur neue Tabellen und Zeilen (braucht 022). Der Autodeploy haelt
-- bei dieser Migration an, bis sie von Hand eingespielt ist.

CREATE TABLE IF NOT EXISTS strom_zonen (
  code          VARCHAR(12)  NOT NULL,
  land          CHAR(2)      NOT NULL,
  name_de       VARCHAR(80)  NOT NULL,
  name_en       VARCHAR(80)  NOT NULL,
  quelle_code   VARCHAR(20)  NOT NULL,
  spalte        TINYINT      NOT NULL,
  zeile         TINYINT      NOT NULL,
  aktiv         TINYINT(1)   NOT NULL DEFAULT 1,
  PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS strom_preise (
  zone          VARCHAR(12)       NOT NULL,
  zeit_utc      DATETIME          NOT NULL,
  minuten       SMALLINT UNSIGNED NOT NULL,
  liefertag     DATE              NOT NULL,
  stunde        TINYINT UNSIGNED  NOT NULL,
  eur_mwh       DECIMAL(9,2)      NOT NULL,
  quelle_id     INT               NULL,
  abgerufen_am  DATETIME          NOT NULL,
  PRIMARY KEY (zone, zeit_utc),
  KEY idx_strom_tag (liefertag, zone)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS schnappschuesse (
  schluessel    VARCHAR(80)  NOT NULL,
  inhalt        LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(inhalt)),
  quelle_id     INT          NULL,
  erzeugt_am    DATETIME     NOT NULL,
  PRIMARY KEY (schluessel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- spalte und zeile: Platz der Zone auf der Kachelkarte der App (grob nach Lage)
INSERT INTO strom_zonen (code, land, name_de, name_en, quelle_code, spalte, zeile) VALUES
  ('NO2',    'NO', 'Norwegen Süd',           'Southern Norway',     'NO2',      3, 0),
  ('SE4',    'SE', 'Schweden Süd',           'Southern Sweden',     'SE4',      4, 0),
  ('DK1',    'DK', 'Dänemark West',          'Western Denmark',     'DK1',      3, 1),
  ('DK2',    'DK', 'Dänemark Ost',           'Eastern Denmark',     'DK2',      4, 1),
  ('NL',     'NL', 'Niederlande',            'Netherlands',         'NL',       2, 2),
  ('DE-LU',  'DE', 'Deutschland, Luxemburg', 'Germany, Luxembourg', 'DE-LU',    3, 2),
  ('PL',     'PL', 'Polen',                  'Poland',              'PL',       4, 2),
  ('BE',     'BE', 'Belgien',                'Belgium',             'BE',       2, 3),
  ('CZ',     'CZ', 'Tschechien',             'Czechia',             'CZ',       4, 3),
  ('FR',     'FR', 'Frankreich',             'France',              'FR',       1, 4),
  ('CH',     'CH', 'Schweiz',                'Switzerland',         'CH',       2, 4),
  ('AT',     'AT', 'Österreich',             'Austria',             'AT',       3, 4),
  ('HU',     'HU', 'Ungarn',                 'Hungary',             'HU',       4, 4),
  ('IT-N',   'IT', 'Italien Nord',           'Northern Italy',      'IT-North', 2, 5),
  ('SI',     'SI', 'Slowenien',              'Slovenia',            'SI',       3, 5)
ON DUPLICATE KEY UPDATE code = code;

INSERT INTO data_sources
  (slug, name, name_de, provider, url, api_endpoint, license, license_url, area, note_de, note_en, update_freq, stale_after_hours)
VALUES
  ('energy-charts', 'Energy-Charts', 'Energy-Charts', 'Fraunhofer ISE',
   'https://www.energy-charts.info/', 'https://api.energy-charts.info/price',
   'CC BY 4.0', 'https://creativecommons.org/licenses/by/4.0/', 'europa',
   'Day-Ahead-Preise an der Strombörse je Gebotszone. Daten: Bundesnetzagentur | SMARD.de, aufbereitet von Fraunhofer ISE.',
   'Day-ahead power exchange prices per bidding zone. Data: Bundesnetzagentur | SMARD.de, processed by Fraunhofer ISE.',
   'daily', 30)
ON DUPLICATE KEY UPDATE slug = slug;
