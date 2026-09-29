-- 018: Versammlungen, historische Daten (Import vorhandener Datensaetze)
--
-- Vor dem eigenen Archiv (ab 29.09.2026) gibt es fuer Berlin bereits
-- veroeffentlichte Einzeldaten aus IFG-Anfragen:
--   2018-2022  The German Protest Registrations Dataset (David Pomerenke,
--              Zenodo 10.5281/zenodo.10094245, CC BY-SA 4.0), Berlin-Teil aus
--              FragDenStaat-Anfragen; Datum, Thema, Teilnehmende angemeldet
--              und tatsaechlich, aber kein Ort, keine Uhrzeit, kein Typ
--   2023-04/2024  FragDenStaat-Anfrage "Versammlungen 2023 und 2024" (Polizei
--              Berlin, XLSX) mit Ort, Uhrzeit, Aufzugsstrecke und Teilnehmenden
-- Importiert von scripts/import_versammlungen_altdaten.py, je als eigene
-- `quelle`, nie mit dem laufenden Archiv vermischt.

-- Typ ist in manchen Quellen unbekannt (Pomerenke: kein Ort, keine Strecke).
-- NULL statt 'kundgebung', sonst waeren Aufzuege systematisch untergezaehlt.
ALTER TABLE versammlungen
  MODIFY COLUMN typ ENUM('kundgebung','aufzug') NULL DEFAULT 'kundgebung';

-- 'angemeldet': die bei der Anzeige erwartete Teilnehmendenzahl. Eine
-- Erwartung der Anmeldenden VOR der Versammlung, nicht dasselbe wie eine
-- Veranstalterangabe danach ('veranstalter').
ALTER TABLE versammlung_zahlen
  MODIFY COLUMN quelle_typ ENUM('angemeldet','veranstalter','polizei','presse','schaetzung') NOT NULL;

-- Abgedeckte Zeitraeume je Quelle. Ohne diese Tabelle waere eine Luecke im
-- Bestand (Berlin Mai 2024 bis September 2026) nicht von "keine
-- Versammlungen" zu unterscheiden. `bis` NULL = laufend.
CREATE TABLE IF NOT EXISTS versammlungen_abdeckung (
  quelle        VARCHAR(32)  NOT NULL,
  land          CHAR(2)      NOT NULL,
  stadt         VARCHAR(100) NOT NULL,
  von           DATE         NOT NULL,
  bis           DATE         NULL,
  name          VARCHAR(255) NOT NULL,
  url           VARCHAR(500) NULL,
  lizenz        VARCHAR(255) NULL,
  hinweis       TEXT         NULL,
  PRIMARY KEY (quelle, stadt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO versammlungen_abdeckung (quelle, land, stadt, von, bis, name, url, lizenz, hinweis)
VALUES ('polizei_berlin', 'BE', 'Berlin', '2026-09-29', NULL,
        'Polizei Berlin, Versammlungsbehörde (laufendes Archiv)',
        'https://www.berlin.de/polizei/service/versammlungsbehoerde/versammlungen-aufzuege/',
        'eingeschränkte Nutzung laut daten.berlin.de, Anfrage läuft',
        'Täglich gesicherte Liste angezeigter Versammlungen nach § 12 VersFG BE.')
ON DUPLICATE KEY UPDATE von = VALUES(von);
