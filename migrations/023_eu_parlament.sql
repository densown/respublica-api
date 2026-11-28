-- 023: Namentliche Abstimmungen im EU-Parlament (Europa-Daten, Stufe 1)
--
-- Quelle: HowTheyVote.eu, taeglicher CSV-Export
-- (https://github.com/HowTheyVote/data/releases/latest). Lizenz ODbL 1.0, Inhalte
-- unter Database Contents License; Fotos und Zusammenfassungen des Parlaments
-- sind davon ausgenommen und werden nicht uebernommen. Namensnennung ist Pflicht,
-- die API liefert sie bei jeder Antwort mit (Feld quelle).
--
-- scripts/fetch_ep.py fuellt die Tabellen. Gespeichert werden nur
-- Hauptabstimmungen (is_main im Export: Schlussabstimmungen ueber einen Text,
-- keine einzelnen Aenderungsantraege) der 10. Wahlperiode ab 16.07.2024:
-- Stand 28.11.2026 sind das 766 Abstimmungen und 550.544 Einzelstimmen.
--
-- ep_abstimmungen.art: worueber abgestimmt wurde. Wichtig fuer die Deutung:
-- bei 'ablehnungsantrag' heisst "angenommen", dass der Text gescheitert ist.
-- titel_de schreibt scripts/uebersetze_ep_titel.py (der Export ist englisch).
-- de_* sind die Stimmen der deutschen Abgeordneten, vorberechnet fuer die Liste.
-- zeit ist die Angabe aus dem Export (Ortszeit der Sitzung).
--
-- Anwenden: nur neue Tabellen und eine Zeile in data_sources (braucht 022).
-- Der Autodeploy haelt bei dieser Migration an, bis sie von Hand eingespielt ist.

CREATE TABLE IF NOT EXISTS ep_fraktionen (
  code          VARCHAR(20)  NOT NULL,
  name          VARCHAR(200) NOT NULL,
  kurz          VARCHAR(40)  NOT NULL,
  PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ep_abgeordnete (
  id            INT UNSIGNED NOT NULL,
  vorname       VARCHAR(100) NOT NULL,
  nachname      VARCHAR(100) NOT NULL,
  land          CHAR(3)      NOT NULL,
  geburtsdatum  DATE         NULL,
  fraktion      VARCHAR(20)  NULL,
  partei        VARCHAR(80)  NULL,
  im_amt        TINYINT(1)   NOT NULL DEFAULT 0,
  abgerufen_am  DATETIME     NOT NULL,
  PRIMARY KEY (id),
  KEY idx_ep_abg_land (land, im_amt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ep_abstimmungen (
  id                INT UNSIGNED      NOT NULL,
  zeit              DATETIME          NOT NULL,
  titel_en          VARCHAR(700)      NOT NULL,
  titel_de          VARCHAR(700)      NULL,
  referenz          VARCHAR(60)       NULL,
  beschreibung      VARCHAR(255)      NULL,
  art               ENUM('text','einigung','ablehnungsantrag','verhandlungsmandat','dringlichkeit') NOT NULL DEFAULT 'text',
  verfahren         VARCHAR(30)       NULL,
  verfahren_typ     VARCHAR(8)        NULL,
  lesung            TINYINT UNSIGNED  NULL,
  ja                SMALLINT UNSIGNED NOT NULL,
  nein              SMALLINT UNSIGNED NOT NULL,
  enthaltung        SMALLINT UNSIGNED NOT NULL,
  nicht_abgestimmt  SMALLINT UNSIGNED NOT NULL,
  ergebnis          ENUM('angenommen','abgelehnt','hinfaellig') NULL,
  text_referenz     VARCHAR(40)       NULL,
  de_ja             SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  de_nein           SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  de_enthaltung     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  de_nicht          SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  quelle_id         INT               NULL,
  abgerufen_am      DATETIME          NOT NULL,
  PRIMARY KEY (id),
  KEY idx_ep_abst_zeit (zeit),
  KEY idx_ep_abst_verfahren (verfahren)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ep_stimmen (
  abstimmung_id INT UNSIGNED NOT NULL,
  mitglied_id   INT UNSIGNED NOT NULL,
  position      ENUM('ja','nein','enthaltung','nicht') NOT NULL,
  land          CHAR(3)      NOT NULL,
  fraktion      VARCHAR(20)  NULL,
  partei        VARCHAR(80)  NULL,
  PRIMARY KEY (abstimmung_id, mitglied_id),
  KEY idx_ep_stimmen_mitglied (mitglied_id),
  KEY idx_ep_stimmen_land (abstimmung_id, land)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO data_sources
  (slug, name, name_de, provider, url, api_endpoint, license, license_url, area, note_de, note_en, update_freq, stale_after_hours)
VALUES
  ('howtheyvote', 'HowTheyVote.eu', 'HowTheyVote.eu', 'HowTheyVote.eu',
   'https://howtheyvote.eu/', 'https://github.com/HowTheyVote/data/releases/latest',
   'ODbL 1.0', 'https://opendatacommons.org/licenses/odbl/1-0/', 'europa',
   'Namentliche Abstimmungen im Europäischen Parlament, aufbereitet aus den Protokollen des Parlaments.',
   'Roll-call votes in the European Parliament, compiled from the Parliament''s minutes.',
   'daily', 72)
ON DUPLICATE KEY UPDATE slug = slug;
