-- 012: Wahlen-Modul Phase 0 — Wahltermine, Parteien, Umfragen (dawum)
--
-- Legt ausschliesslich NEUE Tabellen an. Der Bestand wird nicht angefasst:
-- `wahlen` (49.857 GERDA-Ergebniszeilen) bleibt unveraendert, deshalb heisst
-- die Termin-Tabelle hier `wahltermine`.
--
-- Themenfelder, Wahlprogramme, Positionen, Koalitionsvertraege und die
-- Bridge-Tabelle `rechenschaft` kommen in einer spaeteren Migration — sie
-- haengen an der noch festzulegenden Themenfelder-Taxonomie.
--
-- Anwenden: siehe Kopf von 011 — dieser Migration fehlt jede Sperrwirkung
-- (nur CREATE TABLE), sie ist jederzeit gefahrlos einspielbar.

-- ---------------------------------------------------------------------------
-- Parteien
-- ---------------------------------------------------------------------------
-- `kuerzel` entspricht bewusst exakt den Slugs aus dem Frontend
-- (src/pages/elections/partyColors.ts). Dadurch greifen partyColorsForTheme()
-- und PARTY_LABELS ohne zusaetzliche Mapping-Schicht.
CREATE TABLE IF NOT EXISTS parteien (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  kuerzel     VARCHAR(32)  NOT NULL UNIQUE,
  name        VARCHAR(255) NOT NULL,
  farbe_hex   CHAR(7)      NULL,
  sortierung  SMALLINT     NOT NULL DEFAULT 100,
  created_at  TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- dawum kennt mehrere IDs fuer dieselbe Partei: 1 = CDU/CSU, 101 = CDU,
-- 102 = CSU. In Landtagsumfragen wird 101 verwendet, im Bund 1. Ein einzelnes
-- Feld `parteien.dawum_id` (so im urspruenglichen Entwurf) kann das nicht
-- abbilden — ohne diese m:1-Tabelle fehlt die Union in jeder Landes-Zeitreihe.
CREATE TABLE IF NOT EXISTS parteien_dawum (
  dawum_id   INT NOT NULL PRIMARY KEY,
  partei_id  INT NOT NULL,
  dawum_name VARCHAR(128) NULL,
  INDEX idx_partei (partei_id),
  CONSTRAINT fk_pd_partei FOREIGN KEY (partei_id) REFERENCES parteien(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Wahltermine
-- ---------------------------------------------------------------------------
-- `dawum_parliament_id` ist NICHT unique: dasselbe Parlament wird mehrfach
-- gewaehlt. Der Importer ordnet eine Umfrage dem naechsten Wahltermin
-- desselben Parlaments am oder nach dem Erhebungsdatum zu.
CREATE TABLE IF NOT EXISTS wahltermine (
  id                  INT AUTO_INCREMENT PRIMARY KEY,
  slug                VARCHAR(64) NOT NULL UNIQUE,
  ebene               ENUM('bund','land','eu') NOT NULL,
  land                VARCHAR(64) NULL,
  name_de             VARCHAR(255) NOT NULL,
  name_en             VARCHAR(255) NOT NULL,
  datum               DATE NULL,
  dawum_parliament_id INT NULL,
  status              ENUM('kommend','laufend','abgeschlossen') NOT NULL DEFAULT 'kommend',
  created_at          TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_datum (datum),
  INDEX idx_status (status),
  INDEX idx_dawum_parl (dawum_parliament_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Umfragen (dawum-Spiegel, ODbL — Attribution ist Pflicht)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS umfragen (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  dawum_survey_id INT NOT NULL UNIQUE,
  wahltermin_id   INT NOT NULL,
  institut        VARCHAR(128) NOT NULL,
  auftraggeber    VARCHAR(128) NULL,
  erhebung_start  DATE NULL,
  erhebung_ende   DATE NULL,
  veroeffentlicht DATE NOT NULL,
  befragte        INT NULL,
  methode         VARCHAR(64) NULL,
  aktualisiert_am TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_wahl_datum (wahltermin_id, veroeffentlicht),
  CONSTRAINT fk_umfrage_wahltermin FOREIGN KEY (wahltermin_id) REFERENCES wahltermine(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- dawum korrigiert Werte rueckwirkend, deshalb schreibt der Importer die
-- Werte einer Umfrage immer komplett neu (DELETE + INSERT). ON DELETE CASCADE
-- haelt das sauber, falls eine Umfrage selbst verschwindet.
CREATE TABLE IF NOT EXISTS umfrage_werte (
  umfrage_id INT NOT NULL,
  partei_id  INT NOT NULL,
  prozent    DECIMAL(4,1) NOT NULL,
  PRIMARY KEY (umfrage_id, partei_id),
  INDEX idx_partei (partei_id),
  CONSTRAINT fk_uw_umfrage FOREIGN KEY (umfrage_id) REFERENCES umfragen(id) ON DELETE CASCADE,
  CONSTRAINT fk_uw_partei  FOREIGN KEY (partei_id)  REFERENCES parteien(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
