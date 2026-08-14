-- 013: Themenfelder-Taxonomie (Wahlen-Modul Phase 1)
--
-- Die Taxonomie ist die Achse, auf der spaeter Wahlprogramm-Positionen,
-- Koalitionsvertrags-Zusagen und Gesetze verglichen werden. Ohne sie ist
-- kein Vergleich zwischen Parteien und zwischen Wahlen moeglich.
--
-- Grundlage sind die DIP21-Sachgebiete des Bundestags, wie abgeordnetenwatch
-- sie fuehrt: 29 Ober- und 24 Unterthemen, bereits zweistufig. Sie werden
-- uebernommen statt neu erfunden — und, entscheidend, sie haengen dort schon
-- an den namentlichen Abstimmungen. Dadurch liegen "versprochen" und
-- "abgestimmt" von Anfang an auf derselben Achse.

CREATE TABLE IF NOT EXISTS themenfelder (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  slug            VARCHAR(64)  NOT NULL UNIQUE,
  name_de         VARCHAR(255) NOT NULL,
  name_en         VARCHAR(255) NOT NULL,
  parent_id       INT NULL,
  -- Herkunftsschluessel: macht den Reimport idempotent und erlaubt es,
  -- Aenderungen der Quelle nachzuziehen, ohne Slugs zu brechen.
  aw_topic_id     INT NULL UNIQUE,
  -- 0 = parlamentarisches Verfahren oder historisch (Geschaeftsordnung,
  -- Immunitaet, Petitionen, Wahlpruefung, Deutsche Einheit bis 1990).
  -- Solche Felder sind fuer die Zuordnung von Wahlprogramm-Positionen
  -- untauglich; die Extraktion validiert kuenftig gegen fuer_positionen = 1.
  fuer_positionen TINYINT(1) NOT NULL DEFAULT 1,
  sortierung      SMALLINT NOT NULL DEFAULT 100,
  created_at      TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_parent (parent_id),
  INDEX idx_positionen (fuer_positionen),
  CONSTRAINT fk_themenfeld_parent FOREIGN KEY (parent_id) REFERENCES themenfelder(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bruecke Abstimmung <-> Themenfeld.
--
-- Referenziert poll_id direkt und nicht per Fremdschluessel: eine Tabelle
-- `polls` gibt es nicht, poll_id taucht in `abstimmungen`, `votes` und
-- `aenderungen` als gemeinsamer Schluessel auf. Eine Abstimmung kann mehrere
-- Themen haben.
CREATE TABLE IF NOT EXISTS poll_themenfelder (
  poll_id       INT NOT NULL,
  themenfeld_id INT NOT NULL,
  PRIMARY KEY (poll_id, themenfeld_id),
  INDEX idx_themenfeld (themenfeld_id),
  CONSTRAINT fk_pt_themenfeld FOREIGN KEY (themenfeld_id) REFERENCES themenfelder(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
