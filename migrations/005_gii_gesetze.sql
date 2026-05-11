-- GII-Pipeline: Metadaten-Spalten fuer gesetze-im-internet.de (Abschnitt 5 PIPELINE-PLAN)
-- Datenbank: respublica_gesetze

ALTER TABLE gesetze
  ADD COLUMN titel_offiziell VARCHAR(500) NULL AFTER name,
  ADD COLUMN amtliche_abkuerzung VARCHAR(50) NULL AFTER titel_offiziell,
  ADD COLUMN ausfertigung_datum DATE NULL,
  ADD COLUMN fundstelle_periodikum VARCHAR(20) NULL,
  ADD COLUMN fundstelle_zitstelle VARCHAR(50) NULL,
  ADD COLUMN letzter_stand TEXT NULL,
  ADD COLUMN gii_slug VARCHAR(100) NULL,
  ADD COLUMN gii_doknr VARCHAR(50) NULL,
  ADD COLUMN gii_builddate VARCHAR(20) NULL,
  ADD COLUMN gii_last_synced DATETIME NULL,
  ADD COLUMN status ENUM('aktiv', 'aufgehoben', 'unbekannt') NOT NULL DEFAULT 'unbekannt',
  ADD INDEX idx_gii_doknr (gii_doknr),
  ADD INDEX idx_gii_slug (gii_slug),
  ADD INDEX idx_titel (titel_offiziell(255));

CREATE TABLE IF NOT EXISTS gesetze_sync_log (
  id INT NOT NULL AUTO_INCREMENT,
  run_started_at DATETIME NOT NULL,
  run_ended_at DATETIME NULL,
  status ENUM('running', 'success', 'failed') NOT NULL,
  gesetze_total INT DEFAULT 0,
  gesetze_neu INT DEFAULT 0,
  gesetze_geaendert INT DEFAULT 0,
  gesetze_fehler INT DEFAULT 0,
  error_message TEXT NULL,
  PRIMARY KEY (id),
  INDEX idx_run_started (run_started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
