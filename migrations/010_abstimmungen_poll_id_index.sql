-- Index für Poll-Lookups, vorher als Startup-DDL in api/index.js (ensureAbstimmungenPollIdIndex)
-- Idempotent: prüft Existenz via information_schema
SET @idx_exists = (SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = 'respublica_gesetze'
  AND table_name = 'abstimmungen' AND index_name = 'idx_poll_id');
SET @sql = IF(@idx_exists = 0,
  'ALTER TABLE abstimmungen ADD INDEX idx_poll_id (poll_id)',
  'SELECT "idx_poll_id exists"');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
