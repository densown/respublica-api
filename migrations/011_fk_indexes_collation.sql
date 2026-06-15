-- 011: FK-Spalten-Indizes + Collation-Drift beheben (M-014)
--
-- ACHTUNG: NICHT automatisch angewandt. Vorher:
--   1. Backup bestaetigen (scripts/backup_gesetze_db.sh bzw. frisches mysqldump)
--   2. Ausserhalb des ETL-Fensters (nicht 04:00-07:10 UTC) ausfuehren — der
--      Index auf aenderungen (~228 MB) sperrt die Tabelle kurz.
-- Anwenden:  python scripts/migrate.py   (Runner protokolliert in schema_migrations)

-- Fehlende Indizes auf FK-aehnlichen Spalten (Lookups scannen sonst die Tabelle)
CREATE INDEX IF NOT EXISTS idx_aenderungen_poll_id ON aenderungen (poll_id);
CREATE INDEX IF NOT EXISTS idx_abgeordnete_politiker_id ON abgeordnete (politiker_id);
CREATE INDEX IF NOT EXISTS idx_votes_fraction_id ON votes (fraction_id);
CREATE INDEX IF NOT EXISTS idx_lobby_proj_printing ON lobby_regulatory_projects (printing_number);

-- Collation-Drift: diese zwei Tabellen sind utf8mb4_general_ci, der Rest
-- utf8mb4_unicode_ci -> "Illegal mix of collations" bei Text-Joins moeglich.
ALTER TABLE eu_urteile CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE eu_urteil_rechtsakte CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
