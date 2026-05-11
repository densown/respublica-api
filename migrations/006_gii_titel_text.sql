-- titel_offiziell: lange amtliche Titel (GII) uebersteigen VARCHAR(500)
ALTER TABLE gesetze DROP INDEX idx_titel;
ALTER TABLE gesetze MODIFY COLUMN titel_offiziell TEXT NULL;
ALTER TABLE gesetze ADD INDEX idx_titel (titel_offiziell(255));
