-- Einzelnormen: fundstelle_zitstelle kann >50 Zeichen sein (GII)
ALTER TABLE gesetze MODIFY COLUMN fundstelle_periodikum VARCHAR(64) NULL;
ALTER TABLE gesetze MODIFY COLUMN fundstelle_zitstelle VARCHAR(512) NULL;
