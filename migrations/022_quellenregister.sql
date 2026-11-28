-- 022: Quellenregister fuer alle Datenquellen (Fundament Europa-Daten)
--
-- data_sources und data_update_log gibt es seit der Weltkarte. Bisher stehen
-- dort nur deren Quellen. Ab jetzt traegt sich jede neue Quelle hier ein:
-- scripts/lib/quelle.py protokolliert jeden Abruf, /api/quellen liefert der
-- App Namen, Lizenz und Link zum Original, /api/stand sagt, welche Quelle
-- veraltet ist (App: Stand-Stempel, Brain: Morgen-Briefing).
--
-- Neue Spalten:
--   name_de             Anzeigename deutsch (name bleibt der englische)
--   license_url         Link zum Lizenztext
--   area                Bereich der App, z. B. 'welt', 'europa'
--   note_de, note_en    ein Satz: was die Quelle liefert
--   stale_after_hours   ab wie vielen Stunden ohne erfolgreichen Abruf die
--                       Quelle als veraltet gilt; NULL = Vorgabe nach
--                       update_freq (api/lib/quellen.js)
-- update_freq bekommt den Wert 'hourly' (hinten angehaengt, damit die
-- bestehenden Werte ihre Nummern behalten).
--
-- Anwenden: nur neue Spalten mit NULL, keine Wirkung auf die Weltkarte. Der
-- Autodeploy haelt bei dieser Migration an, bis sie von Hand eingespielt ist.

ALTER TABLE data_sources
  MODIFY COLUMN update_freq ENUM('daily','weekly','monthly','quarterly','yearly','manual','hourly') DEFAULT 'weekly',
  ADD COLUMN IF NOT EXISTS name_de VARCHAR(200) NULL AFTER name,
  ADD COLUMN IF NOT EXISTS license_url VARCHAR(500) NULL AFTER license,
  ADD COLUMN IF NOT EXISTS area VARCHAR(30) NULL AFTER license_url,
  ADD COLUMN IF NOT EXISTS note_de VARCHAR(500) NULL AFTER area,
  ADD COLUMN IF NOT EXISTS note_en VARCHAR(500) NULL AFTER note_de,
  ADD COLUMN IF NOT EXISTS stale_after_hours INT UNSIGNED NULL AFTER update_freq;

UPDATE data_sources SET area = 'welt' WHERE area IS NULL;
