-- 020: Kurztext je Abstimmung, automatisch zusammengefasst
--
-- Fuer die Abstimmungsliste der Bundestag-Uebersicht und das Bild 4:5. Der
-- Einleitungstext von abgeordnetenwatch beginnt oft mit Vorgeschichte und ist
-- fuer eine Karte zu lang. scripts/kurztexte_abstimmungen.py schreibt daraus
-- zwei sachliche Saetze auf Deutsch und Englisch (Groq). Luca hat am
-- 19.10.2026 entschieden: ohne Freigabe, die Seite kennzeichnet sie als
-- automatisch zusammengefasst.
--
-- kurztext_hash ist SHA1(intro) zum Zeitpunkt des Schreibens. Aendert
-- abgeordnetenwatch den Text, passt der Hash nicht mehr und der naechste Lauf
-- schreibt den Kurztext neu.
--
-- Anwenden: nur neue Spalten mit NULL, keine Sperrwirkung auf den Bestand.
-- Die API fragt die Spalten direkt ab. Der Autodeploy haelt deshalb bei
-- dieser Migration an, bis sie eingespielt ist.

ALTER TABLE abstimmung_details
  ADD COLUMN IF NOT EXISTS kurztext        TEXT        NULL AFTER aw_accepted,
  ADD COLUMN IF NOT EXISTS kurztext_en     TEXT        NULL AFTER kurztext,
  ADD COLUMN IF NOT EXISTS kurztext_hash   CHAR(40)    NULL AFTER kurztext_en,
  ADD COLUMN IF NOT EXISTS kurztext_modell VARCHAR(64) NULL AFTER kurztext_hash,
  ADD COLUMN IF NOT EXISTS kurztext_am     DATETIME    NULL AFTER kurztext_modell;
