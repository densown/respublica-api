-- 019: Beschreibung, Quellen und Ergebnis je Abstimmung; Mandatsdauer
--
-- Fuer die neue Abstimmungsseite im Dashboard. abgeordnetenwatch liefert je
-- namentlicher Abstimmung einen Einleitungstext (field_intro, HTML) mit Links
-- auf die Drucksachen und eine eigene Seite. Bisher kam davon nichts in die
-- Datenbank, die Seite konnte weder erklaeren, worum es ging, noch auf die
-- Vorlage verlinken.
--
-- Befuellt von scripts/fetch_abstimmungen.py aus der Liste, die das Skript
-- ohnehin abruft (keine zusaetzlichen Anfragen). Der erste Lauf nach dieser
-- Migration traegt alle Abstimmungen der Wahlperiode nach.
--
-- Anwenden: CREATE TABLE und zwei neue Spalten mit NULL, keine Sperrwirkung
-- auf den Bestand.

CREATE TABLE IF NOT EXISTS abstimmung_details (
  poll_id       INT          NOT NULL,
  -- Einleitungstext ohne HTML und ohne "(PDF)", Absaetze durch Leerzeile
  intro         MEDIUMTEXT   NULL,
  -- Links aus dem Einleitungstext als JSON-Liste:
  -- [{"titel": "Gesetzentwurf", "url": "...", "dokument": "Drucksache 21/1234"}]
  links         TEXT         NULL,
  -- Unveraenderter Einleitungstext, damit sich intro und links bei einem
  -- besseren Parser ohne neuen Abruf neu erzeugen lassen
  intro_html    MEDIUMTEXT   NULL,
  quelle_url    VARCHAR(500) NULL,
  -- Ergebnis laut Schlusssatz des Einleitungstexts: 1 angenommen,
  -- 0 abgelehnt, NULL wenn der Satz es nicht eindeutig sagt. Nicht aus
  -- field_accepted: das Feld stimmt nicht immer (6724: true, laut Text und
  -- Stimmen abgelehnt). Wichtig bei Abstimmungen mit besonderer Mehrheit,
  -- dort reicht "mehr Ja als Nein" nicht.
  angenommen    TINYINT(1)   NULL,
  -- field_accepted der Quelle, nur zum Abgleich
  aw_accepted   TINYINT(1)   NULL,
  aktualisiert  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (poll_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Mandatsdauer. abgeordnetenwatch setzt start_date und end_date nur, wenn sie
-- von der Wahlperiode abweichen (Nachruecken, Mandatsverzicht, Tod). Ohne das
-- Ende stuenden Ausgeschiedene weiter unter den aktuellen Abgeordneten, sobald
-- ihre Nachruecker importiert sind, und das Sitzhalbrund haette mehr Personen
-- als Sitze.
ALTER TABLE abgeordnete
  ADD COLUMN IF NOT EXISTS mandat_von DATE NULL AFTER typ,
  ADD COLUMN IF NOT EXISTS mandat_bis DATE NULL AFTER mandat_von;
