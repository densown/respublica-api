-- 021: Tracker Kleine Anfragen (Antworten der Bundesregierung)
--
-- Eine Zeile je Antwort-Drucksache auf eine Kleine Anfrage, Quelle DIP
-- (search.dip.bundestag.de, Endpunkte drucksache-text und vorgang).
-- scripts/fetch_kleine_anfragen.py fuellt Metadaten und Volltext, der
-- Volltext kommt im DIP einige Tage nach der Drucksache (Stand 21.11.2026:
-- etwa 5 Tage). scripts/summarize_kleine_anfragen.py schreibt Zusammenfassung,
-- Kernzahlen und eine Einschaetzung, ob die Antwort ein Thema hergibt.
--
-- kernzahlen: JSON-Liste [{wert, einheit, bedeutung, zitat}]. Jede Zahl ist
-- gegen den Volltext geprueft: das Zitat steht woertlich in einem Teil der
-- Bundesregierung (nicht in der Vorbemerkung der Fragesteller, nicht in einer
-- Frage). Nicht belegte Zahlen speichert das Skript nicht.
--
-- nicht_erhoben zaehlt Formulierungen wie "statistisch nicht erfasst" oder
-- "mit vertretbarem Aufwand nicht". Ohne KI, aus dem Volltext.
--
-- Anwenden: neue Tabelle, keine Wirkung auf den Bestand. Der Autodeploy haelt
-- bei dieser Migration an, bis sie von Hand eingespielt ist.

CREATE TABLE IF NOT EXISTS kleine_anfragen (
  id                   INT UNSIGNED      NOT NULL,
  dokumentnummer       VARCHAR(16)       NOT NULL,
  wahlperiode          TINYINT UNSIGNED  NOT NULL,
  datum                DATE              NOT NULL,
  titel                VARCHAR(700)      NOT NULL,
  vorgang_id           INT UNSIGNED      NULL,
  frage_dokumentnummer VARCHAR(16)       NULL,
  fraktion             VARCHAR(16)       NULL,
  initiative           VARCHAR(255)      NULL,
  fragesteller         TEXT              NULL,
  ressort              VARCHAR(255)      NULL,
  sachgebiete          TEXT              NULL,
  deskriptoren         TEXT              NULL,
  einzelfragen         SMALLINT UNSIGNED NULL,
  pdf_url              VARCHAR(255)      NULL,
  volltext             MEDIUMTEXT        NULL,
  text_zeichen         INT UNSIGNED      NULL,
  text_hash            CHAR(40)          NULL,
  nicht_erhoben        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  hat_anlage           TINYINT(1)        NOT NULL DEFAULT 0,
  eingestuft           TINYINT(1)        NOT NULL DEFAULT 0,
  dip_aktualisiert     DATETIME          NULL,
  zusammenfassung      TEXT              NULL,
  zusammenfassung_en   TEXT              NULL,
  schlagzeile          VARCHAR(300)      NULL,
  kernzahlen           TEXT              NULL,
  story_wert           TINYINT UNSIGNED  NULL,
  story_grund          VARCHAR(600)      NULL,
  ki_text_hash         CHAR(40)          NULL,
  ki_modell            VARCHAR(64)       NULL,
  ki_am                DATETIME          NULL,
  ki_fehlversuche      TINYINT UNSIGNED  NOT NULL DEFAULT 0,
  importiert_am        TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP,
  aktualisiert_am      TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_ka_dokumentnummer (dokumentnummer),
  KEY idx_ka_datum (datum),
  KEY idx_ka_fraktion_datum (fraktion, datum),
  KEY idx_ka_ressort (ressort(64)),
  KEY idx_ka_story (story_wert, datum),
  FULLTEXT KEY ft_ka_kurz (titel, zusammenfassung, deskriptoren),
  FULLTEXT KEY ft_ka_volltext (volltext)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
