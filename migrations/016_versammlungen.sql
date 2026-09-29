-- 016: Versammlungen (Demonstrations-Tracker, Phase 0/1)
--
-- Hintergrund: Fuer Deutschland gibt es keine Statistik darueber, wie viele
-- Menschen demonstrieren. Berlin ist das einzige Land mit gesetzlicher
-- Veroeffentlichungspflicht (§ 12 VersFG BE). Die Polizei stellt die
-- angezeigten Versammlungen zweimal taeglich maschinenlesbar bereit, zeigt
-- aber nur kommende Termine. Niemand archiviert das. Wer es taeglich sichert,
-- hat nach einem Jahr eine Zeitreihe, die es sonst nirgends gibt.
--
-- Drei Tabellen:
--   versammlungen_rohdaten  unveraenderte Abrufe (Phase 0, das eigentliche Archiv)
--   versammlungen           normalisierte Ereignisse, eine Zeile pro Versammlung
--   versammlung_zahlen      Teilnehmerzahlen je Quelle (ab Phase 2 befuellt)
--
-- Datenschutz: Es werden KEINE Namen von Anmeldenden, Leitenden oder
-- Teilnehmenden gespeichert. Ort, Zeit und Thema sind oeffentliche Angaben,
-- Personen sind es nicht. Diese Trennlinie zieht auch das Berliner Gesetz.
--
-- Anwenden: nur CREATE TABLE, keine Sperrwirkung auf Bestand.

-- ---------------------------------------------------------------------------
-- Rohdaten-Archiv
-- ---------------------------------------------------------------------------
-- Jeder Abruf, dessen Inhalt sich vom vorherigen unterscheidet (sha256), wird
-- gzip-komprimiert abgelegt. Liegt bewusst in der DB statt nur auf Platte:
-- so ist das Archiv im taeglichen mysqldump-Backup enthalten. Aus diesen
-- Zeilen laesst sich `versammlungen` jederzeit komplett neu aufbauen, falls
-- sich der Parser aendert.
CREATE TABLE IF NOT EXISTS versammlungen_rohdaten (
  id          INT          NOT NULL AUTO_INCREMENT,
  quelle      VARCHAR(32)  NOT NULL,
  abgerufen   DATETIME     NOT NULL,
  sha256      CHAR(64)     NOT NULL,
  anzahl      INT          NULL,
  bytes       INT          NOT NULL,
  inhalt_gz   MEDIUMBLOB   NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_quelle_sha (quelle, sha256),
  INDEX idx_quelle_abgerufen (quelle, abgerufen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Versammlungen
-- ---------------------------------------------------------------------------
-- `quelle_id` ist die ID der Quelle, falls sie eine liefert, sonst ein
-- stabiler Hash aus Datum, Beginn, PLZ und Thema. UNIQUE (quelle, quelle_id)
-- macht den Import idempotent.
--
-- `status` ist vorsichtig formuliert. Berlin veroeffentlicht *angezeigte*
-- Versammlungen. Ob eine stattgefunden hat, steht nirgends:
--   angezeigt           in der aktuellen Liste enthalten
--   vergangen           Termin liegt zurueck, war bis zuletzt gelistet
--   vor_termin_entfernt verschwand vor dem Termin aus der Liste
--                       (meist Absage oder Verlegung, sicher ist das nicht)
--
-- `kategorie` wird von classify_versammlungen.py (Claude CLI) gesetzt,
-- `thema` bleibt immer der Originaltext der Quelle.
CREATE TABLE IF NOT EXISTS versammlungen (
  id                 INT          NOT NULL AUTO_INCREMENT,
  quelle             VARCHAR(32)  NOT NULL,
  quelle_id          VARCHAR(64)  NOT NULL,
  land               CHAR(2)      NOT NULL,
  stadt              VARCHAR(100) NOT NULL,
  datum              DATE         NOT NULL,
  von                TIME         NULL,
  bis                TIME         NULL,
  thema              TEXT         NULL,
  plz                VARCHAR(10)  NULL,
  ort                VARCHAR(500) NULL,
  aufzugsstrecke     TEXT         NULL,
  typ                ENUM('kundgebung','aufzug') NOT NULL DEFAULT 'kundgebung',
  kategorie          VARCHAR(32)  NULL,
  status             ENUM('angezeigt','vergangen','vor_termin_entfernt')
                     NOT NULL DEFAULT 'angezeigt',
  erstmals_gesehen   DATETIME     NOT NULL,
  zuletzt_gesehen    DATETIME     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_quelle_id (quelle, quelle_id),
  INDEX idx_datum (datum),
  INDEX idx_land_datum (land, datum),
  INDEX idx_kategorie_datum (kategorie, datum),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Teilnehmerzahlen (Phase 2)
-- ---------------------------------------------------------------------------
-- Eine Zeile pro genannter Zahl, nie ein einzelner Wert pro Versammlung.
-- Der Konflikt zwischen Veranstalter- und Polizeiangabe (Berlin, 21.01.2024:
-- 350.000 gegen 100.000) ist die eigentliche Information und soll sichtbar
-- bleiben statt in einem Mittelwert zu verschwinden.
CREATE TABLE IF NOT EXISTS versammlung_zahlen (
  id              INT          NOT NULL AUTO_INCREMENT,
  versammlung_id  INT          NOT NULL,
  quelle_typ      ENUM('veranstalter','polizei','presse','schaetzung') NOT NULL,
  wert            INT          NOT NULL,
  wert_bis        INT          NULL,
  quelle_name     VARCHAR(255) NULL,
  quelle_url      VARCHAR(500) NULL,
  genannt_am      DATETIME     NULL,
  PRIMARY KEY (id),
  INDEX idx_versammlung (versammlung_id),
  CONSTRAINT fk_vz_versammlung FOREIGN KEY (versammlung_id)
    REFERENCES versammlungen(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
