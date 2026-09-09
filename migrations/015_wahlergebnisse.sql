-- 015: Amtliche Wahlergebnisse zu den Wahlterminen
--
-- Bis hierher kennt das Wahlen-Modul nur Umfragen (012). Was fehlte, war der
-- Endpunkt: das tatsaechliche Ergebnis. Ohne ihn kann die Umfrageseite eine
-- gelaufene Wahl nur als "kommend" oder als stummes Archiv zeigen — und die
-- interessanteste Groesse, die Abweichung zwischen letzter Umfrage und
-- Ergebnis, ist gar nicht berechenbar.
--
-- Bewusst NICHT in `wahlen` (GERDA, 49.857 Zeilen): die Tabelle ist
-- AGS-/gemeindebasiert, hat feste Partei-Spalten statt Verknuepfungen und
-- keinen Platz fuer Sitze. Sie bleibt unberuehrt.
--
-- `wahl_ergebnisse` spiegelt absichtlich den Aufbau von `umfrage_werte`
-- (wahltermin_id + partei_id + prozent). Dadurch laesst sich ein Ergebnis im
-- Frontend ohne Sonderfall wie eine weitere Umfragezeile behandeln.
--
-- Anwenden: nur ADD COLUMN + CREATE TABLE, keine Sperrwirkung auf Bestand.

-- ---------------------------------------------------------------------------
-- Kopfdaten am Wahltermin
-- ---------------------------------------------------------------------------
-- Diese Werte gelten fuer die Wahl als Ganzes, nicht je Partei, und gehoeren
-- deshalb an `wahltermine` statt in die Ergebnistabelle.
--
-- `ergebnis_status` ist wichtiger als es aussieht: zwischen vorlaeufigem und
-- endgueltigem amtlichem Ergebnis liegen ein bis zwei Wochen, in denen sich
-- Zahlen noch verschieben. Die Seite muss den Unterschied ausweisen koennen,
-- sonst behauptet sie eine Endgueltigkeit, die es noch nicht gibt.
-- NULL = kein Ergebnis erfasst (Wahl steht noch aus).
ALTER TABLE wahltermine
  ADD COLUMN wahlbeteiligung  DECIMAL(5,2) NULL AFTER status,
  ADD COLUMN sitze_gesamt     SMALLINT     NULL AFTER wahlbeteiligung,
  ADD COLUMN ergebnis_status  ENUM('vorlaeufig','endgueltig') NULL AFTER sitze_gesamt,
  ADD COLUMN ergebnis_stand   DATETIME     NULL AFTER ergebnis_status,
  ADD COLUMN ergebnis_quelle  VARCHAR(255) NULL AFTER ergebnis_stand,
  ADD COLUMN ergebnis_quelle_url VARCHAR(500) NULL AFTER ergebnis_quelle;

-- ---------------------------------------------------------------------------
-- Ergebnis je Partei
-- ---------------------------------------------------------------------------
-- `prozent` mit zwei Nachkommastellen, nicht einer wie bei `umfrage_werte`:
-- amtliche Ergebnisse werden so veroeffentlicht (43,79 %), und beim Vergleich
-- mit Umfragen soll die Rundung nicht aus dem Ergebnis kommen.
--
-- `stimmen` ist die absolute Zahl der gueltigen Stimmen. Sie ist die einzige
-- Groesse, aus der sich Prozente jederzeit neu ableiten lassen, falls die
-- Landeswahlleitung nachkorrigiert.
--
-- Sitze getrennt nach Direkt- und Listenmandat, weil genau diese Aufteilung
-- die Aussage traegt: 2026 holte die AfD in Sachsen-Anhalt 38 von 41
-- Direktmandaten, die CDU keines — im Prozentwert allein ist das unsichtbar.
-- NULL bei `sitze*` heisst "nicht in den Landtag eingezogen" bzw. bei
-- Sammelzeilen wie `other` schlicht nicht sinnvoll.
CREATE TABLE IF NOT EXISTS wahl_ergebnisse (
  wahltermin_id INT          NOT NULL,
  partei_id     INT          NOT NULL,
  prozent       DECIMAL(5,2) NOT NULL,
  stimmen       INT          NULL,
  sitze         SMALLINT     NULL,
  sitze_direkt  SMALLINT     NULL,
  sitze_liste   SMALLINT     NULL,
  PRIMARY KEY (wahltermin_id, partei_id),
  INDEX idx_partei (partei_id),
  CONSTRAINT fk_we_wahltermin FOREIGN KEY (wahltermin_id)
    REFERENCES wahltermine(id) ON DELETE CASCADE,
  CONSTRAINT fk_we_partei FOREIGN KEY (partei_id)
    REFERENCES parteien(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
