# Datenbankschema – respublica_gesetze

## gesetze
Stammdaten aller getrackten Gesetze.
- kuerzel: z.B. "MiLoG", "SGB II"
- pfad: Pfad im bundestag/gesetze Repo z.B. "m/milog/index.md"

## aenderungen
Jede Gesetzesänderung als eigener Eintrag.
- diff: Raw git diff Text
- zusammenfassung: Automatisch generiert
- kontext: Manuell von Res.Publica Redaktion
- bgbl_referenz: z.B. "BGBl. I Nr. 88"

## abstimmungen
Abstimmungsergebnis pro Partei für jede Änderung.
- Verknüpft mit aenderungen via aenderung_id
- Daten kommen von Abgeordnetenwatch API
