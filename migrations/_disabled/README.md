# Deaktivierte Migrationen

Dateien hier werden vom Runner (`scripts/migrate.py`) **nicht** angewandt.

- `003_news_items.sql` — verwaiste Migration: die Tabelle `news_items` existiert
  in der DB nicht (nie angewandt oder später entfernt) und kollidierte mit der
  Nummer von `003_wahlen.sql`. Deaktiviert (M-015). Bei Bedarf zurück nach
  `migrations/` verschieben und `python scripts/migrate.py` ausführen.
