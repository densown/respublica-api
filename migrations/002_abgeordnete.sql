CREATE TABLE IF NOT EXISTS abgeordnete (
  id INT PRIMARY KEY AUTO_INCREMENT,
  aw_id INT UNIQUE,
  politiker_id INT,
  vorname VARCHAR(100),
  nachname VARCHAR(100),
  name VARCHAR(200),
  fraktion VARCHAR(100),
  wahlkreis VARCHAR(200),
  wahlkreis_nr INT,
  listenplatz INT,
  profil_url VARCHAR(500),
  foto_url VARCHAR(500),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
