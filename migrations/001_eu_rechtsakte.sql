CREATE TABLE eu_rechtsakte (
    id INT AUTO_INCREMENT PRIMARY KEY,
    celex VARCHAR(50) UNIQUE NOT NULL,
    titel_de TEXT,
    titel_en TEXT,
    typ ENUM('REG', 'DIR', 'DEC', 'REC', 'OTHER') NOT NULL DEFAULT 'OTHER',
    typ_label VARCHAR(100),
    datum DATE,
    in_kraft VARCHAR(20),
    eurovoc_tags JSON,
    zusammenfassung TEXT,
    rechtsgebiet VARCHAR(100),
    eurlex_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE eu_rechtsakt_gesetze (
    id INT AUTO_INCREMENT PRIMARY KEY,
    eu_rechtsakt_id INT NOT NULL,
    gesetz_id INT NOT NULL,
    FOREIGN KEY (eu_rechtsakt_id) REFERENCES eu_rechtsakte(id) ON DELETE CASCADE,
    FOREIGN KEY (gesetz_id) REFERENCES gesetze(id) ON DELETE CASCADE,
    UNIQUE KEY unique_link (eu_rechtsakt_id, gesetz_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
