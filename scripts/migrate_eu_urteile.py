#!/usr/bin/env python3
"""Erstellt eu_urteile und eu_urteil_rechtsakte (falls nicht vorhanden)."""
import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv('/root/apps/gesetze/.env')

DDL = [
    """
CREATE TABLE IF NOT EXISTS eu_urteile (
    id INT AUTO_INCREMENT PRIMARY KEY,
    celex VARCHAR(50) NOT NULL UNIQUE,
    ecli VARCHAR(100),
    gericht ENUM('EuGH', 'EuG') NOT NULL,
    typ VARCHAR(100),
    datum DATE,
    parteien TEXT,
    betreff TEXT,
    keywords TEXT,
    leitsatz TEXT,
    zusammenfassung_de TEXT,
    zusammenfassung_en TEXT,
    auswirkung_de TEXT,
    auswirkung_en TEXT,
    rechtsgebiet VARCHAR(100),
    eurlex_url VARCHAR(500),
    curia_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""",
    """
CREATE TABLE IF NOT EXISTS eu_urteil_rechtsakte (
    id INT AUTO_INCREMENT PRIMARY KEY,
    eu_urteil_id INT NOT NULL,
    eu_rechtsakt_id INT,
    rechtsakt_celex VARCHAR(50),
    FOREIGN KEY (eu_urteil_id) REFERENCES eu_urteile(id) ON DELETE CASCADE,
    INDEX idx_urteil (eu_urteil_id),
    INDEX idx_rechtsakt (eu_rechtsakt_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""",
]


def main():
    db = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
    )
    cur = db.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    db.commit()
    print('Migration eu_urteile / eu_urteil_rechtsakte OK.')
    cur.close()
    db.close()


if __name__ == '__main__':
    main()
