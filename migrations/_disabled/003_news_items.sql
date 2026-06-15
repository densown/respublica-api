CREATE TABLE IF NOT EXISTS news_items (
  id INT AUTO_INCREMENT PRIMARY KEY,
  guid VARCHAR(512) UNIQUE NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  content TEXT,
  url VARCHAR(512),
  published_at DATETIME,
  fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  source_key VARCHAR(100),
  source_name VARCHAR(200),
  category VARCHAR(100),
  language VARCHAR(10) DEFAULT 'de',
  groq_summary TEXT,
  summarized_at DATETIME,
  INDEX idx_category (category),
  INDEX idx_published (published_at),
  INDEX idx_source (source_key)
);
