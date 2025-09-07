CREATE TABLE IF NOT EXISTS processed_files (
  id SERIAL PRIMARY KEY,
  file_path TEXT,
  source_hash TEXT UNIQUE,
  collection TEXT,
  points_count INTEGER,
  processed_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS processed_files_unique_hash ON processed_files (source_hash);