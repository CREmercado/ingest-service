import psycopg2
from psycopg2.extras import execute_values
from .config import POSTGRES_DSN
from typing import Optional

def get_db_conn():
    return psycopg2.connect(POSTGRES_DSN)

def ensure_processed_table():
    sql = """
    CREATE TABLE IF NOT EXISTS processed_files (
        id SERIAL PRIMARY KEY,
        file_path TEXT,
        source_hash TEXT UNIQUE,
        collection TEXT,
        points_count INTEGER,
        processed_at TIMESTAMP DEFAULT NOW()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS processed_files_unique_hash ON processed_files (source_hash);
    """
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def mark_as_processed(file_path: str, source_hash: str, collection: str, points_count: int):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO processed_files (file_path, source_hash, collection, points_count, processed_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (source_hash) DO UPDATE SET processed_at = NOW(), file_path = EXCLUDED.file_path, points_count = EXCLUDED.points_count;
        """,
        (file_path, source_hash, collection, points_count),
    )
    conn.commit()
    cur.close()
    conn.close()

def already_ingested(source_hash: str) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM processed_files WHERE source_hash = %s;", (source_hash,))
    ok = cur.fetchone() is not None
    cur.close()
    conn.close()
    return ok