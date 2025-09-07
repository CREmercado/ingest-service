import psycopg2
from psycopg2.extras import execute_values
from .config import POSTGRES_DSN, ADVISORY_LOCK_KEY
from typing import Tuple, Optional

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

def try_acquire_advisory_lock(key: int = ADVISORY_LOCK_KEY) -> Tuple[Optional[psycopg2.extensions.connection], bool]:
    """
    Try to acquire an advisory lock using a dedicated DB connection.
    Returns (conn, True) if acquired, or (None, False) if lock not acquired.
    IMPORTANT: if you acquire the lock, keep the returned conn open until you release.
    """
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s);", (key,))
        got = cur.fetchone()[0]
        # do not close conn if got==True; keep it open to hold the lock
        cur.close()
        if not got:
            # release connection because we didn't acquire lock
            conn.close()
            return (None, False)
        return (conn, True)
    except Exception:
        if conn:
            conn.close()
        raise

def release_advisory_lock(conn: psycopg2.extensions.connection, key: int = ADVISORY_LOCK_KEY) -> bool:
    """
    Release advisory lock and close connection.
    Returns True if unlocked, False otherwise.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_unlock(%s);", (key,))
        ok = cur.fetchone()[0]
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return ok