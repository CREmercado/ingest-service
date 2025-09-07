from pathlib import Path
import os

# Paths
UPLOADS_DIR = Path(os.getenv("UPLOADS_DIR", "/data/uploads"))

# External services
TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998/tika")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "rag_docs")

# DB
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/postgres")

# Chunking / batching
CHUNK_MAX_CHARS = int(os.getenv("CHUNK_MAX_CHARS", "2000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "500"))
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "50"))

# Scheduler
SCHEDULE_MINUTES = int(os.getenv("SCHEDULE_MINUTES", "0"))

# Misc
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# any int64 to be the lock key
ADVISORY_LOCK_KEY = int(os.getenv("ADVISORY_LOCK_KEY", "987654321"))