import uuid
import datetime
from pathlib import Path
from typing import Dict, Any, List
from .logger import setup_logging
from .config import (
    CHUNK_MAX_CHARS, CHUNK_OVERLAP, UPSERT_BATCH_SIZE,
    UPLOADS_DIR, OLLAMA_EMBED_MODEL, QDRANT_COLLECTION
)
from .db import ensure_processed_table, already_ingested, mark_as_processed
from .clients.tika_client import extract_text
from .clients.ollama_client import embed_text
from .clients.qdrant_client import upsert_points
from .chunker import chunk_text
from hashlib import sha3_256

log = setup_logging()

def sha3_256_bytes(b: bytes) -> str:
    return sha3_256(b).hexdigest()

def list_files(upload_dir: Path) -> List[Path]:
    files = []
    for p in upload_dir.rglob("*"):
        if p.is_file() and not p.name.startswith("."):
            files.append(p)
    return sorted(files)

def process_file(path: Path, embed_model: str = None) -> Dict[str, Any]:
    embed_model = embed_model or OLLAMA_EMBED_MODEL
    log.info(f"Processing file {path}")
    text_bytes = path.read_bytes()
    source_hash = sha3_256_bytes(text_bytes)
    if already_ingested(source_hash):
        log.info(f"Skipping (already ingested): {path}")
        return {"skipped": True, "path": str(path)}

    text = extract_text(text_bytes)
    if not text or not text.strip():
        log.warning(f"No text extracted for {path}; marking as processed with 0 points.")
        mark_as_processed(str(path), source_hash, QDRANT_COLLECTION, 0)
        return {"skipped": False, "path": str(path), "points": 0}

    chunks = chunk_text(text, CHUNK_MAX_CHARS, CHUNK_OVERLAP)
    log.info(f"File {path} produced {len(chunks)} chunks.")

    points = []
    for idx, chunk in enumerate(chunks):
        vec = embed_text(chunk, model=embed_model)
        point_id = str(uuid.uuid4())
        points.append({
            "id": point_id,
            "vector": vec,
            "payload": {
                "source_file": str(path),
                "source_hash": source_hash,
                "chunkIndex": idx,
                "text": chunk,
                "ingested_at": datetime.datetime.utcnow().isoformat()
            }
        })

    for i in range(0, len(points), UPSERT_BATCH_SIZE):
        batch = points[i:i+UPSERT_BATCH_SIZE]
        upsert_points(batch, collection=QDRANT_COLLECTION)

    mark_as_processed(str(path), source_hash, QDRANT_COLLECTION, len(points))
    return {"skipped": False, "path": str(path), "points": len(points)}

def process_all(upload_dir: Path = None, embed_model: str = None):
    ensure_processed_table()
    upload_dir = upload_dir or UPLOADS_DIR
    files = list_files(upload_dir)
    results = {"processed": [], "skipped": []}
    for f in files:
        try:
            r = process_file(f, embed_model=embed_model)
            if r.get("skipped"):
                results["skipped"].append(r)
            else:
                results["processed"].append(r)
        except Exception as exc:
            log.exception(f"Error processing {f}: {exc}")
            results.setdefault("errors", []).append({"path": str(f), "error": str(exc)})
    return results