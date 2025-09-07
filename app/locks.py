from .processor import process_file, process_all
from .db import try_acquire_advisory_lock, release_advisory_lock
from .config import ADVISORY_LOCK_KEY, UPLOADS_DIR

import logging

log = logging.getLogger("ingest-service")

def guarded_process_all(upload_dir=UPLOADS_DIR, embed_model=None):
    """
    Try to obtain a Postgres advisory lock, and only run process_all if lock obtained.
    Returns a dict with a status field: 'started', 'locked', or results.
    """
    conn, got = try_acquire_advisory_lock(ADVISORY_LOCK_KEY)
    if not got:
        log.info("Ingest already running (advisory lock held). Skipping this run.")
        return {"status": "locked", "message": "Another ingest is currently running"}
    try:
        log.info("Advisory lock acquired — starting ingestion")
        results = process_all(upload_dir, embed_model=embed_model)
        return {"status": "finished", "results": results}
    except Exception as e:
        log.exception("Error during guarded_process_all")
        return {"status": "error", "error": str(e)}
    finally:
        released = release_advisory_lock(conn, ADVISORY_LOCK_KEY)
        log.info(f"Advisory lock released: {released}")

def guarded_process_all_for_paths(paths, embed_model=None):
    conn, got = try_acquire_advisory_lock(ADVISORY_LOCK_KEY)
    if not got:
        return {"status": "locked"}
    try:
        results = {"processed": [], "skipped": []}
        for p in paths:
            if p.exists() and p.is_file():
                r = process_file(p, embed_model=embed_model)
                if r.get("skipped"):
                    results["skipped"].append(r)
                else:
                    results["processed"].append(r)
        return {"status": "finished", "results": results}
    finally:
        release_advisory_lock(conn, ADVISORY_LOCK_KEY)
