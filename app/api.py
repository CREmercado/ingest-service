import requests
from pathlib import Path
from fastapi import FastAPI, BackgroundTasks
from .logger import setup_logging
from .config import SCHEDULE_MINUTES, UPLOADS_DIR, OLLAMA_EMBED_MODEL, TIKA_URL
from .schemas import IngestRequest
from .processor import process_all, process_file
from .db import ensure_processed_table, get_db_conn
from .scheduler import start_scheduler
from .locks import guarded_process_all, guarded_process_all_for_paths

log = setup_logging()
app = FastAPI(title="Ingest Service")

@app.on_event("startup")
def startup():
    ensure_processed_table()
    if SCHEDULE_MINUTES and SCHEDULE_MINUTES > 0:
        start_scheduler(lambda: guarded_process_all(UPLOADS_DIR), SCHEDULE_MINUTES)

@app.get("/health") 
def health(): 
    # basic health info: DB connectivity and Tika reachable 
    try: 
        conn = get_db_conn() 
        conn.close() 
        db_ok = True 
    except Exception as e: 
        db_ok = False 
        log.exception("DB health check failed") 
        
    try: 
        r = requests.get(TIKA_URL, timeout=5) 
        tika_ok = r.status_code == 200 or r.status_code == 405 or r.status_code == 400 
    except Exception: 
        tika_ok = False 
    
    return {"status": "ok" if db_ok and tika_ok else "degraded", "db": db_ok, "tika": tika_ok}

@app.post("/ingest")
def ingest(req: IngestRequest = None, background_tasks: BackgroundTasks = None):
    req = req or IngestRequest()
    model_to_use = req.model_id or OLLAMA_EMBED_MODEL

    if req.paths:
        paths = [Path(p if os.path.isabs(p) else UPLOADS_DIR / p) for p in req.paths]
        def run_paths():
            ensure_processed_table()
            # If want locking even for partial paths, we can still use guarded wrapper:
            return guarded_process_all_for_paths(paths, embed_model=model_to_use)

        if req.sync:
            return run_paths()
        else:
            background_tasks.add_task(run_paths)
            return {"status": "accepted", "message": "ingest started in background for specified paths"}

    if req.sync:
        return guarded_process_all(UPLOADS_DIR, embed_model=model_to_use)
    else:
        background_tasks.add_task(guarded_process_all, UPLOADS_DIR, model_to_use)
        return {"status": "accepted", "message": "ingest started in background (processing all files)"}