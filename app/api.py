from fastapi import FastAPI, BackgroundTasks
from .logger import setup_logging
from .config import SCHEDULE_MINUTES, UPLOADS_DIR, OLLAMA_EMBED_MODEL
from .schemas import IngestRequest
from .processor import process_all, process_file
from .db import ensure_processed_table
from .scheduler import start_scheduler

log = setup_logging()
app = FastAPI(title="Ingest Service")

@app.on_event("startup")
def startup():
    ensure_processed_table()
    if SCHEDULE_MINUTES and SCHEDULE_MINUTES > 0:
        start_scheduler(lambda: process_all(UPLOADS_DIR), SCHEDULE_MINUTES)

@app.get("/health")
def health():
    # minimal health: DB table exists; more checks can be added
    try:
        ensure_processed_table()
        return {"status": "ok"}
    except Exception as e:
        log.exception("health check failed")
        return {"status": "degraded"}

@app.post("/ingest")
def ingest(req: IngestRequest = None, background_tasks: BackgroundTasks = None):
    req = req or IngestRequest()
    model_to_use = req.model_id or OLLAMA_EMBED_MODEL

    if req.paths:
        paths = [Path(p if os.path.isabs(p) else UPLOADS_DIR / p) for p in req.paths]
        def run_paths():
            ensure_processed_table()
            results = {"processed": [], "skipped": []}
            for p in paths:
                if p.exists() and p.is_file():
                    results_part = process_file(p, embed_model=model_to_use)
                    if results_part.get("skipped"):
                        results["skipped"].append(results_part)
                    else:
                        results["processed"].append(results_part)
            return results
        if req.sync:
            return run_paths()
        else:
            background_tasks.add_task(run_paths)
            return {"status": "accepted", "message": "ingest started in background for specified paths"}

    if req.sync:
        return process_all(UPLOADS_DIR, embed_model=model_to_use)
    else:
        background_tasks.add_task(process_all, UPLOADS_DIR, model_to_use)
        return {"status": "accepted", "message": "ingest started in background (processing all files)"}