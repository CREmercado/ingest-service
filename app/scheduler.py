from apscheduler.schedulers.background import BackgroundScheduler
from logger import setup_logging

log = setup_logging()

_scheduler = None

def start_scheduler(job_func, minutes: int):
    global _scheduler
    if minutes <= 0:
        return None
    _scheduler = BackgroundScheduler()
    _scheduler.add_job(job_func, 'interval', minutes=minutes, id="ingest_job")
    _scheduler.start()
    log.info(f"Scheduler started every {minutes} minutes")
    return _scheduler

def stop_scheduler():
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        log.info("Scheduler stopped")