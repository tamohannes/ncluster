import time
import logging
import threading
import os

log = logging.getLogger("server.progress_scraper")

def _progress_scraper_loop():
    from .config import CLUSTERS, _cache, _cache_lock, PROGRESS_TTL_SEC, CRASH_TTL_SEC
    from .db import cache_db_put
    from .jobs import extract_progress, detect_crash, _LOG_ERROR_PREFIXES
    from .logs import _db_log_path, _try_local_discovery, tail_local_file
    from .mounts import resolve_mounted_path
    from .routes import _cache_get, _cache_set, _progress_cache, _progress_source_cache, _crash_cache, _log_content_cache
    from .poller import get_poller, bump_version
    
    while True:
        try:
            p = get_poller()
            if getattr(p, "_idle", False):
                time.sleep(5)
                continue
                
            any_changed = False
            for cluster in CLUSTERS:
                if cluster == "local":
                    continue
                with _cache_lock:
                    data = _cache.get(cluster, {})
                    active_jobs = [j["jobid"] for j in data.get("jobs", []) 
                                   if j.get("state", "").upper() in ("RUNNING", "COMPLETING")]
                
                for jid in active_jobs:
                    try:
                        db_path = _db_log_path(cluster, jid)
                        if not db_path:
                            continue
                            
                        db_path = db_path.replace("%j", str(jid))
                        
                        # Use local mount discovery to find ALL logs (main, server, sandbox, sbatch)
                        local_result = _try_local_discovery(cluster, jid, db_path)
                        if not local_result:
                            continue
                            
                        files = local_result.get("files", [])
                        
                        # Check logs in priority order (main -> server -> sandbox -> sbatch)
                        for f in files:
                            mt = resolve_mounted_path(cluster, f["path"], want_dir=False)
                            if mt and os.path.isfile(mt):
                                content = tail_local_file(mt, lines=220)
                                if content and not any(content.startswith(err) for err in _LOG_ERROR_PREFIXES):
                                    src = f.get("label", "")
                                    _cache_set(_log_content_cache, (cluster, str(jid), src), content)
                                    
                                    crash = detect_crash(content)
                                    if crash:
                                        _cache_set(_crash_cache, (cluster, jid), crash)
                                        try:
                                            cache_db_put("crash", f"{cluster}:{jid}", crash, CRASH_TTL_SEC)
                                        except Exception:
                                            pass
                                    
                                    pct = extract_progress(content)
                                    if pct is not None:
                                        old_pct = _cache_get(_progress_cache, (cluster, str(jid)), PROGRESS_TTL_SEC)
                                        if old_pct != pct:
                                            any_changed = True
                                        _cache_set(_progress_cache, (cluster, str(jid)), pct)
                                        _cache_set(_progress_source_cache, (cluster, str(jid)), src)
                                        try:
                                            cache_db_put("progress", f"{cluster}:{jid}", pct, PROGRESS_TTL_SEC)
                                            cache_db_put("progress_source", f"{cluster}:{jid}", src, PROGRESS_TTL_SEC)
                                        except Exception:
                                            pass
                                        # Found progress, stop checking fallback logs
                                        break
                                        
                    except Exception as ex:
                        log.debug(f"Error scraping progress for {cluster}:{jid} - {ex}")
            
            if any_changed:
                bump_version()
                
        except Exception as e:
            log.error("Progress scraper error: %s", e)
            
        time.sleep(4)

def start_progress_scraper():
    t = threading.Thread(target=_progress_scraper_loop, daemon=True, name="progress_scraper")
    t.start()
    log.info("progress scraper started")
    return t
