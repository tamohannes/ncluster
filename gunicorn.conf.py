"""Gunicorn configuration for clausius.

Single worker with many threads: the app relies on shared in-memory state
(poller, progress scraper, job cache, board version) that cannot be split
across processes.  One process with 32 threads handles I/O-bound SSH work
well while keeping all caches consistent.

Self-healing:
  - max_requests: worker is recycled after N requests, clearing any
    accumulated state corruption (leaked FDs, stuck semaphores, etc.)
  - max_requests_jitter: spreads restarts so they don't all hit at once
    (matters less with 1 worker, but good practice)
  - timeout: if a worker thread blocks beyond this, gunicorn's arbiter
    kills and respawns the worker — the app auto-recovers within seconds.
"""

import threading

from server.config import APP_PORT

bind = f"0.0.0.0:{APP_PORT}"
workers = 1
worker_class = "gthread"
threads = 32
timeout = 30
graceful_timeout = 10
keepalive = 5

max_requests = 4000
max_requests_jitter = 400

accesslog = None
errorlog = "-"
loglevel = "info"


def post_fork(server, worker):
    from app import _run_init
    _run_init()
