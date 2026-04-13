"""Background poller — owns all periodic SSH data-fetching.

No HTTP request should ever trigger SSH for data collection.  The poller
cycles through clusters with adaptive scheduling: healthy clusters are
polled every HEALTHY_INTERVAL seconds; failing clusters back off
exponentially up to MAX_BACKOFF seconds.

Demand-driven: the poller only runs when someone is watching.  A consumer
(frontend /api/jobs, MCP tool call) signals demand via touch_demand().
If no demand has been seen for DEMAND_IDLE_SEC, polling pauses until the
next consumer arrives.

A global version counter is bumped on every data change so the /api/jobs
endpoint can serve instant 304 Not Modified responses when nothing changed.
"""

import logging
import queue
import threading
import time

from .config import CLUSTERS

log = logging.getLogger("server.poller")

# ── Version counter ──────────────────────────────────────────────────────────

_board_version = 0
_board_version_lock = threading.Lock()


def bump_version():
    global _board_version
    with _board_version_lock:
        _board_version += 1
    return _board_version


def get_version():
    with _board_version_lock:
        return _board_version


# ── Demand signal ────────────────────────────────────────────────────────────

_last_demand = 0.0
_demand_lock = threading.Lock()


def touch_demand():
    """Called by API routes and MCP tools to signal that someone wants data."""
    global _last_demand
    with _demand_lock:
        _last_demand = time.monotonic()


def _demand_age():
    with _demand_lock:
        return time.monotonic() - _last_demand if _last_demand else float("inf")


# ── Poller ────────────────────────────────────────────────────────────────────

class Poller:
    HEALTHY_INTERVAL = 15
    MAX_BACKOFF = 120
    PRIORITY_COOLDOWN = 5
    DEMAND_IDLE_SEC = 120

    def __init__(self):
        self._schedules = {}
        self._failures = {}
        self._last_priority = {}
        self._priority = queue.Queue()
        self._stop = threading.Event()
        self._last_success = {}
        self._idle = False

    def run(self):
        now = time.monotonic()
        for i, name in enumerate(CLUSTERS):
            self._schedules[name] = now + i * 0.5

        while not self._stop.is_set():
            # Priority requests always go through regardless of demand
            handled = self._drain_priority()
            if handled:
                continue

            if _demand_age() > self.DEMAND_IDLE_SEC:
                if not self._idle:
                    log.info("poller idle — no consumers for %ds", self.DEMAND_IDLE_SEC)
                    self._idle = True
                self._stop.wait(2)
                continue

            if self._idle:
                log.info("poller resumed — consumer detected")
                self._idle = False
                now = time.monotonic()
                for i, name in enumerate(CLUSTERS):
                    self._schedules[name] = now + i * 0.5

            cluster = self._next_due()
            if cluster:
                self._poll_one(cluster)
            else:
                self._stop.wait(1)

    def _drain_priority(self):
        """Process one priority request if available. Returns True if handled."""
        try:
            name = self._priority.get_nowait()
        except queue.Empty:
            return False

        if name not in CLUSTERS:
            return True

        now = time.monotonic()
        last = self._last_priority.get(name, 0)
        if now - last >= self.PRIORITY_COOLDOWN:
            self._last_priority[name] = now
            self._poll_one(name)
        return True

    def _next_due(self):
        now = time.monotonic()
        best, best_at = None, float("inf")
        for name, at in self._schedules.items():
            if name not in CLUSTERS:
                continue
            if at <= now and at < best_at:
                best, best_at = name, at
        return best

    POLL_TIMEOUT = 45

    def _poll_one(self, name):
        from .jobs import poll_cluster
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        try:
            prev_data = self._snapshot_ids(name)

            with ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"poll-{name}") as pool:
                fut = pool.submit(poll_cluster, name)
                try:
                    fut.result(timeout=self.POLL_TIMEOUT)
                except FuturesTimeout:
                    log.warning("poll %s timed out after %ds, moving on",
                                name, self.POLL_TIMEOUT)
                    raise TimeoutError(f"poll {name} exceeded {self.POLL_TIMEOUT}s")

            curr_data = self._snapshot_ids(name)

            changed = prev_data != curr_data
            if changed:
                bump_version()
                log.debug("poll %s: data changed (v%d)", name, get_version())

            self._failures.pop(name, None)
            self._last_success[name] = time.monotonic()
            self._reschedule(name, self.HEALTHY_INTERVAL)

        except Exception as e:
            count = self._failures.get(name, 0) + 1
            self._failures[name] = min(count, 10)
            delay = min(self.HEALTHY_INTERVAL * (2 ** count), self.MAX_BACKOFF)
            self._reschedule(name, delay)
            log.warning("poll %s failed (#%d), backoff %ds: %s",
                        name, count, delay, e)

    def _snapshot_ids(self, name):
        """Return a hashable snapshot of the current cached job state."""
        from .config import _cache_lock, _cache
        with _cache_lock:
            data = _cache.get(name, {})
            jobs = data.get("jobs", [])
            status = data.get("status", "")
            updated = data.get("updated", "")
        job_set = frozenset(
            (j.get("jobid", ""), j.get("state", ""))
            for j in jobs
        )
        return (status, updated, job_set)

    def _reschedule(self, name, delay):
        self._schedules[name] = time.monotonic() + delay

    def request_priority(self, cluster):
        """Signal the poller to poll a cluster ASAP (manual retry)."""
        if cluster in CLUSTERS:
            self._priority.put(cluster)

    def get_status(self):
        """Per-cluster poller state for API responses."""
        now = time.monotonic()
        out = {}
        for name in CLUSTERS:
            failures = self._failures.get(name, 0)
            next_at = self._schedules.get(name, now)
            last_ok = self._last_success.get(name)
            staleness = round(now - last_ok, 1) if last_ok else None

            if self._idle:
                state = "idle"
            elif failures == 0:
                state = "healthy"
            elif failures <= 2:
                state = "retrying"
            else:
                state = "backoff"

            out[name] = {
                "state": state,
                "failure_count": failures,
                "next_poll_sec": max(0, round(next_at - now, 1)),
                "staleness_sec": staleness,
            }
        return out

    def stop(self):
        self._stop.set()


# ── Module-level singleton ────────────────────────────────────────────────────

_poller = None
_poller_lock = threading.Lock()


def get_poller():
    """Return the singleton Poller instance, creating it if needed."""
    global _poller
    if _poller is None:
        with _poller_lock:
            if _poller is None:
                _poller = Poller()
    return _poller


def start_poller():
    """Start the poller background thread (called from _run_init)."""
    p = get_poller()
    t = threading.Thread(target=p.run, daemon=True, name="poller")
    t.start()
    log.info("poller started (%d clusters, interval=%ds)",
             len(CLUSTERS), Poller.HEALTHY_INTERVAL)
    return t
