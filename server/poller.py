"""Background poller for periodic live snapshot refreshes.

Normal board reads consume the DB-backed live snapshot.  The poller keeps
that snapshot fresh on a schedule, while explicit manual refreshes may call
``poll_now()`` for one bounded live fetch.

Healthy clusters are polled every HEALTHY_INTERVAL seconds; failing clusters
back off exponentially up to MAX_BACKOFF seconds.

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
from datetime import datetime

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
    LOCAL_INTERVAL = 30
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
        self._last_duration_ms = {}
        self._last_error = {}
        self._last_started_at = {}
        self._last_completed_at = {}
        self._idle = False
        self._inflight = set()
        self._inflight_lock = threading.Lock()

    def run(self):
        now = time.monotonic()
        remote = [n for n in CLUSTERS if n != "local"]
        for i, name in enumerate(remote):
            self._schedules[name] = now + i * 0.5
        if "local" in CLUSTERS:
            self._schedules["local"] = now + 1.0

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
                remote = [n for n in CLUSTERS if n != "local"]
                for i, name in enumerate(remote):
                    self._schedules[name] = now + i * 0.5
                if "local" in CLUSTERS:
                    self._schedules["local"] = now + 1.0

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

    def _claim_inflight(self, name):
        with self._inflight_lock:
            if name in self._inflight:
                return False
            self._inflight.add(name)
            return True

    def _release_inflight(self, name):
        with self._inflight_lock:
            self._inflight.discard(name)

    def _record_poll_success(self, name, duration_ms):
        self._failures.pop(name, None)
        self._last_error.pop(name, None)
        self._last_success[name] = time.monotonic()
        self._last_duration_ms[name] = duration_ms
        self._last_completed_at[name] = datetime.now().isoformat(timespec="seconds")
        interval = self.LOCAL_INTERVAL if name == "local" else self.HEALTHY_INTERVAL
        self._reschedule(name, interval)

    def _record_poll_failure(self, name, duration_ms, error):
        count = self._failures.get(name, 0) + 1
        self._failures[name] = min(count, 10)
        delay = min(self.HEALTHY_INTERVAL * (2 ** count), self.MAX_BACKOFF)
        self._last_error[name] = str(error)
        self._last_duration_ms[name] = duration_ms
        self._last_completed_at[name] = datetime.now().isoformat(timespec="seconds")
        self._reschedule(name, delay)
        log.warning("poll %s failed (#%d), backoff %ds: %s",
                    name, count, delay, error)

    def _run_poll(self, name):
        from .jobs import poll_cluster

        if not self._claim_inflight(name):
            return {"status": "busy", "cluster": name, "changed": False}

        started_mono = time.monotonic()
        self._last_started_at[name] = datetime.now().isoformat(timespec="seconds")
        try:
            prev_data = self._snapshot_ids(name)
            result = poll_cluster(name) or {"status": "ok", "cluster": name}
            curr_data = self._snapshot_ids(name)
            changed = prev_data != curr_data
            duration_ms = round((time.monotonic() - started_mono) * 1000)
            result["duration_ms"] = duration_ms
            result["changed"] = changed

            if result.get("status") == "ok":
                if changed:
                    bump_version()
                    log.debug("poll %s: data changed (v%d)", name, get_version())
                self._record_poll_success(name, duration_ms)
                return result

            self._record_poll_failure(name, duration_ms, result.get("error", "poll failed"))
            return result
        except Exception as e:
            duration_ms = round((time.monotonic() - started_mono) * 1000)
            self._record_poll_failure(name, duration_ms, e)
            return {
                "status": "error",
                "cluster": name,
                "error": str(e),
                "duration_ms": duration_ms,
                "changed": False,
            }
        finally:
            self._release_inflight(name)

    def _poll_one(self, name):
        self._run_poll(name)

    def poll_now(self, cluster):
        """Run one bounded live poll immediately for explicit user refreshes."""
        return self._run_poll(cluster)

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
                "inflight": name in self._inflight,
                "failure_count": failures,
                "next_poll_sec": max(0, round(next_at - now, 1)),
                "staleness_sec": staleness,
                "last_duration_ms": self._last_duration_ms.get(name),
                "last_error": self._last_error.get(name),
                "last_started_at": self._last_started_at.get(name),
                "last_completed_at": self._last_completed_at.get(name),
                "view_state": (
                    "stale" if staleness is not None and staleness > 60
                    else "degraded" if failures
                    else "live"
                ),
            }
        return out

    def stop(self):
        self._stop.set()


# ── Module-level singleton ────────────────────────────────────────────────────

_poller = None
_poller_lock = threading.Lock()
_poller_thread = None  # currently running poller thread (or None)


def get_poller():
    """Return the singleton Poller instance, creating it if needed."""
    global _poller
    if _poller is None:
        with _poller_lock:
            if _poller is None:
                _poller = Poller()
    return _poller


def start_poller():
    """Start the poller background thread.

    Called from `_run_init` (gunicorn) at boot, and from the MCP follower
    when it decides to take over after detecting gunicorn is unreachable.
    Idempotent: if the poller is already running, this is a no-op.
    """
    global _poller_thread
    # Resolve the singleton OUTSIDE the lock — `get_poller()` itself acquires
    # `_poller_lock`, and re-entering a non-reentrant lock would deadlock.
    p = get_poller()
    with _poller_lock:
        if _poller_thread is not None and _poller_thread.is_alive():
            return _poller_thread
        # Fresh `_stop` event so a stop+start cycle (follower handover)
        # actually resumes polling instead of exiting on the very next tick.
        p._stop = threading.Event()
        t = threading.Thread(target=p.run, daemon=True, name="poller")
        t.start()
        _poller_thread = t
    log.info("poller started (%d clusters, interval=%ds)",
             len(CLUSTERS), Poller.HEALTHY_INTERVAL)
    return t


def stop_poller(timeout=5.0):
    """Signal the poller loop to exit and wait briefly for the thread to die.

    The MCP follower calls this when gunicorn becomes reachable again so the
    leader poller can resume sole ownership of polling. Returns True if the
    thread terminated within `timeout`.
    """
    global _poller_thread
    with _poller_lock:
        thread = _poller_thread
        _poller_thread = None
    if _poller is not None:
        _poller.stop()
    if thread is None:
        return True
    thread.join(timeout=timeout)
    alive = thread.is_alive()
    if not alive:
        log.info("poller stopped")
    else:
        log.warning("poller stop timed out after %.1fs (thread still alive)", timeout)
    return not alive


def poller_running():
    """Whether a poller thread is currently active in this process."""
    with _poller_lock:
        return _poller_thread is not None and _poller_thread.is_alive()
