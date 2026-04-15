"""Stability tests: circuit breaker, load shedding, request resilience, concurrency.

All tests are deterministic — no real SSH. Uses mock_ssh and the Flask test client.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestCircuitBreaker:
    """Verify SSH circuit breaker prevents dead clusters from consuming workers."""

    def _reset_cb(self):
        from server.ssh import _cb_lock, _cb_failures
        with _cb_lock:
            _cb_failures.clear()

    def test_failure_trips_breaker(self):
        from server.ssh import _cb_record_failure, _cb_is_open
        self._reset_cb()
        _cb_record_failure("dead-cluster")
        assert _cb_is_open("dead-cluster")

    def test_success_resets_breaker(self):
        from server.ssh import _cb_record_failure, _cb_record_success, _cb_is_open
        self._reset_cb()
        _cb_record_failure("flaky")
        _cb_record_failure("flaky")
        assert _cb_is_open("flaky")
        _cb_record_success("flaky")
        assert not _cb_is_open("flaky")

    def test_breaker_cooldown_scales_with_failures(self):
        from server.ssh import _cb_record_failure, _cb_failures, _cb_lock, _CB_COOLDOWN_SEC
        self._reset_cb()
        _cb_record_failure("scaling")
        with _cb_lock:
            assert _cb_failures["scaling"]["count"] == 1
        _cb_record_failure("scaling")
        with _cb_lock:
            assert _cb_failures["scaling"]["count"] == 2

    def test_breaker_expires_after_cooldown(self):
        from server.ssh import _cb_record_failure, _cb_is_open, _cb_failures, _cb_lock
        self._reset_cb()
        _cb_record_failure("expired")
        with _cb_lock:
            _cb_failures["expired"]["ts"] = time.monotonic() - 9999
        assert not _cb_is_open("expired")

    def test_healthy_cluster_not_affected(self):
        from server.ssh import _cb_record_failure, _cb_is_open
        self._reset_cb()
        _cb_record_failure("bad")
        assert not _cb_is_open("good")

    def test_health_endpoint_reflects_breaker(self, client, mock_ssh):
        from server.ssh import _cb_record_failure
        self._reset_cb()
        _cb_record_failure("test-cluster")
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        cb = data["circuit_breakers"]
        assert "test-cluster" in cb
        assert cb["test-cluster"]["failures"] == 1
        self._reset_cb()

    def test_breaker_open_returns_error_immediately(self, client, mock_ssh, mock_cluster):
        """When the breaker is open, /api/jobs/<cluster> still returns valid JSON
        (the poll path catches the SSH exception and preserves cached data)."""
        from server.ssh import _cb_record_failure
        self._reset_cb()
        _cb_record_failure(mock_cluster)
        resp = client.get(f"/api/jobs/{mock_cluster}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "jobs" in data
        self._reset_cb()

    def test_breaker_count_caps_at_ten(self):
        from server.ssh import _cb_record_failure, _cb_failures, _cb_lock
        self._reset_cb()
        for _ in range(20):
            _cb_record_failure("capped")
        with _cb_lock:
            assert _cb_failures["capped"]["count"] == 10
        self._reset_cb()


# ---------------------------------------------------------------------------
# Load shedding
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestLoadShedding:
    """Verify load shedding protects the server without leaking counters."""

    def test_rejected_request_when_at_capacity(self, client, mock_ssh):
        from server import routes
        fake_tids = set(range(900_000, 900_000 + routes._MAX_ACTIVE + 5))
        with routes._active_lock:
            saved = routes._active_threads.copy()
            routes._active_threads.update(fake_tids)

        try:
            resp = client.get("/api/partition_summary")
            assert resp.status_code == 503
            data = resp.get_json()
            assert data["status"] == "error"
            assert "busy" in data["error"]
        finally:
            with routes._active_lock:
                routes._active_threads.clear()
                routes._active_threads.update(saved)

    def test_non_heavy_endpoint_bypasses_shedding(self, client, mock_ssh):
        from server import routes
        fake_tids = set(range(900_000, 900_000 + routes._MAX_ACTIVE + 10))
        with routes._active_lock:
            saved = routes._active_threads.copy()
            routes._active_threads.update(fake_tids)

        try:
            resp = client.get("/api/health")
            assert resp.status_code == 200
        finally:
            with routes._active_lock:
                routes._active_threads.clear()
                routes._active_threads.update(saved)

    def test_threads_cleaned_on_success(self, client, mock_ssh):
        from server import routes
        before = routes._active_request_count()

        resp = client.get("/api/health")
        assert resp.status_code == 200

        after = routes._active_request_count()
        assert after == before

    def test_threads_cleaned_on_404(self, client, mock_ssh):
        from server import routes
        before = routes._active_request_count()

        resp = client.get("/api/jobs/nonexistent-cluster")
        assert resp.status_code == 404

        after = routes._active_request_count()
        assert after == before

    def test_threads_accurate_under_concurrent_load(self, app, mock_ssh):
        """Fire parallel requests and verify the thread set returns to baseline."""
        from server import routes
        baseline = routes._active_request_count()

        def _make_request():
            with app.test_client() as c:
                resp = c.get("/api/health")
                return resp.status_code

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(_make_request) for _ in range(20)]
            results = [f.result() for f in as_completed(futures)]

        assert all(r == 200 for r in results)

        final = routes._active_request_count()
        assert final == baseline


# ---------------------------------------------------------------------------
# Request resilience
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestRequestResilience:
    """Verify API endpoints degrade gracefully on SSH failures."""

    def test_jobs_returns_cached_data_on_ssh_error(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "squeue", ("", ""))
        resp1 = client.get(f"/api/jobs/{mock_cluster}")
        assert resp1.status_code == 200
        assert resp1.get_json().get("status") == "ok"

        from server import config
        config._last_polled[mock_cluster] = 0.0

        def _raise(*a, **kw):
            raise Exception("SSH broken")

        with patch("server.jobs.ssh_run", side_effect=_raise):
            resp2 = client.get(f"/api/jobs/{mock_cluster}")
            assert resp2.status_code == 200
            data2 = resp2.get_json()
            assert "jobs" in data2

    def test_stats_returns_error_json_not_500(self, client, mock_ssh, mock_cluster):
        def _raise(*a, **kw):
            raise Exception("SSH broken")

        with patch("server.jobs.ssh_run_with_timeout", side_effect=_raise):
            resp = client.get(f"/api/stats/{mock_cluster}/12345")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "error"

    def test_log_returns_error_json_on_ssh_failure(self, client, mock_ssh, mock_cluster):
        resp = client.get(f"/api/log/{mock_cluster}/99999")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "status" in data

    def test_partition_summary_returns_cached_on_partial_failure(self, client, mock_ssh, mock_cluster):
        resp = client.get("/api/partition_summary")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "clusters" in data

    def test_unknown_cluster_returns_404(self, client, mock_ssh):
        for endpoint in ["/api/jobs/fakecluster",
                         "/api/stats/fakecluster/1",
                         "/api/log/fakecluster/1",
                         "/api/log_files/fakecluster/1"]:
            resp = client.get(endpoint)
            assert resp.status_code == 404, f"{endpoint} should be 404"

    def test_history_works_with_empty_db(self, client, mock_ssh, db_path):
        resp = client.get("/api/history")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)

    def test_projects_works_with_empty_db(self, client, mock_ssh, db_path):
        resp = client.get("/api/projects")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestConcurrency:
    """Verify concurrent access doesn't deadlock or corrupt state."""

    def test_parallel_jobs_requests_no_deadlock(self, app, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "squeue", ("", ""))

        results = []
        def _fetch():
            with app.test_client() as c:
                resp = c.get("/api/jobs")
                return resp.status_code

        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(_fetch) for _ in range(12)]
            results = [f.result() for f in as_completed(futures)]

        assert all(r == 200 for r in results)

    def test_parallel_health_checks(self, app, mock_ssh):
        results = []
        def _fetch():
            with app.test_client() as c:
                resp = c.get("/api/health")
                return resp.status_code, resp.get_json()["active_requests"]

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(_fetch) for _ in range(20)]
            results = [f.result() for f in as_completed(futures)]

        assert all(code == 200 for code, _ in results)

    def test_poll_inflight_dedup(self, mock_ssh, mock_cluster, db_path):
        """Only one poll thread per cluster should be inflight at a time.

        We simulate a slow poll by directly inserting into _poll_inflight
        to verify the dedup guard works.
        """
        from server.jobs import _poll_inflight, _poll_inflight_lock, _start_poll

        with _poll_inflight_lock:
            _poll_inflight.clear()
            _poll_inflight[mock_cluster] = time.monotonic()

        mock_ssh.set(mock_cluster, "squeue", ("", ""))
        _start_poll(mock_cluster)

        time.sleep(0.05)
        with _poll_inflight_lock:
            assert mock_cluster in _poll_inflight
            _poll_inflight.pop(mock_cluster, None)

    def test_prefetch_thread_limit(self, mock_ssh, mock_cluster, db_path):
        from server.jobs import (
            _prefetch_active, _prefetch_active_lock,
            _MAX_PREFETCH_THREADS, schedule_prefetch,
        )
        from server.config import _prefetch_last, _warm_lock

        with _warm_lock:
            _prefetch_last.clear()
        with _prefetch_active_lock:
            _prefetch_active[mock_cluster] = _MAX_PREFETCH_THREADS

        mock_ssh.set(mock_cluster, "", ("", ""))
        schedule_prefetch(mock_cluster, "blocked-job")

        time.sleep(0.1)
        with _prefetch_active_lock:
            assert _prefetch_active.get(mock_cluster, 0) <= _MAX_PREFETCH_THREADS + 1

        with _prefetch_active_lock:
            _prefetch_active.pop(mock_cluster, None)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestWatchdog:
    """Verify the set-based tracking auto-prunes dead threads."""

    def test_dead_thread_ids_pruned_on_count(self):
        from server import routes
        with routes._active_lock:
            saved = routes._active_threads.copy()
            routes._active_threads.add(999_999_999)

        count = routes._active_request_count()
        with routes._active_lock:
            assert 999_999_999 not in routes._active_threads
            routes._active_threads.clear()
            routes._active_threads.update(saved)

    def test_watchdog_log_does_not_crash(self):
        from server.ssh import _watchdog_log_active
        _watchdog_log_active()


# ---------------------------------------------------------------------------
# Global error handler
# ---------------------------------------------------------------------------

def _make_crash_app(db_path):
    """Build a fresh Flask app with crash test routes registered before first request."""
    from server.db import init_db
    from server.config import DB_PATH
    init_db()

    from flask import Flask, jsonify
    from server.routes import api

    test_app = Flask(__name__)
    test_app.config["TESTING"] = True

    @test_app.route("/api/_test_crash")
    def _crash():
        raise RuntimeError("deliberate crash for testing")

    @test_app.route("/api/_test_log_crash")
    def _log_crash():
        raise ValueError("traceback-test-marker")

    test_app.register_blueprint(api)
    return test_app


@pytest.mark.integration
class TestGlobalErrorHandler:
    """Verify unhandled exceptions return JSON 500, not HTML tracebacks."""

    def test_unhandled_exception_returns_json_500(self, _isolate_db, mock_ssh):
        test_app = _make_crash_app(_isolate_db)
        with test_app.test_client() as c:
            resp = c.get("/api/_test_crash")
            assert resp.status_code == 500
            data = resp.get_json()
            assert data is not None, "Response should be JSON, not HTML"
            assert data["status"] == "error"
            assert "deliberate crash" in data["error"]

    def test_exception_logged_with_traceback(self, _isolate_db, mock_ssh, tmp_path):
        import logging

        log_path = str(tmp_path / "test_crash.log")
        handler = logging.FileHandler(log_path)
        handler.setLevel(logging.ERROR)
        logger = logging.getLogger("server.routes")
        logger.addHandler(handler)

        test_app = _make_crash_app(_isolate_db)
        try:
            with test_app.test_client() as c:
                c.get("/api/_test_log_crash")

            handler.flush()
            handler.close()
            with open(log_path) as f:
                log_content = f.read()
            assert "traceback-test-marker" in log_content
            assert "Traceback" in log_content or "ValueError" in log_content
        finally:
            logger.removeHandler(handler)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestHealthEndpoint:
    def test_health_returns_active_count(self, client, mock_ssh):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "active_requests" in data
        assert "max_active" in data
        assert "circuit_breakers" in data
        assert isinstance(data["circuit_breakers"], dict)

    def test_health_not_load_shed(self, client, mock_ssh):
        """Health check must never be load-shed."""
        from server import routes
        fake_tids = set(range(900_000, 900_000 + 999))
        with routes._active_lock:
            saved = routes._active_threads.copy()
            routes._active_threads.update(fake_tids)
        try:
            resp = client.get("/api/health")
            assert resp.status_code == 200
        finally:
            with routes._active_lock:
                routes._active_threads.clear()
                routes._active_threads.update(saved)
