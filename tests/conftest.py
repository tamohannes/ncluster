"""Shared fixtures for the clausius test suite.

Two test modes:
  - CI / GitHub:  No config.json, falls back to config.example.json.
                  All tests use a mock cluster injected via CLUSTERS.
  - Local dev:    config.json present with real clusters.
                  Unit/integration tests still use mock cluster.
                  `live` and `local_cluster` tests can use real clusters.

DB isolation: every test automatically gets a temp DB via _isolate_db
(autouse). No test can accidentally write to the production history.db.
"""

import os
import json

import pytest

# ---------------------------------------------------------------------------
# Mock cluster definition — used by all non-live tests on both CI and local
# ---------------------------------------------------------------------------

MOCK_CLUSTER_NAME = "mock-cluster"
MOCK_CLUSTER_CFG = {
    "host": "mock-login.example.com",
    "user": "testuser",
    "key": "/tmp/nonexistent_key",
    "port": 22,
    "gpu_type": "H100",
}


def _has_real_config():
    """True if a real config.json exists (local dev environment)."""
    from server.config import PROJECT_ROOT
    return os.path.isfile(os.path.join(PROJECT_ROOT, "conf", "config.json"))


def _first_real_cluster():
    """Return (name, cfg) of the first non-local cluster from config.json, or None."""
    from server.config import CLUSTERS
    for name, cfg in CLUSTERS.items():
        if name != "local" and cfg.get("host"):
            return name, cfg
    return None, None


# ---------------------------------------------------------------------------
# Inject mock cluster into CLUSTERS for every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _ensure_mock_cluster():
    """Ensure MOCK_CLUSTER_NAME is always present in CLUSTERS so that
    unit and integration tests have a deterministic remote cluster to use
    without depending on config.json contents."""
    from server import config
    if MOCK_CLUSTER_NAME not in config.CLUSTERS:
        config.CLUSTERS[MOCK_CLUSTER_NAME] = dict(MOCK_CLUSTER_CFG)
    yield
    config.CLUSTERS.pop(MOCK_CLUSTER_NAME, None)


# ---------------------------------------------------------------------------
# DB isolation — every test gets its own temporary DB
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    """Redirect ALL DB and config access to per-test temp files.
    This is autouse so no test can accidentally write to production files."""
    p = str(tmp_path / "test_history.db")
    monkeypatch.setattr("server.config.DB_PATH", p)
    monkeypatch.setattr("server.db.DB_PATH", p)
    monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "test_config.json"))
    yield p


# ---------------------------------------------------------------------------
# Flask test client
# ---------------------------------------------------------------------------

@pytest.fixture()
def app(_isolate_db):
    """Create a Flask app wired to the isolated test DB."""
    from server.db import init_db
    init_db()

    from app import app as flask_app
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


# ---------------------------------------------------------------------------
# Cache / state reset
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_caches():
    """Clear all in-memory caches between tests."""
    from server import config
    with config._cache_lock:
        config._cache.clear()
        config._seen_jobs.clear()
        config._last_polled.clear()
    with config._warm_lock:
        config._log_index_cache.clear()
        config._log_content_cache.clear()
        config._stats_cache.clear()
        config._dir_list_cache.clear()
        config._progress_cache.clear()
        config._est_start_cache.clear()
        config._prefetch_last.clear()
    yield


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(_isolate_db):
    """Provide an initialized isolated DB and return its path."""
    from server.db import init_db
    init_db()
    return _isolate_db


# ---------------------------------------------------------------------------
# SSH / subprocess mocking
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_ssh(monkeypatch):
    """Patch ssh_run and ssh_run_with_timeout everywhere they are imported.

    Usage:
        mock_ssh.set("cluster", "command-substring", ("stdout", "stderr"))
    Falls back to ("", "") for unmatched commands.
    """
    class _MockSSH:
        def __init__(self):
            self._responses = {}
            self._calls = []

        def set(self, cluster, substr, response):
            self._responses[(cluster, substr)] = response

        def _lookup(self, cluster, command):
            self._calls.append((cluster, command))
            for (c, sub), resp in self._responses.items():
                if c == cluster and sub in command:
                    return resp
            return ("", "")

    m = _MockSSH()
    targets = [
        "server.ssh.ssh_run",
        "server.ssh.ssh_run_with_timeout",
        "server.jobs.ssh_run",
        "server.jobs.ssh_run_with_timeout",
        "server.logs.ssh_run",
        "server.logs.ssh_run_with_timeout",
        "server.routes.ssh_run",
        "server.routes.ssh_run_with_timeout",
    ]
    for t in targets:
        if "with_timeout" in t:
            monkeypatch.setattr(t, lambda c, cmd, timeout_sec=20, _m=m: _m._lookup(c, cmd))
        else:
            monkeypatch.setattr(t, lambda c, cmd, _m=m: _m._lookup(c, cmd))
    return m


# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_cluster():
    """Return the name of the injected mock cluster (always available)."""
    return MOCK_CLUSTER_NAME


@pytest.fixture()
def first_real_cluster():
    """Return the name of the first real cluster from config.json.
    Skips the test if no real config.json is present."""
    name, _ = _first_real_cluster()
    if not name:
        pytest.skip("No real cluster available (config.json missing or empty)")
    return name
