"""Shared fixtures for the clausius test suite.

DB isolation: every test automatically gets a temp DB via ``_isolate_db``
(autouse). No test can accidentally write to the production history.db.

In v4 there is no on-disk JSON config — all config lives in the per-test
SQLite DB. The mock cluster used by integration tests is injected
directly via the ``server.clusters`` CRUD layer at the start of every
test (autouse fixture below).
"""

import os
import shutil
import tempfile

import pytest

# ---------------------------------------------------------------------------
# Mock cluster definition — used by all non-live tests on both CI and local
# ---------------------------------------------------------------------------

MOCK_CLUSTER_NAME = "mock-cluster"
MOCK_CLUSTER_CFG = {
    "host": "mock-login.example.com",
    "ssh_user": "testuser",
    "ssh_key": "/tmp/nonexistent_key",
    "port": 22,
    "gpu_type": "H100",
}


def _first_real_cluster():
    """Return (name, cfg) of the first non-local cluster, or (None, None)."""
    from server.config import CLUSTERS
    for name, cfg in CLUSTERS.items():
        if name != "local" and cfg.get("host"):
            return name, cfg
    return None, None


# ---------------------------------------------------------------------------
# DB isolation — every test gets its own temporary DB
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_db(monkeypatch):
    """Redirect every DB read/write to a per-test temp file.

    Autouse so no test can accidentally write to ``data/history.db``.
    Also evicts the thread-local cached DB connection at start AND end of
    each test, otherwise a connection cached during one test would still
    point at the previous test's tmp DB (or the production DB if the test
    ran before any other test).

    The DB lives in its own ``tempfile.mkdtemp()`` directory — kept
    separate from ``tmp_path`` so tests that list ``tmp_path`` for their
    own file fixtures (e.g. ``test_mounts_resolution``) don't see DB
    artefacts polluting their listings.
    """
    from server.db import _force_close_thread_local_db
    _force_close_thread_local_db()

    db_dir = tempfile.mkdtemp(prefix="clausius_test_db_")
    p = os.path.join(db_dir, "test_history.db")
    monkeypatch.setattr("server.config.DB_PATH", p)
    monkeypatch.setattr("server.db.DB_PATH", p)
    yield p

    _force_close_thread_local_db()
    shutil.rmtree(db_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Inject the mock cluster into the per-test DB
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _ensure_mock_cluster(_isolate_db):
    """Seed the per-test DB with the mock cluster used by integration tests.

    Runs after ``_isolate_db`` so the writes go to the tmp DB, not the
    real one. ``init_db`` is called first because most tests don't trigger
    schema setup themselves.
    """
    from server.db import init_db
    from server.clusters import add_cluster, get_cluster, remove_cluster

    init_db()
    if get_cluster(MOCK_CLUSTER_NAME) is None:
        add_cluster(MOCK_CLUSTER_NAME, **MOCK_CLUSTER_CFG)
    yield
    # tmp DB is discarded with the tmp_path; explicit remove_cluster only
    # matters when a test points at a long-lived DB (none do today).
    if get_cluster(MOCK_CLUSTER_NAME) is not None:
        remove_cluster(MOCK_CLUSTER_NAME)


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
    from server import db
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
    with db._pinned_cache_lock:
        db._pinned_cache.clear()
        db._pinned_cache_ts.clear()
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
