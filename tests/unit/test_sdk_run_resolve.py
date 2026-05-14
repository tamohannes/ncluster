"""Unit tests for ``clausius_sdk.resolve_run_uuid``.

The SDK is stdlib-only so we stub the HTTP roundtrip via
``urllib.request.urlopen``. Tests cover the DB-direct hot path, the HTTP
fallback for in-container/remote runs, precedence inside the DB query,
resume alias collapsing, and the resilience guarantees (empty on every
failure path).
"""

from __future__ import annotations

import io
import json
import socket
import sqlite3
import sys
from pathlib import Path


def _load_runs_module(monkeypatch):
    """Drop and re-import ``clausius_sdk.runs`` so each test starts with a
    fresh ``lru_cache``."""
    for name in list(sys.modules):
        if name == "clausius_sdk" or name.startswith("clausius_sdk."):
            monkeypatch.delitem(sys.modules, name, raising=False)
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root not in sys.path:
        monkeypatch.syspath_prepend(repo_root)
    import clausius_sdk.runs as runs_mod
    runs_mod._clear_cache_for_tests()
    return runs_mod


def _make_db(tmp_path, runs, aliases=()):
    """Build a minimal Clausius DB with only the columns the SDK reads."""
    path = tmp_path / "fake_clausius.db"
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE runs ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " cluster TEXT NOT NULL DEFAULT '',"
        " run_name TEXT NOT NULL DEFAULT '',"
        " run_uuid TEXT NOT NULL DEFAULT '',"
        " source TEXT NOT NULL DEFAULT 'legacy',"
        " primary_output_dir TEXT NOT NULL DEFAULT ''"
        ")"
    )
    con.execute(
        "CREATE TABLE sdk_run_aliases ("
        " alias_uuid TEXT PRIMARY KEY,"
        " canonical_uuid TEXT NOT NULL"
        ")"
    )
    for row in runs:
        con.execute(
            "INSERT INTO runs (cluster, run_name, run_uuid, source, primary_output_dir)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                row.get("cluster", ""),
                row.get("run_name", ""),
                row["run_uuid"],
                row.get("source", "sdk"),
                row["primary_output_dir"],
            ),
        )
    for alias_uuid, canonical_uuid in aliases:
        con.execute(
            "INSERT INTO sdk_run_aliases (alias_uuid, canonical_uuid) VALUES (?, ?)",
            (alias_uuid, canonical_uuid),
        )
    con.commit()
    con.close()
    return str(path)


def _fake_urlopen_factory(captured, response_body, status=200):
    """Build a urlopen stub recording every called URL."""

    class _FakeResp:
        def __init__(self, body):
            self._buf = io.BytesIO(body.encode("utf-8"))
            self.status = status

        def read(self):
            return self._buf.read()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    def _fake_urlopen(req, timeout=2.5):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        captured.append(url)
        return _FakeResp(response_body)

    return _fake_urlopen


# ── No-op gate ────────────────────────────────────────────────────────────────


def test_returns_empty_when_url_unset(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.delenv("CLAUSIUS_URL", raising=False)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "abc", "primary_output_dir": "/x"},
    ])
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    # Even with a valid DB, no CLAUSIUS_URL means we respect the SDK's
    # "tracking disabled" gate and stay a no-op.
    assert runs_mod.resolve_run_uuid("/x") == ""


def test_empty_output_dir_returned_unchanged(monkeypatch):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")

    def _track(req, timeout=2.5):
        raise AssertionError("should not hit HTTP for empty output_dir")

    monkeypatch.setattr("urllib.request.urlopen", _track)
    assert runs_mod.resolve_run_uuid("") == ""


# ── Direct DB path (preferred) ────────────────────────────────────────────────


def test_db_output_dir_hit(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "uuid-canonical", "primary_output_dir": "/work/run-r3"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)

    def _no_http(req, timeout=2.5):
        raise AssertionError("DB hit must short-circuit HTTP")

    monkeypatch.setattr("urllib.request.urlopen", _no_http)
    assert runs_mod.resolve_run_uuid("/work/run-r3") == "uuid-canonical"


def test_db_trailing_slash_normalized(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "uuid-canonical", "primary_output_dir": "/work/run-r3"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    assert runs_mod.resolve_run_uuid("/work/run-r3/") == "uuid-canonical"


def test_db_precedence_cluster_name_dir_wins(monkeypatch, tmp_path):
    """When multiple SDK rows share an output_dir, the strongest tiebreak
    (cluster + run_name) must win over the weaker (run_name only / dir only).
    """
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        # Same dir, different cluster/name combos.
        {"run_uuid": "uuid-dir-only", "primary_output_dir": "/work/r3"},
        {"run_uuid": "uuid-name-match", "run_name": "hle_test", "primary_output_dir": "/work/r3"},
        {"run_uuid": "uuid-full-match", "cluster": "aws-cmh", "run_name": "hle_test",
         "primary_output_dir": "/work/r3"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    assert runs_mod.resolve_run_uuid(
        "/work/r3", run_name="hle_test", cluster="aws-cmh"
    ) == "uuid-full-match"


def test_db_precedence_name_dir_when_cluster_drift(monkeypatch, tmp_path):
    """Cluster name drift (e.g. aws-cmh-science vs aws-cmh) falls back to
    name+dir, which still uniquely identifies the run."""
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "uuid-dir-only", "primary_output_dir": "/work/r3"},
        {"run_uuid": "uuid-name-match", "run_name": "hle_test",
         "primary_output_dir": "/work/r3"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    assert runs_mod.resolve_run_uuid(
        "/work/r3", run_name="hle_test", cluster="aws-cmh-science"
    ) == "uuid-name-match"


def test_db_ignores_non_sdk_rows(monkeypatch, tmp_path):
    """Legacy rows (``source != 'sdk'``) must never be returned because
    the caller's downstream code only emits SDK metrics."""
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "uuid-legacy", "primary_output_dir": "/work/r3",
         "source": "legacy"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    captured = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen_factory(captured, json.dumps({"exists": False})),
    )
    assert runs_mod.resolve_run_uuid("/work/r3") == ""
    # The DB miss should have triggered the HTTP fallback.
    assert captured


def test_db_resume_alias_collapsed(monkeypatch, tmp_path):
    """A resume submission stores the alias uuid on the row but
    sdk_run_aliases points it at the canonical uuid; the resolver must
    return the canonical so downstream metrics attach correctly."""
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(
        tmp_path,
        runs=[
            {"run_uuid": "uuid-resume-alias", "primary_output_dir": "/work/r3"},
        ],
        aliases=[("uuid-resume-alias", "uuid-original-canonical")],
    )
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    assert runs_mod.resolve_run_uuid("/work/r3") == "uuid-original-canonical"


def test_db_unknown_dir_falls_through(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "uuid-other", "primary_output_dir": "/work/other"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)

    captured = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen_factory(captured, json.dumps({"exists": False}), status=200),
    )
    assert runs_mod.resolve_run_uuid("/work/missing") == ""
    assert captured  # HTTP was attempted after DB miss


# ── HTTP fallback (in-container / remote launcher) ────────────────────────────


def test_http_used_when_no_local_db(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))
    captured = []
    body = json.dumps({"status": "ok", "exists": True, "run_uuid": "uuid-from-http"})
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen_factory(captured, body))
    assert runs_mod.resolve_run_uuid("/work/r3") == "uuid-from-http"
    assert any("output_dir=" in url for url in captured)


def test_http_passes_run_name_and_cluster(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))
    captured = []
    body = json.dumps({"exists": True, "run_uuid": "uuid-from-http"})
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen_factory(captured, body))
    runs_mod.resolve_run_uuid("/work/r3", run_name="hle_test", cluster="aws-cmh")
    url = captured[0]
    assert "run_name=hle_test" in url
    assert "cluster=aws-cmh" in url


def test_http_bearer_token_attached(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_TOKEN", "shhh")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))

    seen_headers: dict = {}

    class _FakeResp:
        status = 200

        def read(self):
            return json.dumps({"exists": True, "run_uuid": "uuid"}).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    def _fake_urlopen(req, timeout=2.5):
        seen_headers.update(dict(req.header_items()))
        return _FakeResp()

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    runs_mod.resolve_run_uuid("/work/r3")
    assert seen_headers.get("Authorization") == "Bearer shhh"


def test_http_exists_false_returns_empty(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))
    body = json.dumps({"status": "ok", "exists": False})
    monkeypatch.setattr(
        "urllib.request.urlopen", _fake_urlopen_factory([], body),
    )
    assert runs_mod.resolve_run_uuid("/work/r3") == ""


# ── Resilience: HTTP unreachable / timeout / 404 ──────────────────────────────


def test_http_server_unreachable_returns_empty(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://127.0.0.1:65535")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))

    def _refuse(req, timeout=2.5):
        raise ConnectionRefusedError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", _refuse)
    assert runs_mod.resolve_run_uuid("/work/r3") == ""


def test_http_timeout_returns_empty(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))

    def _slow(req, timeout=2.5):
        raise socket.timeout("read timeout")

    monkeypatch.setattr("urllib.request.urlopen", _slow)
    assert runs_mod.resolve_run_uuid("/work/r3") == ""


# ── Cache behaviour ───────────────────────────────────────────────────────────


def test_lru_cache_avoids_repeat_db_io(monkeypatch, tmp_path):
    runs_mod = _load_runs_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        {"run_uuid": "uuid-canonical", "primary_output_dir": "/work/r3"},
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)

    call_count = {"n": 0}
    original_resolver = runs_mod._resolve_via_db

    def _counting_resolver(output_dir, run_name, cluster):
        call_count["n"] += 1
        return original_resolver(output_dir, run_name, cluster)

    monkeypatch.setattr(runs_mod, "_resolve_via_db", _counting_resolver)

    assert runs_mod.resolve_run_uuid("/work/r3") == "uuid-canonical"
    assert runs_mod.resolve_run_uuid("/work/r3") == "uuid-canonical"
    assert call_count["n"] == 1
