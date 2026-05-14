"""Unit tests for ``clausius_sdk.resolve_cluster_name``.

The SDK is stdlib-only so we stub the HTTP roundtrip via
``urllib.request.urlopen``. These tests cover both the preferred direct
SQLite path and the HTTP fallback used when the DB file isn't local.
"""

from __future__ import annotations

import io
import json
import socket
import sqlite3
import sys
from pathlib import Path


def _load_cluster_module(monkeypatch):
    """Drop and re-import ``clausius_sdk.cluster`` so each test starts
    with a fresh ``lru_cache``."""
    for name in list(sys.modules):
        if name == "clausius_sdk" or name.startswith("clausius_sdk."):
            monkeypatch.delitem(sys.modules, name, raising=False)
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root not in sys.path:
        monkeypatch.syspath_prepend(repo_root)
    import clausius_sdk.cluster as cluster_mod
    cluster_mod._clear_cache_for_tests()
    return cluster_mod


def _make_db(tmp_path, rows):
    """Build a fake clausius DB containing only the columns the SDK reads."""
    path = tmp_path / "fake_clausius.db"
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE clusters ("
        " name TEXT PRIMARY KEY,"
        " host TEXT NOT NULL DEFAULT '',"
        " aliases_json TEXT NOT NULL DEFAULT '[]'"
        ")"
    )
    for name, host, aliases in rows:
        con.execute(
            "INSERT INTO clusters (name, host, aliases_json) VALUES (?, ?, ?)",
            (name, host, json.dumps(list(aliases))),
        )
    con.commit()
    con.close()
    return str(path)


def _fake_urlopen_factory(captured, response_body, status=200):
    """Build a urlopen stub recording every called URL and returning the
    requested JSON body."""

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

    def _fake_urlopen(req, timeout=5.0):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        captured.append(url)
        return _FakeResp(response_body)

    return _fake_urlopen


# ── No-op gate ────────────────────────────────────────────────────────────────


def test_returns_input_when_url_unset(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.delenv("CLAUSIUS_URL", raising=False)
    db_path = _make_db(tmp_path, [("aws-cmh", "x", ["aws-cmh-science"])])
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    # Even with a perfectly valid DB present, no CLAUSIUS_URL means we
    # respect the SDK's "tracking disabled" gate and stay a no-op.
    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh-science"


def test_empty_input_returned_unchanged(monkeypatch):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")

    def _track(req, timeout=5.0):
        raise AssertionError("should not hit HTTP for empty input")

    monkeypatch.setattr(cluster_mod, "urlopen", _track)
    assert cluster_mod.resolve_cluster_name("") == ""


# ── Direct DB path (preferred) ────────────────────────────────────────────────


def test_db_alias_lookup(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        ("aws-cmh", "aws-cmh.example.com", ["aws-cmh-science"]),
        ("aws-dfw", "aws-dfw.example.com", ["aws-dfw-science"]),
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)

    def _no_http(req, timeout=5.0):
        raise AssertionError("DB path should win, HTTP must not be called")

    monkeypatch.setattr(cluster_mod, "urlopen", _no_http)
    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh"


def test_db_canonical_lookup(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    db_path = _make_db(tmp_path, [("aws-cmh", "host", ["aws-cmh-science"])])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    assert cluster_mod.resolve_cluster_name("aws-cmh") == "aws-cmh"


def test_db_host_fallback(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    db_path = _make_db(tmp_path, [
        ("aws-cmh", "aws-cmh-slurm-1-login-01.nvidia.com", []),
    ])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)
    out = cluster_mod.resolve_cluster_name(
        "unknown-yaml-name",
        host="aws-cmh-slurm-1-login-01.nvidia.com",
    )
    assert out == "aws-cmh"


def test_db_unknown_name_falls_through(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    db_path = _make_db(tmp_path, [("aws-cmh", "x", [])])
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)

    captured = []
    body = json.dumps({"error": "no_match", "name": "ghost"})
    monkeypatch.setattr(
        cluster_mod, "urlopen",
        _fake_urlopen_factory(captured, body, status=404),
    )
    assert cluster_mod.resolve_cluster_name("ghost") == "ghost"


# ── HTTP fallback (cross-machine launcher) ────────────────────────────────────


def test_http_used_when_no_local_db(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "does_not_exist.db"))
    captured = []
    body = json.dumps({
        "canonical": "aws-cmh",
        "source": "alias",
        "matched_alias": "aws-cmh-science",
    })
    monkeypatch.setattr(
        cluster_mod, "urlopen", _fake_urlopen_factory(captured, body),
    )
    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh"
    assert any("name=aws-cmh-science" in url for url in captured)


def test_http_passes_host_param(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))
    captured = []
    body = json.dumps({"canonical": "aws-cmh", "source": "host"})
    monkeypatch.setattr(
        cluster_mod, "urlopen", _fake_urlopen_factory(captured, body),
    )
    out = cluster_mod.resolve_cluster_name(
        "unknown-yaml-name",
        host="aws-cmh-slurm-1-login-01.nvidia.com",
    )
    assert out == "aws-cmh"
    assert any("host=aws-cmh" in url for url in captured)


def test_http_bearer_token_attached(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_TOKEN", "shhh")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))

    seen_headers: dict = {}

    class _FakeResp:
        status = 200

        def read(self):
            return json.dumps(
                {"canonical": "aws-cmh", "source": "canonical"}
            ).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    def _fake_urlopen(req, timeout=5.0):
        seen_headers.update(dict(req.header_items()))
        return _FakeResp()

    monkeypatch.setattr(cluster_mod, "urlopen", _fake_urlopen)
    cluster_mod.resolve_cluster_name("aws-cmh")
    assert seen_headers.get("Authorization") == "Bearer shhh"


# ── Resilience: HTTP unreachable / slow / 404 ─────────────────────────────────


def test_http_server_unreachable_returns_input(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://127.0.0.1:65535")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))

    def _refuse(req, timeout=5.0):
        raise ConnectionRefusedError("connection refused")

    monkeypatch.setattr(cluster_mod, "urlopen", _refuse)
    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh-science"


def test_http_timeout_returns_input(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", str(tmp_path / "missing.db"))

    def _slow(req, timeout=5.0):
        raise socket.timeout("read timeout")

    monkeypatch.setattr(cluster_mod, "urlopen", _slow)
    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh-science"


# ── Cache behaviour ───────────────────────────────────────────────────────────


def test_lru_cache_avoids_repeat_db_io(monkeypatch, tmp_path):
    cluster_mod = _load_cluster_module(monkeypatch)
    db_path = _make_db(tmp_path, [("aws-cmh", "x", ["aws-cmh-science"])])
    monkeypatch.setenv("CLAUSIUS_URL", "http://localhost:7272")
    monkeypatch.setenv("CLAUSIUS_DB_PATH", db_path)

    call_count = {"n": 0}
    original_resolver = cluster_mod._resolve_via_db

    def _counting_resolver(name, host):
        call_count["n"] += 1
        return original_resolver(name, host)

    monkeypatch.setattr(cluster_mod, "_resolve_via_db", _counting_resolver)

    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh"
    assert cluster_mod.resolve_cluster_name("aws-cmh-science") == "aws-cmh"
    assert call_count["n"] == 1
