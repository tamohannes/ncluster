"""SDK-side resume detection: ``ClausiusSession.start_from_cli`` must call
the server's ``/api/sdk/resolve_run`` endpoint and, on a hit, attach to the
existing run uuid instead of minting a fresh one.

The SDK is stdlib-only, so we stub the HTTP path by monkeypatching
``urllib.request.urlopen``. That keeps tests fast and offline while still
exercising the real ``_resolve_existing_run_uuid`` codepath.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path


def _load_sdk_session(monkeypatch):
    for name in list(sys.modules):
        if name == "clausius_sdk" or name.startswith("clausius_sdk."):
            monkeypatch.delitem(sys.modules, name, raising=False)
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root not in sys.path:
        monkeypatch.syspath_prepend(repo_root)
    import clausius_sdk.session as session_mod
    session_mod.ClausiusSession.reset()
    return session_mod


def _fake_urlopen_factory(captured, response_body):
    """Build a urlopen stand-in that records the called URL and returns the
    chosen JSON body."""

    class _FakeResp:
        def __init__(self, body):
            self._buf = io.BytesIO(body.encode("utf-8"))

        def read(self):
            return self._buf.read()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _fake_urlopen(req, timeout=2.5):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        captured.append(url)
        return _FakeResp(response_body)

    return _fake_urlopen


def test_start_from_cli_attaches_to_existing_uuid_when_server_says_so(tmp_path, monkeypatch):
    session_mod = _load_sdk_session(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_SPOOL_DIR", str(tmp_path))

    captured = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen_factory(
            captured,
            json.dumps({"status": "ok", "exists": True, "run_uuid": "canon01"}),
        ),
    )

    sess = session_mod.ClausiusSession.start_from_cli(
        expname="mcp_eval-r1",
        cluster="aws-iad",
        output_dir="/work/exp-a",
    )
    assert sess.run_uuid == "canon01"
    assert captured, "resolve_run endpoint should have been queried"
    assert "output_dir=%2Fwork%2Fexp-a" in captured[0]
    assert "run_name=mcp_eval-r1" in captured[0]
    assert "cluster=aws-iad" in captured[0]

    # Spooled events all carry the canonical uuid.
    sess.close()
    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
    ]
    assert events, "spool must have captured run_started"
    assert {e["run_uuid"] for e in events} == {"canon01"}


def test_start_from_cli_mints_fresh_uuid_when_no_existing_run(tmp_path, monkeypatch):
    session_mod = _load_sdk_session(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_SPOOL_DIR", str(tmp_path))

    captured = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen_factory(
            captured,
            json.dumps({"status": "ok", "exists": False}),
        ),
    )

    sess = session_mod.ClausiusSession.start_from_cli(
        expname="mcp_eval-r2",
        cluster="aws-iad",
        output_dir="/work/exp-b",
    )
    assert sess.run_uuid and sess.run_uuid != "canon01"
    assert len(sess.run_uuid) == 32  # uuid4 hex
    assert captured  # we still queried, just got exists=False


def test_start_from_cli_fails_open_when_server_unreachable(tmp_path, monkeypatch):
    """Network errors must not block submissions — fall through to fresh uuid."""
    session_mod = _load_sdk_session(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_URL", "http://mock:7272")
    monkeypatch.setenv("CLAUSIUS_SPOOL_DIR", str(tmp_path))

    def _boom(req, timeout=2.5):
        raise OSError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", _boom)

    sess = session_mod.ClausiusSession.start_from_cli(
        expname="mcp_eval-r3",
        cluster="aws-iad",
        output_dir="/work/exp-c",
    )
    assert sess.run_uuid
    assert len(sess.run_uuid) == 32


def test_start_from_cli_skips_lookup_when_clausius_url_unset(tmp_path, monkeypatch):
    """No ``CLAUSIUS_URL`` -> no HTTP call -> straight to fresh uuid path."""
    session_mod = _load_sdk_session(monkeypatch)
    monkeypatch.delenv("CLAUSIUS_URL", raising=False)
    monkeypatch.setenv("CLAUSIUS_SPOOL_DIR", str(tmp_path))

    called = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **kw: called.append(("urlopen", a, kw)) or (_ for _ in ()).throw(
            AssertionError("urlopen must not be called when CLAUSIUS_URL is unset")
        ),
    )

    sess = session_mod.ClausiusSession.start_from_cli(
        expname="mcp_eval-r4",
        cluster="aws-iad",
        output_dir="/work/exp-d",
    )
    assert sess.run_uuid
    assert called == []
