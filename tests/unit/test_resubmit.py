"""Tests for server/resubmit.py and the /api/resubmit_by_hash endpoint."""

from __future__ import annotations

import json
import os
import subprocess
from types import SimpleNamespace

import pytest

from server import resubmit
from server.db import (
    associate_jobs_to_run,
    db_write,
    get_db,
    get_run_hash,
    store_sdk_event,
    upsert_job,
    upsert_run,
)


# ─── eligibility() ───────────────────────────────────────────────────────────


def _sdk_run(jobs, *, submit_command="conda activate env\ncd /tmp\nns eval --x"):
    return {
        "source": "sdk",
        "submit_command": submit_command,
        "jobs": jobs,
    }


@pytest.mark.unit
def test_eligibility_allows_sdk_run_with_all_terminal_jobs():
    run = _sdk_run([{"state": "COMPLETED"}, {"state": "TIMEOUT"}])
    can, reason = resubmit.eligibility(run)
    assert can is True
    assert reason == ""


@pytest.mark.unit
@pytest.mark.parametrize("state", ["RUNNING", "PENDING", "COMPLETING", "SUBMITTING"])
def test_eligibility_blocks_when_any_job_is_active(state):
    run = _sdk_run([{"state": "COMPLETED"}, {"state": state}])
    can, reason = resubmit.eligibility(run)
    assert can is False
    assert "active" in reason.lower()


@pytest.mark.unit
def test_eligibility_blocks_legacy_runs():
    run = {
        "source": "legacy",
        "submit_command": "anything",
        "jobs": [{"state": "COMPLETED"}],
    }
    can, reason = resubmit.eligibility(run)
    assert can is False
    assert "sdk" in reason.lower()


@pytest.mark.unit
def test_eligibility_blocks_when_submit_command_empty():
    run = _sdk_run([{"state": "COMPLETED"}], submit_command="")
    can, reason = resubmit.eligibility(run)
    assert can is False
    assert "command" in reason.lower()


@pytest.mark.unit
def test_eligibility_blocks_when_no_jobs():
    run = _sdk_run([])
    can, reason = resubmit.eligibility(run)
    assert can is False


# ─── derive_conda_init() ─────────────────────────────────────────────────────


@pytest.mark.unit
def test_derive_conda_init_uses_env_subset_conda_prefix(db_path, monkeypatch):
    payload = {
        "env_subset": {"CONDA_PREFIX": "/opt/miniconda/envs/hle-dev"},
        "python_executable": "/opt/miniconda/envs/hle-dev/bin/python",
    }
    store_sdk_event(
        "uuidA", "run_started", 1, 0.0, json.dumps(payload),
    )

    # No filesystem dependency: function builds the path even if conda.sh is absent.
    prefix = resubmit.derive_conda_init("uuidA")
    assert prefix == "source /opt/miniconda/etc/profile.d/conda.sh && "


@pytest.mark.unit
def test_derive_conda_init_falls_back_to_python_executable(db_path):
    payload = {
        "env_subset": {},
        "python_executable": "/home/me/miniforge3/envs/foo/bin/python3.12",
    }
    store_sdk_event("uuidB", "run_started", 1, 0.0, json.dumps(payload))
    prefix = resubmit.derive_conda_init("uuidB")
    assert prefix == "source /home/me/miniforge3/etc/profile.d/conda.sh && "


@pytest.mark.unit
def test_derive_conda_init_returns_empty_when_no_info_and_no_install(
    db_path, monkeypatch
):
    monkeypatch.delenv("CONDA_EXE", raising=False)
    monkeypatch.setattr(os.path, "isfile", lambda p: False)
    monkeypatch.setattr(os.path, "expanduser", lambda p: p)
    payload = {"env_subset": {}}
    store_sdk_event("uuidC", "run_started", 1, 0.0, json.dumps(payload))
    assert resubmit.derive_conda_init("uuidC") == ""


@pytest.mark.unit
def test_derive_conda_init_uses_conda_exe_env_var(db_path, monkeypatch, tmp_path):
    fake_root = tmp_path / "miniconda3"
    (fake_root / "etc" / "profile.d").mkdir(parents=True)
    (fake_root / "etc" / "profile.d" / "conda.sh").write_text("# fake")
    (fake_root / "bin").mkdir()
    (fake_root / "bin" / "conda").write_text("#!/bin/sh\n")
    monkeypatch.setenv("CONDA_EXE", str(fake_root / "bin" / "conda"))
    store_sdk_event("uuidD", "run_started", 1, 0.0, json.dumps({"env_subset": {}}))
    prefix = resubmit.derive_conda_init("uuidD")
    assert prefix == f"source {fake_root}/etc/profile.d/conda.sh && "


# ─── spawn() ─────────────────────────────────────────────────────────────────


class _FakePopen:
    last_kwargs = None
    last_args = None

    def __init__(self, args, **kwargs):
        type(self).last_args = args
        type(self).last_kwargs = kwargs
        self.pid = 4242
        stdout = kwargs.get("stdout")
        if stdout is not None and hasattr(stdout, "write"):
            stdout.write(b"# child running\n")


@pytest.mark.unit
def test_spawn_writes_header_and_calls_popen_detached(
    tmp_path, monkeypatch, db_path
):
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    monkeypatch.setattr(resubmit, "derive_conda_init", lambda uuid: "source x && ")
    monkeypatch.setattr(subprocess, "Popen", _FakePopen)

    run = {
        "run_uuid": "abc123",
        "run_hash": "abcdef012345",
        "cluster": "mock-cluster",
        "submit_command": "echo hello",
        "submit_cwd": str(tmp_path),
    }
    info = resubmit.spawn(run)

    assert info["pid"] == 4242
    assert info["log_name"].startswith("abc123__")
    assert info["log_name"].endswith(".log")
    assert info["log_url"].startswith("/api/resubmit_log/")
    assert info["had_conda_prefix"] is True
    assert os.path.isfile(info["log_path"])
    contents = open(info["log_path"]).read()
    assert "echo hello" in contents
    assert "run_hash: abcdef012345" in contents

    args = _FakePopen.last_args
    kwargs = _FakePopen.last_kwargs
    assert args[:2] == ["bash", "-c"]
    assert args[2] == "source x && echo hello"
    assert kwargs.get("start_new_session") is True
    assert kwargs.get("stdin") is subprocess.DEVNULL
    assert kwargs.get("stderr") is subprocess.STDOUT
    assert kwargs.get("cwd") == str(tmp_path)


@pytest.mark.unit
def test_spawn_skips_invalid_cwd(tmp_path, monkeypatch, db_path):
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    monkeypatch.setattr(resubmit, "derive_conda_init", lambda uuid: "")
    monkeypatch.setattr(subprocess, "Popen", _FakePopen)

    run = {
        "run_uuid": "deadbeef",
        "submit_command": "true",
        "submit_cwd": "/nonexistent/path/12345",
    }
    info = resubmit.spawn(run)
    assert info["had_conda_prefix"] is False
    assert _FakePopen.last_kwargs.get("cwd") is None
    assert _FakePopen.last_args[2] == "true"


@pytest.mark.unit
def test_spawn_rejects_empty_command(db_path):
    with pytest.raises(ValueError):
        resubmit.spawn({"submit_command": "  "})


# ─── read_log() ──────────────────────────────────────────────────────────────


@pytest.mark.unit
def test_read_log_returns_404_for_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    ok, content, status = resubmit.read_log("missing.log")
    assert ok is False
    assert status == 404


@pytest.mark.unit
def test_read_log_rejects_path_traversal(tmp_path, monkeypatch):
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    ok, _content, status = resubmit.read_log("../etc/passwd")
    assert ok is False
    assert status == 400


@pytest.mark.unit
def test_read_log_truncates_large_files(tmp_path, monkeypatch):
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    big = tmp_path / "big.log"
    big.write_bytes(b"A" * 2048)
    ok, content, status = resubmit.read_log("big.log", max_bytes=512)
    assert ok is True
    assert status == 200
    assert "... (truncated)" in content
    assert content.count("A") == 512


# ─── endpoint integration ────────────────────────────────────────────────────


def _seed_sdk_run(cluster, run_uuid, *, jobs_state="COMPLETED"):
    run_id = upsert_run(cluster, "sdk-rootjob", "hle_mpsf_demo-r1", "hle")
    with db_write() as con:
        con.execute(
            """UPDATE runs SET source='sdk', submit_command=?, submit_cwd=?, run_uuid=?
               WHERE id=?""",
            (
                "conda activate env\ncd /tmp\necho hi",
                "/tmp",
                run_uuid,
                run_id,
            ),
        )
    upsert_job(
        cluster,
        {
            "jobid": "100",
            "name": "hle_mpsf_demo-r1",
            "state": jobs_state,
            "submitted": "2026-05-01T00:00:00",
            "started": "2026-05-01T00:00:01",
        },
        terminal=jobs_state in {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"},
    )
    associate_jobs_to_run(cluster, run_id, ["100"])
    return run_id


@pytest.mark.integration
def test_api_resubmit_by_hash_happy_path(client, db_path, monkeypatch, tmp_path):
    run_uuid = "0123456789abcdef" * 2  # 32 hex chars
    _seed_sdk_run("mock-cluster", run_uuid)
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    monkeypatch.setattr(resubmit, "derive_conda_init", lambda uuid: "")
    monkeypatch.setattr(subprocess, "Popen", _FakePopen)

    run_hash = get_run_hash("mock-cluster", "sdk-rootjob", run_uuid)
    res = client.post(f"/api/resubmit_by_hash/mock-cluster/{run_hash}")
    body = res.get_json()
    assert res.status_code == 200, body
    assert body["status"] == "ok"
    assert body["pid"] == 4242
    assert body["log_url"].startswith("/api/resubmit_log/")


@pytest.mark.integration
def test_api_resubmit_by_hash_blocks_active_jobs(client, db_path, monkeypatch):
    run_uuid = "abababab" * 4
    _seed_sdk_run("mock-cluster", run_uuid, jobs_state="RUNNING")
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *a, **k: pytest.fail("subprocess.Popen should not be called"),
    )
    run_hash = get_run_hash("mock-cluster", "sdk-rootjob", run_uuid)
    res = client.post(f"/api/resubmit_by_hash/mock-cluster/{run_hash}")
    body = res.get_json()
    assert res.status_code == 400
    assert "active" in body["error"].lower()


@pytest.mark.integration
def test_run_info_surfaces_can_resubmit_flag(client, db_path):
    run_uuid = "feedface" * 4
    _seed_sdk_run("mock-cluster", run_uuid)
    run_hash = get_run_hash("mock-cluster", "sdk-rootjob", run_uuid)
    res = client.get(f"/api/run_info_by_hash/mock-cluster/{run_hash}")
    body = res.get_json()
    assert res.status_code == 200, body
    assert body["status"] == "ok"
    assert body["run"]["can_resubmit"] is True
    assert "resubmit_blocked_reason" not in body["run"]


@pytest.mark.integration
def test_resubmit_log_endpoint_serves_text(client, db_path, tmp_path, monkeypatch):
    monkeypatch.setattr(resubmit, "_resubmit_logs_dir", lambda: str(tmp_path))
    log_file = tmp_path / "abc__20260512T000000.log"
    log_file.write_text("hello world")
    res = client.get(f"/api/resubmit_log/{log_file.name}")
    assert res.status_code == 200
    assert res.mimetype == "text/plain"
    assert b"hello world" in res.data
