"""Tests for SDK run-params capture, persistence, and the /api/run_info surface.

These cover two independent fixes rolled into one plan:
  1. `_capture_run_metadata` is no longer skipped for SDK runs — they now
     receive batch_script / scontrol_raw / env_vars / conda_state the same
     way legacy runs do, once a real Slurm root_job_id is known.
  2. `RunProvenance.params` round-trips through the SDK events endpoint
     into `runs.params_json` and is surfaced as `run.params` by the API.
"""

from __future__ import annotations

import json
import uuid

import pytest


# ---------------------------------------------------------------------------
# 1. _capture_run_metadata dispatch — SDK runs now qualify once adopted
# ---------------------------------------------------------------------------


def _seed_sdk_run(cluster, root_job_id, run_name):
    """Create a `source='sdk'` run with an un-fetched metadata marker and
    return its run_id. Shared across the dispatch tests."""
    from server.db import upsert_run, db_write

    run_id = upsert_run(cluster, root_job_id, run_name, "hle")
    with db_write() as con:
        con.execute(
            "UPDATE runs SET source='sdk', meta_fetched=0 WHERE id=?",
            (run_id,),
        )
    return run_id


def test_sdk_run_captures_metadata_when_root_is_real_slurm_id(db_path, monkeypatch):
    """Once a real Slurm id replaces the synthetic sdk-xxx root, the metadata
    capture thread should fire (previously skipped for source='sdk')."""
    from server import jobs as jobs_mod

    run_id = _seed_sdk_run("mock-cluster", "123456", "hle_mpsf_run-1")

    captured = []
    monkeypatch.setattr(
        jobs_mod,
        "_capture_run_metadata",
        lambda cluster, root, rid: captured.append((cluster, root, rid)),
    )
    monkeypatch.setattr(
        jobs_mod.threading,
        "Thread",
        lambda target, args, daemon=True: _ImmediateThread(target, args),
    )

    jobs_mod._run_meta_fetched.clear()
    jobs_mod._detect_and_register_runs(
        "mock-cluster",
        [
            {"jobid": "123456", "job_name": "hle_mpsf_run-1", "depends_on": []},
        ],
    )

    assert captured == [("mock-cluster", "123456", run_id)], (
        "SDK runs with a real Slurm root_job_id should trigger metadata capture"
    )


def test_sdk_run_skips_metadata_while_root_is_synthetic(db_path, monkeypatch):
    """A freshly-started SDK run with the synthetic sdk-<uuid> root must NOT
    SSH — scontrol would just return nothing and waste the connection."""
    from server import jobs as jobs_mod

    _seed_sdk_run("mock-cluster", "sdk-deadbeef", "hle_mpsf_run-2")

    captured = []
    monkeypatch.setattr(
        jobs_mod,
        "_capture_run_metadata",
        lambda *a, **k: captured.append(a),
    )
    monkeypatch.setattr(
        jobs_mod.threading,
        "Thread",
        lambda target, args, daemon=True: _ImmediateThread(target, args),
    )

    jobs_mod._run_meta_fetched.clear()
    jobs_mod._detect_and_register_runs(
        "mock-cluster",
        [
            {"jobid": "sdk-deadbeef", "job_name": "hle_mpsf_run-2", "depends_on": []},
        ],
    )

    assert captured == [], (
        "SDK runs whose root_job_id is still a synthetic 'sdk-<uuid>' must not "
        "trigger scontrol-based metadata capture"
    )


class _ImmediateThread:
    """Thread stub that runs target() inline — lets the test observe the
    `_capture_run_metadata` invocation synchronously without threading."""

    def __init__(self, target, args):
        self._target = target
        self._args = args

    def start(self):
        self._target(*self._args)


# ---------------------------------------------------------------------------
# 2. RunProvenance.params round-trip
# ---------------------------------------------------------------------------


def _post_run_started(client, run_uuid, params):
    event = {
        "run_uuid": run_uuid,
        "event_type": "run_started",
        "event_seq": 1,
        "ts": 0.0,
        "payload": {
            "argv": ["python", "-m", "nemo_skills.pipeline.eval"],
            "command": "ns eval --cluster mock-cluster --expname hle_params_test",
            "cwd": "/home/tester",
            "expname": "hle_params_test",
            "cluster": "mock-cluster",
            "output_dir": "/lustre/out",
            "git_commit": "abc1234",
            "hostname": "aiapps-test",
            "env_subset": {},
            "config_overrides": {},
            "conda_env": "test",
            "python_executable": "/usr/bin/python",
            "env_vars_set": [],
            "params": params,
        },
    }
    return client.post(
        "/api/sdk/events",
        data=json.dumps([event]),
        content_type="application/json",
    )


def test_params_persist_to_runs_params_json(client, db_path):
    run_uuid = str(uuid.uuid4())
    params = {
        "model": "meta/llama-3.3-70b",
        "server_type": "sglang",
        "server_gpus": 8,
        "server_nodes": 1,
        "benchmarks": "hle:3,gpqa_diamond:5",
        "split": "test",
        "num_samples": 100,
        "num_chunks": 4,
        "with_sandbox": True,
        "judge_model": "gpt-oss-120b",
        "judge_server_type": "openai",
    }

    res = _post_run_started(client, run_uuid, params)
    assert res.status_code == 200, res.data

    from server.db import get_db
    con = get_db()
    row = con.execute(
        "SELECT params_json FROM runs WHERE run_uuid=?",
        (run_uuid,),
    ).fetchone()
    con.close()
    assert row is not None
    assert row["params_json"], "params_json should be populated from the SDK event"
    assert json.loads(row["params_json"]) == params


def test_api_run_info_exposes_parsed_params(client, db_path):
    run_uuid = str(uuid.uuid4())
    params = {
        "model": "kimi/K2.5",
        "benchmarks": "hle:3",
        "num_samples": 0,
    }
    res = _post_run_started(client, run_uuid, params)
    assert res.status_code == 200

    # Root job is a synthetic "sdk-<uuid[:12]>" at this stage.
    synthetic_root = f"sdk-{run_uuid[:12]}"

    info = client.get(f"/api/run_info/mock-cluster/{synthetic_root}")
    assert info.status_code == 200, info.data
    data = info.get_json()
    assert data["status"] == "ok"
    run = data["run"]
    assert run["params"] == params, "run.params should be the parsed dict"
    assert "params_json" not in run, "raw JSON column should not leak to the client"


def test_api_run_info_empty_params_when_missing(client, db_path):
    """Legacy-style runs without any SDK params should still get run.params={}
    so the frontend can branch cleanly without undefined checks."""
    from server.db import upsert_run

    upsert_run("mock-cluster", "987654", "hle_mpsf_legacy", "hle")

    info = client.get("/api/run_info/mock-cluster/987654")
    assert info.status_code == 200
    run = info.get_json()["run"]
    assert run["params"] == {}


def test_api_run_info_handles_corrupt_params_json(client, db_path):
    """A malformed params_json shouldn't 500 — just degrades to {}."""
    from server.db import upsert_run, db_write

    run_id = upsert_run("mock-cluster", "111222", "hle_mpsf_corrupt", "hle")
    with db_write() as con:
        con.execute("UPDATE runs SET params_json='not-json' WHERE id=?", (run_id,))

    info = client.get("/api/run_info/mock-cluster/111222")
    assert info.status_code == 200
    assert info.get_json()["run"]["params"] == {}


# ---------------------------------------------------------------------------
# 3. SDK-level sanitizer (loaded directly from the source file, outside the
# `nemo_skills.clausius_sdk` namespace — clausius's own Python env doesn't
# have NeMo-Skills installed but the sanitizer is pure stdlib).
# ---------------------------------------------------------------------------


def _load_sanitize_params():
    """Compile just the `_sanitize_params` symbol from sdk/session.py without
    importing the rest of the module (which pulls in `nemo_skills`)."""
    import ast
    import os

    src_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "sdk", "session.py",
    )
    with open(src_path, "r", encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source)
    wanted = {
        "_PARAMS_MAX_DEPTH", "_PARAMS_MAX_STR_LEN", "_PARAMS_MAX_ITEMS",
        "_sanitize_params",
    }
    kept = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in wanted for t in node.targets
        ):
            kept.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted:
            kept.append(node)
    module = ast.Module(body=kept, type_ignores=[])
    ns: dict = {"Any": object}
    exec(compile(module, src_path, "exec"), ns)
    return ns["_sanitize_params"]


def test_sanitize_params_handles_primitives_and_limits():
    sanitize = _load_sanitize_params()

    out = sanitize({
        "model": "ok",
        "gpus": 8,
        "enabled": True,
        "benchmarks": ["hle:3", "gpqa_diamond:5"],
    })
    assert out == {
        "model": "ok",
        "gpus": 8,
        "enabled": True,
        "benchmarks": ["hle:3", "gpqa_diamond:5"],
    }

    long_val = "x" * 5000
    clipped = sanitize({"blob": long_val})
    assert len(clipped["blob"]) < len(long_val)
    assert clipped["blob"].endswith("…")


def test_sanitize_params_stringifies_unknown_objects():
    sanitize = _load_sanitize_params()

    class Opaque:
        def __repr__(self):
            return "<opaque>"

    assert sanitize({"weird": Opaque()}) == {"weird": "<opaque>"}


@pytest.mark.parametrize("value", [None, True, False, 0, 3.14, "str", [], {}])
def test_sanitize_params_passes_through_json_safe_values(value):
    sanitize = _load_sanitize_params()

    assert sanitize(value) == value
