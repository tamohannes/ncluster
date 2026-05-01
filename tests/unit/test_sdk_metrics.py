from __future__ import annotations

import importlib.util
import json
import sys
import types
import uuid
from pathlib import Path


def _load_sdk_run(monkeypatch):
    sdk_dir = Path(__file__).resolve().parents[2] / "sdk"
    for name in list(sys.modules):
        if name == "nemo_skills" or name.startswith("nemo_skills."):
            monkeypatch.delitem(sys.modules, name, raising=False)

    nemo_pkg = types.ModuleType("nemo_skills")
    nemo_pkg.__path__ = []
    clausius_pkg = types.ModuleType("nemo_skills.clausius_sdk")
    clausius_pkg.__path__ = [str(sdk_dir)]
    transports_pkg = types.ModuleType("nemo_skills.clausius_sdk.transports")
    transports_pkg.__path__ = [str(sdk_dir / "transports")]
    monkeypatch.setitem(sys.modules, "nemo_skills", nemo_pkg)
    monkeypatch.setitem(sys.modules, "nemo_skills.clausius_sdk", clausius_pkg)
    monkeypatch.setitem(sys.modules, "nemo_skills.clausius_sdk.transports", transports_pkg)

    def load(mod_name, path):
        spec = importlib.util.spec_from_file_location(mod_name, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, mod_name, module)
        spec.loader.exec_module(module)
        return module

    load("nemo_skills.clausius_sdk.events", sdk_dir / "events.py")
    load("nemo_skills.clausius_sdk.transports.base", sdk_dir / "transports" / "base.py")
    load("nemo_skills.clausius_sdk.session", sdk_dir / "session.py")
    run_mod = load("nemo_skills.clausius_sdk.run", sdk_dir / "run.py")
    return run_mod.Run


def _run_started_event(run_uuid):
    return {
        "run_uuid": run_uuid,
        "event_type": "run_started",
        "event_seq": 1,
        "ts": 0.0,
        "payload": {
            "argv": ["python", "train.py"],
            "command": "python train.py",
            "cwd": "/tmp/work",
            "expname": "hle_sdk_metrics",
            "cluster": "mock-cluster",
            "output_dir": "/tmp/out",
            "git_commit": "abc1234",
            "hostname": "test-host",
            "env_subset": {},
            "config_overrides": {},
            "conda_env": "test",
            "python_executable": "/usr/bin/python",
            "env_vars_set": [],
            "params": {},
        },
    }


def test_manual_run_track_and_metadata_emit_sdk_events(tmp_path, monkeypatch):
    Run = _load_sdk_run(monkeypatch)
    monkeypatch.setenv("CLAUSIUS_SPOOL_DIR", str(tmp_path))

    run = Run(
        run_name="hle_manual_metrics",
        cluster="mock-cluster",
        metadata={"model": "demo", "batch_size": 8},
    )
    run.track("loss", 0.42, step=2, context={"split": "train"})
    run.scalar("final_accuracy", 0.84, split="eval")
    run.close()

    events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    event_types = [event["event_type"] for event in events]
    assert event_types == [
        "run_started",
        "metadata_logged",
        "metric_logged",
        "scalar_logged",
        "run_finished",
    ]

    metric = next(event for event in events if event["event_type"] == "metric_logged")
    assert metric["payload"] == {
        "key": "loss",
        "value": 0.42,
        "step": 2,
        "context": {"split": "train"},
    }

    metadata = next(event for event in events if event["event_type"] == "metadata_logged")
    assert metadata["payload"]["metadata"] == {"model": "demo", "batch_size": 8}

    scalar = next(event for event in events if event["event_type"] == "scalar_logged")
    assert scalar["payload"] == {
        "key": "final_accuracy",
        "value": 0.84,
        "context": {"split": "eval"},
    }


def test_sdk_ingest_persists_generic_metrics_and_metadata(client, db_path):
    run_uuid = uuid.uuid4().hex
    events = [
        _run_started_event(run_uuid),
        {
            "run_uuid": run_uuid,
            "event_type": "metadata_logged",
            "event_seq": 2,
            "ts": 1.0,
            "payload": {"metadata": {"model": "demo", "lr": 1e-5}},
        },
        {
            "run_uuid": run_uuid,
            "event_type": "metric_logged",
            "event_seq": 3,
            "ts": 2.0,
            "payload": {"key": "loss", "value": 0.5, "step": 1, "context": {"split": "train"}},
        },
        {
            "run_uuid": run_uuid,
            "event_type": "metric_logged",
            "event_seq": 4,
            "ts": 3.0,
            "payload": {"key": "phase", "value": "warmup"},
        },
        {
            "run_uuid": run_uuid,
            "event_type": "scalar_logged",
            "event_seq": 5,
            "ts": 4.0,
            "payload": {"key": "final_accuracy", "value": 0.75, "context": {"split": "eval"}},
        },
        {
            "run_uuid": run_uuid,
            "event_type": "metric_logged",
            "event_seq": 6,
            "ts": 5.0,
            "payload": {"key": "progress", "value": 10},
        },
    ]

    res = client.post("/api/sdk/events", data=json.dumps(events), content_type="application/json")
    assert res.status_code == 200, res.data
    assert res.get_json()["accepted"] == len(events)

    root_job_id = f"sdk-{run_uuid[:12]}"
    metrics = client.get(f"/api/run_metrics/mock-cluster/{root_job_id}")
    assert metrics.status_code == 200, metrics.data
    payload = metrics.get_json()
    metrics_by_hash = client.get(f"/api/run_metrics_by_hash/mock-cluster/{run_uuid[:8]}")
    assert metrics_by_hash.status_code == 200, metrics_by_hash.data
    assert metrics_by_hash.get_json()["series"]["loss"] == payload["series"]["loss"]
    assert payload["metadata"] == {"model": "demo", "lr": 1e-5}
    assert set(payload["series"]) == {"loss"}
    assert payload["series"]["loss"][0]["value"] == 0.5
    assert payload["series"]["loss"][0]["value_num"] == 0.5
    assert payload["series"]["loss"][0]["context"] == {"split": "train"}
    assert set(payload["scalars"]) == {"phase", "final_accuracy"}
    assert payload["scalars"]["phase"][0]["value"] == "warmup"
    assert payload["scalar_latest"]["final_accuracy"]["value"] == 0.75
    assert payload["scalar_latest"]["final_accuracy"]["value_num"] == 0.75

    from server.db import get_db
    con = get_db()
    sdk_events = con.execute("SELECT COUNT(*) AS n FROM sdk_events WHERE run_uuid=?", (run_uuid,)).fetchone()["n"]
    run_metrics = con.execute("SELECT COUNT(*) AS n FROM run_metrics WHERE run_uuid=?", (run_uuid,)).fetchone()["n"]
    run_scalars = con.execute("SELECT COUNT(*) AS n FROM run_scalars WHERE run_uuid=?", (run_uuid,)).fetchone()["n"]
    con.close()
    assert sdk_events == len(events)
    assert run_metrics == 1
    assert run_scalars == 2


def test_sdk_resume_reuses_existing_run_by_output_dir(client, db_path):
    first_uuid = uuid.uuid4().hex
    resume_uuid = uuid.uuid4().hex
    first_started = _run_started_event(first_uuid)
    resume_started = _run_started_event(resume_uuid)

    events = [
        first_started,
        {
            "run_uuid": first_uuid,
            "event_type": "metric_logged",
            "event_seq": 2,
            "ts": 1.0,
            "payload": {"key": "loss", "value": 0.7, "step": 1},
        },
        {
            "run_uuid": first_uuid,
            "event_type": "run_failed",
            "event_seq": 3,
            "ts": 2.0,
            "payload": {"status": "failed"},
        },
        resume_started,
        {
            "run_uuid": resume_uuid,
            "event_type": "metric_logged",
            "event_seq": 2,
            "ts": 3.0,
            "payload": {"key": "loss", "value": 0.4, "step": 2},
        },
    ]

    res = client.post("/api/sdk/events", data=json.dumps(events), content_type="application/json")
    assert res.status_code == 200, res.data
    assert res.get_json()["accepted"] == len(events)

    from server.db import get_db
    con = get_db()
    runs = con.execute(
        "SELECT id, root_job_id, run_uuid, sdk_status, ended_at FROM runs WHERE run_name=?",
        ("hle_sdk_metrics",),
    ).fetchall()
    aliases = con.execute(
        "SELECT alias_uuid, canonical_uuid FROM sdk_run_aliases"
    ).fetchall()
    con.close()

    assert len(runs) == 1
    assert runs[0]["run_uuid"] == first_uuid
    assert runs[0]["sdk_status"] == "submitting"
    assert runs[0]["ended_at"] is None
    assert [(r["alias_uuid"], r["canonical_uuid"]) for r in aliases] == [
        (resume_uuid, first_uuid)
    ]

    metrics = client.get(f"/api/run_metrics/mock-cluster/{runs[0]['root_job_id']}")
    assert metrics.status_code == 200, metrics.data
    loss_points = metrics.get_json()["series"]["loss"]
    assert [p["value"] for p in loss_points] == [0.7, 0.4]


def test_init_db_collapses_existing_resume_duplicates(_isolate_db):
    from server.db import get_db, init_db

    init_db()
    con = get_db()
    con.execute(
        """INSERT INTO runs
              (cluster, root_job_id, run_name, run_uuid, source, primary_output_dir, sdk_status)
           VALUES (?, ?, ?, ?, 'sdk', ?, ?)""",
        ("mock-cluster", "sdk-first", "hle_sdk_metrics", "uuid-first", "/tmp/out", "failed"),
    )
    first_id = con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    con.execute(
        """INSERT INTO runs
              (cluster, root_job_id, run_name, run_uuid, source, primary_output_dir, sdk_status)
           VALUES (?, ?, ?, ?, 'sdk', ?, ?)""",
        ("mock-cluster", "sdk-second", "hle_sdk_metrics", "uuid-second", "/tmp/out/", "active"),
    )
    second_id = con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    con.execute(
        """INSERT INTO job_history (cluster, job_id, job_name, run_id)
           VALUES (?, ?, ?, ?)""",
        ("mock-cluster", "123", "hle_sdk_metrics", second_id),
    )
    con.commit()

    init_db()

    rows = con.execute("SELECT id, run_uuid, sdk_status FROM runs WHERE run_name=?", ("hle_sdk_metrics",)).fetchall()
    alias = con.execute("SELECT alias_uuid, canonical_uuid FROM sdk_run_aliases").fetchone()
    job = con.execute("SELECT run_id FROM job_history WHERE cluster=? AND job_id=?", ("mock-cluster", "123")).fetchone()
    con.close()

    assert len(rows) == 1
    assert rows[0]["id"] == first_id
    assert rows[0]["run_uuid"] == "uuid-first"
    assert rows[0]["sdk_status"] == "active"
    assert (alias["alias_uuid"], alias["canonical_uuid"]) == ("uuid-second", "uuid-first")
    assert job["run_id"] == first_id
