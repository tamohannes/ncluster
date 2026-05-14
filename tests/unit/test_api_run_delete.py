"""Tests for DELETE /api/run/<id> and server.db.delete_run_completely."""

from __future__ import annotations

import pytest

from server.db import (
    db_write,
    delete_run_completely,
    get_db,
    upsert_run,
)


def _seed_run_with_artifacts(cluster, root_job_id, run_name, project, uuid):
    """Create a run row plus a representative slice of every dependent table."""
    run_id = upsert_run(cluster, root_job_id, run_name, project)
    with db_write() as con:
        con.execute("UPDATE runs SET run_uuid=? WHERE id=?", (uuid, run_id))
        con.execute(
            "INSERT INTO job_history (cluster, job_id, job_name, state, run_id) "
            "VALUES (?,?,?,?,?)",
            (cluster, root_job_id, run_name, "COMPLETED", run_id),
        )
        con.execute(
            "INSERT INTO sdk_events (run_uuid, event_type, event_seq, ts, payload_json) "
            "VALUES (?,?,?,?,?)",
            (uuid, "metric", 0, 0.0, "{}"),
        )
        con.execute(
            "INSERT INTO run_metrics (run_uuid, event_seq, key, step, ts, value_num) "
            "VALUES (?,?,?,?,?,?)",
            (uuid, 0, "loss", 1, 0.0, 1.5),
        )
        con.execute(
            "INSERT INTO run_scalars (run_uuid, event_seq, key, ts, value_num) "
            "VALUES (?,?,?,?,?)",
            (uuid, 1, "accuracy", 0.0, 0.83),
        )
        con.execute(
            "INSERT INTO sdk_run_aliases (alias_uuid, canonical_uuid) VALUES (?,?)",
            (uuid + "-alias", uuid),
        )
        con.execute(
            "INSERT INTO run_metrics (run_uuid, event_seq, key, step, ts, value_num) "
            "VALUES (?,?,?,?,?,?)",
            (uuid + "-alias", 0, "loss", 2, 0.0, 1.0),
        )
        con.execute(
            "INSERT INTO job_stats_snapshots (cluster, job_id, ts, gpu_util) "
            "VALUES (?,?,?,?)",
            (cluster, root_job_id, "2026-01-01T00:00:00", 50.0),
        )
    return run_id


@pytest.mark.unit
def test_delete_run_completely_wipes_metrics_metadata_aliases(db_path):
    run_id = _seed_run_with_artifacts(
        "mock-cluster", "del-1", "hle_mpsf_a", "hle",
        "11111111-aaaa-bbbb-cccc-222222222222",
    )

    result = delete_run_completely(run_id)

    assert result["status"] == "ok"
    counts = result["counts"]
    assert counts["runs"] == 1
    assert counts["sdk_events"] == 1
    assert counts["run_metrics"] == 2
    assert counts["run_scalars"] == 1
    assert counts["sdk_run_aliases"] == 1
    assert counts["job_history_unlinked"] == 1
    assert counts["job_history_deleted"] == 0
    assert counts["job_stats_snapshots"] == 0

    con = get_db()
    assert con.execute("SELECT COUNT(*) FROM runs WHERE id=?", (run_id,)).fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM sdk_events").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM run_metrics").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM run_scalars").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM sdk_run_aliases").fetchone()[0] == 0
    leftover = con.execute(
        "SELECT run_id FROM job_history WHERE cluster=? AND job_id=?",
        ("mock-cluster", "del-1"),
    ).fetchone()
    con.close()
    assert leftover is not None
    assert leftover["run_id"] is None


@pytest.mark.unit
def test_delete_run_completely_delete_jobs_removes_history(db_path):
    run_id = _seed_run_with_artifacts(
        "mock-cluster", "del-2", "hle_mpsf_b", "hle",
        "22222222-aaaa-bbbb-cccc-333333333333",
    )

    result = delete_run_completely(run_id, delete_jobs=True)

    assert result["status"] == "ok"
    counts = result["counts"]
    assert counts["runs"] == 1
    assert counts["job_history_unlinked"] == 0
    assert counts["job_history_deleted"] == 1
    assert counts["job_stats_snapshots"] == 1

    con = get_db()
    assert con.execute(
        "SELECT COUNT(*) FROM job_history WHERE cluster=? AND job_id=?",
        ("mock-cluster", "del-2"),
    ).fetchone()[0] == 0
    assert con.execute(
        "SELECT COUNT(*) FROM job_stats_snapshots WHERE cluster=? AND job_id=?",
        ("mock-cluster", "del-2"),
    ).fetchone()[0] == 0
    con.close()


@pytest.mark.unit
def test_delete_run_completely_returns_not_found_for_unknown_id(db_path):
    result = delete_run_completely(99999)
    assert result["status"] == "not_found"
    assert result["counts"]["runs"] == 0


@pytest.mark.unit
def test_api_delete_run_round_trip(client, db_path):
    run_id = _seed_run_with_artifacts(
        "mock-cluster", "del-3", "hle_mpsf_c", "hle",
        "33333333-aaaa-bbbb-cccc-444444444444",
    )

    res = client.delete(f"/api/run/{run_id}")
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["status"] == "ok"
    assert payload["counts"]["runs"] == 1

    res2 = client.delete(f"/api/run/{run_id}")
    assert res2.status_code == 404
    assert res2.get_json()["status"] == "not_found"


@pytest.mark.unit
def test_api_delete_run_delete_jobs_flag(client, db_path):
    run_id = _seed_run_with_artifacts(
        "mock-cluster", "del-4", "hle_mpsf_d", "hle",
        "44444444-aaaa-bbbb-cccc-555555555555",
    )

    res = client.delete(f"/api/run/{run_id}?delete_jobs=1")
    assert res.status_code == 200
    counts = res.get_json()["counts"]
    assert counts["job_history_deleted"] == 1
    assert counts["job_history_unlinked"] == 0
