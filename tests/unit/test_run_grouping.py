"""Unit tests for run grouping and rerun separation in server/jobs.py."""

import pytest

from server.db import associate_jobs_to_run, get_db, upsert_job, upsert_run
from server.jobs import _detect_and_register_runs, _group_jobs_for_runs


def _job(jobid, name, submitted, *, state="PENDING", depends_on=None):
    depends_on = depends_on or []
    dependency = ",".join(f"afterany:{pid}(unfulfilled)" for pid in depends_on) or "(null)"
    return {
        "jobid": str(jobid),
        "name": name,
        "state": state,
        "submitted": submitted,
        "started": "N/A" if state == "PENDING" else submitted,
        "depends_on": [str(pid) for pid in depends_on],
        "dependents": [],
        "dependency": dependency,
        "project": "hle",
    }


@pytest.mark.unit
def test_group_jobs_for_runs_splits_same_name_reruns_by_submission_gap():
    jobs = [
        _job("100", "hle_same-run", "2026-04-13T00:00:00"),
        _job("101", "hle_same-run", "2026-04-13T00:00:20"),
        _job("200", "hle_same-run", "2026-04-13T00:20:00"),
        _job("201", "hle_same-run", "2026-04-13T00:20:10"),
        _job("202", "hle_same-run-judge-rs0", "2026-04-13T00:20:12", depends_on=["200", "201"]),
    ]

    groups = _group_jobs_for_runs(jobs)

    grouped_sets = {frozenset(job_ids) for _, _, job_ids in groups}
    assert grouped_sets == {
        frozenset({"100", "101"}),
        frozenset({"200", "201", "202"}),
    }


@pytest.mark.unit
def test_detect_and_register_runs_repairs_stray_old_row_from_new_run(
    db_path,
    mock_cluster,
    monkeypatch,
):
    class _DummyThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            pass

    monkeypatch.setattr("server.jobs.threading.Thread", _DummyThread)

    old_jobs = [
        _job("100", "hle_same-run", "2026-04-13T00:00:00", state="CANCELLED"),
        _job("101", "hle_same-run", "2026-04-13T00:00:20", state="CANCELLED"),
        _job("103", "hle_same-run", "2026-04-13T00:00:40", state="COMPLETED"),
    ]
    current_jobs = [
        _job("200", "hle_same-run", "2026-04-13T00:20:00"),
        _job("201", "hle_same-run", "2026-04-13T00:20:10"),
        _job("202", "hle_same-run-judge-rs0", "2026-04-13T00:20:12", depends_on=["200", "201"]),
    ]

    for job in old_jobs + current_jobs:
        upsert_job(mock_cluster, job, terminal=job["state"] != "PENDING")

    old_run_id = upsert_run(mock_cluster, "100", "hle_same-run", "hle")
    associate_jobs_to_run(mock_cluster, old_run_id, ["100", "101"])

    contaminated_new_run_id = upsert_run(mock_cluster, "200", "hle_same-run", "hle")
    associate_jobs_to_run(mock_cluster, contaminated_new_run_id, ["103"])

    _detect_and_register_runs(mock_cluster, old_jobs + current_jobs)

    con = get_db()
    rows = con.execute(
        """
        SELECT job_id, run_id
        FROM job_history
        WHERE cluster=? AND job_id IN ('100','101','103','200','201','202')
        ORDER BY job_id
        """,
        (mock_cluster,),
    ).fetchall()
    con.close()

    run_ids = {row["job_id"]: row["run_id"] for row in rows}
    assert run_ids["100"] == old_run_id
    assert run_ids["101"] == old_run_id
    assert run_ids["103"] == old_run_id
    assert run_ids["200"] == contaminated_new_run_id
    assert run_ids["201"] == contaminated_new_run_id
    assert run_ids["202"] == contaminated_new_run_id


@pytest.mark.unit
def test_detect_and_register_runs_splits_fully_contaminated_run_by_root_owner(
    db_path,
    mock_cluster,
    monkeypatch,
):
    class _DummyThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            pass

    monkeypatch.setattr("server.jobs.threading.Thread", _DummyThread)

    old_jobs = [
        _job("100", "hle_same-run", "2026-04-13T00:00:00", state="CANCELLED"),
        _job("101", "hle_same-run", "2026-04-13T00:00:20", state="CANCELLED"),
    ]
    current_jobs = [
        _job("200", "hle_same-run", "2026-04-13T00:20:00", state="FAILED"),
        _job("201", "hle_same-run", "2026-04-13T00:20:10", state="FAILED"),
        _job("202", "hle_same-run-judge-rs0", "2026-04-13T00:20:12", state="FAILED", depends_on=["200", "201"]),
    ]

    for job in old_jobs + current_jobs:
        upsert_job(mock_cluster, job, terminal=True)

    contaminated_run_id = upsert_run(mock_cluster, "100", "hle_same-run", "hle")
    associate_jobs_to_run(mock_cluster, contaminated_run_id, ["100", "101", "200", "201", "202"])

    _detect_and_register_runs(mock_cluster, old_jobs + current_jobs)

    con = get_db()
    rows = con.execute(
        """
        SELECT job_id, run_id
        FROM job_history
        WHERE cluster=? AND job_id IN ('100','101','200','201','202')
        ORDER BY job_id
        """,
        (mock_cluster,),
    ).fetchall()
    con.close()

    run_ids = {row["job_id"]: row["run_id"] for row in rows}
    assert run_ids["100"] == contaminated_run_id
    assert run_ids["101"] == contaminated_run_id
    assert run_ids["200"] == run_ids["201"] == run_ids["202"]
    assert run_ids["200"] != contaminated_run_id
