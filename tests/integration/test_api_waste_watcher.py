"""Integration tests for the WasteWatcher Flask routes + audit pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from server import waste_watcher as ww
from server import waste_watcher_state as wws
from server.db import db_write, upsert_run


@pytest.mark.integration
class TestWasteCandidatesRoute:
    def test_empty_candidates_by_default(self, client, db_path):
        resp = client.get("/api/waste/candidates")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["candidates"] == []

    def test_lists_suspicious_and_wasteful_only(self, client, db_path, mock_cluster):
        wws.upsert_state(wws.WatcherState(
            cluster=mock_cluster, job_id="100",
            state=wws.STATE_COLD,
        ))
        wws.upsert_state(wws.WatcherState(
            cluster=mock_cluster, job_id="101",
            state=wws.STATE_SUSPICIOUS,
            suspected_reason="idle_gpu_sustained",
            suspected_confidence="medium",
        ))
        wws.upsert_state(wws.WatcherState(
            cluster=mock_cluster, job_id="102",
            state=wws.STATE_WASTEFUL,
            suspected_reason="port_mismatch_hang",
            suspected_confidence="high",
        ))
        resp = client.get("/api/waste/candidates")
        body = resp.get_json()
        ids = {c["job_id"] for c in body["candidates"]}
        assert ids == {"101", "102"}

    def test_filter_by_min_confidence_high(self, client, db_path, mock_cluster):
        wws.upsert_state(wws.WatcherState(
            cluster=mock_cluster, job_id="101",
            state=wws.STATE_SUSPICIOUS,
            suspected_confidence="medium",
        ))
        wws.upsert_state(wws.WatcherState(
            cluster=mock_cluster, job_id="102",
            state=wws.STATE_WASTEFUL,
            suspected_confidence="high",
        ))
        resp = client.get("/api/waste/candidates?min_confidence=high")
        body = resp.get_json()
        ids = {c["job_id"] for c in body["candidates"]}
        assert ids == {"102"}


@pytest.mark.integration
class TestWasteRunsRoute:
    def test_lists_runs_with_wasteful_flag(self, client, db_path, mock_cluster):
        run_id = upsert_run(mock_cluster, "waste-run-1", "hle_mpsf_x", "hle")
        with db_write() as con:
            con.execute(
                "UPDATE runs SET wasteful=1, waste_reason=?, "
                "waste_detected_at=? WHERE id=?",
                ("port_mismatch_hang", datetime.now().isoformat(timespec="seconds"), run_id),
            )
        resp = client.get("/api/waste/runs")
        body = resp.get_json()
        assert body["status"] == "ok"
        assert len(body["runs"]) == 1
        assert body["runs"][0]["waste_reason"] == "port_mismatch_hang"

    def test_filter_by_project(self, client, db_path, mock_cluster):
        r1 = upsert_run(mock_cluster, "wr-2a", "hle_mpsf_x", "hle")
        r2 = upsert_run(mock_cluster, "wr-2b", "mcp_run_y", "mcp")
        now_iso = datetime.now().isoformat(timespec="seconds")
        with db_write() as con:
            con.execute(
                "UPDATE runs SET wasteful=1, waste_reason='manual', "
                "waste_detected_at=? WHERE id IN (?, ?)",
                (now_iso, r1, r2),
            )
        resp = client.get("/api/waste/runs?project=mcp")
        body = resp.get_json()
        names = {r["project"] for r in body["runs"]}
        assert names == {"mcp"}


@pytest.mark.integration
class TestWastePauseAndExempt:
    def test_pause_and_resume_global(self, client, db_path):
        resp = client.post("/api/waste/pause", json={"duration_min": 5})
        body = resp.get_json()
        assert body["status"] == "ok"
        assert ww.is_paused("any-cluster") is True
        client.post("/api/waste/resume", json={})
        assert ww.is_paused("any-cluster") is False

    def test_pause_unknown_cluster_404(self, client, db_path):
        resp = client.post("/api/waste/pause",
                           json={"cluster": "nonexistent", "duration_min": 1})
        assert resp.status_code == 404

    def test_exempt_job_sets_state(self, client, db_path, mock_cluster):
        resp = client.post("/api/waste/exempt_job",
                           json={"cluster": mock_cluster, "job_id": "555",
                                 "duration_min": 30, "note": "operator-verified"})
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["state"] == "exempt"
        loaded = wws.load_state(mock_cluster, "555")
        assert loaded.state == "exempt"
        assert "operator-verified" in loaded.last_notes

    def test_exempt_requires_cluster_and_job_id(self, client, db_path):
        resp = client.post("/api/waste/exempt_job", json={})
        assert resp.status_code == 400


@pytest.mark.integration
class TestCancelRouteWithReason:
    def test_cancel_with_reason_stamps_job_history(self, client, mock_ssh, mock_cluster, db_path):
        from server.db import upsert_job
        upsert_job(mock_cluster, {"jobid": "777", "state": "RUNNING"}, terminal=False)
        mock_ssh.set(mock_cluster, "scancel", ("__CLAUSIUS_CANCEL__:OK:777\n", ""))
        resp = client.post(
            f"/api/cancel/{mock_cluster}/777",
            json={"reason": "port_mismatch_hang"},
        )
        assert resp.get_json()["status"] == "ok"
        from server.db import get_db
        con = get_db()
        row = con.execute(
            "SELECT waste_reason, waste_cancelled_at FROM job_history "
            "WHERE cluster=? AND job_id=?",
            (mock_cluster, "777"),
        ).fetchone()
        con.close()
        assert row["waste_reason"] == "port_mismatch_hang"
        assert row["waste_cancelled_at"]  # populated


@pytest.mark.integration
class TestRunPatchWasteful:
    def test_patch_run_wasteful_flag(self, client, mock_cluster, db_path):
        run_id = upsert_run(mock_cluster, "wp-1", "hle_mpsf_x", "hle")
        resp = client.patch(
            f"/api/run/{run_id}",
            json={"wasteful": True, "waste_reason": "manual"},
            content_type="application/json",
        )
        assert resp.get_json()["status"] == "ok"
        from server.db import get_db
        con = get_db()
        row = con.execute(
            "SELECT wasteful, waste_reason, waste_detected_at, "
            "waste_cancelled_by_watcher FROM runs WHERE id=?",
            (run_id,),
        ).fetchone()
        con.close()
        assert int(row["wasteful"]) == 1
        assert row["waste_reason"] == "manual"
        assert row["waste_detected_at"]  # populated
        assert int(row["waste_cancelled_by_watcher"]) == 0  # manual flag, not watcher

    def test_run_info_surfaces_wasteful_fields(self, client, mock_cluster, db_path):
        run_id = upsert_run(mock_cluster, "wp-2", "hle_mpsf_y", "hle")
        with db_write() as con:
            con.execute(
                "UPDATE runs SET wasteful=1, waste_reason='dead_server_before_client', "
                "waste_detected_at=?, waste_cancelled_by_watcher=1, "
                "run_uuid=? WHERE id=?",
                (datetime.now().isoformat(timespec="seconds"),
                 "11111111-2222-3333-4444-555555555555", run_id),
            )
        from server.db import get_run_hash
        rh = get_run_hash(mock_cluster, "wp-2", "11111111-2222-3333-4444-555555555555")
        info = client.get(f"/api/run_info_by_hash/{mock_cluster}/{rh}")
        body = info.get_json()
        assert body["status"] == "ok"
        assert body["run"]["wasteful"] is True
        assert body["run"]["waste_reason"] == "dead_server_before_client"
        assert body["run"]["waste_cancelled_by_watcher"] is True


@pytest.mark.integration
class TestPortMismatchEndToEnd:
    """End-to-end: a port-mismatch-hang job can run in flag-only mode."""

    def test_evaluate_job_flags_run_via_logbook(
        self, client, mock_ssh, mock_cluster, db_path, monkeypatch,
    ):
        # Seed a run + a job_history row so the flag-stamping has a real target.
        run_id = upsert_run(mock_cluster, "999", "mpsf_pipeline_x", "hle")
        from server.db import upsert_job
        upsert_job(mock_cluster, {
            "jobid": "999",
            "state": "RUNNING",
            "gres": "gpu:4",
            "name": "mpsf_pipeline_x-r1-path_server",
            "started_at": (datetime.now() - timedelta(minutes=30)).isoformat(),
        }, terminal=False)
        with db_write() as con:
            con.execute("UPDATE job_history SET run_id=? WHERE job_id=?", (run_id, "999"))

        # Force flag-only mode. Even if we somehow tried to scancel, the
        # mock SSH would catch it.
        from server.settings import set_setting
        set_setting("waste_watcher_cancel_enabled", False)

        # Build a fake job dict mirroring the live-cache shape.
        job = {
            "cluster": mock_cluster,
            "jobid": "999",
            "name": "mpsf_pipeline_x-r1-path_server",
            "state": "RUNNING",
            "gres": "gpu:4",
            "started": (datetime.now() - timedelta(minutes=30)).isoformat(),
            "started_at": (datetime.now() - timedelta(minutes=30)).isoformat(),
            "run_root_job_id": "999",
        }

        # Stub _probe_job / get_stats_snapshots / _read_log_tail so we
        # don't actually SSH anywhere.
        monkeypatch.setattr(ww, "_probe_job", lambda c, j: {
            "status": "ok",
            "gpus": [{"index": "0", "util": "0%"}, {"index": "1", "util": "0%"}],
        })
        monkeypatch.setattr(ww, "get_stats_snapshots", lambda c, j: [
            {"ts": "2026-01-01T00:00:00", "gpu_util": 0,
             "per_gpu": [{"util": "0%"}, {"util": "0%"}]},
            {"ts": "2026-01-01T00:01:00", "gpu_util": 0,
             "per_gpu": [{"util": "0%"}, {"util": "0%"}]},
            {"ts": "2026-01-01T00:02:00", "gpu_util": 0,
             "per_gpu": [{"util": "0%"}, {"util": "0%"}]},
        ])
        monkeypatch.setattr(ww, "_read_log_tail", lambda c, j, lines=80:
                            "Server hostname written: pool0-0010\nApplication startup complete\n")
        monkeypatch.setattr(ww, "_enumerate_all_jobs_for_cluster", lambda c: [job])

        cfg = ww._settings_snapshot()
        det = ww._evaluate_job(job, cfg)
        # The port-mismatch rule should fire
        assert det is not None
        assert det.reason == "port_mismatch_hang"
        # Act on the detection: since cancel_enabled=False, expect flag-only.
        ww._act_on_detection(job, det, cfg)

        from server.db import get_db
        con = get_db()
        run_row = con.execute(
            "SELECT wasteful, waste_reason FROM runs WHERE id=?",
            (run_id,),
        ).fetchone()
        con.close()
        assert int(run_row["wasteful"]) == 1
        assert run_row["waste_reason"] == "port_mismatch_hang"

    def test_verification_rejection_does_not_stamp_wasteful(
        self, client, mock_cluster, db_path, monkeypatch,
    ):
        run_id = upsert_run(mock_cluster, "1000", "mpsf_pipeline_y", "hle")
        from server.db import upsert_job
        upsert_job(mock_cluster, {
            "jobid": "1000",
            "state": "RUNNING",
            "gres": "gpu:4",
            "name": "mpsf_pipeline_y-r1-path_server",
            "started_at": (datetime.now() - timedelta(minutes=30)).isoformat(),
        }, terminal=False)
        with db_write() as con:
            con.execute("UPDATE job_history SET run_id=? WHERE job_id=?", (run_id, "1000"))

        from server.settings import set_setting
        set_setting("waste_watcher_cancel_enabled", True)
        monkeypatch.setattr(ww, "verify_wasteful", lambda **kwargs: (False, "log_still_growing"))

        det = ww.Detection(
            reason="port_mismatch_hang",
            confidence="high",
            target_jobs=[(mock_cluster, "1000")],
            summary="candidate rejected by verification",
            evidence={"verification": {"1000": "log_still_growing"}},
        )
        ww._act_on_detection({"cluster": mock_cluster, "jobid": "1000"}, det, ww._settings_snapshot())

        from server.db import get_db
        con = get_db()
        run_row = con.execute(
            "SELECT wasteful, waste_reason FROM runs WHERE id=?",
            (run_id,),
        ).fetchone()
        job_row = con.execute(
            "SELECT waste_reason FROM job_history WHERE cluster=? AND job_id=?",
            (mock_cluster, "1000"),
        ).fetchone()
        con.close()
        assert int(run_row["wasteful"]) == 0
        assert run_row["waste_reason"] == ""
        assert job_row["waste_reason"] == ""
