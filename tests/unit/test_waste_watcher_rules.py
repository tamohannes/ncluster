"""Unit tests for the pure detection rules in :mod:`server.waste_watcher_rules`.

Each test feeds a hand-crafted ``job`` / ``snapshots`` / ``run_jobs``
fixture into one rule and asserts the resulting Detection (or None).
Rules are intentionally pure so this file never touches the DB, the
network, or the watcher daemon — keeping the suite fast and
deterministic.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from server import waste_watcher_rules as rules


# ─── Fixtures ───────────────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now()


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _gpu_server_job(*, jobid="100", started_min_ago=30):
    """Build a synthetic GPU-server job dict matching live-cache shape."""
    return {
        "cluster": "mock-cluster",
        "jobid": jobid,
        "job_id": jobid,
        "name": f"mpsf_pipeline_x-r{jobid}-path_server",
        "state": "RUNNING",
        "gres": "gpu:4",
        "started": _iso(_now() - timedelta(minutes=started_min_ago)),
        "started_at": _iso(_now() - timedelta(minutes=started_min_ago)),
    }


def _snapshots_zero(count=6):
    """A series of snapshots all showing 0% GPU utilization."""
    base = _now() - timedelta(minutes=count)
    return [
        {
            "ts": _iso(base + timedelta(minutes=i)),
            "gpu_util": 0.0,
            "per_gpu": [
                {"index": "0", "util": "0%"},
                {"index": "1", "util": "0%"},
            ],
        }
        for i in range(count)
    ]


def _snapshots_busy(count=3):
    return [
        {"ts": _iso(_now() - timedelta(minutes=count - i)),
         "gpu_util": 80.0,
         "per_gpu": [{"index": "0", "util": "80%"}, {"index": "1", "util": "40%"}]}
        for i in range(count)
    ]


# ─── port_mismatch_hang ─────────────────────────────────────────────────────


@pytest.mark.unit
class TestPortMismatchHang:
    def test_fires_when_server_bound_and_no_client_calls(self):
        job = _gpu_server_job()
        snaps = _snapshots_zero(6)
        tail = (
            "vLLM API server starting\n"
            "Application startup complete\n"
            "Server hostname written: pool0-0010\n"
        )
        det = rules.detect_port_mismatch_hang(
            job=job, snapshots=snaps, log_tail=tail,
            min_runtime_min=10, busy_threshold_pct=5.0,
        )
        assert det is not None
        assert det.reason == rules.WASTE_REASON_PORT_MISMATCH
        assert det.confidence == rules.CONFIDENCE_HIGH
        assert det.target_jobs == [("mock-cluster", "100")]

    def test_abstains_when_client_request_observed(self):
        job = _gpu_server_job()
        snaps = _snapshots_zero(6)
        tail = (
            "Server hostname written: pool0-0010\n"
            "POST /v1/chat/completions 200 OK\n"
        )
        det = rules.detect_port_mismatch_hang(
            job=job, snapshots=snaps, log_tail=tail,
            min_runtime_min=10, busy_threshold_pct=5.0,
        )
        assert det is None

    def test_abstains_when_too_young(self):
        # Just started running — vLLM weight loading; don't flag yet.
        job = _gpu_server_job(started_min_ago=2)
        snaps = _snapshots_zero(2)
        tail = "Server hostname written: x"
        det = rules.detect_port_mismatch_hang(
            job=job, snapshots=snaps, log_tail=tail,
            min_runtime_min=10, busy_threshold_pct=5.0,
        )
        assert det is None

    def test_abstains_when_any_gpu_busy(self):
        job = _gpu_server_job()
        snaps = _snapshots_zero(6)
        snaps[-1]["per_gpu"][0]["util"] = "47%"
        tail = "Server hostname written: x"
        det = rules.detect_port_mismatch_hang(
            job=job, snapshots=snaps, log_tail=tail,
            min_runtime_min=10, busy_threshold_pct=5.0,
        )
        assert det is None


# ─── dead_server_before_client ──────────────────────────────────────────────


@pytest.mark.unit
class TestDeadServerBeforeClient:
    def test_fires_when_parent_ended_before_client_started(self):
        parent_end = _now() - timedelta(minutes=2)
        client_start = _now() - timedelta(minutes=1)  # AFTER parent died
        parent = {
            "cluster": "mock-cluster", "jobid": "200", "job_id": "200",
            "name": "mpsf_pipeline_x-r2-turn_server-1",
            "state": "COMPLETED",
            "ended_at": _iso(parent_end),
        }
        child = {
            "cluster": "mock-cluster", "jobid": "201", "job_id": "201",
            "name": "mpsf_pipeline_x-r2-turn-generate-1",
            "state": "RUNNING",
            "started": _iso(client_start),
            "started_at": _iso(client_start),
            "depends_on": ["200"],
        }
        det = rules.detect_dead_server_before_client(
            job=child, run_jobs=[parent, child],
        )
        assert det is not None
        assert det.reason == rules.WASTE_REASON_DEAD_SERVER
        assert det.target_jobs == [("mock-cluster", "201")]
        # The detection should explicitly NOT target the dead parent.
        assert ("mock-cluster", "200") not in det.target_jobs

    def test_abstains_when_parent_still_running(self):
        parent = {
            "cluster": "mock-cluster", "jobid": "200", "job_id": "200",
            "state": "RUNNING", "ended_at": None,
        }
        child = {
            "cluster": "mock-cluster", "jobid": "201", "job_id": "201",
            "state": "RUNNING",
            "started": _iso(_now() - timedelta(minutes=5)),
            "depends_on": ["200"],
        }
        det = rules.detect_dead_server_before_client(
            job=child, run_jobs=[parent, child],
        )
        assert det is None

    def test_abstains_when_parent_ended_after_client_started(self):
        # Parent overlapped the child for a while — healthy normal case.
        child_start = _now() - timedelta(minutes=5)
        parent_end = _now() - timedelta(minutes=2)
        parent = {
            "cluster": "mock-cluster", "jobid": "200", "job_id": "200",
            "state": "COMPLETED",
            "ended_at": _iso(parent_end),
        }
        child = {
            "cluster": "mock-cluster", "jobid": "201", "job_id": "201",
            "state": "RUNNING",
            "started": _iso(child_start),
            "started_at": _iso(child_start),
            "depends_on": ["200"],
        }
        det = rules.detect_dead_server_before_client(
            job=child, run_jobs=[parent, child],
        )
        assert det is None


# ─── dependency_cascade ─────────────────────────────────────────────────────


@pytest.mark.unit
class TestDependencyCascade:
    def test_fires_for_transitive_pending_dependents(self):
        failed = {
            "cluster": "mock-cluster", "jobid": "300", "job_id": "300",
            "name": "x-gate-prep", "state": "FAILED",
        }
        # Direct dependent
        d1 = {
            "cluster": "mock-cluster", "jobid": "301", "job_id": "301",
            "name": "x-gate-judge-generate", "state": "PENDING",
            "reason": "DependencyNeverSatisfied",
            "depends_on": ["300"],
        }
        # Transitive dependent (depends on d1)
        d2 = {
            "cluster": "mock-cluster", "jobid": "302", "job_id": "302",
            "name": "x-gate-classify", "state": "PENDING",
            "reason": "DependencyNeverSatisfied",
            "depends_on": ["301"],
        }
        detections = rules.detect_dependency_cascade(
            failed_job=failed, run_jobs=[failed, d1, d2],
        )
        target_ids = {det.target_jobs[0][1] for det in detections}
        assert target_ids == {"301", "302"}
        assert all(d.reason == rules.WASTE_REASON_DEPENDENCY_CASCADE for d in detections)
        assert all(d.confidence == rules.CONFIDENCE_HIGH for d in detections)

    def test_abstains_when_failed_job_is_actually_completed(self):
        completed = {
            "cluster": "mock-cluster", "jobid": "300", "job_id": "300",
            "state": "COMPLETED",
        }
        d1 = {
            "cluster": "mock-cluster", "jobid": "301", "job_id": "301",
            "state": "PENDING", "reason": "DependencyNeverSatisfied",
            "depends_on": ["300"],
        }
        assert rules.detect_dependency_cascade(
            failed_job=completed, run_jobs=[completed, d1],
        ) == []

    def test_skips_dependents_with_other_reasons(self):
        # Pending for resources, not because the parent died — leave alone.
        failed = {
            "cluster": "mock-cluster", "jobid": "300", "job_id": "300",
            "state": "FAILED",
        }
        d1 = {
            "cluster": "mock-cluster", "jobid": "301", "job_id": "301",
            "state": "PENDING",
            "reason": "Priority",  # not DependencyNeverSatisfied
            "depends_on": ["300"],
        }
        assert rules.detect_dependency_cascade(
            failed_job=failed, run_jobs=[failed, d1],
        ) == []


# ─── qos_self_deadlock ──────────────────────────────────────────────────────


@pytest.mark.unit
class TestQosSelfDeadlock:
    def test_fires_for_wait_path_sentinels_blocked_by_qos(self):
        wait_job = {
            "cluster": "mock-cluster", "jobid": "400", "job_id": "400",
            "name": "mpsf_mpsf2_x-r19-wait-path-sentinels",
            "state": "RUNNING",
        }
        blocker = {
            "cluster": "mock-cluster", "jobid": "401", "job_id": "401",
            "name": "mpsf_mpsf2_x-r19-merge-path-analytical",
            "state": "PENDING",
            "reason": "QOSMaxNodePerUserLimit",
        }
        det = rules.detect_qos_self_deadlock(
            running_job=wait_job, run_jobs=[wait_job, blocker],
        )
        assert det is not None
        assert det.reason == rules.WASTE_REASON_QOS_DEADLOCK
        assert det.confidence == rules.CONFIDENCE_MEDIUM
        assert det.target_jobs == [("mock-cluster", "400")]
        assert len(det.evidence["blockers"]) == 1

    def test_abstains_when_no_qos_blocked_producer(self):
        wait_job = {
            "cluster": "mock-cluster", "jobid": "400", "job_id": "400",
            "name": "mpsf_mpsf2_x-r19-wait-path-sentinels",
            "state": "RUNNING",
        }
        unrelated_pending = {
            "cluster": "mock-cluster", "jobid": "402", "job_id": "402",
            "name": "other_project_run-something-else",
            "state": "PENDING", "reason": "Priority",
        }
        assert rules.detect_qos_self_deadlock(
            running_job=wait_job, run_jobs=[wait_job, unrelated_pending],
        ) is None


# ─── idle_gpu_sustained ─────────────────────────────────────────────────────


@pytest.mark.unit
class TestIdleGpuSustained:
    def test_fires_when_idle_and_log_quiet(self):
        job = _gpu_server_job(started_min_ago=30)
        snaps = _snapshots_zero(10)
        det = rules.detect_idle_gpu_sustained(
            job=job, snapshots=snaps,
            min_runtime_min=5,
            suspicious_confirm_min=5,
            busy_threshold_pct=5.0,
            log_quiet_min=5,
            log_age_min=8.0,
        )
        assert det is not None
        assert det.reason == rules.WASTE_REASON_IDLE_GPU
        assert det.confidence == rules.CONFIDENCE_MEDIUM

    def test_abstains_when_log_still_growing(self):
        job = _gpu_server_job(started_min_ago=30)
        snaps = _snapshots_zero(10)
        det = rules.detect_idle_gpu_sustained(
            job=job, snapshots=snaps,
            min_runtime_min=5,
            suspicious_confirm_min=5,
            busy_threshold_pct=5.0,
            log_quiet_min=5,
            log_age_min=1.0,  # log changed 1 min ago — still alive
        )
        assert det is None

    def test_abstains_when_recent_busy_sample(self):
        job = _gpu_server_job(started_min_ago=30)
        snaps = _snapshots_zero(10)
        # Latest sample shows activity, even if older samples were idle.
        snaps[-1]["per_gpu"][0]["util"] = "60%"
        det = rules.detect_idle_gpu_sustained(
            job=job, snapshots=snaps,
            min_runtime_min=5,
            suspicious_confirm_min=5,
            busy_threshold_pct=5.0,
            log_quiet_min=5,
            log_age_min=None,
        )
        assert det is None


# ─── gpu_allocation_mismatch ────────────────────────────────────────────────


@pytest.mark.unit
class TestGpuAllocationMismatch:
    def test_fires_when_log_world_size_smaller_than_allocation(self):
        job = _gpu_server_job(started_min_ago=30)
        job["gres"] = "gres/gpu:8"
        snaps = _snapshots_busy(3)
        tail = (
            "vLLM engine initializing\n"
            "parallel_config={tensor_parallel_size=4, world_size=4}\n"
        )
        det = rules.detect_gpu_allocation_mismatch(
            job=job,
            snapshots=snaps,
            log_tail=tail,
            min_runtime_min=5,
            busy_threshold_pct=5.0,
        )
        assert det is not None
        assert det.reason == rules.WASTE_REASON_GPU_ALLOCATION_MISMATCH
        assert det.confidence == rules.CONFIDENCE_MEDIUM
        assert det.evidence["allocated_gpus"] == 8
        assert det.evidence["server_gpu_count"] == 4

    def test_abstains_when_world_size_matches_allocation(self):
        job = _gpu_server_job(started_min_ago=30)
        job["gres"] = "gres/gpu:8"
        snaps = _snapshots_busy(3)
        tail = "parallel_config={tensor_parallel_size=8, world_size=8}"
        det = rules.detect_gpu_allocation_mismatch(
            job=job,
            snapshots=snaps,
            log_tail=tail,
            min_runtime_min=5,
            busy_threshold_pct=5.0,
        )
        assert det is None

    def test_fires_when_same_gpu_subset_stays_idle(self):
        job = _gpu_server_job(started_min_ago=30)
        job["gres"] = "gres/gpu:8"
        snaps = []
        for _ in range(3):
            snaps.append({
                "gpu_util": 50.0,
                "per_gpu": [
                    {"index": str(i), "util": "60%" if i < 4 else "0%"}
                    for i in range(8)
                ],
            })
        det = rules.detect_gpu_allocation_mismatch(
            job=job,
            snapshots=snaps,
            log_tail="",
            min_runtime_min=5,
            busy_threshold_pct=5.0,
        )
        assert det is not None
        assert det.reason == rules.WASTE_REASON_GPU_ALLOCATION_MISMATCH
        assert det.evidence["common_idle_gpu_indices"] == ["4", "5", "6", "7"]

    def test_abstains_when_all_gpus_idle(self):
        job = _gpu_server_job(started_min_ago=30)
        job["gres"] = "gres/gpu:8"
        snaps = []
        for _ in range(3):
            snaps.append({
                "gpu_util": 0.0,
                "per_gpu": [{"index": str(i), "util": "0%"} for i in range(8)],
            })
        det = rules.detect_gpu_allocation_mismatch(
            job=job,
            snapshots=snaps,
            log_tail="",
            min_runtime_min=5,
            busy_threshold_pct=5.0,
        )
        assert det is None


# ─── manifest_only_failure ──────────────────────────────────────────────────


@pytest.mark.unit
class TestManifestOnlyFailure:
    def test_fires_when_evals_ok_but_manifest_failed(self):
        eval_job = {
            "cluster": "mock-cluster", "jobid": "500", "job_id": "500",
            "name": "x-summarize-results", "state": "COMPLETED",
            "exit_code": "0:0",
        }
        manifest = {
            "cluster": "mock-cluster", "jobid": "501", "job_id": "501",
            "name": "x-manifest", "state": "FAILED",
            "exit_code": "15:0",
        }
        det = rules.detect_manifest_only_failure(run_jobs=[eval_job, manifest])
        assert det is not None
        assert det.reason == rules.WASTE_REASON_MANIFEST_ONLY
        assert det.confidence == rules.CONFIDENCE_HIGH
        assert det.target_jobs == []   # informational; nothing to cancel

    def test_abstains_when_eval_also_failed(self):
        eval_job = {
            "cluster": "mock-cluster", "jobid": "500", "job_id": "500",
            "name": "x-summarize-results", "state": "FAILED",
        }
        manifest = {
            "cluster": "mock-cluster", "jobid": "501", "job_id": "501",
            "name": "x-manifest", "state": "FAILED",
        }
        assert rules.detect_manifest_only_failure(
            run_jobs=[eval_job, manifest],
        ) is None


# ─── helpers ────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestHelpers:
    def test_exempt_name_regex_match(self):
        assert rules.is_exempt_name("foo-judge-aggregate", r"(judge-aggregate|manifest)")
        assert rules.is_exempt_name("foo-manifest", r"(judge-aggregate|manifest)")
        assert not rules.is_exempt_name("foo-path", r"(judge-aggregate|manifest)")

    def test_exempt_name_regex_bad_pattern_is_safe(self):
        # A bad regex shouldn't crash callers — just decline to match.
        assert rules.is_exempt_name("anything", "[invalid(") is False

    def test_is_gpu_busy_threshold(self):
        gpus = [{"util": "0%"}, {"util": "10%"}]
        assert rules.is_gpu_busy(gpus, 5.0) is True
        assert rules.is_gpu_busy(gpus, 15.0) is False
        assert rules.is_gpu_busy([], 5.0) is False
