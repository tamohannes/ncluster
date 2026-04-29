"""Unit tests for the WDS scoring formula.

The bug being guarded against: ``_compute_wds`` used to gate the
resource term on ``idle_nodes / req_nodes``. On busy preemptable
clusters every node is in mixed/alloc state most of the time, so the
hard gate would drive WDS to 0 even when the user had hundreds of GPUs
of PPP headroom and the scheduler would actually start their job
instantly. The fix: drop the ``idle_nodes`` term from the resource gate
and let ``queue_score`` (which uses pending vs. idle ratio) handle
queue pressure on its own.
"""

import pytest

from server.wds import _compute_wds


def _wds(**overrides):
    """Compute WDS with sensible defaults; override only what each case probes."""
    args = dict(
        free_for_team=256,
        ppp_headroom=262,
        idle_nodes=0,
        pending_queue=0,
        my_level_fs=0.0,
        ppp_level_fs=10.0,
        team_num=256,
        occ_pct=87,
        req_nodes=1,
        req_gpn=8,
    )
    args.update(overrides)
    return _compute_wds(**args)


@pytest.mark.unit
class TestResourceGateNoLongerKilledByIdleNodes:
    """A busy cluster with PPP headroom should still score high."""

    def test_dfw_today_scenario_is_high(self):
        """Reproduces the live DFW snapshot that prompted the fix."""
        result = _wds()
        assert result["resource_gate"] == 1.0
        assert result["wds"] >= 75

    def test_idle_zero_pending_zero_with_headroom_scores_high(self):
        result = _wds(idle_nodes=0, pending_queue=0)
        assert result["wds"] >= 75

    def test_huge_headroom_caps_resource_gate_at_one(self):
        result = _wds(ppp_headroom=10_000, free_for_team=10_000, req_gpn=8)
        assert result["resource_gate"] == 1.0


@pytest.mark.unit
class TestResourceGateStillRespectsHeadroom:
    """Insufficient headroom must still degrade the resource gate."""

    def test_no_headroom_drives_resource_gate_to_zero(self):
        result = _wds(free_for_team=0, ppp_headroom=0, team_num=0)
        assert result["resource_gate"] == 0.0
        assert result["wds"] == 0

    def test_partial_headroom_scales_proportionally(self):
        result = _wds(free_for_team=2, ppp_headroom=2, team_num=0,
                      req_nodes=1, req_gpn=8)
        assert result["resource_gate"] == pytest.approx(0.25, abs=0.01)


@pytest.mark.unit
class TestQueueScoreStillReflectsIdleVsPending:
    """idle_nodes is no longer a hard gate but still feeds queue_score."""

    def test_no_queue_no_idle_keeps_queue_score_one(self):
        result = _wds(idle_nodes=0, pending_queue=0)
        assert result["queue_score"] == 1.0

    def test_heavy_queue_drops_queue_score_and_wds(self):
        no_queue = _wds(idle_nodes=0, pending_queue=0)
        heavy_queue = _wds(idle_nodes=0, pending_queue=200)
        assert heavy_queue["queue_score"] < 0.2
        # 25% of the priority blend is queue_score, so heavy queue should
        # drop WDS noticeably below the no-queue baseline.
        assert heavy_queue["wds"] < no_queue["wds"] * 0.85


@pytest.mark.unit
class TestTeamPenalty:
    """Going over the informal team allocation still applies the 0.7 penalty."""

    def test_team_over_quota_applies_penalty(self):
        no_penalty = _wds(team_num=8, free_for_team=8, team_running=7)
        over_quota = _wds(team_num=8, free_for_team=0, team_running=8)
        # 0.7x penalty must visibly reduce WDS for the over-quota case.
        assert over_quota["wds"] < no_penalty["wds"]
        assert over_quota["wds"] == pytest.approx(no_penalty["wds"] * 0.7, abs=2)

    def test_no_team_quota_means_no_penalty(self):
        result = _wds(team_num=None, free_for_team=0, ppp_headroom=100)
        assert result["resource_gate"] > 0
        assert result["wds"] > 0


@pytest.mark.unit
class TestLiveSchedulerSignals:
    """Live starts and pending-only queues should move WDS beyond static PPP math."""

    def test_running_jobs_rescue_negative_headroom(self):
        result = _wds(
            free_for_team=0,
            ppp_headroom=-937,
            pending_queue=4000,
            idle_nodes=80,
            my_level_fs=1.0,
            ppp_level_fs=0.75,
            my_running=120,
            team_running=120,
        )
        assert result["capacity_gate"] == 0.0
        assert result["live_gate"] >= 0.7
        assert result["resource_gate"] >= 0.7
        assert result["wds"] > 0

    def test_pending_only_jobs_apply_wait_drag(self):
        no_jobs = _wds(my_running=0, my_pending=0)
        stuck = _wds(my_running=0, my_pending=64, pending_queue=200, idle_nodes=0)
        assert stuck["my_wait_factor"] < 0.5
        assert stuck["wds"] < no_jobs["wds"] * 0.6
