"""Tests for the WasteWatcher state-machine row CRUD + transitions."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from server import waste_watcher as ww
from server import waste_watcher_state as wws


def _seed_state(cluster="mock-cluster", job_id="100", state=wws.STATE_COLD, **kwargs):
    s = wws.WatcherState(cluster=cluster, job_id=job_id, state=state, **kwargs)
    wws.upsert_state(s)
    return wws.load_state(cluster, job_id)


@pytest.mark.unit
class TestStateCrud:
    def test_round_trip_state_row(self, db_path):
        s = wws.WatcherState(
            cluster="mock-cluster", job_id="100",
            state=wws.STATE_COLD,
            next_probe_due=datetime.now() + timedelta(seconds=60),
            consecutive_zero_util_samples=2,
            last_log_hash="abc",
            suspected_reason="port_mismatch_hang",
            suspected_confidence="high",
        )
        wws.upsert_state(s)
        round_tripped = wws.load_state("mock-cluster", "100")
        assert round_tripped is not None
        assert round_tripped.state == wws.STATE_COLD
        assert round_tripped.consecutive_zero_util_samples == 2
        assert round_tripped.last_log_hash == "abc"
        assert round_tripped.suspected_reason == "port_mismatch_hang"
        assert round_tripped.suspected_confidence == "high"

    def test_upsert_updates_existing_row(self, db_path):
        _seed_state(state=wws.STATE_COLD)
        s = wws.load_state("mock-cluster", "100")
        s.state = wws.STATE_WARM
        s.consecutive_zero_util_samples = 5
        wws.upsert_state(s)
        again = wws.load_state("mock-cluster", "100")
        assert again.state == wws.STATE_WARM
        assert again.consecutive_zero_util_samples == 5

    def test_delete_state_row(self, db_path):
        _seed_state()
        wws.delete_state("mock-cluster", "100")
        assert wws.load_state("mock-cluster", "100") is None

    def test_prune_terminal_states(self, db_path):
        _seed_state(job_id="100")
        _seed_state(job_id="101")
        # Only "100" is still live
        dropped = wws.prune_terminal_states([("mock-cluster", "100")])
        assert dropped == 1
        assert wws.load_state("mock-cluster", "100") is not None
        assert wws.load_state("mock-cluster", "101") is None

    def test_list_candidates_filters_by_state(self, db_path):
        _seed_state(job_id="100", state=wws.STATE_COLD)
        _seed_state(job_id="101", state=wws.STATE_SUSPICIOUS)
        _seed_state(job_id="102", state=wws.STATE_WASTEFUL)
        results = wws.list_candidates()
        ids = {r.job_id for r in results}
        assert ids == {"101", "102"}

    def test_set_exempt_marks_with_window(self, db_path):
        s = wws.set_exempt("mock-cluster", "100", duration_min=10, note="manual")
        assert s.state == wws.STATE_EXEMPT
        assert s.is_exempt_now() is True
        assert "manual" in s.last_notes


@pytest.mark.unit
class TestStateMachineTransitions:
    """Drive ``_transition_after_probe`` end-to-end with synthetic samples."""

    cfg = {
        "cold_probe_sec": 60,
        "warm_probe_sec": 900,
        "suspicious_probe_sec": 30,
        "cold_grace_min": 15,
        "warm_idle_min": 10,
        "suspicious_confirm_min": 5,
        "log_quiet_min": 5,
    }

    def test_cold_stays_cold_inside_grace_window(self):
        state = wws.WatcherState(
            cluster="x", job_id="1",
            state=wws.STATE_COLD,
            state_entered_at=datetime.now(),
        )
        out = ww._transition_after_probe(
            state, is_idle=True, log_age_min=None, cfg=self.cfg,
        )
        assert out.state == wws.STATE_COLD

    def test_cold_promotes_to_suspicious_after_grace(self):
        state = wws.WatcherState(
            cluster="x", job_id="1",
            state=wws.STATE_COLD,
            # Pretend the row has been cold for 20 minutes
            state_entered_at=datetime.now() - timedelta(minutes=20),
        )
        out = ww._transition_after_probe(
            state, is_idle=True, log_age_min=None, cfg=self.cfg,
        )
        assert out.state == wws.STATE_SUSPICIOUS

    def test_busy_sample_promotes_to_warm_and_resets_streak(self):
        state = wws.WatcherState(
            cluster="x", job_id="1",
            state=wws.STATE_SUSPICIOUS,
            consecutive_zero_util_samples=3,
            state_entered_at=datetime.now() - timedelta(minutes=10),
        )
        out = ww._transition_after_probe(
            state, is_idle=False, log_age_min=None, cfg=self.cfg,
        )
        assert out.state == wws.STATE_WARM
        assert out.consecutive_zero_util_samples == 0

    def test_suspicious_promotes_to_wasteful_when_quiet_long_enough(self):
        state = wws.WatcherState(
            cluster="x", job_id="1",
            state=wws.STATE_SUSPICIOUS,
            state_entered_at=datetime.now() - timedelta(minutes=6),
        )
        out = ww._transition_after_probe(
            state, is_idle=True, log_age_min=10.0, cfg=self.cfg,
        )
        assert out.state == wws.STATE_WASTEFUL

    def test_suspicious_holds_while_log_still_growing(self):
        # Even after suspicious_confirm_min minutes, a fresh log change
        # should hold the row in suspicious rather than promote.
        state = wws.WatcherState(
            cluster="x", job_id="1",
            state=wws.STATE_SUSPICIOUS,
            state_entered_at=datetime.now() - timedelta(minutes=6),
        )
        out = ww._transition_after_probe(
            state, is_idle=True, log_age_min=1.0, cfg=self.cfg,
        )
        assert out.state == wws.STATE_SUSPICIOUS


@pytest.mark.unit
class TestSettingsSnapshot:
    def test_settings_snapshot_uses_defaults(self, db_path):
        cfg = ww._settings_snapshot()
        # Default flags from APP_SETTINGS_DEFAULTS:
        assert cfg["enabled"] is True
        assert cfg["cancel_enabled"] is True
        assert isinstance(cfg["tick_sec"], int)
        assert cfg["tick_sec"] > 0
        assert cfg["util_busy_threshold"] >= 0
