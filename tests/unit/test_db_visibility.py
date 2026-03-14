"""Unit tests for server/db.py visibility and upsert logic."""

import pytest

from server.db import (
    init_db, upsert_job, dismiss_job, dismiss_all,
    dismiss_by_state_prefix, get_board_pinned, get_history,
    get_db, cleanup_local_on_startup,
)


@pytest.fixture()
def fresh_db(db_path):
    """Return db_path after init_db has run."""
    return db_path


class TestUpsertJob:
    @pytest.mark.unit
    def test_insert_new_job(self, fresh_db):
        job = {"jobid": "100", "name": "eval-math", "state": "RUNNING", "elapsed": "00:05:00"}
        upsert_job("test-cluster", job)
        rows = get_history("test-cluster")
        assert len(rows) == 1
        assert rows[0]["job_id"] == "100"
        assert rows[0]["state"] == "RUNNING"

    @pytest.mark.unit
    def test_terminal_sets_board_visible(self, fresh_db):
        job = {"jobid": "100", "name": "eval-math", "state": "FAILED"}
        upsert_job("test-cluster", job, terminal=True)
        con = get_db()
        row = con.execute("SELECT board_visible FROM job_history WHERE job_id='100'").fetchone()
        con.close()
        assert row["board_visible"] == 1

    @pytest.mark.unit
    def test_terminal_pins_even_after_regular_upsert(self, fresh_db):
        """Job polled as RUNNING (board_visible=0), then finalized → must be pinned."""
        job = {"jobid": "100", "name": "eval-math", "state": "RUNNING"}
        upsert_job("test-cluster", job, terminal=False)
        con = get_db()
        row = con.execute("SELECT board_visible FROM job_history WHERE job_id='100'").fetchone()
        assert row["board_visible"] == 0

        upsert_job("test-cluster", {**job, "state": "FAILED"}, terminal=True)
        row = con.execute("SELECT board_visible FROM job_history WHERE job_id='100'").fetchone()
        con.close()
        assert row["board_visible"] == 1

    @pytest.mark.unit
    def test_dismiss_stays_dismissed(self, fresh_db):
        """User dismiss via set_board_visible=0 is respected."""
        job = {"jobid": "100", "name": "eval-math", "state": "FAILED"}
        upsert_job("test-cluster", job, terminal=True)
        dismiss_job("test-cluster", "100")
        con = get_db()
        row = con.execute("SELECT board_visible FROM job_history WHERE job_id='100'").fetchone()
        con.close()
        assert row["board_visible"] == 0

    @pytest.mark.unit
    def test_set_board_visible_overrides(self, fresh_db):
        job = {"jobid": "100", "name": "eval-math", "state": "RUNNING"}
        upsert_job("test-cluster", job)
        upsert_job("test-cluster", job, set_board_visible=1)
        pinned = get_board_pinned("test-cluster")
        assert any(p["job_id"] == "100" for p in pinned)

    @pytest.mark.unit
    def test_dependency_column_sanitized(self, fresh_db):
        job = {"jobid": "100", "name": "j", "state": "PENDING", "dependency": "(null)"}
        upsert_job("test-cluster", job)
        rows = get_history("test-cluster")
        assert rows[0].get("dependency", "") in ("", None)

    @pytest.mark.unit
    def test_upsert_updates_state(self, fresh_db):
        job = {"jobid": "100", "name": "eval-math", "state": "RUNNING"}
        upsert_job("test-cluster", job)
        upsert_job("test-cluster", {**job, "state": "COMPLETED"})
        rows = get_history("test-cluster")
        assert rows[0]["state"] == "COMPLETED"


class TestDismiss:
    @pytest.mark.unit
    def test_dismiss_job(self, fresh_db):
        upsert_job("c", {"jobid": "1", "state": "FAILED"}, terminal=True)
        dismiss_job("c", "1")
        assert len(get_board_pinned("c")) == 0

    @pytest.mark.unit
    def test_dismiss_all(self, fresh_db):
        upsert_job("c", {"jobid": "1", "state": "FAILED"}, terminal=True)
        upsert_job("c", {"jobid": "2", "state": "CANCELLED"}, terminal=True)
        dismiss_all("c")
        assert len(get_board_pinned("c")) == 0

    @pytest.mark.unit
    def test_dismiss_by_state_prefix(self, fresh_db):
        upsert_job("c", {"jobid": "1", "state": "FAILED"}, terminal=True)
        upsert_job("c", {"jobid": "2", "state": "COMPLETED"}, terminal=True)
        dismiss_by_state_prefix("c", ["FAIL"])
        con = get_db()
        r1 = con.execute("SELECT board_visible FROM job_history WHERE job_id='1'").fetchone()
        r2 = con.execute("SELECT board_visible FROM job_history WHERE job_id='2'").fetchone()
        con.close()
        assert r1["board_visible"] == 0
        assert r2["board_visible"] == 1

    @pytest.mark.unit
    def test_dismiss_by_state_prefix_empty_list(self, fresh_db):
        upsert_job("c", {"jobid": "1", "state": "FAILED"}, terminal=True)
        dismiss_by_state_prefix("c", [])
        con = get_db()
        row = con.execute("SELECT board_visible FROM job_history WHERE job_id='1'").fetchone()
        con.close()
        assert row["board_visible"] == 1


class TestGetHistory:
    @pytest.mark.unit
    def test_cluster_filter(self, fresh_db):
        upsert_job("a", {"jobid": "1", "state": "COMPLETED"})
        upsert_job("b", {"jobid": "2", "state": "COMPLETED"})
        assert len(get_history("a")) == 1
        all_rows = get_history("all")
        assert len([r for r in all_rows if r["cluster"] in ("a", "b")]) == 2

    @pytest.mark.unit
    def test_limit(self, fresh_db):
        for i in range(10):
            upsert_job("c", {"jobid": str(i), "state": "COMPLETED"})
        assert len(get_history("c", limit=3)) == 3


class TestCleanupLocalOnStartup:
    @pytest.mark.unit
    def test_dismissed_remote_stays_dismissed(self, fresh_db):
        """User-dismissed remote jobs must not come back after restart."""
        from datetime import datetime
        job = {"jobid": "1", "state": "FAILED", "ended_at": datetime.now().isoformat()}
        upsert_job("test-cluster", job, terminal=True)
        dismiss_job("test-cluster", "1")
        cleanup_local_on_startup()
        pinned = get_board_pinned("test-cluster")
        assert not any(p["job_id"] == "1" for p in pinned)

    @pytest.mark.unit
    def test_pinned_remote_survives_restart(self, fresh_db):
        """Pinned remote jobs must persist across restarts untouched."""
        from datetime import datetime
        job = {"jobid": "2", "state": "FAILED", "ended_at": datetime.now().isoformat()}
        upsert_job("test-cluster", job, terminal=True)
        cleanup_local_on_startup()
        con = get_db()
        row = con.execute("SELECT board_visible FROM job_history WHERE job_id='2'").fetchone()
        con.close()
        assert row["board_visible"] == 1

    @pytest.mark.unit
    def test_dismisses_local_on_startup(self, fresh_db):
        """Local PIDs are ephemeral and dismissed on startup."""
        from datetime import datetime
        job = {"jobid": "1", "state": "FAILED", "ended_at": datetime.now().isoformat()}
        upsert_job("local", job, terminal=False)
        con = get_db()
        con.execute("UPDATE job_history SET board_visible=1 WHERE job_id='1'")
        con.commit()
        con.close()
        cleanup_local_on_startup()
        con = get_db()
        row = con.execute("SELECT board_visible FROM job_history WHERE cluster='local' AND job_id='1'").fetchone()
        con.close()
        assert row["board_visible"] == 0
