"""Unit tests for server/schema.py.

The schema is the contract between every module and the SQLite file —
if a CREATE TABLE drifts from what the CRUD code expects, runtime errors
appear in random API endpoints. These tests verify the schema is
self-consistent and that ``init_db()`` produces every named table.
"""

import sqlite3

import pytest

from server import schema
from server.db import get_db, init_db


_EXPECTED_TABLES = {
    "job_history",
    "runs",
    "logbook_entries",
    "logbook_fts",
    "logbook_links",
    "job_stats_snapshots",
    "wds_history",
    "live_jobs",
    "cluster_state",
    "cache_store",
    "sdk_events",
    "projects",
    "clusters",
    "team_members",
    "ppp_accounts",
    "path_bases",
    "process_filters",
    "app_settings",
}


@pytest.mark.unit
class TestSchemaInstallation:
    def test_init_db_creates_every_expected_table(self, _isolate_db):
        init_db()
        con = get_db()
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'virtual') ORDER BY name"
        ).fetchall()
        names = {r["name"] for r in rows}
        missing = _EXPECTED_TABLES - names
        assert not missing, f"init_db() did not create: {sorted(missing)}"

    def test_init_db_is_idempotent(self, _isolate_db):
        init_db()
        # Second call must not raise — every CREATE/ALTER is guarded.
        init_db()
        init_db()

    def test_indexes_exist(self, _isolate_db):
        init_db()
        con = get_db()
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        names = {r["name"] for r in rows}
        # Spot-check a few critical indexes
        for expected in ("idx_jh_cluster_board", "idx_logbook_project",
                         "idx_clusters_position", "idx_path_bases_kind"):
            assert expected in names, f"missing index {expected}"

    def test_logbook_fts_triggers_installed(self, _isolate_db):
        init_db()
        con = get_db()
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        names = {r["name"] for r in rows}
        assert {"logbook_ai", "logbook_ad", "logbook_au"} <= names


@pytest.mark.unit
class TestAppSettingsRegistry:
    def test_every_default_has_three_tuple(self):
        for key, entry in schema.APP_SETTINGS_DEFAULTS.items():
            default, coercer, description = entry
            assert callable(coercer), f"{key} coercer must be callable"
            assert isinstance(description, str) and description, \
                f"{key} needs a non-empty description"
            # The coercer must accept its own default cleanly.
            coercer(default)

    def test_no_unknown_types_in_defaults(self):
        # Only str / int are used in the v4 defaults — surface drift loudly.
        allowed = {str, int}
        for key, (_, coercer, _) in schema.APP_SETTINGS_DEFAULTS.items():
            assert coercer in allowed, f"{key} uses unexpected coercer {coercer}"


@pytest.mark.unit
class TestSchemaSelfConsistency:
    def test_every_table_referenced_in_indexes_exists(self, _isolate_db):
        init_db()
        con = get_db()
        # SQLite tracks the table each index belongs to in sqlite_master.tbl_name.
        idx_rows = con.execute(
            "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        tbl_rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'virtual')"
        ).fetchall()
        tables = {r["name"] for r in tbl_rows}
        for r in idx_rows:
            assert r["tbl_name"] in tables, f"index points at unknown table {r['tbl_name']}"

    def test_migrations_target_real_columns_after_install(self, _isolate_db):
        init_db()
        con = get_db()
        # Every column listed in MIGRATIONS must be present after init_db()
        # — otherwise the ADD COLUMN was either misspelled or never landed
        # in the canonical CREATE TABLE.
        for table, column, _ in schema.MIGRATIONS:
            cols = {r["name"] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}
            assert column in cols, f"{table}.{column} listed in MIGRATIONS but missing from schema"
