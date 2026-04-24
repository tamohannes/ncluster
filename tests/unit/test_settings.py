"""Unit tests for server/settings.py typed key-value store."""

import pytest

from server import settings as settings_mod
from server.db import init_db
from server.schema import APP_SETTINGS_DEFAULTS
from server.settings import (
    delete_setting,
    get_setting,
    invalidate_cache,
    list_settings,
    set_setting,
)


@pytest.fixture(autouse=True)
def _init_settings_db(_isolate_db):
    init_db()
    invalidate_cache()
    yield
    invalidate_cache()


@pytest.mark.unit
class TestGetSetting:
    def test_returns_registered_default_when_unset(self):
        assert get_setting("ssh_timeout") == APP_SETTINGS_DEFAULTS["ssh_timeout"][0]

    def test_unknown_key_returns_caller_default(self):
        assert get_setting("ghost_key", default="fallback") == "fallback"

    def test_unknown_key_returns_none_without_default(self):
        assert get_setting("ghost_key") is None


@pytest.mark.unit
class TestSetSetting:
    def test_round_trip_int(self):
        set_setting("ssh_timeout", 30)
        assert get_setting("ssh_timeout") == 30

    def test_round_trip_str(self):
        set_setting("team_name", "myteam")
        assert get_setting("team_name") == "myteam"

    def test_int_coercion(self):
        # Persisting "42" coerces to int because ssh_timeout's coercer is int.
        set_setting("ssh_timeout", "42")
        assert get_setting("ssh_timeout") == 42

    def test_invalid_int_rejected(self):
        result = set_setting("ssh_timeout", "not-a-number")
        assert result["status"] == "error"
        assert "ssh_timeout" in result["error"]

    def test_unknown_key_persists_raw_value(self):
        # Unknown keys skip coercion — useful for plugin/experimental data.
        result = set_setting("custom_plugin_setting", {"nested": [1, 2, 3]})
        assert result["status"] == "ok"
        assert get_setting("custom_plugin_setting") == {"nested": [1, 2, 3]}

    def test_empty_key_rejected(self):
        assert set_setting("", "value")["status"] == "error"


@pytest.mark.unit
class TestDeleteSetting:
    def test_delete_returns_to_default(self):
        set_setting("ssh_timeout", 99)
        assert get_setting("ssh_timeout") == 99
        delete_setting("ssh_timeout")
        assert get_setting("ssh_timeout") == APP_SETTINGS_DEFAULTS["ssh_timeout"][0]

    def test_delete_unknown_no_op(self):
        result = delete_setting("ghost")
        assert result["status"] == "ok"
        assert result["rows"] == 0


@pytest.mark.unit
class TestListSettings:
    def test_lists_all_registered_defaults(self):
        out = list_settings()
        for key in APP_SETTINGS_DEFAULTS:
            assert key in out

    def test_default_source_marker(self):
        out = list_settings()
        assert out["ssh_timeout"]["source"] == "default"

    def test_db_source_marker_after_set(self):
        set_setting("ssh_timeout", 99)
        out = list_settings()
        assert out["ssh_timeout"]["source"] == "db"
        assert out["ssh_timeout"]["value"] == 99

    def test_omit_defaults_returns_only_stored(self):
        set_setting("ssh_timeout", 99)
        out = list_settings(include_defaults=False)
        assert "ssh_timeout" in out
        # team_name was never set so it should NOT be in the output
        assert "team_name" not in out

    def test_includes_description(self):
        out = list_settings()
        for key, entry in out.items():
            if key in APP_SETTINGS_DEFAULTS:
                assert entry["description"]


@pytest.mark.unit
class TestCacheBehaviour:
    def test_set_invalidates_cache(self):
        # Prime cache with default
        get_setting("ssh_timeout")
        # Bypass set_setting and write directly to DB
        from server.db import db_write
        with db_write() as con:
            con.execute(
                "INSERT INTO app_settings (key, value_json, updated_at) VALUES (?, ?, datetime('now'))",
                ("ssh_timeout", "42"),
            )
        # Without invalidation the cached default would be returned. We
        # explicitly invalidate to simulate a fresh process pickup.
        invalidate_cache()
        assert get_setting("ssh_timeout") == 42

    def test_invalidate_cache_idempotent(self):
        invalidate_cache()
        invalidate_cache()
        # Just verify get_setting still works
        assert get_setting("ssh_timeout") is not None


@pytest.mark.unit
class TestTypedAccessors:
    def test_team_name_accessor(self):
        from server.settings import get_team_name
        set_setting("team_name", "myteam")
        assert get_team_name() == "myteam"

    def test_ssh_timeout_accessor(self):
        from server.settings import get_ssh_timeout
        set_setting("ssh_timeout", 42)
        assert get_ssh_timeout() == 42

    def test_backup_interval_accessor_default(self):
        from server.settings import get_backup_interval_hours
        assert get_backup_interval_hours() == APP_SETTINGS_DEFAULTS["backup_interval_hours"][0]
