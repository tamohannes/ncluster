"""Unit tests for server/bootstrap.py.

Bootstrap resolution is the foundation of every other v4 module — getting
this wrong silently corrupts everything downstream (DB lives in the wrong
place, SSH calls use the wrong user, etc.). The matrix below covers every
field across env / file / default precedence levels.
"""

import os

import pytest

from server.bootstrap import (
    DEFAULTS,
    PROJECT_ROOT,
    Bootstrap,
    _coerce_port,
    _expand_user,
    _resolve_data_dir,
    load_bootstrap,
)


@pytest.fixture(autouse=True)
def _scrub_env(monkeypatch):
    """Strip every CLAUSIUS_* env var so tests start from a clean slate."""
    for name in (
        "CLAUSIUS_DATA_DIR",
        "CLAUSIUS_PORT",
        "CLAUSIUS_SSH_USER",
        "CLAUSIUS_SSH_KEY",
        "CLAUSIUS_BOOTSTRAP_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.mark.unit
class TestDefaults:
    def test_no_file_no_env_uses_defaults(self, tmp_path, monkeypatch):
        missing = tmp_path / "does-not-exist.toml"
        b = load_bootstrap(path=str(missing))
        # data_dir default is ``./data`` resolved against PROJECT_ROOT
        assert b.data_dir == os.path.normpath(os.path.join(PROJECT_ROOT, "data"))
        assert b.port == 7272
        assert b.ssh_key.endswith("/.ssh/id_ed25519")
        assert b.ssh_user  # never empty
        assert b.source_file is None

    def test_db_path_lives_under_data_dir(self, tmp_path):
        b = load_bootstrap(path=str(tmp_path / "missing.toml"))
        assert b.db_path == os.path.join(b.data_dir, "history.db")
        assert b.backups_dir == os.path.join(b.data_dir, "backups")
        assert b.logbook_images_dir == os.path.join(b.data_dir, "logbook_images")
        assert b.log_path == os.path.join(b.data_dir, "clausius.log")


@pytest.mark.unit
class TestFileLoading:
    def test_full_toml_file(self, tmp_path):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text(
            '[bootstrap]\n'
            f'data_dir = "{tmp_path}/mydata"\n'
            'port = 9090\n'
            '\n'
            '[ssh]\n'
            'user = "alice"\n'
            'key = "/keys/alice_ed25519"\n'
        )
        b = load_bootstrap(path=str(cfg))
        assert b.data_dir == str(tmp_path / "mydata")
        assert b.port == 9090
        assert b.ssh_user == "alice"
        assert b.ssh_key == "/keys/alice_ed25519"
        assert b.source_file == str(cfg)

    def test_partial_toml_falls_back_to_defaults(self, tmp_path):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[bootstrap]\nport = 8123\n')
        b = load_bootstrap(path=str(cfg))
        assert b.port == 8123
        # data_dir falls back to default when file omits it
        assert b.data_dir == os.path.normpath(os.path.join(PROJECT_ROOT, "data"))

    def test_empty_toml_file_uses_defaults(self, tmp_path):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text("")
        b = load_bootstrap(path=str(cfg))
        assert b.port == DEFAULTS["port"]

    def test_bootstrap_file_env_var_picks_alternate(self, tmp_path, monkeypatch):
        cfg = tmp_path / "alt.toml"
        cfg.write_text('[bootstrap]\nport = 5555\n')
        monkeypatch.setenv("CLAUSIUS_BOOTSTRAP_FILE", str(cfg))
        b = load_bootstrap()
        assert b.port == 5555

    def test_malformed_toml_raises(self, tmp_path):
        import tomllib
        cfg = tmp_path / "broken.toml"
        cfg.write_text("[bootstrap\nport = 7272\n")
        with pytest.raises(tomllib.TOMLDecodeError):
            load_bootstrap(path=str(cfg))


@pytest.mark.unit
class TestEnvOverrides:
    """Env vars MUST beat the TOML file. No exceptions."""

    def test_env_data_dir_beats_file(self, tmp_path, monkeypatch):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text(f'[bootstrap]\ndata_dir = "{tmp_path}/file_dir"\n')
        monkeypatch.setenv("CLAUSIUS_DATA_DIR", str(tmp_path / "env_dir"))
        b = load_bootstrap(path=str(cfg))
        assert b.data_dir == str(tmp_path / "env_dir")

    def test_env_port_beats_file(self, tmp_path, monkeypatch):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[bootstrap]\nport = 1111\n')
        monkeypatch.setenv("CLAUSIUS_PORT", "2222")
        b = load_bootstrap(path=str(cfg))
        assert b.port == 2222

    def test_env_ssh_user_beats_file(self, tmp_path, monkeypatch):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[ssh]\nuser = "fileuser"\n')
        monkeypatch.setenv("CLAUSIUS_SSH_USER", "envuser")
        b = load_bootstrap(path=str(cfg))
        assert b.ssh_user == "envuser"

    def test_env_ssh_key_beats_file(self, tmp_path, monkeypatch):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[ssh]\nkey = "/from/file"\n')
        monkeypatch.setenv("CLAUSIUS_SSH_KEY", "/from/env")
        b = load_bootstrap(path=str(cfg))
        assert b.ssh_key == "/from/env"

    def test_empty_env_var_treated_as_unset(self, tmp_path, monkeypatch):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[bootstrap]\nport = 4444\n')
        monkeypatch.setenv("CLAUSIUS_PORT", "")
        b = load_bootstrap(path=str(cfg))
        assert b.port == 4444


@pytest.mark.unit
class TestUserExpansion:
    def test_dollar_user_substituted(self, tmp_path, monkeypatch):
        monkeypatch.setenv("USER", "myname")
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[ssh]\nuser = "$USER"\n')
        b = load_bootstrap(path=str(cfg))
        assert b.ssh_user == "myname"

    def test_tilde_in_key_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[ssh]\nkey = "~/.ssh/test_key"\n')
        b = load_bootstrap(path=str(cfg))
        assert b.ssh_key == str(tmp_path / ".ssh/test_key")

    def test_dollar_user_in_key_uses_resolved_user(self, tmp_path, monkeypatch):
        cfg = tmp_path / "clausius.toml"
        cfg.write_text(
            '[ssh]\n'
            'user = "alice"\n'
            'key = "/keys/$USER/id"\n'
        )
        b = load_bootstrap(path=str(cfg))
        assert b.ssh_key == "/keys/alice/id"

    def test_blank_user_falls_back_to_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("USER", "envuser")
        cfg = tmp_path / "clausius.toml"
        cfg.write_text('[ssh]\nuser = ""\n')
        b = load_bootstrap(path=str(cfg))
        assert b.ssh_user == "envuser"


@pytest.mark.unit
class TestPortValidation:
    def test_string_port_coerced(self):
        assert _coerce_port("8080") == 8080

    def test_int_port_passthrough(self):
        assert _coerce_port(7272) == 7272

    def test_zero_rejected(self):
        with pytest.raises(ValueError):
            _coerce_port(0)

    def test_negative_rejected(self):
        with pytest.raises(ValueError):
            _coerce_port(-1)

    def test_too_high_rejected(self):
        with pytest.raises(ValueError):
            _coerce_port(70000)

    def test_non_numeric_rejected(self):
        with pytest.raises(ValueError):
            _coerce_port("not a port")


@pytest.mark.unit
class TestDataDirResolution:
    def test_absolute_path_passthrough(self, tmp_path):
        absolute = str(tmp_path / "abs")
        assert _resolve_data_dir(absolute) == absolute

    def test_relative_anchored_at_project_root(self):
        result = _resolve_data_dir("./mydata")
        assert result == os.path.normpath(os.path.join(PROJECT_ROOT, "mydata"))

    def test_tilde_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        result = _resolve_data_dir("~/clausius_data")
        assert result == str(tmp_path / "clausius_data")


@pytest.mark.unit
class TestExpandUser:
    def test_substitutes_dollar_user(self):
        assert _expand_user("/home/$USER/key", fallback_user="bob") == "/home/bob/key"

    def test_returns_non_strings_unchanged(self):
        assert _expand_user(42, fallback_user="bob") == 42
        assert _expand_user(None, fallback_user="bob") is None
