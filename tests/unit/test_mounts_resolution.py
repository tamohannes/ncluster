"""Unit tests for server/mounts.py path resolution and helpers."""

import os
import pytest

from server.mounts import (
    _local_candidates_for_remote_path,
    resolve_mounted_path,
    resolve_file_path,
    remote_path_from_mounted,
    run_mount_script,
    list_local_dir,
)


class TestLocalCandidatesForRemotePath:
    @pytest.mark.unit
    def test_basic_lustre_path(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/c1"]})
        candidates = _local_candidates_for_remote_path("c1", "/lustre/data/file.txt")
        paths = [os.path.normpath(c) for c in candidates]
        assert any("c1" in p and "data/file.txt" in p for p in paths)

    @pytest.mark.unit
    def test_no_roots(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {})
        assert _local_candidates_for_remote_path("c1", "/lustre/x") == []

    @pytest.mark.unit
    def test_empty_path(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/c1"]})
        assert _local_candidates_for_remote_path("c1", "") == []

    @pytest.mark.unit
    def test_relative_path_rejected(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/c1"]})
        assert _local_candidates_for_remote_path("c1", "relative/path") == []

    @pytest.mark.unit
    def test_multiple_roots(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/a", "/mnt/b"]})
        candidates = _local_candidates_for_remote_path("c1", "/lustre/data/f.txt")
        assert len(candidates) >= 2


class TestResolveMountedPath:
    @pytest.mark.unit
    def test_file_found(self, tmp_path, monkeypatch):
        f = tmp_path / "data" / "file.txt"
        f.parent.mkdir(parents=True)
        f.write_text("hello")
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": [str(tmp_path)]})
        result = resolve_mounted_path("c1", f"/data/file.txt", want_dir=False)
        assert result and os.path.isfile(result)

    @pytest.mark.unit
    def test_dir_found(self, tmp_path, monkeypatch):
        d = tmp_path / "data" / "subdir"
        d.mkdir(parents=True)
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": [str(tmp_path)]})
        result = resolve_mounted_path("c1", f"/data/subdir", want_dir=True)
        assert result and os.path.isdir(result)

    @pytest.mark.unit
    def test_no_match(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/nonexistent"]})
        assert resolve_mounted_path("c1", "/data/file.txt") == ""

    @pytest.mark.unit
    def test_empty_path(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt"]})
        assert resolve_mounted_path("c1", "") == ""

    @pytest.mark.unit
    def test_home_shortcut(self, tmp_path, monkeypatch):
        import tempfile, os
        home = os.path.expanduser("~")
        test_dir = os.path.join(home, ".jm-test-tmp")
        os.makedirs(test_dir, exist_ok=True)
        fpath = os.path.join(test_dir, "file.txt")
        try:
            with open(fpath, "w") as fh:
                fh.write("data")
            monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt"]})
            result = resolve_mounted_path("c1", fpath, want_dir=False)
            assert result == fpath
        finally:
            os.unlink(fpath)
            os.rmdir(test_dir)


class TestResolveFilePath:
    @pytest.mark.unit
    def test_local_cluster(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        path, source = resolve_file_path("local", str(f))
        assert path == str(f)
        assert source == "local"

    @pytest.mark.unit
    def test_local_missing(self):
        path, source = resolve_file_path("local", "/nonexistent/file.txt")
        assert path is None
        assert source == "local"

    @pytest.mark.unit
    def test_remote_ssh_fallback(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/nonexistent"]})
        path, source = resolve_file_path("c1", "/remote/path.txt")
        assert path is None
        assert source == "ssh"


class TestRemotePathFromMounted:
    @pytest.mark.unit
    def test_basic(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/c1"]})
        monkeypatch.setattr("server.mounts.MOUNT_REMOTE_MAP", {"c1": ["/remote/base"]})
        monkeypatch.setattr("server.mounts._proc_mount_points", lambda: {"/mnt/c1"})
        result = remote_path_from_mounted("c1", "/mnt/c1/data/file.txt")
        assert result == "/remote/base/data/file.txt"

    @pytest.mark.unit
    def test_root_returns_base(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/c1"]})
        monkeypatch.setattr("server.mounts.MOUNT_REMOTE_MAP", {"c1": ["/remote/base"]})
        monkeypatch.setattr("server.mounts._proc_mount_points", lambda: {"/mnt/c1"})
        result = remote_path_from_mounted("c1", "/mnt/c1")
        assert result == "/remote/base"

    @pytest.mark.unit
    def test_no_mount_returns_empty(self, monkeypatch):
        monkeypatch.setattr("server.mounts.MOUNT_MAP", {"c1": ["/mnt/c1"]})
        monkeypatch.setattr("server.mounts.MOUNT_REMOTE_MAP", {"c1": []})
        monkeypatch.setattr("server.mounts._proc_mount_points", lambda: set())
        assert remote_path_from_mounted("c1", "/any/path") == ""


class TestRunMountScript:
    @pytest.mark.unit
    def test_invalid_action(self):
        ok, msg = run_mount_script("invalid")
        assert not ok
        assert "Invalid" in msg

    @pytest.mark.unit
    def test_missing_script(self, monkeypatch, mock_cluster):
        monkeypatch.setattr("server.mounts.MOUNT_SCRIPT_PATH", "/nonexistent/script.sh")
        ok, msg = run_mount_script("mount", mock_cluster)
        assert not ok
        assert "not found" in msg

    @pytest.mark.unit
    def test_unknown_cluster(self, monkeypatch):
        monkeypatch.setattr("server.config.CLUSTERS", {"local": {}})
        ok, msg = run_mount_script("mount", "nonexistent")
        assert not ok


class TestListLocalDir:
    @pytest.mark.unit
    def test_basic_listing(self, tmp_path):
        (tmp_path / "file.txt").write_text("hello")
        (tmp_path / "subdir").mkdir()
        entries = list_local_dir(str(tmp_path))
        names = {e["name"] for e in entries}
        assert "file.txt" in names
        assert "subdir" in names

    @pytest.mark.unit
    def test_file_has_size(self, tmp_path):
        (tmp_path / "file.txt").write_text("12345")
        entries = list_local_dir(str(tmp_path))
        f = next(e for e in entries if e["name"] == "file.txt")
        assert f["size"] == 5
        assert not f["is_dir"]

    @pytest.mark.unit
    def test_dir_has_no_size(self, tmp_path):
        (tmp_path / "subdir").mkdir()
        entries = list_local_dir(str(tmp_path))
        d = next(e for e in entries if e["name"] == "subdir")
        assert d["is_dir"]
        assert d["size"] is None
