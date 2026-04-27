"""Unit tests for server/logs.py helper functions."""

import json
import os
import pytest

from server.logs import (
    extract_progress,
    get_job_log_files,
    label_log,
    label_and_sort_files,
    _extract_arg_value,
    read_jsonl_index,
    read_jsonl_record,
    extract_custom_metrics,
)
from server.db import upsert_job, set_custom_log_dir, set_custom_metrics_config


class TestExtractProgress:
    @pytest.mark.unit
    def test_typical_tqdm(self):
        content = "Epoch 1: 45%|████      | 450/1000"
        assert extract_progress(content) == 45

    @pytest.mark.unit
    def test_hundred_percent(self):
        assert extract_progress("100%|██████████|") == 100

    @pytest.mark.unit
    def test_zero_percent(self):
        assert extract_progress("0%|          |") == 0

    @pytest.mark.unit
    def test_over_100_rejected(self):
        assert extract_progress("101%|") is None

    @pytest.mark.unit
    def test_no_match(self):
        assert extract_progress("just some log output") is None

    @pytest.mark.unit
    def test_empty_and_none(self):
        assert extract_progress("") is None
        assert extract_progress(None) is None

    @pytest.mark.unit
    def test_last_match_wins(self):
        content = "10%|█         |\n50%|█████     |"
        assert extract_progress(content) == 50

    @pytest.mark.unit
    def test_truncated_content(self):
        content = "x" * 5000 + "75%|███████   |"
        assert extract_progress(content) == 75


class TestLabelLog:
    @pytest.mark.unit
    @pytest.mark.parametrize("name, expected", [
        ("main_eval-math_100_srun.log", "main output"),
        ("server_eval_200_srun.log", "server output"),
        ("sandbox_eval_300_srun.log", "sandbox output"),
        ("eval-math_100_sbatch.log", "sbatch log"),
        ("output.out", "stdout"),
        ("error.err", "stderr"),
        ("random_file.txt", "random_file.txt"),
    ])
    def test_label_categories(self, name, expected):
        assert label_log(name) == expected


class TestLabelAndSortFiles:
    @pytest.mark.unit
    def test_sort_order(self):
        paths = [
            "/logs/sbatch.log",
            "/logs/main_srun.log",
            "/logs/server_srun.log",
        ]
        result = label_and_sort_files(paths)
        labels = [f["label"] for f in result]
        assert labels.index("main output") < labels.index("server output")
        assert labels.index("server output") < labels.index("sbatch log")

    @pytest.mark.unit
    def test_unknown_label_sorted_last(self):
        paths = ["/logs/random.txt", "/logs/main_srun.log"]
        result = label_and_sort_files(paths)
        assert result[0]["label"] == "main output"


class TestExtractArgValue:
    @pytest.mark.unit
    def test_space_separated(self):
        tokens = ["python", "--output_dir", "/data/out"]
        assert _extract_arg_value(tokens, "--output_dir") == "/data/out"

    @pytest.mark.unit
    def test_equals_form(self):
        tokens = ["python", "--output_dir=/data/out"]
        assert _extract_arg_value(tokens, "--output_dir") == "/data/out"

    @pytest.mark.unit
    def test_not_found(self):
        assert _extract_arg_value(["python", "train.py"], "--output_dir") == ""

    @pytest.mark.unit
    def test_key_at_end_no_value(self):
        assert _extract_arg_value(["--output_dir"], "--output_dir") == ""


class TestReadJsonlIndex:
    @pytest.mark.unit
    def test_last_mode(self, tmp_path):
        p = tmp_path / "data.jsonl"
        lines = [json.dumps({"id": i}) for i in range(10)]
        p.write_text("\n".join(lines) + "\n")
        result = read_jsonl_index(str(p), limit=3, mode="last")
        assert result["status"] == "ok"
        assert result["count"] == 3
        assert result["total"] == 10

    @pytest.mark.unit
    def test_first_mode(self, tmp_path):
        p = tmp_path / "data.jsonl"
        lines = [json.dumps({"id": i}) for i in range(10)]
        p.write_text("\n".join(lines) + "\n")
        result = read_jsonl_index(str(p), limit=3, mode="first")
        assert result["count"] == 3

    @pytest.mark.unit
    def test_all_mode(self, tmp_path):
        p = tmp_path / "data.jsonl"
        lines = [json.dumps({"id": i}) for i in range(5)]
        p.write_text("\n".join(lines) + "\n")
        result = read_jsonl_index(str(p), mode="all")
        assert result["count"] == 5

    @pytest.mark.unit
    def test_invalid_json_lines(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"ok": true}\nnot json\n{"ok": true}\n')
        result = read_jsonl_index(str(p), mode="all")
        assert result["count"] == 3
        invalid = [r for r in result["records"] if not r["valid"]]
        assert len(invalid) == 1

    @pytest.mark.unit
    def test_empty_file(self, tmp_path):
        p = tmp_path / "empty.jsonl"
        p.write_text("")
        result = read_jsonl_index(str(p), mode="all")
        assert result["count"] == 0

    @pytest.mark.unit
    def test_count_only_mode(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a":1}\n{"b":2}\n')
        result = read_jsonl_index(str(p), limit=0, mode="first")
        assert result["count"] == 0
        assert result["total"] >= 0


class TestReadJsonlRecord:
    @pytest.mark.unit
    def test_valid_line(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"id": 0}\n{"id": 1}\n{"id": 2}\n')
        result = read_jsonl_record(str(p), 1)
        assert result["status"] == "ok"
        assert '"id": 1' in result["content"]

    @pytest.mark.unit
    def test_out_of_range(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"id": 0}\n')
        result = read_jsonl_record(str(p), 99)
        assert result["status"] == "error"

    @pytest.mark.unit
    def test_missing_file(self):
        result = read_jsonl_record("/nonexistent/path.jsonl", 0)
        assert result["status"] == "error"


class TestMountedLogDiscovery:
    @pytest.mark.unit
    def test_returns_mounted_output_dirs_without_ssh(self, db_path, tmp_path, monkeypatch, mock_cluster):
        run_dir = tmp_path / "run"
        log_dir = run_dir / "logs"
        eval_results = run_dir / "eval-results"
        log_dir.mkdir(parents=True)
        eval_results.mkdir()

        upsert_job(mock_cluster, {
            "jobid": "123",
            "name": "demo_eval",
            "state": "COMPLETED",
            "log_path": "/remote/run/logs/slurm-123.out",
        })

        mapping = {
            "/remote/run/logs": str(log_dir),
            "/remote/run": str(run_dir),
            "/remote/run/eval-results": str(eval_results),
        }
        monkeypatch.setattr("server.logs.resolve_mounted_path", lambda cluster, path, want_dir=False: mapping.get(path, ""))
        monkeypatch.setattr(
            "server.logs.ssh_run_with_timeout",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ssh should not be used")),
        )

        result = get_job_log_files(mock_cluster, "123")

        assert result["files"] == []
        assert {d["path"] for d in result["dirs"]} == {"/remote/run", "/remote/run/eval-results"}

    @pytest.mark.unit
    def test_discovers_eval_logs_files_from_mount_without_ssh(self, db_path, tmp_path, monkeypatch, mock_cluster):
        run_dir = tmp_path / "run"
        log_dir = run_dir / "logs"
        eval_logs = run_dir / "eval-logs"
        log_dir.mkdir(parents=True)
        eval_logs.mkdir()
        (eval_logs / "main_123_srun.log").write_text("mounted log")

        upsert_job(mock_cluster, {
            "jobid": "123",
            "name": "demo_eval",
            "state": "COMPLETED",
            "log_path": "/remote/run/logs/slurm-123.out",
        })

        mapping = {
            "/remote/run/logs": str(log_dir),
            "/remote/run": str(run_dir),
            "/remote/run/eval-logs": str(eval_logs),
        }
        monkeypatch.setattr("server.logs.resolve_mounted_path", lambda cluster, path, want_dir=False: mapping.get(path, ""))
        monkeypatch.setattr(
            "server.logs.ssh_run_with_timeout",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ssh should not be used")),
        )

        result = get_job_log_files(mock_cluster, "123")

        assert any(f["path"] == "/remote/run/eval-logs/main_123_srun.log" for f in result["files"])
        assert any(f["label"] == "main output" for f in result["files"])


class TestExtractCustomMetrics:
    @pytest.mark.unit
    def test_shell_script_keeps_regex_and_glob_out_of_shell(self, db_path, monkeypatch):
        upsert_job("mock-cluster", {"jobid": "123", "name": "eval-math", "state": "RUNNING"})
        set_custom_log_dir("mock-cluster", "123", "/tmp/metrics-logs")

        regex = r"loss=(\d+) \$\(whoami\)"
        file_glob = "$(touch /tmp/pwned)*{job_id}*"
        set_custom_metrics_config("mock-cluster", "123", json.dumps({
            "file_glob": file_glob,
            "extractors": [{"name": "loss", "regex": regex, "group": 1, "mode": "last"}],
        }))

        captured = {}

        def fake_ssh(cluster, command, timeout_sec=20):
            captured["cluster"] = cluster
            captured["command"] = command
            return ("===METRIC_0===\nloss=7 $(whoami)\n", "")

        monkeypatch.setattr("server.logs.ssh_run_with_timeout", fake_ssh)

        result = extract_custom_metrics("mock-cluster", "123")

        assert result["status"] == "ok"
        assert result["metrics"] == [{"name": "loss", "value": "7", "match_count": 1}]
        assert captured["cluster"] == "mock-cluster"
        assert regex not in captured["command"]
        assert file_glob.replace("{job_id}", "123") not in captured["command"]
        assert 'compgen -G "$job_glob"' in captured["command"]
        assert 'grep -oP -f "$REGEX_DIR/metric_0.re"' in captured["command"]
