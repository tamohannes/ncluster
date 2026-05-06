import json

import pytest


@pytest.mark.integration
def test_run_results_by_hash_returns_metrics_json(client, monkeypatch, tmp_path):
    metrics_dir = tmp_path / "eval-results" / "gpqa"
    metrics_dir.mkdir(parents=True)
    metrics_path = metrics_dir / "metrics.json"
    metrics_path.write_text(json.dumps({"accuracy": 0.54, "num_entries": 500}), encoding="utf-8")

    def fake_load_run(cluster, run_hash, allow_on_demand=False):
        return {
            "root_job_id": "123",
            "run_uuid": "run-uuid",
            "run_name": "demo_gpqa_run",
            "primary_output_dir": str(tmp_path),
            "jobs": [{"output_dir": str(metrics_dir)}],
        }

    monkeypatch.setitem(__import__("server.routes").routes.CLUSTERS, "local", {"host": None})
    monkeypatch.setattr("server.routes._load_run_by_ref", fake_load_run)

    resp = client.get("/api/run_results_by_hash/local/abc123?benchmark=gpqa")

    assert resp.status_code == 200, resp.data
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["metrics_path"] == str(metrics_path)
    assert data["metrics_json_content"] == metrics_path.read_text(encoding="utf-8")
    assert data["metrics"]["accuracy"] == 0.54
    assert data["metrics_files"][0]["path"] == str(metrics_path)


@pytest.mark.integration
def test_run_results_by_hash_reports_incomplete_without_metrics_json(client, monkeypatch, tmp_path):
    (tmp_path / "eval-results" / "gpqa").mkdir(parents=True)

    def fake_load_run(cluster, run_hash, allow_on_demand=False):
        return {
            "root_job_id": "123",
            "run_uuid": "run-uuid",
            "run_name": "demo_gpqa_run",
            "primary_output_dir": str(tmp_path),
            "jobs": [],
        }

    monkeypatch.setitem(__import__("server.routes").routes.CLUSTERS, "local", {"host": None})
    monkeypatch.setattr("server.routes._load_run_by_ref", fake_load_run)

    resp = client.get("/api/run_results_by_hash/local/abc123?benchmark=gpqa")

    assert resp.status_code == 200, resp.data
    data = resp.get_json()
    assert data["status"] == "incomplete"
    assert data["complete"] is False
    assert data["metrics_path"] == ""
    assert data["metrics"] is None


@pytest.mark.integration
def test_run_results_by_hash_ignores_non_nemo_metrics_files(client, monkeypatch, tmp_path):
    other_dir = tmp_path / "tmp" / "nested"
    other_dir.mkdir(parents=True)
    (other_dir / "metrics.json").write_text('{"wrong": true}', encoding="utf-8")

    def fake_load_run(cluster, run_hash, allow_on_demand=False):
        return {
            "root_job_id": "123",
            "run_uuid": "run-uuid",
            "run_name": "demo_gpqa_run",
            "primary_output_dir": str(tmp_path),
            "jobs": [],
        }

    monkeypatch.setitem(__import__("server.routes").routes.CLUSTERS, "local", {"host": None})
    monkeypatch.setattr("server.routes._load_run_by_ref", fake_load_run)

    resp = client.get("/api/run_results_by_hash/local/abc123")

    assert resp.status_code == 200, resp.data
    assert resp.get_json()["status"] == "incomplete"
