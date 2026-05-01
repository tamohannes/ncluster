import json
import uuid

import pytest


@pytest.mark.integration
def test_metrics_views_crud(client, db_path, mock_cluster):
    create = client.post(
        "/api/metrics_views",
        data=json.dumps({"title": "Demo metrics view", "state": {"runs": ["mock/abc"]}}),
        content_type="application/json",
    )
    assert create.status_code == 200, create.data
    view = create.get_json()["view"]
    assert view["title"] == "Demo metrics view"
    assert view["state"]["runs"] == ["mock/abc"]

    listing = client.get("/api/metrics_views")
    assert listing.status_code == 200
    assert any(v["id"] == view["id"] for v in listing.get_json()["views"])

    patch = client.patch(
        f"/api/metrics_views/{view['id']}",
        data=json.dumps({"title": "Renamed", "pinned": True}),
        content_type="application/json",
    )
    assert patch.status_code == 200, patch.data
    assert patch.get_json()["view"]["title"] == "Renamed"
    assert patch.get_json()["view"]["pinned"] == 1

    delete = client.delete(f"/api/metrics_views/{view['id']}")
    assert delete.status_code == 200


@pytest.mark.integration
def test_resolve_run_hash_accepts_unique_prefix(client, db_path):
    run_uuid = uuid.uuid4().hex
    events = [{
        "run_uuid": run_uuid,
        "event_type": "run_started",
        "event_seq": 1,
        "ts": 0.0,
        "payload": {
            "command": "python train.py",
            "cwd": "/tmp",
            "expname": "metrics_prefix_demo",
            "cluster": "mock-cluster",
            "output_dir": "/tmp/metrics-prefix-demo",
            "git_commit": "abc",
            "hostname": "host",
            "env_subset": {},
            "config_overrides": {},
            "conda_env": "test",
            "python_executable": "/usr/bin/python",
            "env_vars_set": [],
            "params": {},
        },
    }]
    res = client.post("/api/sdk/events", data=json.dumps(events), content_type="application/json")
    assert res.status_code == 200, res.data

    resp = client.get(f"/api/resolve_run_hash/{run_uuid[:8]}")
    assert resp.status_code == 200, resp.data
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["run_hash"] == run_uuid[:12]
    assert data["run_name"] == "metrics_prefix_demo"
