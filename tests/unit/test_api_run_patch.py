"""Tests for PATCH /api/run/<id> (user-editable run fields)."""

from __future__ import annotations

import pytest

from server.db import db_write, upsert_run


@pytest.mark.unit
def test_patch_run_malfunctioned_round_trip(client, db_path):
    run_id = upsert_run("mock-cluster", "patch-mal-1", "hle_mpsf_x", "hle")
    res = client.patch(
        f"/api/run/{run_id}",
        json={"malfunctioned": True},
        content_type="application/json",
    )
    assert res.status_code == 200
    assert res.get_json().get("status") == "ok"

    from server.db import get_db

    con = get_db()
    row = con.execute(
        "SELECT malfunctioned FROM runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    con.close()
    assert int(row["malfunctioned"]) == 1

    res2 = client.patch(
        f"/api/run/{run_id}",
        json={"malfunctioned": False},
        content_type="application/json",
    )
    assert res2.status_code == 200
    con = get_db()
    row2 = con.execute(
        "SELECT malfunctioned FROM runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    con.close()
    assert int(row2["malfunctioned"]) == 0


@pytest.mark.unit
def test_patch_run_requires_at_least_one_field(client, db_path):
    run_id = upsert_run("mock-cluster", "patch-mal-2", "hle_mpsf_y", "hle")
    res = client.patch(
        f"/api/run/{run_id}",
        json={},
        content_type="application/json",
    )
    assert res.status_code == 400


@pytest.mark.unit
def test_run_info_surfaces_malfunctioned_bool(client, db_path):
    run_id = upsert_run("mock-cluster", "888777", "hle_mpsf_z", "hle")
    with db_write() as con:
        con.execute(
            "UPDATE runs SET malfunctioned = 1, run_uuid = ? WHERE id = ?",
            ("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", run_id),
        )
    from server.db import get_run_hash

    rh = get_run_hash("mock-cluster", "888777", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    info = client.get(f"/api/run_info_by_hash/mock-cluster/{rh}")
    assert info.status_code == 200
    body = info.get_json()
    assert body.get("status") == "ok"
    assert body["run"].get("malfunctioned") is True
