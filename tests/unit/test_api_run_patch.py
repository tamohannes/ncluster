"""Tests for PATCH /api/run/<id> (user-editable run fields)."""

from __future__ import annotations

import pytest

from server.db import db_write, upsert_run


@pytest.mark.unit
def test_patch_run_tags_round_trip_and_keeps_malfunctioned_compat(client, db_path):
    run_id = upsert_run("mock-cluster", "patch-mal-1", "hle_mpsf_x", "hle")
    res = client.patch(
        f"/api/run/{run_id}",
        json={"tags": ["smoke", "malfunctioned"]},
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
    tags = {
        r["tag"]
        for r in con.execute("SELECT tag FROM run_tags WHERE run_id = ?", (run_id,)).fetchall()
    }
    con.close()
    assert int(row["malfunctioned"]) == 1
    assert tags == {"smoke", "malfunctioning"}

    res2 = client.patch(
        f"/api/run/{run_id}",
        json={"tags": ["smoke"]},
        content_type="application/json",
    )
    assert res2.status_code == 200
    con = get_db()
    row2 = con.execute(
        "SELECT malfunctioned FROM runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    tags2 = [
        r["tag"]
        for r in con.execute("SELECT tag FROM run_tags WHERE run_id = ? ORDER BY tag", (run_id,)).fetchall()
    ]
    con.close()
    assert int(row2["malfunctioned"]) == 0
    assert tags2 == ["smoke"]


@pytest.mark.unit
def test_run_tag_colors_are_shared_metadata(client, db_path):
    run_id = upsert_run("mock-cluster", "patch-tag-color-1", "hle_mpsf_tags", "hle")
    res = client.patch(
        f"/api/run/{run_id}",
        json={"tags": ["smoke"]},
        content_type="application/json",
    )
    assert res.status_code == 200

    listing = client.get("/api/run_tags")
    assert listing.status_code == 200
    tags = {row["tag"]: row for row in listing.get_json()["tags"]}
    assert tags["smoke"]["run_count"] == 1
    assert tags["smoke"]["color"].startswith("#")

    update = client.put("/api/run_tags/smoke", json={"color": "#123abc"})
    assert update.status_code == 200
    assert update.get_json()["tag"]["color"] == "#123abc"

    listing2 = client.get("/api/run_tags")
    tags2 = {row["tag"]: row for row in listing2.get_json()["tags"]}
    assert tags2["smoke"]["color"] == "#123abc"
    assert tags2["smoke"]["run_count"] == 1


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
    assert body["run"].get("tags") == ["malfunctioning"]
