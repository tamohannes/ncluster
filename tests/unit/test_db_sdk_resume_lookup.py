"""Resume-aware SDK run lookup used by ``ClausiusSession.start_from_cli``.

The SDK queries the server before minting a fresh uuid so a resubmission of
the same expname (``ns eval ++skip_filled=True``, cluster-name drift, etc.)
attaches to the canonical run row instead of spawning a duplicate. The
matching strategy is captured in ``find_sdk_run_uuid_by_output_dir`` —
strongest signal is ``primary_output_dir``, with ``run_name`` and
``cluster`` used only as tiebreaks.
"""

from __future__ import annotations


def _seed_run(uuid, cluster, run_name, output_dir, source="sdk"):
    from server.db import db_write
    with db_write() as con:
        con.execute(
            """INSERT INTO runs
                   (cluster, root_job_id, run_name, run_uuid, source, primary_output_dir,
                    sdk_status, started_at, created_at, meta_fetched)
               VALUES (?, ?, ?, ?, ?, ?, 'active', datetime('now'), datetime('now'), 1)""",
            (cluster, f"job-{uuid[:8]}", run_name, uuid, source, output_dir.rstrip("/")),
        )


def test_lookup_returns_none_when_output_dir_missing(_isolate_db):
    from server.db import find_sdk_run_uuid_by_output_dir
    assert find_sdk_run_uuid_by_output_dir("") is None
    assert find_sdk_run_uuid_by_output_dir(None) is None  # type: ignore[arg-type]


def test_lookup_returns_none_when_no_match(_isolate_db):
    from server.db import find_sdk_run_uuid_by_output_dir
    _seed_run("aaa111", "aws-cmh", "mcp_eval-r1", "/work/exp-a")
    assert find_sdk_run_uuid_by_output_dir("/work/other") is None


def test_lookup_matches_by_output_dir_only(_isolate_db):
    """Same output_dir, different cluster + run_name still resolves — this
    is the exact aws-cmh-science / aws-iad drift case we want to dedupe."""
    from server.db import find_sdk_run_uuid_by_output_dir
    _seed_run("aaa111", "aws-cmh-science", "mcpv2_eval-r1", "/work/exp-a")
    assert find_sdk_run_uuid_by_output_dir("/work/exp-a") == "aaa111"
    # cluster + run_name hints don't have to match to find the candidate.
    assert find_sdk_run_uuid_by_output_dir("/work/exp-a", "mcp_eval-r1", "aws-iad") == "aaa111"


def test_lookup_prefers_exact_cluster_and_run_name(_isolate_db):
    """When multiple SDK rows share an output_dir, the exact (cluster, run_name)
    match wins over the looser dir-only match."""
    from server.db import find_sdk_run_uuid_by_output_dir
    _seed_run("aaa111", "aws-cmh-science", "mcpv2_eval-r1", "/work/exp-a")
    _seed_run("bbb222", "aws-iad",         "mcp_eval-r1",  "/work/exp-a")
    got = find_sdk_run_uuid_by_output_dir("/work/exp-a", "mcp_eval-r1", "aws-iad")
    assert got == "bbb222"


def test_lookup_prefers_matching_run_name_when_cluster_unspecified(_isolate_db):
    from server.db import find_sdk_run_uuid_by_output_dir
    _seed_run("aaa111", "aws-cmh-science", "mcpv2_eval-r1", "/work/exp-a")
    _seed_run("bbb222", "aws-iad",         "mcp_eval-r1",  "/work/exp-a")
    got = find_sdk_run_uuid_by_output_dir("/work/exp-a", run_name="mcp_eval-r1")
    assert got == "bbb222"


def test_lookup_skips_legacy_rows(_isolate_db):
    """Only SDK-sourced runs are eligible — legacy poller rows must not match."""
    from server.db import find_sdk_run_uuid_by_output_dir
    _seed_run("legacy1", "aws-cmh", "mcp_eval-r1", "/work/exp-a", source="legacy")
    assert find_sdk_run_uuid_by_output_dir("/work/exp-a") is None


def test_lookup_normalizes_trailing_slashes(_isolate_db):
    from server.db import find_sdk_run_uuid_by_output_dir
    _seed_run("aaa111", "aws-cmh", "mcp_eval-r1", "/work/exp-a/")
    assert find_sdk_run_uuid_by_output_dir("/work/exp-a") == "aaa111"
    assert find_sdk_run_uuid_by_output_dir("/work/exp-a/") == "aaa111"


def test_lookup_resolves_alias_to_canonical(_isolate_db):
    """Resume aliases get redirected to their canonical uuid."""
    from server.db import db_write, find_sdk_run_uuid_by_output_dir
    _seed_run("canon01", "aws-cmh", "mcp_eval-r1", "/work/exp-a")
    with db_write() as con:
        con.execute(
            """INSERT INTO sdk_run_aliases (alias_uuid, canonical_uuid, reason)
               VALUES (?, ?, ?)""",
            ("aliasA", "canon01", "test"),
        )
    # Even if our seed row stored "canon01", a lookup that happens to find an
    # alias mapped to it should still return the canonical (not the alias).
    assert find_sdk_run_uuid_by_output_dir("/work/exp-a") == "canon01"


def test_resolve_run_endpoint_returns_existing_uuid(client, _isolate_db):
    _seed_run("aaa111", "aws-cmh-science", "mcpv2_eval-r1", "/work/exp-a")
    resp = client.get(
        "/api/sdk/resolve_run",
        query_string={
            "output_dir": "/work/exp-a",
            "run_name": "mcp_eval-r1",
            "cluster": "aws-iad",
        },
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body == {"status": "ok", "exists": True, "run_uuid": "aaa111"}


def test_resolve_run_endpoint_missing_output_dir(client, _isolate_db):
    resp = client.get("/api/sdk/resolve_run")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "exists": False}


def test_resolve_run_endpoint_no_match(client, _isolate_db):
    resp = client.get(
        "/api/sdk/resolve_run",
        query_string={"output_dir": "/work/never-seen"},
    )
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "exists": False}
