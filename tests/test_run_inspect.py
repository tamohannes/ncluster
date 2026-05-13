"""Unit tests for server.run_inspect helpers."""

import pytest

from server.run_inspect import (
    build_reproducibility_snapshot,
    filter_env_vars,
    filter_library_lines,
    filter_metrics_payload,
    parse_env_vars,
    query_metadata,
    truncate_text,
)


def test_parse_env_json():
    d = parse_env_vars('{"A": "1", "B": "two"}')
    assert d == {"A": "1", "B": "two"}


def test_parse_env_lines():
    d = parse_env_vars("FOO=bar\nEMPTY=\nNOEQ")
    assert d["FOO"] == "bar"
    assert d["EMPTY"] == ""
    assert d["NOEQ"] == ""


def test_filter_env_keys_and_search():
    env = {"PATH": "/usr/bin", "CUDA_HOME": "/cuda", "FOO": "x"}
    assert filter_env_vars(env, keys=["PATH"]) == {"PATH": "/usr/bin"}
    r = filter_env_vars(env, search="cuda")
    assert "CUDA_HOME" in r
    assert "PATH" not in r


def test_query_metadata_prefix():
    meta = {"a": {"b": 1}, "c": "hello"}
    q = query_metadata(meta, key_prefix="a", query=None)
    assert q == {"a.b": 1}


def test_query_run_metrics_series_last():
    payload = {
        "metadata": {},
        "series": {
            "loss": [
                {"step": 0, "ts": 1, "value": 2.0, "value_num": 2.0, "context": {}},
                {"step": 1, "ts": 2, "value": 1.0, "value_num": 1.0, "context": {}},
            ],
        },
        "latest": {},
        "scalars": {},
        "scalar_latest": {},
    }
    out = filter_metrics_payload(
        payload,
        metric_substring="loss",
        kinds=["series"],
        series_mode="last",
        max_points_per_series=100,
    )
    assert out["series"]["loss"][0]["step"] == 1


def test_truncate_text_head_tail():
    t = "\n".join(f"line{i}" for i in range(20))
    r = truncate_text(t, head_lines=3, tail_lines=2, max_chars=10000)
    assert "line0" in r["content"]
    assert "line19" in r["content"]
    assert "omitted" in r["content"]


@pytest.mark.parametrize(
    "search,expect_sub",
    [
        ("torch", "torch=="),
        ("torch cuda", "cuda118"),
    ],
)
def test_filter_library_lines(search, expect_sub):
    text = "numpy==1\n#c\ntorch==2.1+cuda118\ncuda-toolkit==12\nfoo==1\n"
    r = filter_library_lines(text, search=search)
    assert any(expect_sub in ln for ln in r["lines"])


def test_build_reproducibility_snapshot_lists_metrics():
    run = {
        "run_hash": "abc",
        "root_job_id": "1",
        "params": {"model": "m"},
        "submit_command": "ns eval",
        "git_commit": "dead",
        "env_vars": "A=1\nB=2",
        "conda_state": "torch==1\nnumpy==2",
        "metadata": {"x": 1},
        "malfunctioned": 0,
        "jobs": [{"job_id": "1"}],
    }
    metrics = {
        "series": {"loss": [{"step": 0, "ts": 1, "value": 1.0, "value_num": 1.0, "context": {}}]},
        "scalars": {},
        "scalar_latest": {"acc": {"ts": 1, "value": 0.9, "value_num": 0.9, "context": {}}},
        "metadata": {"y": 2},
    }
    snap = build_reproducibility_snapshot(run, metrics, include_full_env=True)
    assert snap["status"] == "ok"
    assert "loss" in snap["sdk_metrics"]["series_metric_names"]
    assert "acc" in snap["sdk_metrics"]["scalar_metric_names"]
    assert snap["environment"]["count"] == 2
