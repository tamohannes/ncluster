"""Pure helpers for MCP / API run inspection: env parsing, text search, metadata and metrics filtering."""

from __future__ import annotations

import json
import re
from typing import Any, Optional


def parse_env_vars(env_str: str) -> dict[str, str]:
    """Parse run ``env_vars`` text into a flat string dict.

    Accepts JSON object or ``KEY=value`` lines (first ``=`` splits key/value).
    """
    text = str(env_str or "").strip()
    if not text:
        return {}
    if text.startswith("{"):
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return {str(k): str(v) for k, v in obj.items()}
        except (ValueError, TypeError):
            pass
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        eq = line.find("=")
        if eq < 0:
            out[line] = ""
        else:
            out[line[:eq]] = line[eq + 1 :]
    return out


def filter_env_vars(
    env: dict[str, str],
    *,
    search: Optional[str] = None,
    keys: Optional[list[str]] = None,
) -> dict[str, str]:
    """Filter env dict by exact key list and/or substring match on key or value (case-insensitive)."""
    items = dict(env)
    if keys:
        want = {str(k) for k in keys}
        items = {k: v for k, v in items.items() if k in want}
    q = (search or "").strip().lower()
    if not q:
        return items
    return {
        k: v
        for k, v in items.items()
        if q in k.lower() or q in str(v).lower()
    }


def filter_text_lines(
    text: str,
    *,
    search: Optional[str] = None,
    max_lines: int = 2000,
) -> dict[str, Any]:
    """Return lines (optionally filtered by substring) with truncation metadata."""
    raw = str(text or "").splitlines()
    lines = raw
    q = (search or "").strip().lower()
    if q:
        lines = [ln for ln in raw if q in ln.lower()]
    n_all = len(lines)
    truncated = n_all > max_lines
    lines = lines[:max_lines]
    return {
        "line_count": len(lines),
        "matched_total": n_all,
        "truncated": truncated,
        "lines": lines,
    }


def truncate_text(
    text: str,
    *,
    max_chars: int = 256_000,
    head_lines: Optional[int] = None,
    tail_lines: Optional[int] = None,
) -> dict[str, Any]:
    """Truncate long text for MCP payloads; prefer head+tail when both set."""
    s = str(text or "")
    n = len(s)
    if head_lines is not None and tail_lines is not None:
        lines = s.splitlines()
        hi = max(0, head_lines)
        ti = max(0, tail_lines)
        if hi + ti >= len(lines):
            body = s
            omitted_lines = 0
        else:
            head = "\n".join(lines[:hi])
            tail = "\n".join(lines[-ti:]) if ti else ""
            omitted_lines = len(lines) - hi - ti
            body = head + f"\n\n… ({omitted_lines} lines omitted) …\n\n" + tail
        return {
            "truncated": omitted_lines > 0 or len(body) > max_chars,
            "chars": min(len(body), max_chars),
            "content": body[:max_chars],
        }
    if n <= max_chars:
        return {"truncated": False, "chars": n, "content": s}
    return {
        "truncated": True,
        "chars": max_chars,
        "content": s[:max_chars],
        "note": f"truncated to first {max_chars} characters; use head_lines/tail_lines for middle omission",
    }


_WS_SPLIT = re.compile(r"\s+")


def search_terms_match_line(line: str, terms: list[str]) -> bool:
    low = line.lower()
    return all(t in low for t in terms if t)


def filter_library_lines(
    conda_state: str,
    *,
    search: Optional[str] = None,
    max_lines: int = 2000,
) -> dict[str, Any]:
    """Filter conda/pip freeze style text; ``search`` splits on whitespace — all terms must appear."""
    raw = str(conda_state or "").splitlines()
    q = (search or "").strip()
    if not q:
        lines = raw
    else:
        terms = [t.lower() for t in _WS_SPLIT.split(q) if t]
        lines = [ln for ln in raw if search_terms_match_line(ln, terms)]
    n_all = len(lines)
    truncated = n_all > max_lines
    lines = lines[:max_lines]
    return {
        "line_count": len(lines),
        "matched_total": n_all,
        "truncated": truncated,
        "lines": lines,
    }


def flatten_metadata(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Dot-join nested dict keys for grep-style filtering."""
    flat: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict) and v:
                flat.update(flatten_metadata(v, path))
            else:
                flat[path] = v
    return flat


def query_metadata(
    metadata: dict[str, Any],
    *,
    key_prefix: Optional[str] = None,
    query: Optional[str] = None,
) -> dict[str, Any]:
    """Return metadata entries whose dotted key starts with ``key_prefix`` and/or match ``query`` in key or value."""
    flat = flatten_metadata(metadata or {})
    pref = (key_prefix or "").strip().lower()
    q = (query or "").strip().lower()
    out: dict[str, Any] = {}
    for path, val in flat.items():
        pl = path.lower()
        if pref and not pl.startswith(pref):
            continue
        if q:
            try:
                blob = json.dumps(val, default=str, sort_keys=True).lower()
            except Exception:
                blob = str(val).lower()
            if q not in pl and q not in blob:
                continue
        out[path] = val
    return out


def _metric_key_match(name: str, substring: str) -> bool:
    if not substring:
        return True
    return substring.lower() in name.lower()


def _filter_points(
    points: list[dict[str, Any]],
    *,
    step_min: Optional[int] = None,
    step_max: Optional[int] = None,
    max_points: int,
    series_mode: str,
) -> list[dict[str, Any]]:
    if not points:
        return []
    filtered = []
    for p in points:
        st = p.get("step")
        if step_min is not None or step_max is not None:
            try:
                sn = int(st)
            except (TypeError, ValueError):
                continue
            if step_min is not None and sn < step_min:
                continue
            if step_max is not None and sn > step_max:
                continue
        filtered.append(p)
    if series_mode == "last" and filtered:
        return [filtered[-1]]
    if max_points and len(filtered) > max_points:
        # Keep the tail so recent training steps are visible.
        return filtered[-max_points:]
    return filtered


def filter_metrics_payload(
    payload: dict[str, Any],
    *,
    metric_substring: str = "",
    kinds: Optional[list[str]] = None,
    series_mode: str = "full",
    step_min: Optional[int] = None,
    step_max: Optional[int] = None,
    max_points_per_series: int = 500,
) -> dict[str, Any]:
    """Filter ``get_run_metrics`` JSON: optional key substring, series tail/last, scalar latest."""
    want = {k.strip().lower() for k in (kinds or []) if k and str(k).strip()}
    if not want:
        want = {"series", "scalars", "metadata"}

    meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    series = payload.get("series") if isinstance(payload.get("series"), dict) else {}
    latest = payload.get("latest") if isinstance(payload.get("latest"), dict) else {}
    scalars = payload.get("scalars") if isinstance(payload.get("scalars"), dict) else {}
    scalar_latest = payload.get("scalar_latest") if isinstance(payload.get("scalar_latest"), dict) else {}

    out: dict[str, Any] = {"status": "ok"}
    if "metadata" in want:
        out["metadata"] = query_metadata(meta, query=metric_substring) if metric_substring else meta

    sm = (series_mode or "full").strip().lower()
    if sm not in ("full", "last", "summary"):
        sm = "full"

    if "series" in want:
        filt_series: dict[str, Any] = {}
        filt_latest: dict[str, Any] = {}
        for name, points in series.items():
            if not isinstance(points, list) or not _metric_key_match(name, metric_substring):
                continue
            if sm == "summary":
                pts = _filter_points(
                    points,
                    step_min=step_min,
                    step_max=step_max,
                    max_points=10**9,
                    series_mode="full",
                )
                if not pts:
                    continue
                steps = [p.get("step") for p in pts if p.get("step") is not None]
                nums = [p.get("value_num") for p in pts if p.get("value_num") is not None]
                filt_series[name] = {
                    "count": len(pts),
                    "step_min": min(steps) if steps else None,
                    "step_max": max(steps) if steps else None,
                    "value_num_min": min(nums) if nums else None,
                    "value_num_max": max(nums) if nums else None,
                    "last": pts[-1],
                }
            else:
                pts2 = _filter_points(
                    points,
                    step_min=step_min,
                    step_max=step_max,
                    max_points=max_points_per_series,
                    series_mode=sm,
                )
                if pts2:
                    filt_series[name] = pts2
                    filt_latest[name] = pts2[-1]
        out["series"] = filt_series
        out["latest"] = filt_latest

    if "scalars" in want:
        fsc: dict[str, Any] = {}
        fsl: dict[str, Any] = {}
        for name, points in scalars.items():
            if not isinstance(points, list) or not _metric_key_match(name, metric_substring):
                continue
            if sm == "last" or sm == "summary":
                if points:
                    fsc[name] = [points[-1]]
                    fsl[name] = points[-1]
            else:
                tail = points[-max_points_per_series:] if max_points_per_series else points
                fsc[name] = tail
                if tail:
                    fsl[name] = tail[-1]
        out["scalars"] = fsc
        out["scalar_latest"] = fsl

    out["truncation"] = {
        "max_points_per_series": max_points_per_series,
        "series_mode": sm,
        "metric_substring": metric_substring or None,
    }
    return out


def build_reproducibility_snapshot(
    run: dict[str, Any],
    metrics_payload: dict[str, Any],
    *,
    include_full_env: bool = False,
    include_full_conda: bool = False,
    batch_script_max_chars: int = 48_000,
    scontrol_max_chars: int = 24_000,
    conda_preview_lines: int = 150,
) -> dict[str, Any]:
    """Assemble a single JSON object for MCP ``audit_run_reproducibility``.

    Bundles identity, launch parameters, truncated Slurm artefacts, env/lib
    fingerprints, metadata key inventory, and SDK metric names with latest scalars
    plus per-series summaries — useful for inspection, debugging, or reproducibility
    checks without multiple round trips.
    """
    env_parsed = parse_env_vars(run.get("env_vars") or "")
    conda_raw = str(run.get("conda_state") or "")

    series = metrics_payload.get("series") if isinstance(metrics_payload.get("series"), dict) else {}
    scalars = metrics_payload.get("scalars") if isinstance(metrics_payload.get("scalars"), dict) else {}
    scalar_latest = (
        metrics_payload.get("scalar_latest")
        if isinstance(metrics_payload.get("scalar_latest"), dict)
        else {}
    )
    series_keys = sorted(series.keys())
    scalar_keys = sorted(set(scalar_latest.keys()) | set(scalars.keys()))

    series_summary = filter_metrics_payload(
        metrics_payload,
        metric_substring="",
        kinds=["series"],
        series_mode="summary",
        max_points_per_series=10**9,
    ).get("series", {})

    run_meta = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    m_meta = (
        metrics_payload.get("metadata")
        if isinstance(metrics_payload.get("metadata"), dict)
        else {}
    )

    mal = run.get("malfunctioned")
    if isinstance(mal, (int, float)):
        malfunctioned = bool(int(mal))
    else:
        malfunctioned = bool(mal)

    return {
        "status": "ok",
        "purpose": (
            "Aggregated run snapshot: provenance, launch parameters, Slurm artefacts, "
            "environment and library fingerprints, flattened metadata keys, and an inventory "
            "of SDK-logged metrics (names + latest scalars + per-series summaries). "
            "Useful as a one-shot overview; use query_run_metrics / get_run_env_vars / "
            "get_run_batch_script when you need full curves or full env values."
        ),
        "cluster_context": {
            "root_job_id": run.get("root_job_id"),
            "run_hash": run.get("run_hash") or "",
            "run_name": run.get("run_name") or run.get("name") or "",
            "project": run.get("project") or "",
            "campaign": run.get("campaign") or "",
            "source": run.get("source") or "",
            "malfunctioned": malfunctioned,
            "job_count": len(run.get("jobs") or []),
        },
        "identity": {
            "git_commit": run.get("git_commit") or "",
            "launcher_hostname": run.get("launcher_hostname") or "",
            "submit_cwd": run.get("submit_cwd") or "",
            "primary_output_dir": run.get("primary_output_dir") or "",
        },
        "execution": {
            "submit_command": run.get("submit_command") or "",
            "params": run.get("params") if isinstance(run.get("params"), dict) else {},
        },
        "slurm_and_files": {
            "batch_script": truncate_text(
                str(run.get("batch_script") or ""),
                max_chars=batch_script_max_chars,
            ),
            "scontrol": truncate_text(
                str(run.get("scontrol_raw") or ""),
                max_chars=scontrol_max_chars,
            ),
        },
        "environment": (
            {"env": env_parsed, "count": len(env_parsed)}
            if include_full_env
            else {
                "env_keys_sorted": sorted(env_parsed.keys()),
                "count": len(env_parsed),
                "note": (
                    "Values omitted by default (large / sensitive). "
                    "Call audit_run_reproducibility(include_full_env=True) or get_run_env_vars for full map."
                ),
            }
        ),
        "libraries": (
            filter_library_lines(conda_raw, search=None, max_lines=5000)
            if include_full_conda
            else filter_library_lines(conda_raw, search=None, max_lines=conda_preview_lines)
        ),
        "metadata_inventory": {
            "run_row_flat_keys": sorted(flatten_metadata(run_meta).keys()),
            "metrics_bundle_flat_keys": sorted(flatten_metadata(m_meta).keys()),
        },
        "sdk_metrics": {
            "series_metric_names": series_keys,
            "scalar_metric_names": scalar_keys,
            "scalar_latest": scalar_latest,
            "series_summary": series_summary,
            "counts": {"series": len(series_keys), "scalars": len(scalar_keys)},
        },
        "suggested_follow_ups": [
            "Compare identity.git_commit and execution.params to the checkout or config you expect.",
            "Use sdk_metrics metric lists with query_run_metrics when you need full time-series or filtered slices.",
            "If a specific env var matters, use get_run_env_vars(keys=[...]) or pass include_full_env=True on audit.",
            "Pin down a package version with get_run_libraries(search=...) against the preview lines.",
            "For benchmark scores from disk, use get_run_results in addition to SDK scalars.",
        ],
    }
