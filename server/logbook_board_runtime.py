"""Runtime data for campaign_board tables (live run status + run-metric grid cells)."""

from __future__ import annotations

import json
import logging
import math

from .db import get_run_by_hash, get_run_metrics, get_run_with_jobs

log = logging.getLogger(__name__)


def summarize_run_logbook_status(run: dict | None) -> str:
    """Single-label aggregate for a run's Slurm jobs (and SDK metadata fallback)."""
    if not run:
        return "not_found"
    jobs = run.get("jobs") or []
    if not jobs:
        ss = (run.get("sdk_status") or "").strip().lower()
        if ss:
            return ss.replace(" ", "_")
        return "no_jobs"
    states = [str(j.get("state") or "").upper() for j in jobs]
    if any(s in ("RUNNING", "COMPLETING") for s in states):
        return "running"
    if any(s in ("PENDING", "SUBMITTING") for s in states):
        return "pending"
    if any("FAIL" in s for s in states):
        return "failed"
    if any(s.startswith("CANCEL") for s in states):
        return "cancelled"
    if any(s == "TIMEOUT" for s in states):
        return "timeout"
    if states and all((s == "COMPLETED") for s in states if s):
        return "completed"
    return "mixed"


def _board_has_run_status_column(data: dict) -> bool:
    for sec in data.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        if str(sec.get("type", "table")).lower() == "run_metric_grid":
            continue
        for c in sec.get("columns") or []:
            if isinstance(c, dict) and str(c.get("type", "string")).lower() == "run_status":
                return True
    return False


def _format_scalar_point(pt: dict | None) -> str | None:
    if not pt or not isinstance(pt, dict):
        return None
    vn = pt.get("value_num")
    if vn is not None and isinstance(vn, (int, float)) and not (isinstance(vn, float) and math.isnan(vn)):
        try:
            f = float(vn)
            if abs(f - round(f)) < 1e-9:
                return str(int(round(f)))
            s = f"{f:.6g}"
            if "e" in s.lower():
                return s
            if "." in s:
                s = s.rstrip("0").rstrip(".")
            return s or "0"
        except (TypeError, ValueError):
            return str(vn)
    val = pt.get("value")
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return _format_scalar_point({"value_num": val})
    s = str(val).strip()
    return s or None


def _cell_snapshot(cluster: str, rh: str, scalar_key: str | None) -> dict:
    """Resolve one grid cell: Slurm/SDK status plus optional scalar_latest value."""
    sk = (scalar_key or "").strip() or ""
    out: dict = {
        "cluster": cluster,
        "run_hash": rh,
        "status": "not_found",
        "value": None,
        "scalar": sk,
        "malfunctioned": False,
    }
    key = f"{cluster}:{rh}"
    try:
        run_row = get_run_by_hash(cluster, rh)
        if not run_row:
            return out
        out["malfunctioned"] = bool(int(run_row.get("malfunctioned") or 0))
        full = get_run_with_jobs(cluster, run_row["root_job_id"])
        out["status"] = summarize_run_logbook_status(full)
        run_uuid = str(run_row.get("run_uuid") or "").strip()
        if run_uuid and sk:
            try:
                metrics = get_run_metrics(run_uuid)
                latest = (metrics.get("scalar_latest") or {}).get(sk)
                dv = _format_scalar_point(latest if isinstance(latest, dict) else None)
                if dv is not None:
                    out["value"] = dv
            except Exception as exc:
                log.debug("scalar lookup %s %s: %s", key, sk, exc)
        return out
    except Exception as exc:
        log.debug("cell snapshot failed %s: %s", key, exc)
        out["status"] = "error"
        return out


def _collect_table_status_pairs(data: dict) -> list[tuple[str, str]]:
    seen: set[str] = set()
    pairs: list[tuple[str, str]] = []
    for sec in data.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        if str(sec.get("type", "table")).lower() == "run_metric_grid":
            continue
        for row in sec.get("rows") or []:
            if not isinstance(row, dict):
                continue
            cluster = str(row.get("cluster") or "").strip().lower()
            rh = str(row.get("run_hash") or "").strip().lower()
            if not cluster or not rh:
                continue
            key = f"{cluster}:{rh}"
            if key not in seen:
                seen.add(key)
                pairs.append((cluster, rh))
    return pairs


def compute_board_runtime(board_json: str) -> dict:
    """Return ``statuses`` (row-level run_status) and ``cells`` (per grid cell).

    Cell keys: ``"<section_index>:<row_id>:<col_id>"`` matching stored board_json.
    """
    try:
        data = json.loads(board_json)
    except (TypeError, json.JSONDecodeError) as e:
        log.debug("compute_board_runtime: invalid JSON: %s", e)
        return {"statuses": {}, "cells": {}}
    if not isinstance(data, dict):
        return {"statuses": {}, "cells": {}}

    statuses: dict[str, str] = {}
    if _board_has_run_status_column(data):
        for cluster, rh in _collect_table_status_pairs(data):
            key = f"{cluster}:{rh}"
            try:
                run_row = get_run_by_hash(cluster, rh)
                if not run_row:
                    statuses[key] = "not_found"
                    continue
                full = get_run_with_jobs(cluster, run_row["root_job_id"])
                statuses[key] = summarize_run_logbook_status(full)
            except Exception as exc:
                log.debug("compute_board_runtime lookup failed %s: %s", key, exc)
                statuses[key] = "error"

    cells_out: dict[str, dict] = {}
    for si, sec in enumerate(data.get("sections") or []):
        if not isinstance(sec, dict):
            continue
        if str(sec.get("type", "table")).lower() != "run_metric_grid":
            continue
        raw_cells = sec.get("cells") or {}
        if not isinstance(raw_cells, dict):
            continue
        col_scalar: dict[str, str] = {}
        for c in sec.get("columns") or []:
            if not isinstance(c, dict):
                continue
            cid = str(c.get("id") or "").strip()
            if not cid:
                continue
            cs = str(c.get("scalar") or "").strip()
            if cs:
                col_scalar[cid] = cs
        for cell_key, spec in raw_cells.items():
            if not isinstance(spec, dict):
                continue
            if not isinstance(cell_key, str) or ":" not in cell_key:
                continue
            row_id, col_id = cell_key.split(":", 1)
            cluster = str(spec.get("cluster") or "").strip().lower()
            rh = str(spec.get("run_hash") or "").strip().lower()
            if not cluster or not rh:
                continue
            cell_sk = str(spec.get("scalar") or "").strip()
            default_sk = col_scalar.get(col_id, "")
            merged = cell_sk or default_sk
            sk = merged.strip() or None
            runtime_key = f"{si}|{row_id}|{col_id}"
            cells_out[runtime_key] = _cell_snapshot(cluster, rh, sk)

    return {"statuses": statuses, "cells": cells_out}


def attach_board_runtime(entry: dict) -> None:
    """Mutate a full logbook entry dict in-place when it is a campaign board."""
    if entry.get("entry_type") != "campaign_board":
        return
    bj = entry.get("board_json")
    if not bj or not str(bj).strip():
        return
    br = compute_board_runtime(str(bj))
    if br.get("statuses") or br.get("cells"):
        entry["board_runtime"] = br
