"""Pure detection rules for the WasteWatcher.

Each rule is a plain function that consumes already-fetched job dicts +
snapshots and returns a :class:`Detection` (or ``None`` to abstain). No
SSH, no DB writes — that way every rule is trivially unit-testable.

The rules are intentionally small so we can ship and iterate on them
independently as we observe real failure modes in production. The plan
calls out six rules; each has its own function with a docstring
explaining the failure mode it targets and the false-positive risks.

Confidence levels:
  ``high``   - reserved for rules whose preconditions are mechanically
               provable (e.g. parent server exited before child started).
               Eligible for auto-cancel once the verification burst
               agrees.
  ``medium`` - heuristic-based rules with a small false-positive risk.
               Flagged but not auto-cancelled by the high-confidence gate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Mapping, Optional, Sequence


# ─── Reason constants (kept in sync with `waste_reason` schema column) ──────

WASTE_REASON_PORT_MISMATCH = "port_mismatch_hang"
WASTE_REASON_DEAD_SERVER = "dead_server_before_client"
WASTE_REASON_DEPENDENCY_CASCADE = "dependency_cascade"
WASTE_REASON_QOS_DEADLOCK = "qos_self_deadlock"
WASTE_REASON_IDLE_GPU = "idle_gpu_sustained"
WASTE_REASON_GPU_ALLOCATION_MISMATCH = "gpu_allocation_mismatch"
WASTE_REASON_MANIFEST_ONLY = "manifest_only_failure"
WASTE_REASON_MANUAL = "manual"

ALL_WASTE_REASONS = (
    WASTE_REASON_PORT_MISMATCH,
    WASTE_REASON_DEAD_SERVER,
    WASTE_REASON_DEPENDENCY_CASCADE,
    WASTE_REASON_QOS_DEADLOCK,
    WASTE_REASON_IDLE_GPU,
    WASTE_REASON_GPU_ALLOCATION_MISMATCH,
    WASTE_REASON_MANIFEST_ONLY,
    WASTE_REASON_MANUAL,
)

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"


@dataclass
class Detection:
    """Outcome of one rule firing.

    ``target_jobs`` is the list of (cluster, job_id) pairs to cancel — the
    *failing* job may differ from the *symptom-carrying* job (e.g. for
    ``dead_server_before_client`` the symptom is on the dead server but we
    cancel the doomed client).
    """

    reason: str
    confidence: str
    target_jobs: List[tuple[str, str]]
    summary: str
    evidence: dict


# ─── Helpers ────────────────────────────────────────────────────────────────

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _parse_util(value) -> float:
    """Return GPU utilization as a float in [0, 100].

    Accepts strings like ``"42%"``, raw floats, and ``None`` (returns 0).
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).rstrip("%").strip()
    try:
        return float(text)
    except (ValueError, TypeError):
        return 0.0


def _job_has_gpu(job: Mapping) -> bool:
    """True when a job dict declares any GPU allocation.

    Used as a fast filter to skip CPU-only jobs entirely.
    """
    gres = str(job.get("gres", "")).lower()
    return "gpu" in gres


def _allocated_gpu_count(job: Mapping, snapshots: Sequence[Mapping] = ()) -> int:
    """Best-effort allocated GPU count from Slurm GRES or per-GPU stats."""
    for key in ("allocated_gpus", "alloc_gpus", "num_gpus"):
        try:
            value = int(job.get(key) or 0)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass

    gres = str(job.get("gres", ""))
    # Handles common forms: gpu:8, gres/gpu:8, gres:gpu:8, gpu:h100:4.
    matches = re.findall(r"(?:gres/|gres:)?gpu(?::[A-Za-z0-9_.-]+)?[:=](\d+)", gres, re.IGNORECASE)
    if matches:
        try:
            return max(int(m) for m in matches)
        except ValueError:
            pass

    for snap in reversed(list(snapshots)):
        per_gpu = snap.get("per_gpu") or []
        if per_gpu:
            return len(per_gpu)
    return 0


def _job_started_at(job: Mapping) -> Optional[datetime]:
    return _parse_iso(job.get("started_at") or job.get("started"))


def _job_ended_at(job: Mapping) -> Optional[datetime]:
    return _parse_iso(job.get("ended_at"))


def _job_minutes_running(job: Mapping, now: Optional[datetime] = None) -> float:
    """Wallclock minutes since the job actually started running."""
    started = _job_started_at(job)
    if not started:
        return 0.0
    now = now or datetime.now()
    return max(0.0, (now - started).total_seconds() / 60.0)


def _job_id(job: Mapping) -> str:
    return str(job.get("jobid") or job.get("job_id") or "")


def is_exempt_name(job_name: str, regex: str) -> bool:
    """True when the job name matches a user-configured exempt pattern.

    Pure helper so the watcher loop can call this before even loading
    state (cheap short-circuit for known-spiky bookkeeping stages like
    ``judge-aggregate`` / ``summarize-results``).
    """
    if not regex:
        return False
    try:
        return re.search(regex, job_name or "") is not None
    except re.error:
        return False


def is_gpu_busy(per_gpu: Sequence[Mapping], busy_threshold_pct: float) -> bool:
    """True when *any* GPU on the node is above ``busy_threshold_pct``.

    A single GPU above threshold counts as "the job is working" — vLLM
    only stresses one GPU per request on small batches, and tool-call
    paths drop everything to ~0% during sandbox waits but spike on
    response generation.
    """
    for gpu in per_gpu or []:
        if _parse_util(gpu.get("util")) > busy_threshold_pct:
            return True
    return False


def _server_gpu_counts_from_log(log_tail: str) -> dict[str, int]:
    """Parse server-side GPU world-size hints from vLLM/sglang logs."""
    text = log_tail or ""
    patterns = {
        "world_size": r"\bworld[_-]size\b['\"]?\s*[:=]\s*(\d+)",
        "tensor_parallel_size": r"\btensor[_-]parallel[_-]size\b['\"]?\s*[:=]\s*(\d+)",
        "tensor_parallel_cli": r"--tensor-parallel-size(?:=|\s+)(\d+)",
    }
    parsed: dict[str, int] = {}
    for name, pattern in patterns.items():
        values = []
        for match in re.findall(pattern, text, re.IGNORECASE):
            try:
                value = int(match)
            except (TypeError, ValueError):
                continue
            if value > 0:
                values.append(value)
        if values:
            parsed[name] = max(values)
    return parsed


def _stable_partial_idle(
    snapshots: Sequence[Mapping],
    busy_threshold_pct: float,
    *,
    min_samples: int = 3,
) -> Optional[dict]:
    """Return stable partially-idle GPU evidence across recent snapshots.

    Requires each recent sample to have at least one busy GPU and at
    least one idle GPU. This deliberately avoids all-idle jobs, which are
    handled by ``detect_idle_gpu_sustained``.
    """
    recent = [s for s in list(snapshots)[-min_samples:] if s.get("per_gpu")]
    if len(recent) < min_samples:
        return None

    common_idle: Optional[set[str]] = None
    idle_counts = []
    busy_counts = []
    gpu_counts = []
    for snap in recent:
        idle: set[str] = set()
        busy: set[str] = set()
        per_gpu = snap.get("per_gpu") or []
        for idx, gpu in enumerate(per_gpu):
            ident = str(gpu.get("index") or gpu.get("gpu") or idx)
            if _parse_util(gpu.get("util")) > busy_threshold_pct:
                busy.add(ident)
            else:
                idle.add(ident)
        if not idle or not busy:
            return None
        common_idle = set(idle) if common_idle is None else common_idle & idle
        idle_counts.append(len(idle))
        busy_counts.append(len(busy))
        gpu_counts.append(len(per_gpu))

    if not common_idle:
        return None
    allocated = max(gpu_counts) if gpu_counts else 0
    # Ignore tiny tail effects on large allocations; require at least 25%
    # of visible GPUs to be consistently idle.
    if len(common_idle) < max(1, int(allocated * 0.25)):
        return None
    return {
        "common_idle_gpu_indices": sorted(common_idle),
        "idle_counts": idle_counts,
        "busy_counts": busy_counts,
        "gpu_counts": gpu_counts,
        "samples": len(recent),
    }


# ─── Detection rules ────────────────────────────────────────────────────────

def detect_port_mismatch_hang(
    *,
    job: Mapping,
    snapshots: Sequence[Mapping],
    log_tail: str,
    min_runtime_min: int,
    busy_threshold_pct: float,
) -> Optional[Detection]:
    """Port-mismatch hang: server bound, every probe at 0% util, client never connects.

    This is the failure mode that burned 96 GPU-hours on 2026-05-20: vLLM
    bound to a random port, the bash ``wait-for-server`` probe succeeded,
    but the Python client kept polling ``:5000`` and timed out at the
    4-hour walltime. Symptoms:

    - GPU server job (gres contains 'gpu', name often ends in ``_server``)
    - RUNNING for >= ``min_runtime_min``
    - **Every** snapshot recorded so far shows GPU util == 0
    - Log tail contains a recognizable "server is up" success message
      (we don't try to parse vLLM port detection — just look for any of
      ``Server hostname written``, ``Application startup complete``,
      ``Uvicorn running on``)
    - Log tail does NOT contain client-side request markers (``POST /v1``,
      ``inference.generate``, ``model_request``)

    Mechanically high-confidence because the server has demonstrably
    written its port file but no client has demonstrably connected.
    """
    if not _job_has_gpu(job):
        return None
    if str(job.get("state", "")).upper() != "RUNNING":
        return None
    if _job_minutes_running(job) < min_runtime_min:
        return None
    if not snapshots:
        return None

    # Every snapshot must show zero-ish GPU util on every GPU.
    for snap in snapshots:
        per_gpu = snap.get("per_gpu") or []
        if per_gpu:
            if is_gpu_busy(per_gpu, busy_threshold_pct):
                return None
        else:
            # Fall back to the averaged column when per-GPU JSON is empty.
            if _parse_util(snap.get("gpu_util")) > busy_threshold_pct:
                return None

    log_tail = log_tail or ""
    server_markers = ("Server hostname written", "Application startup complete",
                      "Uvicorn running on", "vLLM API server")
    client_markers = ("POST /v1", "inference.generate", "model_request",
                      "completion request", "generate_async")
    has_server_up = any(m in log_tail for m in server_markers)
    has_client_call = any(m in log_tail for m in client_markers)
    if not has_server_up or has_client_call:
        return None

    return Detection(
        reason=WASTE_REASON_PORT_MISMATCH,
        confidence=CONFIDENCE_HIGH,
        target_jobs=[(job["cluster"], _job_id(job))],
        summary=(
            f"GPU server has been bound for {_job_minutes_running(job):.0f}min "
            f"with 0% util across {len(snapshots)} probes; no client requests "
            f"observed in log tail. Likely port-mismatch hang."
        ),
        evidence={
            "snapshots_sampled": len(snapshots),
            "minutes_running": round(_job_minutes_running(job), 1),
            "log_contains_server_marker": True,
            "log_contains_client_marker": False,
        },
    )


def detect_dead_server_before_client(
    *,
    job: Mapping,
    run_jobs: Sequence[Mapping],
) -> Optional[Detection]:
    """Dead-server-before-client: parent server exited *before* child client started.

    Specific to the dedicated-server-per-stage pattern (v2/v1.5 turn loop).
    If the server's GPU sleep elapsed and Slurm killed it ~50 seconds
    before the client polled, the client hangs on a dead port until
    walltime. Symptoms:

    - This job is a CPU client (depends_on a GPU server)
    - State is RUNNING
    - At least one ``depends_on`` parent is in a terminal state
      (COMPLETED / FAILED / TIMEOUT / CANCELLED) with ``ended_at <
      self.started_at``

    Mechanically high-confidence because ``ended_at`` and ``started_at``
    are ground-truth from sacct/squeue, not heuristics.

    Returns a detection targeting the client (the doomed job) — never the
    dead parent (which is already finished and would no-op the cancel).
    """
    if str(job.get("state", "")).upper() != "RUNNING":
        return None
    my_start = _job_started_at(job)
    if not my_start:
        return None
    depends_on = job.get("depends_on") or []
    if not depends_on:
        return None

    parent_by_id = {_job_id(j): j for j in run_jobs}
    terminal_states = {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY"}
    dead_parents = []
    for pid in depends_on:
        parent = parent_by_id.get(str(pid))
        if not parent:
            continue
        if str(parent.get("state", "")).upper() not in terminal_states:
            continue
        parent_end = _job_ended_at(parent)
        if not parent_end:
            continue
        if parent_end < my_start:
            dead_parents.append({
                "job_id": _job_id(parent),
                "ended_at": parent.get("ended_at"),
                "state": parent.get("state"),
            })
    if not dead_parents:
        return None

    return Detection(
        reason=WASTE_REASON_DEAD_SERVER,
        confidence=CONFIDENCE_HIGH,
        target_jobs=[(job["cluster"], _job_id(job))],
        summary=(
            f"Client started at {job.get('started') or job.get('started_at')} "
            f"but parent server(s) {[p['job_id'] for p in dead_parents]} "
            f"ended earlier. Client is polling a dead server."
        ),
        evidence={
            "client_started_at": job.get("started") or job.get("started_at"),
            "dead_parents": dead_parents,
        },
    )


def detect_dependency_cascade(
    *,
    failed_job: Mapping,
    run_jobs: Sequence[Mapping],
) -> List[Detection]:
    """Cascade-cancel jobs whose only ancestor is a FAILED job.

    When a parent stage FAILs, Slurm marks ``afterok`` dependents as
    "DependencyNeverSatisfied" — they will provably never run, but they
    still hold their submit-time priority and (occasionally) a QOS slot
    via QOS group caps. Cancelling them frees the slot for retries.

    Symptoms detected here:
    - ``failed_job`` has a terminal failed state (FAILED, TIMEOUT, etc.)
    - At least one transitive dependent in the same run is still PENDING
      with ``reason`` containing ``DependencyNeverSatisfied``

    Returns one detection per affected dependent (so the cancel loop can
    handle them individually and the audit log is granular).
    """
    failed_states = {"FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL"}
    if str(failed_job.get("state", "")).upper() not in failed_states:
        return []

    failed_id = _job_id(failed_job)
    # Build forward adjacency from job -> dependents within this run
    children: dict[str, List[Mapping]] = {}
    for j in run_jobs:
        for parent_id in (j.get("depends_on") or []):
            children.setdefault(str(parent_id), []).append(j)

    # BFS from failed_id to gather every transitive dependent
    visited: set[str] = set()
    queue = list(children.get(failed_id, []))
    doomed: List[Mapping] = []
    while queue:
        cur = queue.pop(0)
        cid = _job_id(cur)
        if cid in visited:
            continue
        visited.add(cid)
        if str(cur.get("state", "")).upper() == "PENDING" and "DependencyNever" in str(cur.get("reason", "")):
            doomed.append(cur)
        queue.extend(children.get(cid, []))

    return [
        Detection(
            reason=WASTE_REASON_DEPENDENCY_CASCADE,
            confidence=CONFIDENCE_HIGH,
            target_jobs=[(d["cluster"], _job_id(d))],
            summary=(
                f"Pending job {_job_id(d)} ({d.get('name', '')}) is downstream "
                f"of FAILED ancestor {failed_id}; "
                f"reason '{d.get('reason', '')}' will never satisfy."
            ),
            evidence={
                "failed_ancestor": failed_id,
                "ancestor_state": failed_job.get("state"),
                "pending_reason": d.get("reason"),
            },
        )
        for d in doomed
    ]


def detect_qos_self_deadlock(
    *,
    running_job: Mapping,
    run_jobs: Sequence[Mapping],
) -> Optional[Detection]:
    """Detect a wait-job holding the QOS slot its own producer needs.

    The 2026-05-20 lockup: a tiny CPU ``wait-*-sentinels`` job grabbed a
    QOS slot by backfilling past the queued ``path-*`` GPU jobs it was
    polling for. The wait job will never finish because the producers
    can't start, and the producers can't start because the wait job holds
    a QOS slot.

    Symptoms:
    - ``running_job`` name matches ``wait-.*-sentinels``
    - At least one job in the same run is PENDING with reason matching
      QOS/Assoc limits (the producer that should make the sentinel file)
    - The PENDING job's name shares the same logical stage prefix
      (``merge-path-``, ``path-``, ``probe-``, etc.) the wait job is
      polling for

    Medium confidence: the name heuristic could match unrelated
    coincidences. Flag-only by default; cancellation requires explicit
    operator opt-in.
    """
    name = str(running_job.get("name", ""))
    if not re.search(r"wait-[a-z]+-sentinels", name):
        return None
    if str(running_job.get("state", "")).upper() != "RUNNING":
        return None

    # Extract the polled stage prefix: 'wait-path-sentinels' -> 'path-'
    m = re.search(r"wait-([a-z]+)-sentinels", name)
    if not m:
        return None
    polled_prefix = m.group(1)

    qos_re = re.compile(r"QOS|Assoc|MaxGres|GrpGRES|GrpCpu", re.IGNORECASE)
    blockers: List[Mapping] = []
    for j in run_jobs:
        if str(j.get("state", "")).upper() != "PENDING":
            continue
        if not qos_re.search(str(j.get("reason", ""))):
            continue
        jname = str(j.get("name", ""))
        if polled_prefix in jname:
            blockers.append(j)
    if not blockers:
        return None

    return Detection(
        reason=WASTE_REASON_QOS_DEADLOCK,
        confidence=CONFIDENCE_MEDIUM,
        target_jobs=[(running_job["cluster"], _job_id(running_job))],
        summary=(
            f"Wait job {_job_id(running_job)} ({name}) is polling for files "
            f"produced by {len(blockers)} jobs blocked on QOS. "
            f"Cancelling the wait job frees the QOS slot."
        ),
        evidence={
            "wait_job_id": _job_id(running_job),
            "polled_prefix": polled_prefix,
            "blockers": [
                {"job_id": _job_id(b), "name": b.get("name"), "reason": b.get("reason")}
                for b in blockers
            ],
        },
    )


def detect_idle_gpu_sustained(
    *,
    job: Mapping,
    snapshots: Sequence[Mapping],
    min_runtime_min: int,
    suspicious_confirm_min: int,
    busy_threshold_pct: float,
    log_quiet_min: int,
    log_age_min: Optional[float],
) -> Optional[Detection]:
    """Fallback rule: long sustained idle on a GPU job, plus a quiet log tail.

    This is the rule that catches every "stuck job" not covered by the
    more specific rules above (dead deadlock, parent crash, port hang).
    Confidence is medium because the underlying cause is unknown — we
    only know "GPU is doing nothing AND the log isn't growing".

    Requires:
    - GPU job, RUNNING >= ``min_runtime_min``
    - At least ``ceil(suspicious_confirm_min / probe_cadence)`` recent
      snapshots all showing 0% util across all GPUs
    - Log tail hash hasn't changed in >= ``log_quiet_min`` minutes
      (``log_age_min`` is the seconds-since-last-log-change observed by
      the watcher; ``None`` skips the log check)
    """
    if not _job_has_gpu(job):
        return None
    if str(job.get("state", "")).upper() != "RUNNING":
        return None
    if _job_minutes_running(job) < min_runtime_min:
        return None

    # Need enough samples to span suspicious_confirm_min, conservatively
    # assume 1-minute spacing.
    needed = max(2, int(suspicious_confirm_min))
    recent = list(snapshots)[-needed:]
    if len(recent) < needed:
        return None
    for snap in recent:
        per_gpu = snap.get("per_gpu") or []
        if per_gpu:
            if is_gpu_busy(per_gpu, busy_threshold_pct):
                return None
        else:
            if _parse_util(snap.get("gpu_util")) > busy_threshold_pct:
                return None

    if log_age_min is not None and log_age_min < log_quiet_min:
        return None

    return Detection(
        reason=WASTE_REASON_IDLE_GPU,
        confidence=CONFIDENCE_MEDIUM,
        target_jobs=[(job["cluster"], _job_id(job))],
        summary=(
            f"GPU job idle for >= {suspicious_confirm_min}min across "
            f"{len(recent)} snapshots and log tail quiet for "
            f"{log_age_min if log_age_min is not None else '?'}min."
        ),
        evidence={
            "snapshots_sampled": len(recent),
            "log_age_min": log_age_min,
        },
    )


def detect_gpu_allocation_mismatch(
    *,
    job: Mapping,
    snapshots: Sequence[Mapping],
    log_tail: str,
    min_runtime_min: int,
    busy_threshold_pct: float,
) -> Optional[Detection]:
    """Detect allocations larger than the server appears to use.

    Targets the "TP=4 on an 8-GPU allocation" class from the wasted-compute
    audit. Unlike ``idle_gpu_sustained``, the job may still be making
    progress on some GPUs; this is therefore medium-confidence and
    flag-only by default.
    """
    if not _job_has_gpu(job):
        return None
    if str(job.get("state", "")).upper() != "RUNNING":
        return None
    if _job_minutes_running(job) < min_runtime_min:
        return None

    allocated = _allocated_gpu_count(job, snapshots)
    if allocated <= 1:
        return None

    parsed_counts = _server_gpu_counts_from_log(log_tail)
    if parsed_counts:
        # Prefer world_size when present; otherwise use the largest TP hint.
        server_count = parsed_counts.get("world_size") or max(parsed_counts.values())
        if 0 < server_count < allocated:
            return Detection(
                reason=WASTE_REASON_GPU_ALLOCATION_MISMATCH,
                confidence=CONFIDENCE_MEDIUM,
                target_jobs=[(job["cluster"], _job_id(job))],
                summary=(
                    f"Job allocated {allocated} GPUs but server log reports "
                    f"GPU world size/TP {server_count}. Likely allocation/TP mismatch."
                ),
                evidence={
                    "allocated_gpus": allocated,
                    "server_gpu_count": server_count,
                    "parsed_log_counts": parsed_counts,
                    "minutes_running": round(_job_minutes_running(job), 1),
                },
            )

    partial = _stable_partial_idle(snapshots, busy_threshold_pct)
    if partial:
        return Detection(
            reason=WASTE_REASON_GPU_ALLOCATION_MISMATCH,
            confidence=CONFIDENCE_MEDIUM,
            target_jobs=[(job["cluster"], _job_id(job))],
            summary=(
                f"Job shows sustained partial GPU idleness: "
                f"{len(partial['common_idle_gpu_indices'])}/{allocated} visible GPUs "
                f"idle while other allocated GPUs are busy."
            ),
            evidence={
                "allocated_gpus": allocated,
                **partial,
                "minutes_running": round(_job_minutes_running(job), 1),
            },
        )
    return None


def detect_manifest_only_failure(run_jobs: Sequence[Mapping]) -> Optional[Detection]:
    """Detect a run where the eval pipeline succeeded but a bookkeeping job died.

    Specifically r18-style: every ``judge``/``summarize-results`` stage
    completed exit 0 but the terminal ``manifest`` step crashed (exit 15
    or similar SIGTERM after bookkeeping completed cleanup work). The
    eval data is on disk and recoverable — we just want to flag the run
    so reviewers don't read 'no metrics.json' as 'the run failed'.

    Returns a Detection with NO target_jobs (nothing to cancel; the
    failing step is already terminal). The cancel-and-audit layer treats
    an empty target_jobs list as "flag only, do not call scancel".
    """
    if not run_jobs:
        return None

    # The terminal bookkeeping stages we recognise.
    manifest_re = re.compile(r"(manifest|cleanup|finalize)", re.IGNORECASE)
    success_re = re.compile(r"(judge|summarize|results?|eval)", re.IGNORECASE)

    failed_manifest = None
    healthy_evals = []
    for j in run_jobs:
        name = str(j.get("name", ""))
        state = str(j.get("state", "")).upper()
        exit_code = str(j.get("exit_code", ""))
        if manifest_re.search(name) and state in {"FAILED", "TIMEOUT", "CANCELLED"}:
            failed_manifest = j
        if success_re.search(name) and state == "COMPLETED" and exit_code.startswith("0:"):
            healthy_evals.append(j)
    if not failed_manifest or not healthy_evals:
        return None

    return Detection(
        reason=WASTE_REASON_MANIFEST_ONLY,
        confidence=CONFIDENCE_HIGH,
        target_jobs=[],
        summary=(
            f"Manifest/cleanup job {_job_id(failed_manifest)} failed but "
            f"{len(healthy_evals)} eval stages completed cleanly. Eval data "
            f"is recoverable on disk; treat as bookkeeping failure, not a "
            f"real run failure."
        ),
        evidence={
            "failed_manifest_job_id": _job_id(failed_manifest),
            "manifest_state": failed_manifest.get("state"),
            "manifest_exit_code": failed_manifest.get("exit_code"),
            "healthy_eval_count": len(healthy_evals),
        },
    )
