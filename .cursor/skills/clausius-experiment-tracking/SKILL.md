---
name: clausius-experiment-tracking
description: Add Clausius experiment tracking and final metric logging to research projects. Use when integrating Clausius SDK metrics, adding final run results, mirroring WandB logging, instrumenting NeMo-Skills jobs, or logging project metrics for Artsiv, MPSF, MCP/tool-use, HLE, GPQA, SWE-Bench, or similar evaluations.
---

# Clausius Experiment Tracking

Use this skill when a user asks to add Clausius tracking, final run metrics, or
WandB-like logging to a project.

## Core Principle

Final project results are **Clausius scalar metrics**.

Do not create a project-specific Clausius storage table, result API, or custom
artifact format unless the user explicitly asks for a new Clausius product
feature. The producing project should send final numbers to Clausius the same
way it sends them to WandB.

Agents and users should inspect results through:

```text
get_run_metrics(cluster, run_hash)
```

Read `scalar_latest` / `scalars` for final run metrics and `series` for
time-series metrics.

## Where To Instrument

Add Clausius logging near code that already:

- Logs final metrics to WandB.
- Writes `metrics.json`.
- Prints a final summary table.
- Computes the final aggregate benchmark score.

For NeMo-Skills, this is usually at the end of a scorer or summarizer after the
final metrics dict is computed. Do not log partial rows as final metrics.

## API To Use

Use no-op-safe SDK wrappers when running inside an active Clausius-tracked job:

```python
from clausius_sdk.metrics import log_scalar, log_metric

log_scalar("pass_at_1", pass_at_1, project="mpsf", benchmark=benchmark, model=model)
log_scalar("num_generated_tokens", mean_tokens, project="mpsf", benchmark=benchmark, stat="mean")
```

Use `log_scalar(...)` for final run metrics.
Use `log_metric(..., step=...)` only for time-series values.

If the code runs in a separate process with a known run UUID, attach explicitly:

```python
from clausius_sdk import Run

run = Run(run_uuid=run_uuid, connect=True)
run.scalar("pass_at_1", pass_at_1, context={"project": "mpsf", "benchmark": benchmark})
run.close()
```

## Naming Rules

Use stable, generic metric names. Put dimensions in `context`, not in metric
names.

Good metric names:

- `resolved_rate`
- `pass_at_1`
- `pass_at_k`
- `accuracy`
- `num_generated_tokens`
- `num_turns`
- `empty_generation_rate`
- `no_patch_rate`
- `apply_fail_rate`
- `samples_completed`

Avoid metric names that encode dimensions:

- `gpqa_pass_at_1`
- `qwen_resolved_rate`
- `mpsf_tokens_mean`
- `artsiv_swe_agent_all_findings_score`

Use context instead:

```python
log_scalar(
    "pass_at_k",
    pass_at_k,
    project="mpsf",
    benchmark="gpqa",
    model="nemotron-super",
    k=8,
)
```

## Context Fields

Include enough context for agents to compare runs without reading local scripts.
Use JSON-safe compact values only.

Recommended context:

- `project`: `artsiv`, `mpsf`, `mcp`, `hle`, etc.
- `benchmark`: `swe-bench`, `gpqa`, `hle`, etc.
- `model`: model identifier used in the run.
- `split`: when relevant.
- `seed`: when relevant.
- `framework`: agent/eval framework such as `swe-agent` or `openhands`.
- `setup`: condition such as `baseline`, `all_artsiv_findings`,
  `quality_filtered`, `no_tool`, or `mpsf`.
- `k`: for `pass_at_k`.
- `stat`: for aggregate stats such as `mean`, `median`, `p95`, `max`.

Never log secrets, credentials, API keys, raw prompts, huge generations, or
large payloads.

## Artsiv E2E

Primary metric: `resolved_rate`.

```python
from clausius_sdk.metrics import log_scalar

ctx = {
    "project": "artsiv",
    "benchmark": "swe-bench",
    "model": model,
    "framework": framework,  # e.g. "swe-agent" or "openhands"
    "setup": setup,          # e.g. "baseline", "all_artsiv_findings"
}

log_scalar("resolved_rate", resolved_rate, **ctx)
log_scalar("no_patch_rate", no_patch_rate, **ctx)
log_scalar("apply_fail_rate", apply_fail_rate, **ctx)
log_scalar("samples_completed", entries, **ctx)
```

Keep percentage convention consistent within the project. If existing Artsiv
tables report `54.0` for 54%, log that convention consistently.

## MPSF

Primary metric is usually `pass_at_1`; `pass_at_k` is a key secondary metric.

```python
from clausius_sdk.metrics import log_scalar

ctx = {
    "project": "mpsf",
    "benchmark": benchmark,
    "model": model,
    "setup": setup,
}

log_scalar("pass_at_1", pass_at_1, **ctx)
log_scalar("pass_at_k", pass_at_k, **ctx, k=k)
log_scalar("num_generated_tokens", mean_tokens, **ctx, stat="mean")
log_scalar("num_turns", mean_turns, **ctx, stat="mean")
log_scalar("samples_completed", samples_completed, **ctx)
```

Optional falsification metrics should also be plain scalar metrics:

```python
log_scalar("falsification_success_rate", rate, **ctx)
log_scalar("paths_generated", paths_generated, **ctx, stat="mean")
```

## MCP / Tool-Use Runs

Primary metric is usually `pass_at_1` or task accuracy. Secondary metrics should
capture tool-use behavior.

```python
from clausius_sdk.metrics import log_scalar

ctx = {
    "project": "mcp",
    "benchmark": benchmark,
    "model": model,
    "setup": setup,
}

log_scalar("pass_at_1", pass_at_1, **ctx)
log_scalar("pass_at_k", pass_at_k, **ctx, k=k)
log_scalar("num_generated_tokens", mean_tokens, **ctx, stat="mean")
log_scalar("num_turns", mean_turns, **ctx, stat="mean")
log_scalar("tool_calls", mean_tool_calls, **ctx, stat="mean")
log_scalar("tool_error_rate", tool_error_rate, **ctx)
log_scalar("samples_completed", samples_completed, **ctx)
```

## Time Series

Use `log_metric` only when there is a step axis:

```python
log_metric("samples_completed", completed, step=chunk_idx, project="mpsf", benchmark="gpqa")
```

Final metrics should use `log_scalar`:

```python
log_scalar("pass_at_1", final_pass_at_1, project="mpsf", benchmark="gpqa")
```

## Verification

After a tracked run completes:

1. Call `get_run_metrics(cluster, run_hash)`.
2. Check that `scalar_latest` contains the expected final metrics.
3. Check that context identifies benchmark/model/setup clearly.
4. Confirm metric values are numeric where plots/comparisons are expected.
5. Confirm no secrets or huge payloads were logged.

## Agent Checklist

When adding Clausius final metric logging:

1. Find where final metrics are computed or sent to WandB.
2. Add `log_scalar(...)` calls for primary and secondary metrics.
3. Keep metric names generic and stable.
4. Put project/model/benchmark/setup/seed/framework details in context.
5. Run a small local or unit test when possible.
6. After a tracked run, verify with `get_run_metrics(cluster, run_hash)`.

