# Clausius Final Metrics Logging Guide

This guide is for project agents adding Clausius tracking to experiment code.

The rule is simple: final project results are **Clausius scalar metrics**. Do
not add a new result table, custom storage format, or project-specific Clausius
API. Log the same final numbers you would log to WandB, from the same part of
the code.

## Goal

Every completed run should expose its final scores through:

```text
get_run_metrics(cluster, run_hash)
```

Agents and users should be able to read `scalar_latest` / `scalars` and see the
canonical run result without parsing logs or searching for output files.

## Where To Add Logging

Add Clausius metric logging near the code that already does one of these:

- Logs final metrics to WandB.
- Writes `metrics.json`.
- Prints a final summary table.
- Computes the final aggregate score for a benchmark.

For NeMo-Skills, this is usually at the end of a scorer or summarizer after the
final metrics dict is computed. Do not log partial/raw rows as final metrics.

## Preferred API

Inside a Clausius-tracked run, use the no-op-safe convenience wrappers:

```python
from clausius_sdk.metrics import log_scalar, log_metric

log_scalar("pass_at_1", pass_at_1, project="mpsf", benchmark=benchmark, model=model)
log_scalar("num_generated_tokens", mean_tokens, project="mpsf", benchmark=benchmark, stat="mean")
```

Use `log_scalar(...)` for final run metrics. Use `log_metric(..., step=...)`
only for time series.

If you are in a separate process and have the run UUID, attach explicitly:

```python
from clausius_sdk import Run

run = Run(run_uuid=run_uuid, connect=True)
run.scalar("pass_at_1", pass_at_1, context={"project": "mpsf", "benchmark": benchmark})
run.close()
```

## Metric Naming

Use stable, generic metric names. Put dimensions in context, not in the metric
name.

Good names:

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

Avoid names like:

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

## Required Context

Include enough context for agents to compare runs without reading local scripts.
Use JSON-safe values only.

Recommended fields:

- `project`: `artsiv`, `mpsf`, `mcp`, etc.
- `benchmark`: `swe-bench`, `gpqa`, `hle`, etc.
- `model`: model identifier used in the experiment.
- `split`: when relevant.
- `seed`: when relevant.
- `framework`: for agent frameworks such as `swe-agent` or `openhands`.
- `setup`: experimental condition such as `baseline`, `all_artsiv_findings`,
  `quality_filtered`, `no_tool`, or `mpsf`.
- `k`: for `pass_at_k`.
- `stat`: for aggregate statistics such as `mean`, `median`, `p95`, `max`.

Do not log secrets, API keys, raw prompts, full model generations, credentials,
or huge payloads.

## Artsiv E2E

Primary metric: `resolved_rate`.

Recommended metrics:

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

Log percentages consistently. If the project already reports `54.0` for 54%,
use that convention for all Artsiv E2E rates.

## MPSF

Primary metric is usually `pass_at_1`, with `pass_at_k` as a key secondary
metric.

Recommended metrics:

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

If the falsification pipeline computes additional useful counters, log them as
plain scalar metrics with clear names and context, for example:

```python
log_scalar("falsification_success_rate", rate, **ctx)
log_scalar("paths_generated", paths_generated, **ctx, stat="mean")
```

## MCP / Tool-Use Runs

Primary metric is usually `pass_at_1` or task accuracy. Secondary metrics
should capture tool-use behavior.

Recommended metrics:

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

## Time Series vs Final Metrics

Use `log_scalar` for final values:

```python
log_scalar("pass_at_1", final_pass_at_1, project="mpsf", benchmark="gpqa")
```

Use `log_metric` only when the value has a step axis:

```python
log_metric("samples_completed", completed, step=chunk_idx, project="mpsf", benchmark="gpqa")
```

## Verification

After a run completes, verify through Clausius MCP:

```text
get_run_metrics(cluster, run_hash)
```

Check:

- `scalar_latest` contains the expected final metrics.
- Context fields identify benchmark/model/setup clearly.
- Values are numeric where plots or comparisons are expected.
- No secrets or huge payloads were logged.

## Agent Checklist

When adding final metric logging to a project:

1. Find the place where final metrics are computed or sent to WandB.
2. Add `log_scalar(...)` calls for the primary and secondary metrics.
3. Keep metric names generic and stable.
4. Put project/model/benchmark/setup/seed/framework details in context.
5. Run a small local/unit test if possible.
6. After a tracked run, verify with `get_run_metrics(cluster, run_hash)`.

