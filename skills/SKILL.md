---
name: clausius
description: Multi-cluster Slurm dashboard and MCP server for monitoring, managing, and exploring GPU jobs across HPC clusters. Use when the user asks about job status, logs, stats, history, cancellation, cluster availability, partitions, submission recommendations, mounts, running scripts on clusters, or anything related to Slurm jobs and GPU compute.
---

# clausius — Multi-Cluster Slurm Dashboard + MCP Agent API

> MCP server name: **`user-clausius`**
> Backend: Flask at `http://localhost:7272`
> Service: `systemctl --user status clausius.service`
> Source: `~/clausius`

All cluster interactions MUST go through the clausius MCP server. Do NOT write ad-hoc `ssh`, `squeue`, `sacct`, `scontrol`, or `scancel` commands.

---

## MCP Tools

### Jobs & Status

| Tool | Purpose | Example |
|------|---------|---------|
| `list_jobs(cluster?)` | Active/pending/failed jobs on all or one cluster | `list_jobs()` or `list_jobs(cluster="my-cluster")` |
| `list_projects()` | All known projects with job counts and colors | `list_projects()` |
| `get_project_jobs(project, cluster?, limit?)` | Live + historical jobs for a project | `get_project_jobs("my-project")` |
| `get_history(cluster?, project?, limit?)` | Past completed/failed/cancelled jobs | `get_history(project="my-project", limit=20)` |
| `get_run_info(cluster, root_job_id)` | Run metadata: batch script, env, conda/pip, scontrol | `get_run_info("my-cluster", "12345")` |

### Logs & Files

| Tool | Purpose | Example |
|------|---------|---------|
| `list_log_files(cluster, job_id)` | Discover log/result files for a job | `list_log_files("my-cluster", "12345")` |
| `get_job_log(cluster, job_id, path?, lines?)` | Read log output (auto-picks best file) | `get_job_log("my-cluster", "12345", lines=200)` |
| `get_job_stats(cluster, job_id)` | GPU/CPU/memory stats for a running job | `get_job_stats("my-cluster", "12345")` |

### Cluster Intelligence

| Tool | Purpose | Example |
|------|---------|---------|
| `get_cluster_availability()` | Real-time utilization from Science dashboard | `get_cluster_availability()` |
| `get_partitions(cluster?)` | Detailed partition data (priority, preemption, queue) | `get_partitions("my-cluster")` |
| `get_partition_summary()` | Compact cross-cluster overview with wait estimates | `get_partition_summary()` |
| `recommend_submission(nodes?, time_limit?, ...)` | Ranked cluster+partition suggestions for a job | see below |
| `get_storage_quota(cluster)` | Lustre storage quota (user + project space/inodes) | `get_storage_quota("my-cluster")` |

### Actions

| Tool | Purpose | Example |
|------|---------|---------|
| `cancel_job(cluster, job_id)` | Cancel a single job (destructive, ask first) | `cancel_job("my-cluster", "12345")` |
| `cancel_jobs(cluster, job_ids)` | Cancel multiple jobs by ID | `cancel_jobs("my-cluster", ["123","456"])` |
| `cleanup_history(days, dry_run?)` | Delete old history records + local logs | `cleanup_history(30, dry_run=True)` |

### Mounts & Board

| Tool | Purpose | Example |
|------|---------|---------|
| `get_mounts()` | Check SSHFS mount status for all clusters | `get_mounts()` |
| `mount_cluster(cluster, action)` | Mount/unmount cluster filesystem | `mount_cluster("my-cluster", "mount")` |
| `clear_failed(cluster)` | Dismiss failed/cancelled pins from board | `clear_failed("my-cluster")` |
| `clear_completed(cluster)` | Dismiss completed pins from board | `clear_completed("my-cluster")` |

### Script Execution

| Tool | Purpose |
|------|---------|
| `run_script(cluster, script, interpreter?, timeout?)` | Run Python/bash on cluster via SSH, returns stdout+stderr |

```python
run_script(
    cluster="my-cluster",
    script="""
import json
path = "/lustre/.../eval-results/output-rs0.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip()]
correct = sum(1 for r in rows if r.get("judgement"))
print(f"Accuracy: {correct}/{len(rows)} = {correct/len(rows)*100:.1f}%")
""",
    interpreter="python3",  # or "bash", "sh"
    timeout=120,            # 1-300 seconds
)
```

Resource: `jobs://summary` — one-line-per-cluster overview (running/pending/failed counts).

---

## Architecture

- **On-demand only**: Clusters are contacted only when a user views the page or an MCP tool is called. No background polling.
- **Three-lane SSH**: primary (Slurm control), background (metadata), data (file I/O to data-copier nodes).
- **Mount-first reads**: Log reads prefer SSHFS mounts, falling back to SSH.
- **PENDING jobs have no logs or stats** — don't attempt to fetch them.
- **`crash_detected`**: Running jobs may have this field set when OOM/segfault/traceback is found in logs. Check before reading full logs.

---

## Job Name Prefix Protocol

Jobs are grouped by project using: `<project>_<run-name>`

| Component | Rules | Example |
|-----------|-------|---------|
| `<project>` | Lowercase letters, digits, hyphens. Starts with a letter. | `my-project`, `eval-suite` |
| `_` | Required underscore separator | |
| `<run-name>` | Experiment/eval name | `eval-math`, `train-v3` |

Dependency chain auto-detection:
- `*-judge-rs<N>` → child of base eval
- `*-summarize-results` → child of judge run

---

## Decision Trees

### What's running right now?

```
Quick overview needed?
├── Read resource: jobs://summary
└── Detailed: list_jobs()

For a specific cluster?
└── list_jobs(cluster="my-cluster")

For a specific project?
└── get_project_jobs("my-project")

Check crash status?
└── list_jobs() → look for crash_detected field
    └── If set → get_job_log() to read details
```

### Investigating a job

```
1. list_jobs() or get_project_jobs() → find the job
2. Check state:
   ├── PENDING → check reason field, no logs/stats available
   ├── RUNNING → get_job_stats() for GPU util, get_job_log() for output
   ├── FAILED  → get_job_log() for error, get_run_info() for batch script
   └── COMPLETED → get_job_log() for results
3. Need files? → list_log_files() to discover, get_job_log(path=...) to read
4. Need batch script / env? → get_run_info(cluster, root_job_id)
```

### Where should I submit a job?

```
Quick scan?
└── get_partition_summary()
    Shows wait estimates per partition across all clusters

Job-specific recommendation?
└── recommend_submission(
        nodes=2,
        time_limit="4:00:00",
        gpu_type="h100",
        can_preempt=False,
    )

Deep dive on a cluster?
└── get_partitions("my-cluster")
    Shows priority tiers, preemption, idle nodes, queue depth

Also check storage?
└── get_storage_quota("my-cluster")
    If >90% space/inodes, cluster may reject jobs
```

### Analysing results on cluster

```
1. list_log_files(cluster, job_id) → discover file paths
2. Option A: get_job_log(cluster, job_id, path=...) for log text
3. Option B: run_script() for programmatic analysis
   └── Use Python to parse JSONL, compute metrics, etc.
```

---

## Logbooks

Per-project structured logbook entries stored in SQLite with FTS5 BM25 search.
Each entry has: id, title, body (markdown), entry_type ("note" or "plan"), created_at, edited_at.

| Tool | Purpose | Example |
|------|---------|---------|
| `list_logbook_entries(project, query?, sort?, limit?, entry_type?)` | List or BM25-search entries | `list_logbook_entries("my-project", query="accuracy")` |
| `read_logbook_entry(project, entry_id)` | Read full entry with markdown body | `read_logbook_entry("my-project", 42)` |
| `create_logbook_entry(project, title, body, entry_type?)` | Create entry ("note" or "plan") | `create_logbook_entry("my-project", "Eval results", "...", entry_type="note")` |
| `update_logbook_entry(project, entry_id, title?, body?)` | Update title and/or body | `update_logbook_entry("my-project", 42, body="updated results")` |
| `delete_logbook_entry(project, entry_id)` | Delete an entry (destructive) | `delete_logbook_entry("my-project", 42)` |
| `search_logbook(query, project?, date_from?, date_to?)` | Cross-project BM25 search | `search_logbook("CUDA error", project="my-project")` |
| `upload_logbook_image(project, image_path)` | Attach plot/figure, returns markdown URL | `upload_logbook_image("my-project", "/path/to/plot.png")` |

Entry types: **note** (experiments, debugging, findings) and **plan** (implementation/research plans).

Body supports full markdown: headers, tables, code blocks, blockquotes (`> `), images (`![caption](url)`),
links (`[text](url)`), and `@run-name` job references. Use `upload_logbook_image` to attach plots/figures,
then embed the returned URL in the body. Use code blocks for system prompts, JSON, configs.
Use blockquotes for model outputs or external sources. Titles must be descriptive (no dates).

**Figure captions**: Every figure MUST have a paper-style caption below it using bold italic.
Number figures sequentially within the entry. The caption should describe what the figure
shows and highlight the key takeaway, not just repeat the title.

```markdown
![](/api/logbook/my-project/images/plot.png)
***Figure 1.** Accuracy vs compute budget across 5 seeds. SciPythonTool (green) matches no-tool baseline at 3x the compute, but eliminates 94% of sandbox errors.*
```

---

## Do NOT

- Run `ssh`, `squeue`, `sacct`, `scontrol`, or `scancel` directly
- Write polling loops or repeated SSH commands
- Attempt to fetch logs/stats for PENDING jobs
- Cancel jobs without explicit user request

Only fall back to SSH if debugging the clausius app itself or the user explicitly requests raw output.

---

## SDK-Tracked Submissions

To enable Clausius experiment tracking, add two env vars to any NeMo-Skills submission:

```bash
CLAUSIUS_URL=http://localhost:7272 \
NEMO_SKILLS_DISABLE_UNCOMMITTED_CHANGES_CHECK=1 \
ns eval --cluster <cluster> --expname "<project>_<campaign>_<run-details>" ...
```

Works with `ns`, `python -m nemo_skills.pipeline.cli`, experiment scripts, or programmatic `eval()` calls.

### What it does
- Runs appear on the board in **SUBMITTING** state immediately
- Transitions to **PENDING** when Slurm accepts the job
- If submission fails, auto-marks as **FAILED**
- Captures: exact command, git commit, hostname, working directory

### Environment variables
| Variable | Required | Purpose |
|----------|----------|---------|
| `CLAUSIUS_URL` | Yes | Clausius server URL for HTTP event push |
| `CLAUSIUS_TOKEN` | No | Bearer token if `sdk_ingest_token` is configured |
| `NEMO_SKILLS_DISABLE_UNCOMMITTED_CHANGES_CHECK` | Recommended | Skip git dirty check in NeMo-Skills |

---

## Modifying the App

Source lives at `~/clausius`. After changes to `server/`, `static/`, `templates/`, `app.py`, or `mcp_server.py`:

```bash
systemctl --user restart clausius.service
systemctl --user is-active clausius.service
```

Key architecture constraints when modifying:
- Keep on-demand — do NOT add background polling loops
- Use `db_connection()` context manager for DB access
- Frontend: guard fetches with `if (document.hidden) return;`
