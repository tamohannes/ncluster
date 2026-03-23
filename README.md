# ncluster

Multi-cluster Slurm dashboard with AI agent integration via MCP.

Monitor, explore, and manage GPU jobs across HPC clusters from a single browser tab — or let your AI coding agent do it through the built-in MCP server.

## Architecture

```
                  ┌─────────────────────────────────┐
                  │           Browser UI             │
                  │  Live Board · Explorer · History  │
                  │  Projects · Logbooks · Settings   │
                  └──────────────┬──────────────────┘
                                 │ HTTP
  ┌──────────┐    ┌──────────────▼──────────────────┐
  │ MCP Agent│◄──►│         Flask Backend            │
  │ (Cursor) │    │   REST API · SQLite · Caches     │
  └──────────┘    └──────┬───────────┬───────────────┘
                         │           │
              ┌──────────▼──┐  ┌─────▼───────────┐
              │ Login Nodes  │  │ Data-Copier Nodes│
              │ squeue/sacct │  │  File I/O (logs, │
              │ scancel      │  │  dirs, JSONL)    │
              └──────────────┘  └─────────────────┘
                  Cluster A        Cluster A
                  Cluster B        Cluster B
                  Cluster N        Cluster N
```

Three-lane SSH connection pool: **primary** (Slurm control), **background** (metadata), **data** (file I/O routed to data-copier nodes with automatic login-node fallback).

## Features

### Live Board
- Multi-cluster job board grouped by run name (active, idle, unreachable, local)
- Slurm dependency chain detection with topological sorting
- Live progress tracking, crash detection (OOM, segfault, traceback)
- Cluster availability tooltip with wait-time estimates, pending reason translation, and team fair-share priority
- Board-pinned terminal jobs persist until dismissed

### Log Explorer
- Mount-first reads with SSH fallback to data-copier nodes
- Nested directory browsing with lazy-loaded tree
- Syntax-aware rendering for `.json`, `.jsonl`, `.jsonl-async`, `.md`
- Full log pagination, JSONL record viewer, clipboard copy

### History and Projects
- SQLite-backed job history with dependency-aware grouping
- Auto-detected projects from job name prefixes
- Per-project detail pages with live jobs, stats, and search
- Per-project logbooks with `@run-name` autocomplete and markdown rendering

### MCP Server (AI Agent API)
- Stdio-based MCP server for Cursor and other MCP-compatible agents
- 25+ tools: job listing, log reading, stats, history, run metadata, script execution, cancellation, logbooks, cluster availability, storage quotas
- `run_script()` — execute Python/bash on a cluster and return stdout/stderr
- `cancel_project_jobs()` / `cancel_all_cluster_jobs()` — bulk cancellation
- No SSH, no DB access — wraps the Flask API cleanly

### Performance
- On-demand architecture: clusters are only contacted when a user or agent requests data
- Three-lane SSH connection pool with data-copier node routing
- Per-cluster caching with configurable TTL
- Prefetch warming for running jobs (log index, content, stats)
- No background polling — login nodes are not contacted when nobody is looking

## Quick Start

```bash
git clone https://github.com/tamohannes/ncluster.git
cd ncluster
pip install flask paramiko
cp conf/config.example.json conf/config.json  # edit with your cluster details
python app.py
```

Open [http://localhost:7272](http://localhost:7272)

### MCP Server Setup

```bash
pip install mcp
```

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "ncluster": {
      "command": "python3",
      "args": ["/path/to/ncluster/mcp_server.py"]
    }
  }
}
```

Reload Cursor to activate. Requires the Flask app to be running.

## Configuration

### config.json

Primary configuration file. Editable from the UI Settings panel or directly.

```json
{
  "port": 7272,
  "clusters": {
    "my-cluster": {
      "host": "login-node.example.com",
      "data_host": "dc-node.example.com",
      "port": 22,
      "gpu_type": "H100"
    }
  },
  "log_search_bases": ["/lustre/.../users/$USER"],
  "nemo_run_bases": ["/lustre/.../users/$USER/nemo-run"],
  "mount_lustre_prefixes": ["lustre/fsw/..."],
  "local_process_filters": {
    "include": ["nemo-skills", "python -m nemo_skills"],
    "exclude": ["cursor", "jupyter"]
  },
  "ssh_timeout": 8,
  "cache_fresh_sec": 30
}
```

The optional `data_host` routes file-explorer I/O to a data-copier node, reducing login-node load. Falls back to `host` when omitted or unreachable.

### Environment Variables

- `JOB_MONITOR_SSH_USER` (default: `$USER`)
- `JOB_MONITOR_SSH_KEY` (default: `~/.ssh/id_ed25519`)
- `JOB_MONITOR_MOUNT_MAP` (JSON map of cluster -> mount roots)

## Job Name Prefix Protocol

Jobs are grouped by project using a name prefix convention:

```
<project>_<run-name>
```

| Component | Rules | Example |
|-----------|-------|---------|
| `<project>` | Lowercase letters, digits, hyphens. Starts with a letter. | `artsiv`, `hle`, `nemo-rl` |
| `_` | Required underscore separator | |
| `<run-name>` | The experiment/eval name | `eval-math`, `train-v3` |

The monitor auto-detects projects on first encounter, assigning a color and emoji. Customize in Settings > Projects.

Dependency chain auto-detection from run name suffixes:
- `*-judge-rs<N>` — linked as child of the base eval
- `*-summarize-results` — linked as child of the judge run

## API Endpoints

### Jobs & Clusters

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/jobs` | All clusters with jobs, mounts, dependency info |
| GET | `/api/jobs/<cluster>` | Force-refresh one cluster |
| GET | `/api/run_info/<cluster>/<root_job_id>` | Run metadata (batch script, env, conda/pip, scontrol) |
| GET | `/api/history?cluster=&limit=&project=` | Job history, filterable by cluster and project |
| GET | `/api/projects` | All known projects with job counts |

### Logs & Files

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/log_files/<cluster>/<job_id>` | Discover log files |
| GET | `/api/log/<cluster>/<job_id>?path=&lines=` | Read log content (tail) |
| GET | `/api/log_full/<cluster>/<job_id>?path=&page=` | Full log with pagination |
| GET | `/api/ls/<cluster>?path=` | Directory listing |
| GET | `/api/jsonl_index/<cluster>/<job_id>?path=&mode=` | JSONL file index |
| GET | `/api/jsonl_record/<cluster>/<job_id>?path=&line=` | Single JSONL record |

### Stats & Prefetch

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/stats/<cluster>/<job_id>` | Job resource stats (GPU/CPU/memory) |
| POST | `/api/prefetch_visible` | Prefetch log index, content, and stats for visible jobs |
| POST | `/api/progress` | Batch-fetch progress for multiple jobs |

### Actions

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/cancel/<cluster>/<job_id>` | Cancel a single job |
| POST | `/api/cancel_jobs/<cluster>` | Cancel multiple jobs (JSON body: `job_ids`) |
| POST | `/api/cancel_all/<cluster>` | Cancel all jobs on cluster |
| POST | `/api/run_script/<cluster>` | Run script on cluster via SSH |
| POST | `/api/clear_failed/<cluster>` | Dismiss all failed pins |
| POST | `/api/clear_completed/<cluster>` | Dismiss completed pins |
| POST | `/api/cleanup` | Delete old history records |

### Mounts & Settings

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/mounts` | Mount status |
| POST | `/api/mount/<action>/<cluster>` | Mount/unmount one cluster |
| GET | `/api/settings` | Current configuration |
| POST | `/api/settings` | Update configuration (hot-reload) |

### Logbooks

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/logbooks/<project>` | List all logbooks for a project |
| POST | `/api/logbook/<project>` | Create a new logbook |
| GET | `/api/logbook/<project>/<name>` | Read logbook content and entries |
| POST | `/api/logbook/<project>/<name>` | Add entry |
| DELETE | `/api/logbook/<project>/<name>` | Delete a logbook |
| PUT | `/api/logbook/<project>/<name>/<index>` | Update entry at index |
| DELETE | `/api/logbook/<project>/<name>/<index>` | Delete entry at index |
| POST | `/api/logbook/<project>/<name>/rename` | Rename logbook |

## Systemd (User Service)

```ini
[Unit]
Description=ncluster
After=network.target

[Service]
Type=simple
WorkingDirectory=%h/ncluster
ExecStart=%h/miniconda3/bin/python %h/ncluster/app.py
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now ncluster.service
```

## Testing

348 tests across unit, integration, MCP, frontend, and live layers.

```bash
pip install pytest pytest-cov
pytest -m "not live"         # all deterministic tests (no SSH, no cluster)
pytest -m unit               # unit tests only
pytest -m integration        # Flask test client with mock cluster
pytest -m mcp                # MCP tool contracts
pytest -m live               # real cluster tests (requires running app)
```

| Layer | Directory | Count | What it covers |
|-------|-----------|-------|----------------|
| Unit | `tests/unit/` | 133 | Parsers, DB ops, cache, mount resolution, config |
| Integration | `tests/integration/` | 69 | All Flask routes via test client |
| MCP | `tests/mcp/` | 38 | Tool contracts, transport errors, edge cases |
| Frontend | `tests/frontend/` | -- | JS utils, log renderers (Vitest) |
| E2E | `tests/e2e/` | -- | Dashboard, explorer, settings (Playwright) |
| Live | `tests/live/` | 19 | Real SSH/Slurm reads + job cancel |

CI runs without `config.json` — falls back to `config.example.json` with a mock cluster.

## Built With

- **Backend**: Python, Flask, Paramiko, SQLite
- **Frontend**: Vanilla JS, CSS custom properties (no build step)
- **Agent API**: MCP (Model Context Protocol)
- **Infrastructure**: SSH connection pooling, SSHFS mounts, systemd

## License

MIT
