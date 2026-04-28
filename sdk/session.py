# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Clausius SDK session: owns run identity, event sequencing, and delivery."""

from __future__ import annotations

import atexit
import ast
import logging
import os
import platform
import re
import shlex
import subprocess
import sys
import threading
import time
import uuid
from typing import Any

from nemo_skills.clausius_sdk.events import (
    Event,
    EventType,
    JobInfo,
    RunProvenance,
)
from nemo_skills.clausius_sdk.transports.base import Transport

LOG = logging.getLogger(__name__)

_BATCH_SIZE = 20
_FLUSH_INTERVAL_SEC = 10.0

_ENV_DENYLIST_SUBSTRINGS = {
    "SECRET", "TOKEN", "PASSWORD", "PASSWD", "CREDENTIAL", "API_KEY",
    "PRIVATE_KEY", "AUTH",
}
_ENV_DENYLIST_EXACT = {
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "GH_TOKEN", "GITHUB_TOKEN", "GITLAB_TOKEN", "HF_TOKEN",
    "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "NGC_API_KEY",
    "CLAUSIUS_TOKEN",
    "LS_COLORS", "LSCOLORS",
    "SSH_AUTH_SOCK", "SSH_AGENT_PID", "GPG_AGENT_INFO",
}
_ENV_MAX_VALUE_LEN = 1024


def _git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
            cwd=os.getcwd(),
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def _is_env_safe(key: str) -> bool:
    upper = key.upper()
    if upper in _ENV_DENYLIST_EXACT:
        return False
    return not any(s in upper for s in _ENV_DENYLIST_SUBSTRINGS)


def _safe_env_subset() -> dict[str, str]:
    out = {}
    for k, v in sorted(os.environ.items()):
        if not _is_env_safe(k):
            continue
        if len(v) > _ENV_MAX_VALUE_LEN:
            v = v[:_ENV_MAX_VALUE_LEN] + "…"
        out[k] = v
    return out


def _detect_conda_env() -> str:
    """Return active conda/venv environment name, or empty string."""
    if os.environ.get("CONDA_DEFAULT_ENV"):
        return os.environ["CONDA_DEFAULT_ENV"]
    venv = os.environ.get("VIRTUAL_ENV", "")
    if venv:
        return os.path.basename(venv)
    return ""


_SUBMIT_ENV_VARS = {
    "CLAUSIUS_URL",
    "CLAUSIUS_TOKEN",
    "NEMO_SKILLS_DISABLE_UNCOMMITTED_CHANGES_CHECK",
    "CUDA_VISIBLE_DEVICES",
    "NEMO_SKILLS_CONFIG",
    "HF_HOME",
    "WANDB_PROJECT",
}


def _detect_env_vars_set() -> list[str]:
    """Return env var assignments that were set at launch time (for reproducing)."""
    return [f"{k}={os.environ[k]}" for k in sorted(_SUBMIT_ENV_VARS) if k in os.environ]


_PARAMS_MAX_DEPTH = 3
_PARAMS_MAX_STR_LEN = 2048
_PARAMS_MAX_ITEMS = 64

_SECRET_PARAM_RE = re.compile(
    r"(^|[._-])(api[_-]?key|auth|bearer|credential|password|passwd|secret|token)($|[._-])",
    re.IGNORECASE,
)

_HYDRA_PARAM_PREFIXES = (
    "inference.",
    "server.",
    "sandbox.",
    "chat_template_kwargs.",
    "parallel_thinking.",
    "eval_config.",
    "tool_overrides.",
    "schema_overrides.",
)
_HYDRA_PARAM_KEYS = {
    "add_generation_stats",
    "code_execution",
    "count_prompt_tokens",
    "enable_litellm_cache",
    "end_reasoning_string",
    "examples_type",
    "generation_key",
    "max_concurrent_requests",
    "max_samples",
    "max_tool_calls",
    "parse_reasoning",
    "prompt_config",
    "prompt_format",
    "prompt_suffix",
    "skip_filled",
    "stop_phrase",
    "structured_output",
    "system_message",
    "total_code_executions_in_prompt",
}
_HYDRA_ALIAS_KEYS = {
    "inference.tokens_to_generate": "tokens_to_generate",
    "inference.temperature": "temperature",
    "inference.top_p": "top_p",
    "inference.top_k": "top_k",
    "inference.min_p": "min_p",
    "inference.random_seed": "random_seed",
    "inference.repetition_penalty": "repetition_penalty",
    "inference.top_logprobs": "top_logprobs",
    "inference.timeout": "inference_timeout",
    "inference.reasoning_effort": "reasoning_effort",
    "max_concurrent_requests": "max_concurrent_requests",
    "max_tool_calls": "max_tool_calls",
    "parse_reasoning": "parse_reasoning",
}
_CLI_STRING_OPTIONS = {
    "--extra-judge-args": "extra_judge_args",
    "--judge-server-args": "judge_server_args",
    "--server-args": "server_args",
}
_SERVER_ARG_KEYS = {
    "max-model-len": "max_model_len",
    "max-num-seqs": "max_num_seqs",
    "max-seq-len-to-capture": "max_seq_len_to_capture",
    "gpu-memory-utilization": "gpu_memory_utilization",
    "tensor-parallel-size": "tensor_parallel_size",
    "pipeline-parallel-size": "pipeline_parallel_size",
    "data-parallel-size": "data_parallel_size",
    "reasoning-parser": "reasoning_parser",
    "reasoning-parser-plugin": "reasoning_parser_plugin",
    "served-model-name": "served_model_name",
    "dtype": "dtype",
    "kv-cache-dtype": "kv_cache_dtype",
    "attention-backend": "attention_backend",
    "enable-chunked-prefill": "enable_chunked_prefill",
    "enable-expert-parallel": "enable_expert_parallel",
    "async-scheduling": "async_scheduling",
}


def _sanitize_params(raw: Any, depth: int = 0) -> Any:
    """Coerce pipeline kwargs into a JSON-safe shape with size limits.

    Drops non-primitive values that can't be represented (e.g. live objects,
    callables), truncates very long strings, and caps container sizes so a
    misbehaving caller can't blow up the event payload.
    """
    if raw is None or isinstance(raw, (bool, int, float)):
        return raw
    if isinstance(raw, str):
        if len(raw) > _PARAMS_MAX_STR_LEN:
            return raw[:_PARAMS_MAX_STR_LEN] + "…"
        return raw
    if depth >= _PARAMS_MAX_DEPTH:
        return str(raw)[:_PARAMS_MAX_STR_LEN]
    if isinstance(raw, dict):
        out: dict[str, Any] = {}
        for k, v in list(raw.items())[:_PARAMS_MAX_ITEMS]:
            if not isinstance(k, str):
                k = str(k)
            out[k] = _sanitize_params(v, depth + 1)
        return out
    if isinstance(raw, (list, tuple, set)):
        return [_sanitize_params(v, depth + 1) for v in list(raw)[:_PARAMS_MAX_ITEMS]]
    try:
        return str(raw)[:_PARAMS_MAX_STR_LEN]
    except Exception:
        return None


def _parse_scalar(value: str) -> Any:
    """Best-effort conversion of CLI strings into JSON-safe primitive values."""
    value = value.strip()
    if value == "":
        return value
    lower = value.lower()
    if lower in {"true", "false"}:
        return lower == "true"
    if lower in {"none", "null"}:
        return None
    try:
        return ast.literal_eval(value)
    except Exception:
        pass
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _split_cli_text(text: str) -> list[str]:
    if not text:
        return []
    try:
        return shlex.split(text)
    except ValueError:
        return text.split()


def _dedupe_args(args: list[str]) -> list[str]:
    out = []
    seen = set()
    for arg in args:
        marker = (len(out), arg) if arg.startswith("-") and "=" not in arg else arg
        if marker in seen:
            continue
        seen.add(marker)
        out.append(arg)
    return out


def _is_safe_param_key(key: str) -> bool:
    return not _SECRET_PARAM_RE.search(key)


def _should_capture_hydra_key(key: str) -> bool:
    if not _is_safe_param_key(key):
        return False
    return key in _HYDRA_PARAM_KEYS or any(key.startswith(prefix) for prefix in _HYDRA_PARAM_PREFIXES)


def _parse_hydra_overrides(args: list[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for arg in args:
        stripped = arg.lstrip("+")
        if "=" not in stripped or stripped == arg:
            continue
        key, value = stripped.split("=", 1)
        if _should_capture_hydra_key(key):
            overrides[key] = _parse_scalar(value)
    return overrides


def _extract_option_values(args: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    idx = 0
    while idx < len(args):
        arg = args[idx]
        opt, sep, inline_value = arg.partition("=")
        if opt in _CLI_STRING_OPTIONS:
            key = _CLI_STRING_OPTIONS[opt]
            if sep:
                values[key] = inline_value
            elif idx + 1 < len(args):
                values[key] = args[idx + 1]
                idx += 1
        idx += 1
    return values


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _parse_server_args(raw: Any) -> dict[str, Any]:
    parsed_by_model: list[dict[str, Any]] = []
    for item in _as_list(raw):
        if not item:
            continue
        tokens = _split_cli_text(str(item))
        parsed: dict[str, Any] = {}
        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            if not token.startswith("--"):
                idx += 1
                continue
            option, sep, inline_value = token[2:].partition("=")
            key = _SERVER_ARG_KEYS.get(option)
            if not key:
                idx += 1
                continue
            if sep:
                value: Any = _parse_scalar(inline_value)
            elif idx + 1 < len(tokens) and not tokens[idx + 1].startswith("--"):
                value = _parse_scalar(tokens[idx + 1])
                idx += 1
            else:
                value = True
            parsed[key] = value
            idx += 1
        if parsed:
            parsed_by_model.append(parsed)

    if not parsed_by_model:
        return {}
    merged: dict[str, Any] = {}
    for key in sorted({k for parsed in parsed_by_model for k in parsed}):
        values = [parsed.get(key) for parsed in parsed_by_model]
        merged[key] = values[0] if len(values) == 1 else values
    return merged


def _set_prefixed_params(params: dict[str, Any], prefix: str, values: dict[str, Any]) -> None:
    for key, value in values.items():
        params.setdefault(f"{prefix}.{key}", value)


def _enrich_submission_params(params: dict[str, Any], argv: list[str], command: str = "") -> dict[str, Any]:
    """Capture common generation/server submission knobs as first-class metadata.

    The raw command is already preserved in RunProvenance. This helper extracts
    the safe, high-signal knobs that users need on the run page for comparing
    submissions: output token caps, sampling params, concurrency, and server
    context/parser settings.
    """
    enriched = dict(params)
    args = _dedupe_args([*argv, *_split_cli_text(command)])
    cli_options = _extract_option_values(args)
    for key, value in cli_options.items():
        enriched.setdefault(key, value)

    hydra_overrides = _parse_hydra_overrides(args)
    for key, value in hydra_overrides.items():
        enriched.setdefault(key, value)
        alias = _HYDRA_ALIAS_KEYS.get(key)
        if alias:
            enriched.setdefault(alias, value)

    server_values = _parse_server_args(enriched.get("server_args"))
    _set_prefixed_params(enriched, "server", server_values)
    if "max_model_len" in server_values:
        enriched.setdefault("max_model_len", server_values["max_model_len"])
        enriched.setdefault("context_length", server_values["max_model_len"])
    if "reasoning_parser" in server_values:
        enriched.setdefault("reasoning_parser", server_values["reasoning_parser"])
    if "reasoning_parser_plugin" in server_values:
        enriched.setdefault("reasoning_parser_plugin", server_values["reasoning_parser_plugin"])

    judge_server_values = _parse_server_args(enriched.get("judge_server_args"))
    _set_prefixed_params(enriched, "judge.server", judge_server_values)
    if "max_model_len" in judge_server_values:
        enriched.setdefault("judge_context_length", judge_server_values["max_model_len"])

    judge_overrides = _parse_hydra_overrides(_split_cli_text(str(enriched.get("extra_judge_args") or "")))
    for key, value in judge_overrides.items():
        enriched.setdefault(f"judge.{key}", value)
        alias = _HYDRA_ALIAS_KEYS.get(key)
        if alias:
            enriched.setdefault(f"judge_{alias}", value)

    return enriched


class ClausiusSession:
    """Tracks a single NeMo-Skills run from launch through completion.

    Thread-safe. Events are buffered in memory and flushed periodically
    or on shutdown. Delivery failures never propagate exceptions to the
    caller; the worst case is silent event loss.
    """

    _instance: ClausiusSession | None = None
    _instance_lock = threading.Lock()

    def __init__(self, transports: list[Transport], run_uuid: str | None = None, seq_start: int = 0):
        self._run_uuid = run_uuid or uuid.uuid4().hex
        self._transports = list(transports)
        self._seq = max(0, int(seq_start or 0))
        self._lock = threading.Lock()
        self._buffer: list[Event] = []
        self._closed = False
        self._finished = False

        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        atexit.register(self._shutdown)

    @property
    def run_uuid(self) -> str:
        return self._run_uuid

    @property
    def active(self) -> bool:
        return not self._closed

    def _next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    _FLUSH_IMMEDIATELY = frozenset({"run_started", "run_finished", "run_failed"})

    def _emit(self, event_type: str | EventType, payload: dict[str, Any] | None = None):
        if self._closed:
            return
        etype = event_type.value if isinstance(event_type, EventType) else event_type
        ev = Event(
            run_uuid=self._run_uuid,
            event_type=etype,
            event_seq=self._next_seq(),
            payload=payload or {},
        )
        with self._lock:
            self._buffer.append(ev)
            if len(self._buffer) >= _BATCH_SIZE or etype in self._FLUSH_IMMEDIATELY:
                self._drain_buffer()

    def _drain_buffer(self):
        """Send buffered events to all transports. Called with self._lock held."""
        if not self._buffer:
            return
        batch = list(self._buffer)
        self._buffer.clear()
        for transport in self._transports:
            try:
                transport.send(batch)
            except Exception as exc:
                LOG.debug("clausius: transport %s failed: %s", type(transport).__name__, exc)

    def _flush_loop(self):
        while not self._closed:
            try:
                time.sleep(_FLUSH_INTERVAL_SEC)
            except Exception:
                return
            try:
                with self._lock:
                    self._drain_buffer()
            except Exception:
                return

    def _shutdown(self):
        if self._closed:
            return
        if not self._finished:
            try:
                self._emit(EventType.RUN_FAILED, {"error": "submission interrupted", "status": "submit_failed"})
            except Exception:
                pass
        self._closed = True
        try:
            with self._lock:
                self._drain_buffer()
            for t in self._transports:
                try:
                    t.flush()
                    t.close()
                except Exception:
                    pass
        except Exception:
            pass

    # ── Public API ────────────────────────────────────────────────────

    def emit_run_started(self, provenance: RunProvenance):
        self._emit(EventType.RUN_STARTED, provenance.to_dict())

    def emit_job_prepared(self, job: JobInfo):
        self._emit(EventType.JOB_PREPARED, job.to_dict())

    def emit_job_submitted(self, job: JobInfo):
        self._emit(EventType.JOB_SUBMITTED, job.to_dict())

    def emit_job_state(self, slurm_job_id: str, state: str, **extra):
        self._emit(EventType.JOB_STATE, {"slurm_job_id": slurm_job_id, "state": state, **extra})

    def log_metric(self, key: str, value: Any, step: int | None = None, **context):
        payload: dict[str, Any] = {"key": key, "value": value}
        if step is not None:
            payload["step"] = step
        if context:
            payload["context"] = context
        self._emit(EventType.METRIC_LOGGED, payload)

    def log_scalar(self, key: str, value: Any, **context):
        payload: dict[str, Any] = {"key": key, "value": value}
        if context:
            payload["context"] = context
        self._emit(EventType.SCALAR_LOGGED, payload)

    def log_params(self, params: dict[str, Any]):
        for k, v in params.items():
            self.log_scalar(f"param.{k}", v)

    def log_metadata(self, metadata: dict[str, Any]):
        self._emit(EventType.METADATA_LOGGED, {"metadata": _sanitize_params(metadata)})

    def log_artifact(self, name: str, path: str, **metadata):
        self._emit(EventType.ARTIFACT_LOGGED, {"name": name, "path": path, **metadata})

    def finish(self, status: str = "completed", **extra):
        self._finished = True
        self._emit(EventType.RUN_FINISHED, {"status": status, **extra})
        self._shutdown()

    def fail(self, error: str = "", **extra):
        self._finished = True
        self._emit(EventType.RUN_FAILED, {"error": error, **extra})
        self._shutdown()

    def close(self):
        """Flush and close the session without emitting a terminal event."""
        self._finished = True
        self._shutdown()

    # ── Factory / singleton ───────────────────────────────────────────

    @classmethod
    def start_from_cli(
        cls,
        expname: str,
        command: str = "",
        output_dir: str = "",
        cluster: str = "",
        config_overrides: dict | None = None,
        params: dict | None = None,
    ) -> ClausiusSession:
        """Create or return the global session, capturing launch provenance."""
        with cls._instance_lock:
            if cls._instance is not None and cls._instance.active:
                return cls._instance

        transports = _build_transports(output_dir)
        session = cls(transports)
        cls._instance = session

        full_command = " ".join(sys.argv)
        provenance = RunProvenance(
            argv=sys.argv[:],
            command=full_command,
            cwd=os.getcwd(),
            expname=expname,
            output_dir=output_dir,
            git_commit=_git_sha(),
            hostname=platform.node(),
            env_subset=_safe_env_subset(),
            cluster=cluster,
            config_overrides=config_overrides or {},
            conda_env=_detect_conda_env(),
            python_executable=sys.executable,
            env_vars_set=_detect_env_vars_set(),
            params=_sanitize_params(_enrich_submission_params(params or {}, sys.argv[1:], command)),
        )
        session.emit_run_started(provenance)
        return session

    @classmethod
    def get(cls) -> ClausiusSession | None:
        """Return the current session or None if tracking is disabled."""
        return cls._instance

    @classmethod
    def reset(cls):
        """Tear down the global session. Primarily for testing."""
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance._shutdown()
            cls._instance = None


def _build_transports(output_dir: str = "") -> list[Transport]:
    """Construct transports from environment variables."""
    transports: list[Transport] = []

    url = os.environ.get("CLAUSIUS_URL", "")
    token = os.environ.get("CLAUSIUS_TOKEN", "")
    if url:
        from nemo_skills.clausius_sdk.transports.http import HttpTransport

        transports.append(HttpTransport(url=url, token=token))

    spool_dir = os.environ.get("CLAUSIUS_SPOOL_DIR", "")
    if not spool_dir and output_dir:
        spool_dir = os.path.join(output_dir, ".clausius")
    if spool_dir:
        try:
            from nemo_skills.clausius_sdk.transports.file_spool import FileSpoolTransport

            transports.append(FileSpoolTransport(os.path.join(spool_dir, "events.jsonl")))
        except OSError:
            LOG.debug("clausius: spool dir %s not writable, skipping file transport", spool_dir)

    if not transports:
        LOG.debug("clausius: no transports configured; tracking is a no-op")

    return transports
