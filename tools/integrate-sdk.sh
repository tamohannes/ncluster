#!/bin/bash
# Integrate the Clausius SDK into a NeMo-Skills checkout.
#
# Usage:
#   ~/clausius/tools/integrate-sdk.sh ~/workspace/hle/NeMo-Skills-pr-hle-answer-extraction-v2
#   ~/clausius/tools/integrate-sdk.sh ~/workspace/artsiv/hovo/NeMo-Skills
#
# What it does:
#   1. Copies the clausius_sdk package from the reference checkout
#   2. Injects SDK hooks into pipeline entrypoints (idempotent — safe to run multiple times)
#   3. Patches exp.py with monitor wrapper and env var injection

set -euo pipefail

TARGET="${1:?Usage: $0 <path-to-nemo-skills-checkout>}"
REFERENCE="$HOME/workspace/profiling/NeMo-Skills"

if [ ! -d "$TARGET/nemo_skills/pipeline" ]; then
    echo "Error: $TARGET does not look like a NeMo-Skills checkout"
    exit 1
fi

echo "=== Clausius SDK Integration ==="
echo "  Target:    $TARGET"
echo "  Reference: $REFERENCE"
echo ""

# ── Step 1: Copy SDK package ─────────────────────────────────────────
echo "[1/3] Copying clausius_sdk package..."
rm -rf "$TARGET/nemo_skills/clausius_sdk"
cp -r "$REFERENCE/nemo_skills/clausius_sdk" "$TARGET/nemo_skills/clausius_sdk"
# Remove __pycache__ from copied package
find "$TARGET/nemo_skills/clausius_sdk" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
echo "  Copied $(find "$TARGET/nemo_skills/clausius_sdk" -name "*.py" | wc -l) Python files."

# ── Step 2: Inject hooks into CLI entrypoints ─────────────────────────
echo "[2/3] Injecting SDK hooks into pipeline entrypoints..."

# Sentinel-bounded hook block. One uniform block works for every pipeline
# entrypoint because we reflect over locals() to capture whatever subset of
# the known pipeline kwargs is in scope (model, benchmarks, num_samples,
# judge_model, …). When we re-run this script we delete anything between
# the sentinels and inject a fresh copy so the hook is always current.
HOOK_START_SENTINEL='# CLAUSIUS_SDK_HOOK_START'
HOOK_END_SENTINEL='# CLAUSIUS_SDK_HOOK_END'
HOOK_BLOCK='
    # CLAUSIUS_SDK_HOOK_START
    try:
        from nemo_skills.clausius_sdk.hooks import maybe_start_session
        _cl_local = locals()
        _cl_keys = (
            "model", "server_type", "server_gpus", "server_nodes", "server_args",
            "benchmarks", "split", "num_samples", "num_chunks", "with_sandbox",
            "judge_model", "judge_server_type", "judge_server_gpus", "judge_server_args",
            "dependent_jobs", "input_file", "input_dir", "dataset",
            "prompt_format", "prompt_config", "prompt_template",
            "preprocess_cmd", "sandbox_container", "container",
        )
        _cl_ctx = _cl_local.get("ctx")
        maybe_start_session(
            expname=expname,
            command=" ".join(getattr(_cl_ctx, "args", []) or []),
            output_dir=_cl_local.get("output_dir") or "",
            cluster=cluster or "",
            params={k: _cl_local[k] for k in _cl_keys if k in _cl_local},
        )
    except Exception:
        pass
    # CLAUSIUS_SDK_HOOK_END
'

inject_hook() {
    local file="$1"
    local after_pattern="$2"

    if [ ! -f "$file" ]; then
        echo "  SKIP (not found): $file"
        return
    fi
    if ! grep -q "$after_pattern" "$file" 2>/dev/null; then
        echo "  SKIP (pattern not found): $(basename "$file")"
        return
    fi

    # Strip any existing hook block (sentinel-bounded OR the legacy
    # pre-sentinel "# ── Clausius SDK: start tracking session" form), then
    # inject a fresh copy. This keeps the script idempotent across SDK
    # upgrades — users re-run integrate-sdk.sh after pulling/rebasing.
    FILE_PATH="$file" \
        AFTER_PATTERN="$after_pattern" \
        HOOK_START="$HOOK_START_SENTINEL" \
        HOOK_END="$HOOK_END_SENTINEL" \
        HOOK_BLOCK="$HOOK_BLOCK" \
        python3 <<'PY'
import os, re, sys
path = os.environ["FILE_PATH"]
after = os.environ["AFTER_PATTERN"]
start = os.environ["HOOK_START"]
end = os.environ["HOOK_END"]
block = os.environ["HOOK_BLOCK"]
content = open(path).read()
# Drop sentinel-bounded hook if present.
content = re.sub(
    r"\n[ \t]*" + re.escape(start) + r"[\s\S]*?" + re.escape(end) + r"[ \t]*\n",
    "\n",
    content,
)
# Drop legacy pre-sentinel hook (marker: the banner comment).
content = re.sub(
    r"\n[ \t]*# ── Clausius SDK: start tracking session[\s\S]*?except Exception:\s*\n[ \t]*pass\s*\n",
    "\n",
    content,
)
idx = content.find(after)
if idx < 0:
    sys.exit(0)
end_of_line = content.index("\n", idx)
new_content = content[: end_of_line + 1] + block + content[end_of_line + 1 :]
open(path, "w").write(new_content)
PY
    echo "  DONE: $(basename "$file")"
}

ENTRYPOINTS=(
    "run_cmd.py"
    "eval.py"
    "generate.py"
    "convert.py"
    "prepare_data.py"
    "nemo_evaluator.py"
    "megatron_lm/train.py"
    "nemo_rl/sft.py"
    "nemo_rl/grpo.py"
    "verl/ppo.py"
)

for ep in "${ENTRYPOINTS[@]}"; do
    inject_hook "$TARGET/nemo_skills/pipeline/$ep" "setup_logging(disable_hydra_logs="
done

# ── Step 3: Patch exp.py ──────────────────────────────────────────────
echo "[3/3] Patching exp.py..."
EXP_FILE="$TARGET/nemo_skills/pipeline/utils/exp.py"

if [ ! -f "$EXP_FILE" ]; then
    echo "  SKIP: exp.py not found"
else
    # Copy the clausius functions from reference if not already present
    if grep -q "_clausius_monitor_wrap" "$EXP_FILE" 2>/dev/null; then
        echo "  exp.py already has monitor wrapper. Updating..."
        # Extract and replace the clausius functions block
        python3 -c "
ref = open('$REFERENCE/nemo_skills/pipeline/utils/exp.py').read()
tgt = open('$EXP_FILE').read()

# Find clausius block in reference
start_marker = 'def _clausius_env_vars():'
end_marker = '# TODO: this function has become too cumbersome'
ref_start = ref.index(start_marker)
ref_end = ref.index(end_marker)
ref_block = ref[ref_start:ref_end]

# Replace in target
tgt_start = tgt.index(start_marker)
tgt_end = tgt.index(end_marker)
new_tgt = tgt[:tgt_start] + ref_block + tgt[tgt_end:]
open('$EXP_FILE', 'w').write(new_tgt)
print('  Updated clausius functions block.')
" 2>/dev/null || echo "  Could not update existing block."
    else
        # Insert clausius functions before the add_task function
        python3 -c "
import re
ref = open('$REFERENCE/nemo_skills/pipeline/utils/exp.py').read()
tgt = open('$EXP_FILE').read()

# Extract clausius block from reference
start_marker = 'def _clausius_env_vars():'
end_marker = '# TODO: this function has become too cumbersome'
ref_start = ref.index(start_marker)
ref_end = ref.index(end_marker)
ref_block = ref[ref_start:ref_end]

# Insert before the TODO comment in target
if end_marker in tgt:
    tgt_insert = tgt.index(end_marker)
    new_tgt = tgt[:tgt_insert] + ref_block + tgt[tgt_insert:]
    open('$EXP_FILE', 'w').write(new_tgt)
    print('  Inserted clausius functions block.')
else:
    print('  Could not find insertion point in exp.py. Manual patching needed.')
" 2>/dev/null || echo "  Could not insert block."
    fi

    # Wire _clausius_env_vars() and _clausius_monitor_wrap() into add_task()
    if ! grep -q 'env_updates.update(_clausius_env_vars())' "$EXP_FILE" 2>/dev/null; then
        python3 -c "
tgt = open('$EXP_FILE').read()
old = '''            with temporary_env_update(cluster_config, {\"NEMO_SKILLS_SANDBOX_PORT\": sandbox_port}):
                cur_cmd = install_packages_wrap(cur_cmd, installation_command)
                commands.append(cur_cmd)'''
new = '''            env_updates = {\"NEMO_SKILLS_SANDBOX_PORT\": sandbox_port}
            env_updates.update(_clausius_env_vars())
            with temporary_env_update(cluster_config, env_updates):
                cur_cmd = install_packages_wrap(cur_cmd, installation_command)
                cur_cmd = _clausius_monitor_wrap(cur_cmd)
                commands.append(cur_cmd)'''
if old in tgt:
    tgt = tgt.replace(old, new, 1)
    open('$EXP_FILE', 'w').write(tgt)
    print('  Wired _clausius_env_vars + _clausius_monitor_wrap into add_task().')
else:
    print('  add_task() wiring pattern not found (may already be wired or different version).')
" 2>/dev/null || echo "  Could not wire add_task() automatically."
    else
        echo "  add_task() env injection already wired."
    fi

    # Inject on_task_prepared hook in add_task if not present
    if ! grep -q "on_task_prepared" "$EXP_FILE" 2>/dev/null; then
        python3 -c "
tgt = open('$EXP_FILE').read()
# Find the exp.add call pattern and inject before it
marker = 'if len(commands) == 1:'
if marker in tgt:
    idx = tgt.index(marker)
    hook = '''    # ── Clausius SDK: emit job_prepared ─────────────────────────────
    try:
        from nemo_skills.clausius_sdk.hooks import on_task_prepared
        on_task_prepared(
            task_name=task_name,
            cluster=cluster_config.get(\"executor\", \"\"),
            partition=partition or cluster_config.get(\"partition\", \"\"),
            account=account or cluster_config.get(\"account\", \"\"),
            num_nodes=num_nodes,
            num_gpus=num_gpus,
            num_tasks=num_tasks[0] if isinstance(num_tasks, list) else num_tasks,
            container=containers[0] if containers else \"\",
            dependencies=[str(d) for d in (task_dependencies or [])],
        )
    except Exception:
        pass

'''
    new_tgt = tgt[:idx] + hook + tgt[idx:]
    open('$EXP_FILE', 'w').write(new_tgt)
    print('  Injected on_task_prepared hook.')
" 2>/dev/null || echo "  Could not inject on_task_prepared."
    else
        echo "  on_task_prepared hook already present."
    fi

    # Inject on_run_submitted hook in run_exp if not present
    if ! grep -q "on_run_submitted" "$EXP_FILE" 2>/dev/null; then
        python3 -c "
tgt = open('$EXP_FILE').read()
marker = 'REUSE_CODE_EXP[cur_tunnel_hash] = exp'
if marker in tgt:
    idx = tgt.index(marker) + len(marker)
    hook = '''

    # ── Clausius SDK: emit job_submitted ──────────────────────────
    try:
        from nemo_skills.clausius_sdk.hooks import on_run_submitted
        on_run_submitted(cluster=cluster_config.get(\"executor\", \"\"), dry_run=dry_run)
    except Exception:
        pass'''
    new_tgt = tgt[:idx] + hook + tgt[idx:]
    open('$EXP_FILE', 'w').write(new_tgt)
    print('  Injected on_run_submitted hook.')
" 2>/dev/null || echo "  Could not inject on_run_submitted."
    else
        echo "  on_run_submitted hook already present."
    fi

    echo "  exp.py patching complete."
fi

# ── Summary ───────────────────────────────────────────────────────────
echo ""
FOUND=$(grep -rl "clausius_sdk" "$TARGET/nemo_skills/pipeline/" 2>/dev/null | wc -l)
SDK_FILES=$(find "$TARGET/nemo_skills/clausius_sdk" -name "*.py" 2>/dev/null | wc -l)
echo "=== Integration complete ==="
echo "  SDK package:  $SDK_FILES Python files"
echo "  Hooked files: $FOUND pipeline files"
echo ""
echo "To verify, run:"
echo "  cd $TARGET"
echo "  CLAUSIUS_URL=http://localhost:7272 \\"
echo "  python -m nemo_skills.pipeline.cli run_cmd \\"
echo "    --cluster eos --expname test_sdk-verify \\"
echo "    --num-gpus 0 --num-nodes 1 \\"
echo '    "echo sdk-ok" --dry-run'
