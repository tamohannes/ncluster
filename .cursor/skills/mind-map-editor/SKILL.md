---
name: mind-map-editor
description: Safely edit a Clausius per-campaign mind_map (the single source of truth for the campaign). Use whenever the user asks to add/remove/update nodes or edges, flip a node's status, mark a run as done/failed/abandoned, branch off a new exploration, or otherwise mutate the per-campaign mind map. ALWAYS load this skill before calling `patch_mind_map`, `update_mind_map`, `create_mind_map`, or `convert_campaign_board_to_mind_map`.
---

# Mind Map Editor — Consult-Before-Edit Protocol

The `mind_map` entry in Clausius is the **single source of truth** for everything happening inside a campaign — tasks, bugs, failures, successful runs, decisions. Edits are team-visible and load-bearing. This skill enforces a strict, repeatable protocol so agents never mutate the mind map without explicit, informed user consent.

The full editing rules live in `~/.cursor/rules/mind-map.mdc`. This skill is the **step-by-step runbook** that fires whenever an edit is imminent.

## When this skill fires

Trigger on **any** of:

- User asks to add a task / bug / experiment / decision to a campaign.
- User reports a run finished (success, failure, regression) and asks to "record it" or "update the board / map".
- User wants to change a node's status, summary, or description.
- User wants to branch off a follow-up or mark a path abandoned.
- User asks you to convert a legacy `campaign_board` into a `mind_map`.
- User asks you to create a new mind map for a campaign.

If you find yourself about to call `patch_mind_map`, `update_mind_map`, `create_mind_map`, or `convert_campaign_board_to_mind_map` — stop and run this skill first.

## The seven-step runbook

Follow every step in order. Skipping a step is a bug.

### 1. Read first

Always call:

```python
get_mind_map(project="<project>", campaign="<campaign>")
```

- If it returns `not_found`, check whether a legacy `campaign_board` exists: `get_campaign_board(project, campaign)`. If so, propose `convert_campaign_board_to_mind_map` to the user **before** doing anything else (steps 2–4 still apply).
- Cache the returned `graph_json` in your head: node ids, statuses, edges, current `active` set.

### 2. Restate the user's intent in one paragraph

Before listing patch ops, paraphrase what you understood from the user. Example:

> "You're saying the GPT-OSS native tool-calling integration just passed CI on `aws-iad`, so I should mark node `gpt-oss-fix` as `done` and add the next planned step — running the MCPv2 eval suite against the integrated model — as a new `active` node connected by a success edge."

This catches misunderstandings before any DB writes happen.

### 3. List the exact patch ops you intend to apply

In a single bulleted list, one bullet per op, in the order they will be applied. Use the `patch_mind_map` op vocabulary verbatim:

- `add_node {id, title, status, summary?, description?}`
- `update_node {id, patch}`
- `remove_node {id}`
- `add_edge {id, from, to, kind, label?}`
- `update_edge {id, patch}`
- `remove_edge {id}`
- `set_status {id, status}`

Example output:

> Proposed ops on `mcp-tools/mcpv2`:
> 1. `set_status` — node `gpt-oss-fix` → `done`
> 2. `add_node` — `gpt-oss-eval` (title "Run MCPv2 eval suite vs integrated GPT-OSS", status `active`, summary "no-tool baseline first; 5 seeds on GPQA")
> 3. `add_edge` — `e3` from `gpt-oss-fix` to `gpt-oss-eval`, kind `success`, label "after CI green"

### 4. Wait for explicit user confirmation

A word like "yes", "go", "do it", "apply", or "confirmed". **Don't infer consent from "ok"-sounding earlier turns.** If the user hedges ("maybe", "actually I'm not sure"), refine the proposal and ask again.

### 5. Apply the ops in a single `patch_mind_map` call

```python
patch_mind_map(
    project="<project>",
    campaign="<campaign>",
    ops=[
        {"op": "set_status", "id": "gpt-oss-fix", "status": "done"},
        {"op": "add_node", "node": {"id": "gpt-oss-eval", "title": "...", "status": "active", "summary": "..."}},
        {"op": "add_edge", "edge": {"id": "e3", "from": "gpt-oss-fix", "to": "gpt-oss-eval", "kind": "success", "label": "after CI green"}},
    ],
)
```

- Keep the op count to the minimum needed for this one user-confirmed change. If the user describes two distinct changes, do two `patch_mind_map` calls so each is reviewable.
- The PATCH is atomic — if any op fails validation, none are persisted.

### 6. Re-read with `get_mind_map`

Confirm the new state landed:

```python
get_mind_map(project="<project>", campaign="<campaign>")
```

### 7. Report the diff back to the user

In one short paragraph, summarize: which nodes changed status, which were added/removed, which edges were added/removed. Use node ids and titles. Example:

> "Updated `mcp-tools/mcpv2`:
> - `gpt-oss-fix` → done
> - New active node `gpt-oss-eval` ("Run MCPv2 eval suite vs integrated GPT-OSS")
> - Success edge from `gpt-oss-fix` → `gpt-oss-eval` ("after CI green")."

## Refusals and safety rails

Refuse — and explain why — when the user asks for any of the following without going through steps 2–4 of this runbook:

- "Just delete the whole graph and redo it" → split into removals + additions over multiple `patch_mind_map` calls; require a confirmed proposal at each step.
- "Update every node to status X" → propose the specific list of nodes affected first; only proceed after the user signs off on the exact list.
- "Sync the mind map automatically from the run database" → never do this. The user owns status changes.
- "Add ten nodes describing the last week of work" → propose them as a numbered list first; agree on the wording before any `add_node` call.

## When creating a new mind map

If `get_mind_map` returns `not_found` and there is no legacy board to convert:

1. Discuss with the user what 3–7 nodes the initial graph should contain (tasks, current active work, known blockers). Don't propose 20+ nodes on day one.
2. Propose them as a list, wait for confirmation, then:

```python
create_mind_map(
    project="<project>",
    campaign="<campaign>",
    title="...",
    campaign_goal="One-paragraph plain-text goal",
    graph_json={"version": 1, "nodes": [...], "edges": []},
)
```

3. Re-read with `get_mind_map` and report back.

## When converting a legacy `campaign_board`

```python
convert_campaign_board_to_mind_map(project="<project>", campaign="<campaign>")
```

- Idempotent — returns `{"status": "exists", "existing_id": ...}` if a mind map already exists.
- Copies the board's `body` and `campaign_goal` into a fresh empty mind_map; **leaves the original board intact**.
- After conversion, propose an initial set of nodes to the user based on the structured `board_json` tables and the body markdown. Do **not** auto-translate every table cell to a node — the user picks which entries are worth tracking in the graph.

## Reminders

- The graph is **NOT** auto-derived from runs in the SQLite DB. Status flips are user-owned.
- Don't change node `id`s after creation — they're how subsequent patches reference nodes. Use `update_node` with a `patch` containing `title` / `summary` / `description` instead.
- Multiple `active` nodes are fine. Multiple `failed` nodes are fine. The graph captures dead-ends too.
- When a node represents an experiment with live metrics, embed AimQL inside the description as a fenced ` ```aimql ` block so the popover gets an "Open in Metrics ↗" button. See the `aimql` rule for query syntax.
