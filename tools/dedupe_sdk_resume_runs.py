#!/usr/bin/env python3
"""Merge SDK run pairs that share a ``primary_output_dir``.

Companion to ``dedupe_legacy_runs.py``. That tool handles legacy↔SDK pairs
sharing a ``run_uuid``. This tool handles a different failure mode: TWO
``source='sdk'`` rows with DIFFERENT uuids that ended up describing the
same logical experiment because the server's resume detection
(``_find_resume_canonical_run``) requires ``cluster + run_name + output_dir``
to match exactly. When the cluster name drifts between submissions (e.g.
``aws-cmh-science`` vs ``aws-iad``) or the expname picks up a NeMo-Skills
prefix/suffix (e.g. ``mcpv2lt_eval-r1`` vs ``mcp_mcpv2lt_eval-r1-hle``),
the resume miss creates a duplicate row.

The new SDK-side ``ClausiusSession.start_from_cli`` lookup
(``/api/sdk/resolve_run``) prevents this going forward. This script cleans
up the existing dupes.

Strategy
--------
For every pair of SDK rows that share a non-empty ``primary_output_dir``:

1. Pick the **older** row (lowest ``id``) as canonical — it has the original
   submission's provenance and any accumulated metrics.
2. Insert a row in ``sdk_run_aliases`` mapping the newer row's uuid to the
   canonical uuid, so ``resolve_run_uuid()`` and the metric query family
   helpers stitch the events together transparently.
3. Copy any provenance fields the canonical row is missing (empty string)
   from the dup so we don't lose data the dup row uniquely had.
4. Repoint ``job_history.run_id`` from the dup row to the canonical row.
5. Delete the dup row from ``runs``.

The script is **idempotent** and **dry-run by default** — pass ``--apply``
to actually mutate the DB.

Usage:

    python3 tools/dedupe_sdk_resume_runs.py            # dry-run
    python3 tools/dedupe_sdk_resume_runs.py --apply    # apply
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
DEFAULT_DB = HERE / "data" / "history.db"

SDK_PROVENANCE_FIELDS = [
    "run_name",
    "project",
    "submit_command",
    "submit_cwd",
    "git_commit",
    "launcher_hostname",
    "params_json",
    "metadata_json",
    "env_vars",
    "batch_script",
    "scontrol_raw",
    "conda_state",
    "sdk_status",
    "started_at",
    "ended_at",
    "notes",
]


def find_dup_clusters(con: sqlite3.Connection):
    """Return rows grouped by primary_output_dir where >1 SDK row exists."""
    rows = con.execute(
        """
        SELECT id, cluster, run_name, run_uuid, root_job_id, primary_output_dir,
               COALESCE(started_at, created_at, '') AS sort_ts
        FROM runs
        WHERE source = 'sdk'
          AND run_uuid IS NOT NULL AND run_uuid != ''
          AND primary_output_dir IS NOT NULL
          AND rtrim(primary_output_dir, '/') != ''
        ORDER BY rtrim(primary_output_dir, '/') ASC, id ASC
        """
    ).fetchall()

    by_dir: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        key = (r["primary_output_dir"] or "").rstrip("/")
        if not key:
            continue
        by_dir.setdefault(key, []).append(r)
    return {k: v for k, v in by_dir.items() if len(v) >= 2}


def existing_alias(con: sqlite3.Connection, alias_uuid: str) -> str | None:
    row = con.execute(
        "SELECT canonical_uuid FROM sdk_run_aliases WHERE alias_uuid = ?",
        (alias_uuid,),
    ).fetchone()
    return row["canonical_uuid"] if row else None


def merge_group(con: sqlite3.Connection, group: list[sqlite3.Row], apply: bool):
    """Merge a group of duplicate SDK rows into the oldest one."""
    canonical = group[0]
    dups = group[1:]
    canonical_uuid = canonical["run_uuid"]
    actions: list[str] = []

    for dup in dups:
        dup_uuid = dup["run_uuid"]
        if dup_uuid == canonical_uuid:
            actions.append(f"  - skip dup id={dup['id']} (same uuid as canonical)")
            continue

        actions.append(
            f"  - merge dup id={dup['id']:6d}  cluster={dup['cluster']:16s} "
            f"uuid={dup_uuid[:12]}  run_name='{dup['run_name']}'"
        )

        if apply:
            con.execute(
                """INSERT OR IGNORE INTO sdk_run_aliases
                       (alias_uuid, canonical_uuid, reason)
                   VALUES (?, ?, ?)""",
                (dup_uuid, canonical_uuid, "dedupe-output-dir"),
            )
            sets: list[str] = []
            values: list = []
            for field in SDK_PROVENANCE_FIELDS:
                canonical_val = canonical[field] if field in canonical.keys() else None
                dup_val = dup[field] if field in dup.keys() else None
                if (not canonical_val) and dup_val:
                    sets.append(f"{field} = ?")
                    values.append(dup_val)
            if sets:
                values.append(canonical["id"])
                con.execute(
                    f"UPDATE runs SET {', '.join(sets)} WHERE id = ?", values
                )
            con.execute(
                "UPDATE job_history SET run_id = ? WHERE run_id = ?",
                (canonical["id"], dup["id"]),
            )
            con.execute("DELETE FROM runs WHERE id = ?", (dup["id"],))

    return actions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to history.db")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually apply changes (default: dry-run)",
    )
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    groups = find_dup_clusters(con)

    if not groups:
        print("No SDK duplicate clusters found.")
        return 0

    total_dups = sum(len(g) - 1 for g in groups.values())
    print(f"Found {len(groups)} output_dir cluster(s) with {total_dups} duplicate row(s):\n")

    for output_dir, group in groups.items():
        canonical = group[0]
        print(f"output_dir: {output_dir}")
        print(
            f"  KEEP canonical id={canonical['id']:6d}  cluster={canonical['cluster']:16s} "
            f"uuid={canonical['run_uuid'][:12]}  run_name='{canonical['run_name']}'"
        )
        actions = merge_group(con, group, apply=args.apply)
        for line in actions:
            print(line)
        print()

    if not args.apply:
        print("Dry run only. Re-run with --apply to merge.")
        return 0

    con.commit()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
