#!/usr/bin/env python3
"""One-off cleanup: merge SDK shadow rows into their legacy duplicates.

Many runs ended up with TWO rows in the ``runs`` table — one ``source='legacy'``
created by the Slurm poller (e.g. ``aws-cmh / 218056``) and one
``source='sdk'`` created by the SDK ingest endpoint (e.g.
``aws-cmh-science / sdk-1349546dff06``). They share the same ``run_uuid`` so
they describe the same logical run, but the runs legend and context table
treat them as distinct.

This script merges each pair by:

1. Copying SDK-only provenance (submit_command, submit_cwd, git_commit,
   launcher_hostname, primary_output_dir, params_json, metadata_json,
   env_vars, sdk_status) onto the legacy row when the legacy field is
   empty/null.
2. Marking the legacy row ``source='sdk'`` so the run is treated as a
   first-class SDK-tracked run (it has SDK provenance now).
3. Deleting the synthetic SDK shadow row.

Metrics (``run_metrics`` / ``run_scalars``) are keyed by ``run_uuid`` — both
rows already share that, so no metric rows need to move.

Usage:

    python3 tools/dedupe_legacy_runs.py            # dry-run: prints what would change
    python3 tools/dedupe_legacy_runs.py --apply    # actually merges and deletes
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
DEFAULT_DB = HERE / "data" / "history.db"

# Fields the SDK row contributes when not already set on the legacy row.
SDK_PROVENANCE_FIELDS = [
    "submit_command",
    "submit_cwd",
    "git_commit",
    "launcher_hostname",
    "primary_output_dir",
    "params_json",
    "metadata_json",
    "env_vars",
    "batch_script",
    "scontrol_raw",
    "conda_state",
    "sdk_status",
]


def find_pairs(con: sqlite3.Connection):
    """Return [(legacy_row, sdk_row), ...] for runs that share run_uuid."""
    return con.execute(
        """
        SELECT
            legacy.id          AS legacy_id,
            legacy.cluster     AS legacy_cluster,
            legacy.root_job_id AS legacy_root_job_id,
            legacy.run_name    AS legacy_run_name,
            legacy.source      AS legacy_source,
            sdk.id             AS sdk_id,
            sdk.cluster        AS sdk_cluster,
            sdk.root_job_id    AS sdk_root_job_id,
            sdk.run_name       AS sdk_run_name,
            legacy.run_uuid    AS run_uuid
        FROM runs legacy
        JOIN runs sdk
          ON legacy.run_uuid = sdk.run_uuid
         AND legacy.id != sdk.id
        WHERE legacy.run_uuid IS NOT NULL AND legacy.run_uuid != ''
          AND legacy.source != 'sdk'
          AND sdk.source = 'sdk'
        """
    ).fetchall()


def merge_pair(con: sqlite3.Connection, legacy_id: int, sdk_id: int):
    """Copy SDK provenance into the legacy row and delete the SDK row."""
    legacy = con.execute("SELECT * FROM runs WHERE id = ?", (legacy_id,)).fetchone()
    sdk = con.execute("SELECT * FROM runs WHERE id = ?", (sdk_id,)).fetchone()
    if not legacy or not sdk:
        return

    sets = []
    values = []
    for field in SDK_PROVENANCE_FIELDS:
        legacy_val = legacy[field] if field in legacy.keys() else None
        sdk_val = sdk[field] if field in sdk.keys() else None
        if (not legacy_val) and sdk_val:
            sets.append(f"{field} = ?")
            values.append(sdk_val)
    sets.append("source = 'sdk'")
    if sets:
        values.append(legacy_id)
        con.execute(f"UPDATE runs SET {', '.join(sets)} WHERE id = ?", values)

    # Move any job_history rows pointing at the synthetic SDK row over to
    # the legacy row (typically none — the SDK row has no Slurm jobs).
    con.execute(
        "UPDATE job_history SET run_id = ? WHERE run_id = ?",
        (legacy_id, sdk_id),
    )
    # Delete the SDK shadow row.
    con.execute("DELETE FROM runs WHERE id = ?", (sdk_id,))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to history.db")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes (default: dry-run)")
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    pairs = find_pairs(con)

    if not pairs:
        print("No duplicates found. Nothing to do.")
        return 0

    print(f"Found {len(pairs)} duplicate pair(s):")
    for p in pairs:
        print(
            f"  KEEP legacy  id={p['legacy_id']}  {p['legacy_cluster']:18s} "
            f"root={p['legacy_root_job_id']}  uuid={p['run_uuid']}  '{p['legacy_run_name']}'"
        )
        print(
            f"  DROP sdk     id={p['sdk_id']}  {p['sdk_cluster']:18s} "
            f"root={p['sdk_root_job_id']}  uuid={p['run_uuid']}  '{p['sdk_run_name']}'"
        )

    if not args.apply:
        print()
        print("Dry run only. Re-run with --apply to merge SDK provenance into the")
        print("legacy rows and delete the SDK shadow rows.")
        return 0

    print()
    print(f"Applying {len(pairs)} merges…")
    for p in pairs:
        merge_pair(con, p["legacy_id"], p["sdk_id"])
    con.commit()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
