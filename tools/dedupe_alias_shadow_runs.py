#!/usr/bin/env python3
"""Remove "alias-shadow" duplicate runs.

These are ``runs`` rows whose ``run_uuid`` is registered as an alias in
``sdk_run_aliases`` — i.e. an SDK resume already aliased that uuid to a
canonical row, but a separate ``runs`` row still exists for the alias uuid
and shows up as a distinct selectable run in the UI.

Typical origin
--------------
1. User submitted run-r2. SDK created canonical row with uuid ``A``.
2. User resubmitted as resume. SDK minted fresh uuid ``B``. Server's
   ``_find_resume_canonical_run`` matched ``(cluster, run_name, output_dir)``
   and registered alias ``B → A``. So far so good — no duplicate row.
3. The resubmission produced a new Slurm job with a new ``root_job_id``.
   The poller (back when legacy run creation was still enabled) inserted a
   ``source='legacy'`` row for that job. Then ``_capture_run_metadata``
   parsed ``CLAUSIUS_RUN_UUID=B`` out of the batch script and stamped uuid
   ``B`` onto that legacy row.

Result: a shadow ``runs`` row with the same logical identity as the
canonical, showing the same metric data because ``get_run_metrics()``
resolves both uuids to the same canonical via ``sdk_run_aliases``.

The new ``ClausiusSession.start_from_cli`` resume-aware lookup
(``/api/sdk/resolve_run``) prevents this going forward — the SDK now reuses
the canonical uuid from the very first event. This script is the one-off
cleanup for the existing shadow rows.

Strategy
--------
For each ``(shadow, canonical)`` pair:

1. Repoint any ``job_history.run_id`` rows that point at the shadow row to
   the canonical row's id.
2. Copy provenance fields from the shadow to the canonical row only when the
   canonical's field is empty (don't clobber canonical's data).
3. Delete the shadow ``runs`` row.

Run metrics, scalars, and events are keyed by ``run_uuid`` — they already
resolve correctly via the existing alias entry, so no row migration is
needed there.

Usage
-----

    python3 tools/dedupe_alias_shadow_runs.py            # dry-run
    python3 tools/dedupe_alias_shadow_runs.py --apply    # apply
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
DEFAULT_DB = HERE / "data" / "history.db"

# Fields the shadow row may have that the canonical row could benefit from
# (only copied if canonical field is empty / NULL).
COPYABLE_FIELDS = [
    "batch_script",
    "scontrol_raw",
    "env_vars",
    "conda_state",
    "submit_command",
    "submit_cwd",
    "git_commit",
    "launcher_hostname",
    "primary_output_dir",
    "notes",
]


def find_shadow_pairs(con: sqlite3.Connection):
    """Return [(shadow_row, canonical_row)] for every shadow we can resolve."""
    rows = con.execute(
        """
        SELECT
            shadow.id          AS shadow_id,
            shadow.cluster     AS shadow_cluster,
            shadow.root_job_id AS shadow_root_job_id,
            shadow.run_name    AS shadow_run_name,
            shadow.source      AS shadow_source,
            shadow.run_uuid    AS shadow_uuid,
            canonical.id       AS canonical_id,
            canonical.cluster  AS canonical_cluster,
            canonical.run_name AS canonical_run_name,
            canonical.run_uuid AS canonical_uuid
        FROM runs shadow
        JOIN sdk_run_aliases a ON a.alias_uuid = shadow.run_uuid
        JOIN runs canonical    ON canonical.run_uuid = a.canonical_uuid
        WHERE shadow.id != canonical.id
        ORDER BY shadow.run_name, shadow.id
        """
    ).fetchall()
    return rows


def merge_pair(con: sqlite3.Connection, pair: sqlite3.Row, apply: bool):
    shadow_id = pair["shadow_id"]
    canonical_id = pair["canonical_id"]

    shadow = con.execute("SELECT * FROM runs WHERE id = ?", (shadow_id,)).fetchone()
    canonical = con.execute("SELECT * FROM runs WHERE id = ?", (canonical_id,)).fetchone()
    if not shadow or not canonical:
        return f"  - SKIP shadow={shadow_id} canonical={canonical_id} (row vanished)"

    sets: list[str] = []
    values: list = []
    for field in COPYABLE_FIELDS:
        if field not in shadow.keys() or field not in canonical.keys():
            continue
        canonical_val = canonical[field]
        shadow_val = shadow[field]
        if (not canonical_val) and shadow_val:
            sets.append(f"{field} = ?")
            values.append(shadow_val)

    if apply:
        if sets:
            values.append(canonical_id)
            con.execute(
                f"UPDATE runs SET {', '.join(sets)} WHERE id = ?", values
            )
        con.execute(
            "UPDATE job_history SET run_id = ? WHERE run_id = ?",
            (canonical_id, shadow_id),
        )
        con.execute("DELETE FROM runs WHERE id = ?", (shadow_id,))

    field_note = f" (+copy {','.join(s.split(' ')[0] for s in sets)})" if sets else ""
    return (
        f"  - merge shadow id={shadow_id:6d}  cluster={pair['shadow_cluster']:16s} "
        f"src={pair['shadow_source']:7s} uuid={pair['shadow_uuid'][:12]} "
        f"→ canonical id={canonical_id}{field_note}"
    )


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
    pairs = find_shadow_pairs(con)

    if not pairs:
        print("No alias-shadow runs found.")
        return 0

    print(f"Found {len(pairs)} alias-shadow run(s):\n")
    for pair in pairs:
        canonical_label = (
            f"canonical id={pair['canonical_id']:6d}  "
            f"cluster={pair['canonical_cluster']:16s} uuid={pair['canonical_uuid'][:12]} "
            f"name='{pair['canonical_run_name']}'"
        )
        print(canonical_label)
        line = merge_pair(con, pair, apply=args.apply)
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
