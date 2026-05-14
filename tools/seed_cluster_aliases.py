#!/usr/bin/env python3
"""Idempotent seed: NeMo-Skills cluster YAML names -> Clausius canonical names.

Why this script exists
----------------------
NeMo-Skills cluster YAML files use account/variant-encoded names (e.g.
``aws-cmh-science.yaml``) that don't match the physical cluster Clausius
polls Slurm against (``aws-cmh``). When the SDK emits a ``run_started``
event with the YAML name, the synthetic row would land on a virtual
cluster nothing else writes to, leaving the run perpetually "PENDING"
even after Slurm finishes the real job.

The fix is to teach Clausius the alias map. Each canonical cluster row
gets an ``aliases`` list; the resolver in ``server/clusters.py`` and the
``/api/cluster_resolve`` endpoint use that list to normalize incoming
names back to the canonical cluster. This script seeds the initial map.

Usage
-----
    python tools/seed_cluster_aliases.py

Safe to re-run. Only updates aliases for clusters that are already
registered in the DB; unknown clusters are skipped with a notice so
operators see them and can register the cluster first. Existing alias
entries on a row are preserved — new entries are merged in (union).
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from server.clusters import get_cluster, update_cluster  # noqa: E402
from server.db import init_db  # noqa: E402


# Canonical Clausius cluster -> list of NeMo-Skills YAML basenames that
# should resolve to it. Add new entries here when a project ships a new
# alias variant; safe to re-run.
NEMO_SKILLS_ALIASES: dict[str, list[str]] = {
    "aws-cmh": ["aws-cmh-science"],
    "aws-dfw": ["aws-dfw-science"],
    "aws-iad": ["aws-iad-science"],
    "dfw":     ["dfw-longctx", "dfw-reasoning", "dfw-science"],
    "eos":     ["eos-longctx", "eos-robustness", "eos-science", "eos-science-pipfix"],
    "hsg":     ["hsg-longctx"],
    "iad":     ["iad-robustness", "iad-science"],
    "ord":     ["ord-robustness", "ord-science"],
}


def _merge_aliases(existing: list[str], new: list[str]) -> list[str]:
    """Return the union of ``existing`` and ``new`` preserving order."""
    out: list[str] = list(existing)
    seen = set(existing)
    for alias in new:
        if alias not in seen:
            out.append(alias)
            seen.add(alias)
    return out


def run() -> int:
    init_db()

    updated = 0
    skipped_no_change = 0
    missing_clusters: list[str] = []
    errors: list[str] = []

    for canonical, new_aliases in NEMO_SKILLS_ALIASES.items():
        record = get_cluster(canonical)
        if record is None:
            missing_clusters.append(canonical)
            continue
        existing = list(record.get("aliases") or [])
        merged = _merge_aliases(existing, new_aliases)
        if merged == existing:
            skipped_no_change += 1
            continue
        result = update_cluster(canonical, aliases=merged)
        if result.get("status") == "ok":
            updated += 1
            added = [a for a in merged if a not in existing]
            print(f"  {canonical:<10} <- aliases += {added}")
        else:
            errors.append(f"{canonical}: {result.get('error')}")

    print()
    print(f"Updated:  {updated}")
    print(f"No-op:    {skipped_no_change}")
    if missing_clusters:
        print(f"Missing clusters (skip): {sorted(missing_clusters)}")
    if errors:
        print("Errors:")
        for line in errors:
            print(f"  {line}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
