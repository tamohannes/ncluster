#!/usr/bin/env python3
"""One-shot importer: v3 conf/config.json -> v4 SQLite tables.

Usage:
    python tools/import_legacy_config.py [path/to/config.json]

Defaults to ``conf/config.json`` relative to the project root. After a
successful import, renames the source file to ``config.json.bak`` so
the v4 bootstrap code never finds it again.

This is a convenience wrapper around ``python -m server.cli import-json``
that also handles the rename. Safe to re-run — skips entries that
already exist in the DB.
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from server.cli import main  # noqa: E402


def run():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("conf", "config.json")
    if not os.path.isfile(path):
        print(f"error: {path} not found — nothing to import", file=sys.stderr)
        return 1

    print(f"Importing {path} ...")
    rc = main(["import-json", path])
    if rc != 0:
        return rc

    bak = path + ".bak"
    if not os.path.exists(bak):
        os.rename(path, bak)
        print(f"Renamed {path} -> {bak}")
    else:
        print(f"{bak} already exists — original file left in place")

    print("\nDone. Restart the service to pick up the imported config:\n"
          "  systemctl --user restart clausius.service")
    return 0


if __name__ == "__main__":
    sys.exit(run())
