"""Command-line interface for clausius v4 administration.

Run as ``python -m server.cli <subcommand> [args]`` or, after install,
just ``clausius <subcommand>``. Replaces the v3 "edit conf/config.json"
workflow with safe per-namespace commands that go through the same CRUD
layer the UI and MCP tools use.

Examples
--------

    # Bootstrap a fresh install
    python -m server.cli setup

    # Add your first cluster
    python -m server.cli add-cluster my-cluster \\
        --host login-node.example.com \\
        --gpu-type H100 --gpus-per-node 8 \\
        --account my_ppp_acct \\
        --mount-path /shared/storage/$USER

    # List clusters
    python -m server.cli list-clusters

    # Tweak a setting
    python -m server.cli set ssh_timeout 10
    python -m server.cli get ssh_timeout

    # Add team members + PPP accounts
    python -m server.cli add-team-member alice
    python -m server.cli add-ppp my_ppp_acct --id 10595

    # Manage path lists
    python -m server.cli add-path --kind log_search /lustre/$USER/logs

Every command exits with status 0 on success, non-zero on validation /
runtime error. Errors are printed to stderr.
"""

from __future__ import annotations

import argparse
import json as _json
import os
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence


def _print_json(obj: Any) -> None:
    """Pretty-print a JSON-serialisable result to stdout."""
    print(_json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True))


def _fail(msg: str, code: int = 1) -> int:
    print(f"error: {msg}", file=sys.stderr)
    return code


def _check(result: Dict[str, Any]) -> int:
    """Convert a CRUD ``{"status": "ok"|"error", ...}`` dict into an exit code.

    Prints the success payload on stdout and the error message on stderr.
    """
    if result.get("status") == "ok":
        _print_json(result)
        return 0
    return _fail(result.get("error", "unknown error"))


# ─── setup ───────────────────────────────────────────────────────────────────

def cmd_setup(args: argparse.Namespace) -> int:
    """Initialise a fresh data directory and DB.

    Creates the bootstrap data dir (``data_dir`` from
    ``conf/clausius.toml`` or ``CLAUSIUS_DATA_DIR``), runs every CREATE
    statement in :mod:`server.schema`, and offers an interactive
    walk-through for the first cluster + team setup. Idempotent — safe
    to re-run on an existing install.
    """
    from .bootstrap import get_bootstrap, reset_bootstrap

    reset_bootstrap()
    boot = get_bootstrap()
    print(f"clausius v4 setup")
    print(f"  data dir   : {boot.data_dir}")
    print(f"  db file    : {boot.db_path}")
    print(f"  port       : {boot.port}")
    print(f"  ssh user   : {boot.ssh_user}")
    print(f"  ssh key    : {boot.ssh_key}")
    print(f"  bootstrap  : {boot.source_file or '(defaults — no clausius.toml)'}")

    os.makedirs(boot.data_dir, exist_ok=True)
    os.makedirs(boot.backups_dir, exist_ok=True)
    os.makedirs(boot.logbook_images_dir, exist_ok=True)

    from .db import init_db
    init_db()
    print(f"\nschema initialised at {boot.db_path}")

    if args.non_interactive:
        return 0

    print("\nNext steps (skip with Ctrl+C; you can run these later):\n")

    # Cluster prompt
    if _prompt_yes_no("Add your first cluster now?", default=False):
        name = input("  cluster name (e.g. dfw): ").strip()
        host = input("  login host: ").strip()
        if name and host:
            from .clusters import add_cluster
            gpu_type = input("  gpu type (e.g. H100, B200) [optional]: ").strip()
            gpus = input("  gpus per node [8]: ").strip() or "8"
            account = input("  default Slurm account [optional]: ").strip()
            mount = input("  remote mount path with $USER (e.g. /lustre/$USER) [optional]: ").strip()
            result = add_cluster(
                name,
                host=host,
                gpu_type=gpu_type or "",
                gpus_per_node=int(gpus),
                account=account or "",
                mount_paths=[mount] if mount else None,
            )
            if result.get("status") == "ok":
                print(f"  -> added cluster {name!r}")
            else:
                print(f"  -> ERROR: {result.get('error')}", file=sys.stderr)

    if _prompt_yes_no("Set your team name now?", default=False):
        team = input("  team name: ").strip()
        if team:
            from .settings import set_setting
            set_setting("team_name", team)
            print(f"  -> team_name set to {team!r}")

    print("\nDone. Start the server with:  python app.py")
    return 0


def _prompt_yes_no(prompt: str, *, default: bool) -> bool:
    """Tiny y/n prompt helper. Returns ``default`` on EOF (non-tty)."""
    suffix = " [Y/n] " if default else " [y/N] "
    try:
        ans = input(prompt + suffix).strip().lower()
    except EOFError:
        return default
    if not ans:
        return default
    return ans in ("y", "yes")


# ─── clusters ────────────────────────────────────────────────────────────────

def cmd_list_clusters(args: argparse.Namespace) -> int:
    from .clusters import list_clusters
    rows = list_clusters(include_local=False, only_enabled=False)
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        print("(no clusters registered — `clausius add-cluster <name> --host ...`)")
        return 0
    width = max(len(c["name"]) for c in rows)
    for c in rows:
        flag = "" if c["enabled"] else "  [disabled]"
        print(f"{c['name']:<{width}}  {c['host']:<35}  gpu={c['gpu_type'] or '-':<6}  "
              f"per_node={c['gpus_per_node']}  acct={c['account'] or '-'}{flag}")
    return 0


def cmd_add_cluster(args: argparse.Namespace) -> int:
    from .clusters import add_cluster
    result = add_cluster(
        args.name,
        host=args.host,
        data_host=args.data_host or "",
        port=args.port,
        ssh_user=args.ssh_user or "",
        ssh_key=args.ssh_key or "",
        account=args.account or "",
        gpu_type=args.gpu_type or "",
        gpu_mem_gb=args.gpu_mem_gb or 0,
        gpus_per_node=args.gpus_per_node or 0,
        aihub_name=args.aihub_name or "",
        mount_paths=args.mount_path or None,
        team_gpu_alloc=args.team_gpu_alloc or "",
    )
    return _check(result)


def cmd_remove_cluster(args: argparse.Namespace) -> int:
    from .clusters import remove_cluster
    return _check(remove_cluster(args.name))


def cmd_show_cluster(args: argparse.Namespace) -> int:
    from .clusters import get_cluster
    cluster = get_cluster(args.name)
    if cluster is None:
        return _fail(f"cluster {args.name!r} not found")
    _print_json(cluster)
    return 0


# ─── team ───────────────────────────────────────────────────────────────────

def cmd_list_team(args: argparse.Namespace) -> int:
    from .team import list_team_members
    rows = list_team_members()
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        print("(no team members)")
        return 0
    for m in rows:
        suffix = f" — {m['display_name']}" if m["display_name"] else ""
        print(f"  {m['username']}{suffix}")
    return 0


def cmd_add_team_member(args: argparse.Namespace) -> int:
    from .team import add_team_member
    return _check(add_team_member(
        args.username,
        display_name=args.display_name or "",
        email=args.email or "",
    ))


def cmd_remove_team_member(args: argparse.Namespace) -> int:
    from .team import remove_team_member
    return _check(remove_team_member(args.username))


# ─── PPP accounts ───────────────────────────────────────────────────────────

def cmd_list_ppp(args: argparse.Namespace) -> int:
    from .team import list_ppp_accounts
    rows = list_ppp_accounts()
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        print("(no PPP accounts)")
        return 0
    for a in rows:
        idstr = f" id={a['ppp_id']}" if a["ppp_id"] else ""
        descstr = f" — {a['description']}" if a["description"] else ""
        print(f"  {a['name']}{idstr}{descstr}")
    return 0


def cmd_add_ppp(args: argparse.Namespace) -> int:
    from .team import add_ppp_account
    return _check(add_ppp_account(
        args.name,
        ppp_id=args.id or "",
        description=args.description or "",
    ))


def cmd_remove_ppp(args: argparse.Namespace) -> int:
    from .team import remove_ppp_account
    return _check(remove_ppp_account(args.name))


# ─── path lists ─────────────────────────────────────────────────────────────

def cmd_list_paths(args: argparse.Namespace) -> int:
    from .paths import PATH_KINDS, list_path_bases
    rows = list_path_bases(kind=args.kind)
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        if args.kind:
            print(f"(no {args.kind} paths)")
        else:
            print(f"(no paths registered; kinds: {', '.join(PATH_KINDS)})")
        return 0
    for p in rows:
        print(f"  [{p['kind']}] {p['path']}")
    return 0


def cmd_add_path(args: argparse.Namespace) -> int:
    from .paths import add_path_base
    return _check(add_path_base(args.kind, args.path))


def cmd_remove_path(args: argparse.Namespace) -> int:
    from .paths import remove_path_base
    return _check(remove_path_base(args.kind, args.path))


def cmd_list_filters(args: argparse.Namespace) -> int:
    from .paths import FILTER_MODES, list_process_filters
    rows = list_process_filters(mode=args.mode)
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        if args.mode:
            print(f"(no {args.mode} patterns)")
        else:
            print(f"(no process filters; modes: {', '.join(FILTER_MODES)})")
        return 0
    for f in rows:
        print(f"  [{f['mode']}] {f['pattern']}")
    return 0


def cmd_add_filter(args: argparse.Namespace) -> int:
    from .paths import add_process_filter
    return _check(add_process_filter(args.mode, args.pattern))


def cmd_remove_filter(args: argparse.Namespace) -> int:
    from .paths import remove_process_filter
    return _check(remove_process_filter(args.mode, args.pattern))


# ─── app_settings ───────────────────────────────────────────────────────────

def cmd_settings(args: argparse.Namespace) -> int:
    from .settings import list_settings
    settings = list_settings()
    if args.json:
        _print_json(settings)
        return 0
    width = max(len(k) for k in settings) if settings else 0
    for k in sorted(settings):
        entry = settings[k]
        marker = "*" if entry["source"] != "default" else " "
        print(f"{marker} {k:<{width}}  {entry['value']!r:<25}  {entry['description']}")
    if width:
        print("\n(* = overridden, otherwise registered default)")
    return 0


def cmd_get(args: argparse.Namespace) -> int:
    from .settings import get_setting
    value = get_setting(args.key)
    if value is None and args.key not in _all_setting_keys():
        return _fail(f"unknown setting key {args.key!r}")
    if args.json:
        _print_json(value)
    else:
        print(value)
    return 0


def cmd_set(args: argparse.Namespace) -> int:
    from .settings import set_setting
    parsed = _parse_value(args.value)
    return _check(set_setting(args.key, parsed))


def cmd_unset(args: argparse.Namespace) -> int:
    from .settings import delete_setting
    return _check(delete_setting(args.key))


def _all_setting_keys() -> List[str]:
    from .schema import APP_SETTINGS_DEFAULTS
    return list(APP_SETTINGS_DEFAULTS.keys())


def _parse_value(raw: str) -> Any:
    """Parse a CLI value into JSON-compatible Python.

    Tries JSON first (so ``true``, ``42``, ``["a","b"]`` all work), falls
    back to the raw string.
    """
    try:
        return _json.loads(raw)
    except _json.JSONDecodeError:
        return raw


# ─── import-json (one-shot legacy importer) ─────────────────────────────────

def cmd_import_json(args: argparse.Namespace) -> int:
    """Import a v3 ``conf/config.json`` into the v4 tables.

    Used once during the v3 -> v4 migration. Skips fields that already
    exist in the DB so re-runs are safe. Prints a per-namespace summary.
    """
    from .clusters import add_cluster, get_cluster
    from .paths import add_path_base, add_process_filter
    from .settings import set_setting
    from .team import add_ppp_account, add_team_member, get_ppp_account, get_team_member

    if not os.path.isfile(args.path):
        return _fail(f"file not found: {args.path}")
    with open(args.path) as fh:
        cfg = _json.load(fh)

    summary: Dict[str, int] = {
        "clusters_added": 0, "clusters_skipped": 0,
        "team_members_added": 0, "team_members_skipped": 0,
        "ppp_added": 0, "ppp_skipped": 0,
        "paths_added": 0, "filters_added": 0, "settings_set": 0,
    }

    # Clusters
    team_allocs = cfg.get("team_gpu_allocations", {}) or {}
    for name, c in (cfg.get("clusters") or {}).items():
        if get_cluster(name) is not None:
            summary["clusters_skipped"] += 1
            continue
        result = add_cluster(
            name,
            host=c.get("host") or "",
            data_host=c.get("data_host") or "",
            port=c.get("port") or 22,
            ssh_user=c.get("user") or "",
            ssh_key=c.get("key") or "",
            account=c.get("account") or "",
            gpu_type=c.get("gpu_type") or "",
            gpu_mem_gb=c.get("gpu_mem_gb") or 0,
            gpus_per_node=c.get("gpus_per_node") or 0,
            aihub_name=c.get("aihub_name") or "",
            mount_paths=c.get("mount_paths") or [],
            mount_aliases=c.get("mount_aliases") or {},
            team_gpu_alloc=str(team_allocs.get(name, "")) if team_allocs.get(name) is not None else "",
        )
        if result.get("status") == "ok":
            summary["clusters_added"] += 1
        else:
            print(f"  cluster {name!r}: {result.get('error')}", file=sys.stderr)

    # Team members
    for username in cfg.get("team_members") or []:
        if get_team_member(username) is not None:
            summary["team_members_skipped"] += 1
            continue
        result = add_team_member(username)
        if result.get("status") == "ok":
            summary["team_members_added"] += 1
        else:
            print(f"  team_member {username!r}: {result.get('error')}", file=sys.stderr)

    # PPP accounts
    ppps = cfg.get("ppps") or {}
    for name in cfg.get("ppp_accounts") or []:
        if get_ppp_account(name) is not None:
            summary["ppp_skipped"] += 1
            continue
        result = add_ppp_account(name, ppp_id=str(ppps.get(name, "")))
        if result.get("status") == "ok":
            summary["ppp_added"] += 1

    # Paths
    for kind_in_json, kind in (
        ("log_search_bases", "log_search"),
        ("nemo_run_bases", "nemo_run"),
        ("mount_lustre_prefixes", "mount_lustre_prefix"),
    ):
        for p in cfg.get(kind_in_json) or []:
            result = add_path_base(kind, p)
            if result.get("status") == "ok":
                summary["paths_added"] += 1

    # Process filters
    pf = cfg.get("local_process_filters") or {}
    for mode in ("include", "exclude"):
        for pattern in pf.get(mode) or []:
            result = add_process_filter(mode, pattern)
            if result.get("status") == "ok":
                summary["filters_added"] += 1

    # Scalar settings
    scalar_keys = (
        ("team", "team_name"),
        ("aihub_opensearch_url", "aihub_opensearch_url"),
        ("dashboard_url", "dashboard_url"),
        ("aihub_cache_ttl_sec", "aihub_cache_ttl_sec"),
        ("wds_snapshot_interval_sec", "wds_snapshot_interval_sec"),
        ("ssh_timeout", "ssh_timeout"),
        ("cache_fresh_sec", "cache_fresh_sec"),
        ("stats_interval_sec", "stats_interval_sec"),
        ("backup_interval_hours", "backup_interval_hours"),
        ("backup_max_keep", "backup_max_keep"),
        ("sdk_ingest_token", "sdk_ingest_token"),
    )
    for source_key, dest_key in scalar_keys:
        if source_key in cfg:
            result = set_setting(dest_key, cfg[source_key])
            if result.get("status") == "ok":
                summary["settings_set"] += 1

    _print_json({"status": "ok", "imported": summary})
    return 0


# ─── parser ──────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="clausius",
        description="clausius v4 administration CLI. Manage clusters, team "
                    "members, PPP accounts, search paths, process filters, "
                    "and runtime tunables.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # setup
    p = sub.add_parser("setup", help="Initialise data dir + DB schema; optional interactive walk-through.")
    p.add_argument("--non-interactive", action="store_true",
                   help="Skip the interactive cluster/team prompts.")
    p.set_defaults(func=cmd_setup)

    # clusters
    p = sub.add_parser("list-clusters", help="List registered clusters.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list_clusters)

    p = sub.add_parser("show-cluster", help="Show full record for one cluster.")
    p.add_argument("name")
    p.set_defaults(func=cmd_show_cluster)

    p = sub.add_parser("add-cluster", help="Register a new cluster.")
    p.add_argument("name")
    p.add_argument("--host", required=True)
    p.add_argument("--data-host", default="")
    p.add_argument("--port", type=int, default=22)
    p.add_argument("--ssh-user", default="")
    p.add_argument("--ssh-key", default="")
    p.add_argument("--account", default="")
    p.add_argument("--gpu-type", default="")
    p.add_argument("--gpu-mem-gb", type=int, default=0)
    p.add_argument("--gpus-per-node", type=int, default=0)
    p.add_argument("--aihub-name", default="")
    p.add_argument("--mount-path", action="append", default=[],
                   help="Remote mount path (use $USER as placeholder). Repeatable.")
    p.add_argument("--team-gpu-alloc", default="",
                   help="Informal team GPU quota for this cluster (integer or 'any').")
    p.set_defaults(func=cmd_add_cluster)

    p = sub.add_parser("remove-cluster", help="Remove a registered cluster.")
    p.add_argument("name")
    p.set_defaults(func=cmd_remove_cluster)

    # team
    p = sub.add_parser("list-team", help="List team members.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list_team)

    p = sub.add_parser("add-team-member", help="Add a team member.")
    p.add_argument("username")
    p.add_argument("--display-name", default="")
    p.add_argument("--email", default="")
    p.set_defaults(func=cmd_add_team_member)

    p = sub.add_parser("remove-team-member", help="Remove a team member.")
    p.add_argument("username")
    p.set_defaults(func=cmd_remove_team_member)

    # PPPs
    p = sub.add_parser("list-ppp", help="List PPP accounts.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list_ppp)

    p = sub.add_parser("add-ppp", help="Add a PPP account.")
    p.add_argument("name")
    p.add_argument("--id", default="", help="Numeric PPP id used by AI Hub.")
    p.add_argument("--description", default="")
    p.set_defaults(func=cmd_add_ppp)

    p = sub.add_parser("remove-ppp", help="Remove a PPP account.")
    p.add_argument("name")
    p.set_defaults(func=cmd_remove_ppp)

    # path lists
    p = sub.add_parser("list-paths", help="List path entries (log_search / nemo_run / mount_lustre_prefix).")
    p.add_argument("--kind", default=None,
                   choices=("log_search", "nemo_run", "mount_lustre_prefix"))
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list_paths)

    p = sub.add_parser("add-path", help="Add a path entry.")
    p.add_argument("--kind", required=True,
                   choices=("log_search", "nemo_run", "mount_lustre_prefix"))
    p.add_argument("path")
    p.set_defaults(func=cmd_add_path)

    p = sub.add_parser("remove-path", help="Remove a path entry.")
    p.add_argument("--kind", required=True,
                   choices=("log_search", "nemo_run", "mount_lustre_prefix"))
    p.add_argument("path")
    p.set_defaults(func=cmd_remove_path)

    # process filters
    p = sub.add_parser("list-filters", help="List local-process filters (include / exclude).")
    p.add_argument("--mode", default=None, choices=("include", "exclude"))
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list_filters)

    p = sub.add_parser("add-filter", help="Add an include/exclude pattern.")
    p.add_argument("--mode", required=True, choices=("include", "exclude"))
    p.add_argument("pattern")
    p.set_defaults(func=cmd_add_filter)

    p = sub.add_parser("remove-filter", help="Remove an include/exclude pattern.")
    p.add_argument("--mode", required=True, choices=("include", "exclude"))
    p.add_argument("pattern")
    p.set_defaults(func=cmd_remove_filter)

    # app_settings
    p = sub.add_parser("settings", help="List every app_settings key with its current value and source.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_settings)

    p = sub.add_parser("get", help="Read one app_settings key.")
    p.add_argument("key")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_get)

    p = sub.add_parser("set", help="Set one app_settings key. Value is JSON-parsed if possible.")
    p.add_argument("key")
    p.add_argument("value")
    p.set_defaults(func=cmd_set)

    p = sub.add_parser("unset", help="Delete a stored override (returns to registered default).")
    p.add_argument("key")
    p.set_defaults(func=cmd_unset)

    # legacy importer
    p = sub.add_parser("import-json", help="One-shot import of a v3 conf/config.json into the v4 tables.")
    p.add_argument("path", help="Path to the v3 config.json file.")
    p.set_defaults(func=cmd_import_json)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point. Returns the process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    # Ensure schema is in place for every subcommand except `setup` (which
    # creates it explicitly with a header banner).
    if args.cmd != "setup":
        from .db import init_db
        init_db()

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
