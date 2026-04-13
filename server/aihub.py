"""AI Hub OpenSearch integration for GPU allocation and usage data.

Queries an OpenSearch cluster (configured via `aihub_opensearch_url` in
config.json) for formal PPP allocations, fairshare data, and historical
GPU usage metrics.
"""

import json
import logging
import ssl
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from .config import (
    CLUSTERS, PPP_ACCOUNTS, TEAM_MEMBERS, AIHUB_CACHE_TTL,
    AIHUB_OPENSEARCH_URL, DASHBOARD_URL,
    TEAM_GPU_ALLOC, TEAM_NAME, _cache_get, _cache_set,
)

log = logging.getLogger(__name__)

_opensearch_sem = threading.Semaphore(6)

CLUSTER_NAME_MAP = {
    "eos": "eos",
    "dfw": "cw-dfw-cs-001",
    "aws-dfw": "aws-dfw-cs-001",
    "hsg": "oci-hsg-cs-001",
    "iad": "draco-oci-iad",
    "ord": "cs-oci-ord",
    "svg": "nsc-svg-slurm-1",
}
CLUSTER_NAME_REV = {v: k for k, v in CLUSTER_NAME_MAP.items()}

_aihub_cache = {}

_ssl_ctx = ssl.create_default_context()


def _stamp_team_alloc(result):
    """Re-apply current TEAM_GPU_ALLOC to a (possibly cached) result dict."""
    for friendly, cd in result.get("clusters", {}).items():
        ta = TEAM_GPU_ALLOC.get(friendly)
        if ta is not None:
            cd["team_gpu_alloc"] = ta
        else:
            cd.pop("team_gpu_alloc", None)


def _opensearch_query(body, timeout=10):
    """POST a query to the AI Hub OpenSearch endpoint.

    A semaphore caps concurrent in-flight queries to prevent thread
    exhaustion when OpenSearch is slow or unreachable.
    """
    if not AIHUB_OPENSEARCH_URL:
        log.warning("aihub_opensearch_url not configured")
        return None
    if not _opensearch_sem.acquire(timeout=2):
        log.warning("OpenSearch concurrency limit reached, skipping query")
        return None
    try:
        payload = json.dumps(body).encode()
        req = urllib.request.Request(
            AIHUB_OPENSEARCH_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        log.warning("OpenSearch query failed: %s", exc)
        return None
    finally:
        _opensearch_sem.release()


def _os_cluster_names(clusters=None):
    """Convert our cluster names to OpenSearch cluster names."""
    if clusters:
        return [CLUSTER_NAME_MAP.get(c, c) for c in clusters if c in CLUSTER_NAME_MAP]
    return list(CLUSTER_NAME_MAP.values())


def _friendly_cluster(os_name):
    """Convert OpenSearch cluster name back to our short name."""
    return CLUSTER_NAME_REV.get(os_name, os_name)


def _date_str(days_ago):
    """Return ISO date string for N days ago."""
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%d")


def _pick_best_accounts(cluster_data):
    """Compute two 'best' picks: best for priority and best for capacity."""
    accts = cluster_data.get("accounts", {})
    if not accts:
        cluster_data["best_priority"] = None
        cluster_data["best_capacity"] = None
        return

    best_priority_acct = max(accts, key=lambda a: accts[a]["level_fs"])
    best_priority_fs = accts[best_priority_acct]["level_fs"]
    cluster_data["best_priority"] = {
        "account": best_priority_acct,
        "level_fs": best_priority_fs,
    }

    best_capacity_acct = max(accts, key=lambda a: accts[a]["headroom"])
    best_capacity_headroom = accts[best_capacity_acct]["headroom"]
    cluster_data["best_capacity"] = {
        "account": best_capacity_acct,
        "headroom": best_capacity_headroom,
        "gpus_allocated": accts[best_capacity_acct]["gpus_allocated"],
    }


def _fetch_cluster_occupancy_snapshot(os_clusters=None):
    """Fetch current cluster-wide GPU occupancy (all accounts, all users)."""
    query = {
        "query": {"bool": {"filter": [
            {"term": {"s_doc": "slurm_cluster_occupancy_hourly"}},
            {"range": {"ts_created": {"gte": _date_str(2), "lte": "now", "time_zone": "America/Los_Angeles"}}},
        ]}},
        "size": 0,
        "aggs": {
            "cluster": {
                "terms": {"field": "s_cluster", "size": 50},
                "aggs": {
                    "latest": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{"ts_created": {"order": "desc"}}],
                            "_source": ["l_avg_occupied_gpus", "l_avg_operator_total_gpus"],
                        }
                    },
                },
            }
        },
    }
    if os_clusters:
        query["query"]["bool"]["filter"].insert(0, {"terms": {"s_cluster": os_clusters}})

    resp = _opensearch_query(query)
    if not resp:
        return {}

    result = {}
    for cb in resp.get("aggregations", {}).get("cluster", {}).get("buckets", []):
        friendly = _friendly_cluster(cb["key"])
        hits = cb.get("latest", {}).get("hits", {}).get("hits", [])
        if not hits:
            continue
        src = hits[0].get("_source", {})
        occupied = src.get("l_avg_occupied_gpus", 0) or 0
        total = src.get("l_avg_operator_total_gpus", 0) or 0
        if total > 0:
            result[friendly] = {"occupied": round(occupied), "total": round(total)}
    return result


def get_ppp_allocations(accounts=None, clusters=None, force=False):
    """Get current PPP allocation snapshot across clusters.

    Returns per-cluster, per-account allocation and consumption data
    from the last 24 hours of account_gpus_hourly documents.
    """
    accts = accounts or PPP_ACCOUNTS
    if not accts:
        return {"clusters": {}}

    cluster_key = ",".join(sorted(clusters or [])) or "all"
    cache_key = f"ppp_alloc:{','.join(sorted(accts))}:{cluster_key}"
    if not force:
        cached = _cache_get(_aihub_cache, cache_key, AIHUB_CACHE_TTL)
        if cached is not None:
            _stamp_team_alloc(cached)
            return cached

    os_clusters = _os_cluster_names(clusters)

    _alloc_fields = [
        "l_gpus_allocated", "l_gpus_consumed", "l_gpus_consumed_normal",
        "l_gpus_consumed_free", "l_operator_fairshare_avail_gpus",
        "d_fairshare_normalized", "d_level_fs", "l_gpus_pending_eligible",
    ]
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"s_doc": "account_gpus_hourly"}},
                    {"terms": {"s_account": accts}},
                    {"range": {"ts_created": {
                        "gte": _date_str(2),
                        "lte": "now",
                        "time_zone": "America/Los_Angeles",
                    }}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "cluster": {
                "terms": {"field": "s_cluster", "size": 50},
                "aggs": {
                    "account": {
                        "terms": {"field": "s_account", "size": 20},
                        "aggs": {
                            "latest": {
                                "top_hits": {
                                    "size": 1,
                                    "sort": [{"ts_created": {"order": "desc"}}],
                                    "_source": _alloc_fields,
                                }
                            },
                        },
                    }
                },
            }
        },
    }

    if os_clusters:
        query["query"]["bool"]["filter"].insert(0, {"terms": {"s_cluster": os_clusters}})

    with ThreadPoolExecutor(max_workers=2) as pool:
        alloc_fut = pool.submit(_opensearch_query, query)
        occ_fut = pool.submit(_fetch_cluster_occupancy_snapshot, os_clusters)
        resp = alloc_fut.result()
        occ = occ_fut.result() or {}

    if not resp:
        return {"clusters": {}}

    result = {"clusters": {}}
    agg_clusters = resp.get("aggregations", {}).get("cluster", {}).get("buckets", [])

    for cb in agg_clusters:
        os_name = cb["key"]
        friendly = _friendly_cluster(os_name)
        cluster_data = {"accounts": {}}

        for ab in cb.get("account", {}).get("buckets", []):
            acct = ab["key"]
            hits = ab.get("latest", {}).get("hits", {}).get("hits", [])
            if not hits:
                continue
            src = hits[0].get("_source", {})
            allocated = src.get("l_gpus_allocated", 0) or 0
            consumed = src.get("l_gpus_consumed", 0) or 0
            fs_avail = src.get("l_operator_fairshare_avail_gpus", 0) or 0
            level_fs = src.get("d_level_fs", 0) or 0

            if allocated <= 0:
                continue

            capped_fs = min(level_fs, 10.0)
            headroom = round(fs_avail - consumed)

            cluster_data["accounts"][acct] = {
                "gpus_allocated": round(allocated),
                "gpus_consumed": round(consumed),
                "gpus_consumed_normal": round(src.get("l_gpus_consumed_normal", 0) or 0),
                "gpus_consumed_free": round(src.get("l_gpus_consumed_free", 0) or 0),
                "fairshare_avail_gpus": round(fs_avail),
                "fairshare_normalized": round(src.get("d_fairshare_normalized", 0) or 0, 6),
                "level_fs": round(capped_fs, 3),
                "pending_eligible": round(src.get("l_gpus_pending_eligible", 0) or 0),
                "utilization_pct": round(consumed / allocated * 100, 1) if allocated > 0 else 0,
                "headroom": headroom,
            }

        _pick_best_accounts(cluster_data)

        gpu_type = CLUSTERS.get(friendly, {}).get("gpu_type", "")
        cluster_data["gpu_type"] = gpu_type
        cluster_data["os_name"] = os_name

        if cluster_data["accounts"]:
            result["clusters"][friendly] = cluster_data
    for friendly, occ_data in occ.items():
        if friendly in result["clusters"]:
            result["clusters"][friendly]["cluster_occupied_gpus"] = occ_data["occupied"]
            result["clusters"][friendly]["cluster_total_gpus"] = occ_data["total"]

    _cache_set(_aihub_cache, cache_key, result)
    _stamp_team_alloc(result)
    return result


def get_usage_history(accounts=None, clusters=None, days=14, interval="1d"):
    """Get GPU usage time-series per account per cluster.

    Returns daily (or hourly) buckets with allocation and consumption data.
    """
    cache_key = f"history_{days}_{interval}_{','.join(clusters or ['all'])}"
    cached = _cache_get(_aihub_cache, cache_key, min(AIHUB_CACHE_TTL * 6, 1800))
    if cached is not None:
        return cached

    accts = accounts or PPP_ACCOUNTS
    if not accts:
        return {"clusters": {}}

    os_clusters = _os_cluster_names(clusters)

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"s_doc": "account_gpus_hourly"}},
                    {"terms": {"s_account": accts}},
                    {"range": {"ts_created": {
                        "gte": _date_str(days),
                        "lte": "now",
                        "time_zone": "America/Los_Angeles",
                    }}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "cluster": {
                "terms": {"field": "s_cluster", "size": 50},
                "aggs": {
                    "account": {
                        "terms": {"field": "s_account", "size": 20},
                        "aggs": {
                            "over_time": {
                                "date_histogram": {
                                    "field": "ts_created",
                                    "interval": interval,
                                    "time_zone": "America/Los_Angeles",
                                },
                                "aggs": {
                                    "gpus_allocated": {"avg": {"field": "l_gpus_allocated"}},
                                    "gpus_consumed": {"avg": {"field": "l_gpus_consumed"}},
                                    "fairshare_avail": {"avg": {"field": "l_operator_fairshare_avail_gpus"}},
                                    "gpus_consumed_normal": {"avg": {"field": "l_gpus_consumed_normal"}},
                                    "gpus_consumed_free": {"avg": {"field": "l_gpus_consumed_free"}},
                                },
                            }
                        },
                    }
                },
            }
        },
    }

    if os_clusters:
        query["query"]["bool"]["filter"].insert(0, {"terms": {"s_cluster": os_clusters}})

    resp = _opensearch_query(query, timeout=30)
    if not resp:
        return {"clusters": {}}

    result = {"clusters": {}, "days": days, "interval": interval}
    for cb in resp.get("aggregations", {}).get("cluster", {}).get("buckets", []):
        friendly = _friendly_cluster(cb["key"])
        cluster_series = {}

        for ab in cb.get("account", {}).get("buckets", []):
            acct = ab["key"]
            points = []
            for bucket in ab.get("over_time", {}).get("buckets", []):
                date_str = bucket.get("key_as_string", "")[:10]
                allocated = bucket["gpus_allocated"]["value"] or 0
                if allocated <= 0:
                    continue
                points.append({
                    "date": date_str,
                    "gpus_allocated": round(allocated),
                    "gpus_consumed": round(bucket["gpus_consumed"]["value"] or 0),
                    "fairshare_avail": round(bucket["fairshare_avail"]["value"] or 0),
                    "gpus_consumed_normal": round(bucket["gpus_consumed_normal"]["value"] or 0),
                    "gpus_consumed_free": round(bucket["gpus_consumed_free"]["value"] or 0),
                })
            if points:
                cluster_series[acct] = points

        if cluster_series:
            result["clusters"][friendly] = cluster_series

    _cache_set(_aihub_cache, cache_key, result)
    return result


def get_user_breakdown(account, cluster, days=7):
    """Get per-user GPU consumption breakdown for an account on a cluster."""
    cache_key = f"users_{account}_{cluster}_{days}"
    cached = _cache_get(_aihub_cache, cache_key, AIHUB_CACHE_TTL)
    if cached is not None:
        return cached

    os_cluster = CLUSTER_NAME_MAP.get(cluster, cluster)

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"s_cluster": [os_cluster]}},
                    {"term": {"s_doc": "account_user_gpus_hourly"}},
                    {"term": {"s_account": account}},
                    {"range": {"ts_created": {
                        "gte": _date_str(days),
                        "lte": "now",
                        "time_zone": "America/Los_Angeles",
                    }}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "user": {
                "terms": {"field": "s_user", "size": 200},
                "aggs": {
                    "avg_consumed": {"avg": {"field": "l_gpus_consumed"}},
                    "avg_consumed_normal": {"avg": {"field": "l_gpus_consumed_normal"}},
                    "avg_consumed_free": {"avg": {"field": "l_gpus_consumed_free"}},
                },
            }
        },
    }

    resp = _opensearch_query(query)
    if not resp:
        return {"users": []}

    users = []
    for ub in resp.get("aggregations", {}).get("user", {}).get("buckets", []):
        consumed = ub["avg_consumed"]["value"] or 0
        if consumed < 0.1:
            continue
        users.append({
            "user": ub["key"],
            "avg_gpus_consumed": round(consumed, 1),
            "avg_gpus_consumed_normal": round(ub["avg_consumed_normal"]["value"] or 0, 1),
            "avg_gpus_consumed_free": round(ub["avg_consumed_free"]["value"] or 0, 1),
        })

    users.sort(key=lambda u: -u["avg_gpus_consumed"])
    result = {"account": account, "cluster": cluster, "days": days, "users": users}
    _cache_set(_aihub_cache, cache_key, result)
    return result


def get_cluster_occupancy(clusters=None, days=7):
    """Get cluster-level occupancy metrics over time."""
    cache_key = f"occupancy_{days}_{','.join(clusters or ['all'])}"
    cached = _cache_get(_aihub_cache, cache_key, min(AIHUB_CACHE_TTL * 6, 1800))
    if cached is not None:
        return cached

    os_clusters = _os_cluster_names(clusters)

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"s_doc": "slurm_cluster_occupancy_hourly"}},
                    {"range": {"ts_created": {
                        "gte": _date_str(days),
                        "lte": "now",
                        "time_zone": "America/Los_Angeles",
                    }}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "cluster": {
                "terms": {"field": "s_cluster", "size": 50},
                "aggs": {
                    "over_time": {
                        "date_histogram": {
                            "field": "ts_created",
                            "interval": "1d",
                            "time_zone": "America/Los_Angeles",
                        },
                        "aggs": {
                            "total_gpus": {"avg": {"field": "l_total_gpus"}},
                            "alloc_gpus": {"avg": {"field": "l_alloc_gpus"}},
                            "idle_gpus": {"avg": {"field": "l_idle_gpus"}},
                        },
                    }
                },
            }
        },
    }

    if os_clusters:
        query["query"]["bool"]["filter"].insert(0, {"terms": {"s_cluster": os_clusters}})

    resp = _opensearch_query(query, timeout=30)
    if not resp:
        return {"clusters": {}}

    result = {"clusters": {}, "days": days}
    for cb in resp.get("aggregations", {}).get("cluster", {}).get("buckets", []):
        friendly = _friendly_cluster(cb["key"])
        points = []
        for bucket in cb.get("over_time", {}).get("buckets", []):
            total = bucket["total_gpus"]["value"]
            alloc = bucket["alloc_gpus"]["value"]
            if not total:
                continue
            points.append({
                "date": bucket.get("key_as_string", "")[:10],
                "total_gpus": round(total),
                "alloc_gpus": round(alloc or 0),
                "idle_gpus": round(bucket["idle_gpus"]["value"] or 0),
                "occupancy_pct": round((alloc or 0) / total * 100, 1),
            })
        if points:
            result["clusters"][friendly] = points

    _cache_set(_aihub_cache, cache_key, result)
    return result


def _get_team_members():
    """Get team member list from config, falling back to the cluster dashboard."""
    if TEAM_MEMBERS:
        return list(TEAM_MEMBERS)
    if not DASHBOARD_URL:
        return []
    try:
        req = urllib.request.Request(f"{DASHBOARD_URL}/api/config", method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            cfg = json.loads(resp.read().decode())
        teams = cfg.get("teams", {})
        return teams.get(TEAM_NAME, []) if TEAM_NAME else []
    except Exception:
        return []


def get_user_overlay(users=None, accounts=None, clusters=None, force=False):
    """Get per-user GPU consumption grouped by cluster and account.

    Used to overlay 'my usage' and 'team usage' on the PPP allocation bars.
    Returns {cluster: {account: {user: gpus_consumed}}}.
    """
    accts = accounts or PPP_ACCOUNTS
    if not accts or not users:
        return {"clusters": {}}

    cluster_key = ",".join(sorted(clusters or [])) or "all"
    user_key = ",".join(sorted(users or []))
    acct_key = ",".join(sorted(accts))
    cache_key = f"user_overlay:{user_key}:{acct_key}:{cluster_key}"
    if not force:
        cached = _cache_get(_aihub_cache, cache_key, AIHUB_CACHE_TTL)
        if cached is not None:
            return cached

    os_clusters = _os_cluster_names(clusters)

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"s_doc": "account_user_gpus_hourly"}},
                    {"terms": {"s_account": accts}},
                    {"terms": {"s_user": users}},
                    {"range": {"ts_created": {
                        "gte": _date_str(2),
                        "lte": "now",
                        "time_zone": "America/Los_Angeles",
                    }}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "cluster": {
                "terms": {"field": "s_cluster", "size": 50},
                "aggs": {
                    "account": {
                        "terms": {"field": "s_account", "size": 20},
                        "aggs": {
                            "user": {
                                "terms": {"field": "s_user", "size": 200},
                                "aggs": {
                                    "latest": {
                                        "top_hits": {
                                            "size": 1,
                                            "sort": [{"ts_created": {"order": "desc"}}],
                                            "_source": ["l_gpus_consumed"],
                                        }
                                    },
                                },
                            }
                        },
                    }
                },
            }
        },
    }

    if os_clusters:
        query["query"]["bool"]["filter"].insert(0, {"terms": {"s_cluster": os_clusters}})

    resp = _opensearch_query(query)
    if not resp:
        return {"clusters": {}}

    result = {"clusters": {}, "users": users}
    for cb in resp.get("aggregations", {}).get("cluster", {}).get("buckets", []):
        friendly = _friendly_cluster(cb["key"])
        cluster_data = {}
        for ab in cb.get("account", {}).get("buckets", []):
            acct = ab["key"]
            user_map = {}
            for ub in ab.get("user", {}).get("buckets", []):
                hits = ub.get("latest", {}).get("hits", {}).get("hits", [])
                consumed = 0
                if hits:
                    consumed = hits[0].get("_source", {}).get("l_gpus_consumed", 0) or 0
                if consumed >= 0.5:
                    user_map[ub["key"]] = round(consumed)
            if user_map:
                cluster_data[acct] = user_map
        if cluster_data:
            result["clusters"][friendly] = cluster_data

    _cache_set(_aihub_cache, cache_key, result)
    return result


def get_team_overlay(clusters=None, force=False):
    """Get overlay data for the current user and their team members."""
    from .config import DEFAULT_USER
    team_members = _get_team_members()
    all_users = list(set([DEFAULT_USER] + team_members))
    data = get_user_overlay(users=all_users, clusters=clusters, force=force)
    data["current_user"] = DEFAULT_USER
    data["team_members"] = team_members
    data["team_name"] = TEAM_NAME
    return data


def get_my_fairshare(user=None, accounts=None, clusters=None, force=False):
    """Get per-user fairshare priority across all PPP accounts and clusters.

    Returns the user's personal level_fs and consumption per account per cluster.
    This is the USER's scheduling priority, not the PPP's.
    """
    from .config import DEFAULT_USER
    user = user or DEFAULT_USER
    accts = accounts or PPP_ACCOUNTS
    if not accts:
        return {"user": user, "clusters": {}}

    cluster_key = ",".join(sorted(clusters or [])) or "all"
    cache_key = f"my_fs:{user}:{','.join(sorted(accts))}:{cluster_key}"
    if not force:
        cached = _cache_get(_aihub_cache, cache_key, AIHUB_CACHE_TTL)
        if cached is not None:
            return cached

    os_clusters = _os_cluster_names(clusters)

    _fs_fields = ["d_level_fs", "l_gpus_consumed", "d_norm_shares", "d_norm_usage"]
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"s_doc": "account_user_gpus_hourly"}},
                    {"terms": {"s_account": accts}},
                    {"term": {"s_user": user}},
                    {"range": {"ts_created": {
                        "gte": _date_str(2),
                        "lte": "now",
                        "time_zone": "America/Los_Angeles",
                    }}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "cluster": {
                "terms": {"field": "s_cluster", "size": 50},
                "aggs": {
                    "account": {
                        "terms": {"field": "s_account", "size": 20},
                        "aggs": {
                            "latest": {
                                "top_hits": {
                                    "size": 1,
                                    "sort": [{"ts_created": {"order": "desc"}}],
                                    "_source": _fs_fields,
                                }
                            },
                        },
                    }
                },
            }
        },
    }

    if os_clusters:
        query["query"]["bool"]["filter"].insert(0, {"terms": {"s_cluster": os_clusters}})

    resp = _opensearch_query(query)
    if not resp:
        return {"user": user, "clusters": {}}

    result = {"user": user, "clusters": {}}
    for cb in resp.get("aggregations", {}).get("cluster", {}).get("buckets", []):
        friendly = _friendly_cluster(cb["key"])
        acct_data = {}
        for ab in cb.get("account", {}).get("buckets", []):
            hits = ab.get("latest", {}).get("hits", {}).get("hits", [])
            if not hits:
                continue
            src = hits[0].get("_source", {})
            level_fs = src.get("d_level_fs", 0) or 0
            acct_data[ab["key"]] = {
                "level_fs": round(min(level_fs, 10.0), 3),
                "consumed": round(src.get("l_gpus_consumed", 0) or 0),
            }
        if acct_data:
            result["clusters"][friendly] = acct_data

    _cache_set(_aihub_cache, cache_key, result)
    return result


def get_fairshare_for_recommendations(accounts=None):
    """Get a lightweight fairshare snapshot for the recommendation engine.

    Returns {cluster: {account: {"level_fs": float, "fairshare_avail": int, "consumed": int}}}
    Uses same cache as get_ppp_allocations.
    """
    alloc_data = get_ppp_allocations(accounts=accounts)
    result = {}
    for cluster, cdata in alloc_data.get("clusters", {}).items():
        result[cluster] = {}
        for acct, adata in cdata.get("accounts", {}).items():
            result[cluster][acct] = {
                "level_fs": adata.get("level_fs", 0),
                "fairshare_avail": adata.get("fairshare_avail_gpus", 0),
                "gpus_consumed": adata.get("gpus_consumed", 0),
                "gpus_allocated": adata.get("gpus_allocated", 0),
                "headroom": adata.get("headroom", 0),
            }
    return result
