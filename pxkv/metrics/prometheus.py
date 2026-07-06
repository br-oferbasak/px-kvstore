#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any, Dict

def registry_to_prometheus(metrics: Dict[str, Any]) -> str:
    """
    Convert metrics registry JSON to Prometheus exposition format.
    """
    lines = []
    
    lines.append("# HELP pxkv_requests_total Total number of requests.")
    lines.append("# TYPE pxkv_requests_total counter")
    lines.append(f"pxkv_requests_total {metrics['requests_total']}")
    
    lines.append("# HELP pxkv_errors_total Total number of request errors.")
    lines.append("# TYPE pxkv_errors_total counter")
    lines.append(f"pxkv_errors_total {metrics['errors_total']}")
    
    for method, count in metrics["requests_by_method"].items():
        lines.append(f'pxkv_requests_by_method_total{{method="{method}"}} {count}')
        
    ai = metrics["ai_cache"]
    lines.append("# HELP pxkv_ai_cache_lookups_total Total AI cache lookups.")
    lines.append(f"pxkv_ai_cache_lookups_total {ai['lookups']}")
    lines.append("# HELP pxkv_ai_cache_hits_total Total AI cache hits.")
    lines.append(f"pxkv_ai_cache_hits_total {ai['hits']}")
    lines.append("# HELP pxkv_ai_cache_misses_total Total AI cache misses.")
    lines.append(f"pxkv_ai_cache_misses_total {ai['misses']}")
    lines.append("# HELP pxkv_ai_cache_stores_total Total AI cache stores.")
    lines.append(f"pxkv_ai_cache_stores_total {ai['stores']}")
    
    latency = metrics.get("latency_ms", {})
    by_route = latency.get("by_route", {})
    for route, data in by_route.items():
        r_label = route.replace('"', '\\"')
        lines.append(f'pxkv_request_latency_ms_sum{{route="{r_label}"}} {data["sum_ms"]}')
        lines.append(f'pxkv_request_latency_ms_count{{route="{r_label}"}} {data["count"]}')
        
        buckets = data.get("buckets", {})
        sorted_buckets = sorted([b for b in buckets.keys() if b != "inf"], key=float)
        cumulative = 0
        for b in sorted_buckets:
            cumulative += buckets[b]
            lines.append(f'pxkv_request_latency_ms_bucket{{route="{r_label}",le="{b}"}} {cumulative}')
        cumulative += buckets.get("inf", 0)
        lines.append(f'pxkv_request_latency_ms_bucket{{route="{r_label}",le="+Inf"}} {cumulative}')

    repl = metrics.get("replication", {})
    lines.append("# HELP pxkv_replication_leader_lsn Current leader WAL LSN.")
    lines.append("# TYPE pxkv_replication_leader_lsn gauge")
    lines.append(f"pxkv_replication_leader_lsn {int(repl.get('leader_lsn', 0) or 0)}")
    lines.append("# HELP pxkv_replication_follower_ack_lsn Last acknowledged LSN by follower.")
    lines.append("# TYPE pxkv_replication_follower_ack_lsn gauge")
    lines.append("# HELP pxkv_replication_follower_lag_lsn Leader-to-follower lag in LSN units.")
    lines.append("# TYPE pxkv_replication_follower_lag_lsn gauge")
    followers = repl.get("followers", {})
    for follower, data in followers.items():
        f_label = str(follower).replace('"', '\\"')
        lines.append(
            f'pxkv_replication_follower_ack_lsn{{follower="{f_label}"}} {int(data.get("ack_lsn", 0) or 0)}'
        )
        lines.append(
            f'pxkv_replication_follower_lag_lsn{{follower="{f_label}"}} {int(data.get("lag_lsn", 0) or 0)}'
        )

    q = repl.get("queue", {}) or {}
    lines.append("# HELP pxkv_replication_queue_depth Current replication queue depth.")
    lines.append("# TYPE pxkv_replication_queue_depth gauge")
    lines.append(f"pxkv_replication_queue_depth {int(q.get('depth', 0) or 0)}")
    lines.append("# HELP pxkv_replication_queue_max Configured replication queue max size.")
    lines.append("# TYPE pxkv_replication_queue_max gauge")
    lines.append(f"pxkv_replication_queue_max {int(q.get('max', 0) or 0)}")
    lines.append("# HELP pxkv_replication_queue_drops_total Total number of replication events dropped due to backpressure.")
    lines.append("# TYPE pxkv_replication_queue_drops_total counter")
    lines.append(f"pxkv_replication_queue_drops_total {int(q.get('drops_total', 0) or 0)}")
    lines.append("# HELP pxkv_replication_queue_drops_by_policy_total Replication drops by shedding policy.")
    lines.append("# TYPE pxkv_replication_queue_drops_by_policy_total counter")
    lines.append(
        f'pxkv_replication_queue_drops_by_policy_total{{policy="drop_newest"}} {int(q.get("drops_drop_newest", 0) or 0)}'
    )
    lines.append(
        f'pxkv_replication_queue_drops_by_policy_total{{policy="drop_oldest"}} {int(q.get("drops_drop_oldest", 0) or 0)}'
    )

    disk = metrics.get("disk", {}) or {}
    lines.append("# HELP pxkv_disk_used_bytes Used bytes on the monitored filesystem.")
    lines.append("# TYPE pxkv_disk_used_bytes gauge")
    lines.append(f"pxkv_disk_used_bytes {int(disk.get('used_bytes', 0) or 0)}")
    lines.append("# HELP pxkv_disk_free_bytes Free bytes on the monitored filesystem.")
    lines.append("# TYPE pxkv_disk_free_bytes gauge")
    lines.append(f"pxkv_disk_free_bytes {int(disk.get('free_bytes', 0) or 0)}")
    lines.append("# HELP pxkv_disk_used_percent Used percentage on the monitored filesystem.")
    lines.append("# TYPE pxkv_disk_used_percent gauge")
    lines.append(f"pxkv_disk_used_percent {float(disk.get('used_percent', 0.0) or 0.0)}")
    lines.append("# HELP pxkv_disk_throttled_total Total number of writes delayed by disk throttling.")
    lines.append("# TYPE pxkv_disk_throttled_total counter")
    lines.append(f"pxkv_disk_throttled_total {int(disk.get('throttled_total', 0) or 0)}")
    lines.append("# HELP pxkv_disk_rejected_total Total number of writes rejected by disk throttling.")
    lines.append("# TYPE pxkv_disk_rejected_total counter")
    lines.append(f"pxkv_disk_rejected_total {int(disk.get('rejected_total', 0) or 0)}")

    hk = metrics.get("hot_keys", {}) or {}
    lines.append("# HELP pxkv_hot_keys_enabled Whether hot-key detection is enabled (1=on).")
    lines.append("# TYPE pxkv_hot_keys_enabled gauge")
    lines.append(f"pxkv_hot_keys_enabled {1 if hk.get('enabled') else 0}")
    lines.append("# HELP pxkv_hot_keys_tracked Number of distinct keys currently in the detection window.")
    lines.append("# TYPE pxkv_hot_keys_tracked gauge")
    lines.append(f"pxkv_hot_keys_tracked {int(hk.get('tracked_keys', 0) or 0)}")
    lines.append("# HELP pxkv_hot_keys_current Number of currently-hot keys among top-K.")
    lines.append("# TYPE pxkv_hot_keys_current gauge")
    lines.append(f"pxkv_hot_keys_current {int(hk.get('hot_keys_current', 0) or 0)}")
    lines.append("# HELP pxkv_hot_keys_detected_total Distinct keys flagged hot at least once since start.")
    lines.append("# TYPE pxkv_hot_keys_detected_total counter")
    lines.append(f"pxkv_hot_keys_detected_total {int(hk.get('hot_keys_detected_total', 0) or 0)}")
    top = hk.get("top", []) or []
    if top:
        lines.append("# HELP pxkv_hot_key_qps Observed QPS for hot-key candidates over the detection window.")
        lines.append("# TYPE pxkv_hot_key_qps gauge")
        for entry in top:
            k_label = str(entry.get("key", "")).replace("\\", "\\\\").replace('"', '\\"')
            qps = float(entry.get("qps", 0.0) or 0.0)
            lines.append(f'pxkv_hot_key_qps{{key="{k_label}"}} {qps}')

    hh = metrics.get("heavy_hitters", {}) or {}
    lines.append("# HELP pxkv_heavy_hitters_enabled Whether Count-Min Sketch heavy-hitter tracking is enabled (1=on).")
    lines.append("# TYPE pxkv_heavy_hitters_enabled gauge")
    lines.append(f"pxkv_heavy_hitters_enabled {1 if hh.get('enabled') else 0}")
    lines.append("# HELP pxkv_heavy_hitters_tracked Number of top-K candidate keys currently tracked.")
    lines.append("# TYPE pxkv_heavy_hitters_tracked gauge")
    lines.append(f"pxkv_heavy_hitters_tracked {int(hh.get('tracked_keys', 0) or 0)}")
    lines.append("# HELP pxkv_heavy_hitters_records_total Number of access records processed by heavy-hitter tracking.")
    lines.append("# TYPE pxkv_heavy_hitters_records_total counter")
    lines.append(f"pxkv_heavy_hitters_records_total {int(hh.get('records', 0) or 0)}")
    lines.append("# HELP pxkv_heavy_hitters_evictions_total Number of top-K candidate replacements.")
    lines.append("# TYPE pxkv_heavy_hitters_evictions_total counter")
    lines.append(f"pxkv_heavy_hitters_evictions_total {int(hh.get('evictions', 0) or 0)}")
    lines.append("# HELP pxkv_heavy_hitters_decays_total Number of Count-Min Sketch decay passes.")
    lines.append("# TYPE pxkv_heavy_hitters_decays_total counter")
    lines.append(f"pxkv_heavy_hitters_decays_total {int(hh.get('decays', 0) or 0)}")
    lines.append("# HELP pxkv_heavy_hitters_detected_total Distinct top-K candidates flagged hot at least once since start.")
    lines.append("# TYPE pxkv_heavy_hitters_detected_total counter")
    lines.append(f"pxkv_heavy_hitters_detected_total {int(hh.get('detected_total', 0) or 0)}")
    lines.append("# HELP pxkv_heavy_hitters_cms_width Count-Min Sketch width.")
    lines.append("# TYPE pxkv_heavy_hitters_cms_width gauge")
    lines.append(f"pxkv_heavy_hitters_cms_width {int(hh.get('cms_width', 0) or 0)}")
    lines.append("# HELP pxkv_heavy_hitters_cms_depth Count-Min Sketch depth.")
    lines.append("# TYPE pxkv_heavy_hitters_cms_depth gauge")
    lines.append(f"pxkv_heavy_hitters_cms_depth {int(hh.get('cms_depth', 0) or 0)}")
    hh_top = hh.get("top", []) or []
    if hh_top:
        lines.append("# HELP pxkv_heavy_hitter_estimated_count CMS-estimated access count for top-K candidates.")
        lines.append("# TYPE pxkv_heavy_hitter_estimated_count gauge")
        for entry in hh_top:
            k_label = str(entry.get("key", "")).replace("\\", "\\\\").replace('"', '\\"')
            count = int(entry.get("estimated_count", 0) or 0)
            lines.append(f'pxkv_heavy_hitter_estimated_count{{key="{k_label}"}} {count}')

    return "\n".join(lines) + "\n"
