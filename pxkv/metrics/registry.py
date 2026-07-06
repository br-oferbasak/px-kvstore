#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from typing import Any, Dict

_LATENCY_BUCKETS_MS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]

class MetricsRegistry:
    def __init__(self):
        self._metrics: Dict[str, Any] = {
            "started_at": time.time(),
            "requests_total": 0,
            "requests_by_method": {},
            "errors_total": 0,
            "latency_ms": {
                "buckets_ms": _LATENCY_BUCKETS_MS,
                "by_route": {},
            },
            "ai_cache": {
                "lookups": 0,
                "hits": 0,
                "misses": 0,
                "stores": 0,
            },
            "replication": {
                "leader_lsn": 0,
                "followers": {},
                "queue": {
                    "depth": 0,
                    "max": 0,
                    "drops_total": 0,
                    "drops_drop_newest": 0,
                    "drops_drop_oldest": 0,
                    "last_drop_at": 0.0,
                    "last_drop_reason": "",
                },
            },
            "hot_keys": {
                "enabled": False,
                "window_seconds": 0.0,
                "buckets": 0,
                "top_k": 0,
                "threshold_qps": 0.0,
                "sample_rate": 1.0,
                "tracked_keys": 0,
                "hot_keys_current": 0,
                "hot_keys_detected_total": 0,
                "top": [],
            },
            "heavy_hitters": {
                "enabled": False,
                "k": 0,
                "cms_width": 0,
                "cms_depth": 0,
                "threshold_count": 0,
                "tracked_keys": 0,
                "records": 0,
                "decays": 0,
                "evictions": 0,
                "detected_total": 0,
                "top": [],
            },
            "disk": {
                "enabled": False,
                "paths": [],
                "last_path": "",
                "total_bytes": 0,
                "used_bytes": 0,
                "free_bytes": 0,
                "used_percent": 0.0,
                "soft_exceeded": False,
                "hard_exceeded": False,
                "last_checked_at": 0.0,
                "throttled_total": 0,
                "rejected_total": 0,
                "last_delay_ms": 0.0,
                "last_reject_reason": "",
            },
        }

    def inc_requests(self, method: str, error: bool = False):
        self._metrics["requests_total"] += 1
        by_method = self._metrics.setdefault("requests_by_method", {})
        by_method[method] = by_method.get(method, 0) + 1
        if error:
            self._metrics["errors_total"] += 1

    def observe_latency(self, route: str, elapsed_ms: float):
        if elapsed_ms < 0:
            elapsed_ms = 0.0
        bucket = "inf"
        for b in _LATENCY_BUCKETS_MS:
            if elapsed_ms <= b:
                bucket = str(b)
                break
        latency = self._metrics.setdefault("latency_ms", {"buckets_ms": _LATENCY_BUCKETS_MS, "by_route": {}})
        by_route = latency.setdefault("by_route", {})
        r = by_route.setdefault(
            route,
            {
                "count": 0,
                "sum_ms": 0.0,
                "buckets": {},
            },
        )
        r["count"] += 1
        r["sum_ms"] += float(elapsed_ms)
        buckets = r.setdefault("buckets", {})
        buckets[bucket] = buckets.get(bucket, 0) + 1

    def inc_ai_cache(self, metric: str):
        if metric in self._metrics["ai_cache"]:
            self._metrics["ai_cache"][metric] += 1

    def observe_replication_ack(
        self,
        follower: str,
        leader_lsn: int,
        ack_lsn: int,
        ok: bool,
        error: str = "",
    ) -> None:
        replication = self._metrics.setdefault("replication", {"leader_lsn": 0, "followers": {}})
        replication["leader_lsn"] = int(max(0, leader_lsn))
        followers = replication.setdefault("followers", {})
        item = followers.setdefault(
            follower,
            {
                "ack_lsn": 0,
                "lag_lsn": 0,
                "last_ack_at": 0.0,
                "last_error_at": 0.0,
                "last_error": "",
            },
        )
        ack = int(max(0, ack_lsn))
        item["ack_lsn"] = ack
        item["lag_lsn"] = int(max(0, int(leader_lsn) - ack))
        now = time.time()
        if ok:
            item["last_ack_at"] = now
            item["last_error"] = ""
        else:
            item["last_error_at"] = now
            item["last_error"] = error

    def set_replication_leader_lsn(self, leader_lsn: int) -> None:
        replication = self._metrics.setdefault("replication", {"leader_lsn": 0, "followers": {}})
        replication["leader_lsn"] = int(max(0, leader_lsn))

    def observe_replication_queue(self, depth: int, max_size: int) -> None:
        replication = self._metrics.setdefault("replication", {"leader_lsn": 0, "followers": {}})
        q = replication.setdefault(
            "queue",
            {
                "depth": 0,
                "max": 0,
                "drops_total": 0,
                "drops_drop_newest": 0,
                "drops_drop_oldest": 0,
                "last_drop_at": 0.0,
                "last_drop_reason": "",
            },
        )
        q["depth"] = int(max(0, depth))
        q["max"] = int(max(0, max_size))

    def inc_replication_drop(self, policy: str, reason: str) -> None:
        replication = self._metrics.setdefault("replication", {"leader_lsn": 0, "followers": {}})
        q = replication.setdefault(
            "queue",
            {
                "depth": 0,
                "max": 0,
                "drops_total": 0,
                "drops_drop_newest": 0,
                "drops_drop_oldest": 0,
                "last_drop_at": 0.0,
                "last_drop_reason": "",
            },
        )
        q["drops_total"] = int(q.get("drops_total", 0) or 0) + 1
        p = (policy or "").lower()
        if p == "drop_oldest":
            q["drops_drop_oldest"] = int(q.get("drops_drop_oldest", 0) or 0) + 1
        else:
            q["drops_drop_newest"] = int(q.get("drops_drop_newest", 0) or 0) + 1
        q["last_drop_at"] = time.time()
        q["last_drop_reason"] = str(reason or "")

    def observe_disk_usage(self, sample: Dict[str, Any]) -> None:
        disk = self._metrics.setdefault(
            "disk",
            {
                "enabled": False,
                "paths": [],
                "last_path": "",
                "total_bytes": 0,
                "used_bytes": 0,
                "free_bytes": 0,
                "used_percent": 0.0,
                "soft_exceeded": False,
                "hard_exceeded": False,
                "last_checked_at": 0.0,
                "throttled_total": 0,
                "rejected_total": 0,
                "last_delay_ms": 0.0,
                "last_reject_reason": "",
            },
        )
        disk["enabled"] = bool(sample.get("enabled", False))
        disk["paths"] = list(sample.get("paths", []) or [])
        disk["last_path"] = str(sample.get("last_path", "") or sample.get("path", "") or "")
        disk["total_bytes"] = int(sample.get("total_bytes", 0) or 0)
        disk["used_bytes"] = int(sample.get("used_bytes", 0) or 0)
        disk["free_bytes"] = int(sample.get("free_bytes", 0) or 0)
        disk["used_percent"] = float(sample.get("used_percent", 0.0) or 0.0)
        disk["soft_exceeded"] = bool(sample.get("soft_exceeded", False))
        disk["hard_exceeded"] = bool(sample.get("hard_exceeded", False))
        disk["last_checked_at"] = float(sample.get("last_checked_at", time.time()) or time.time())

    def inc_disk_throttle(self, delay_ms: float) -> None:
        disk = self._metrics.setdefault("disk", {})
        disk["throttled_total"] = int(disk.get("throttled_total", 0) or 0) + 1
        disk["last_delay_ms"] = float(delay_ms or 0.0)

    def inc_disk_reject(self, reason: str) -> None:
        disk = self._metrics.setdefault("disk", {})
        disk["rejected_total"] = int(disk.get("rejected_total", 0) or 0) + 1
        disk["last_reject_reason"] = str(reason or "")

    def observe_hot_keys(self, snapshot: Dict[str, Any]) -> None:
        hk = self._metrics.setdefault(
            "hot_keys",
            {
                "enabled": False,
                "window_seconds": 0.0,
                "buckets": 0,
                "top_k": 0,
                "threshold_qps": 0.0,
                "sample_rate": 1.0,
                "tracked_keys": 0,
                "hot_keys_current": 0,
                "hot_keys_detected_total": 0,
                "top": [],
            },
        )
        hk["enabled"] = bool(snapshot.get("enabled", False))
        hk["window_seconds"] = float(snapshot.get("window_seconds", 0.0) or 0.0)
        hk["buckets"] = int(snapshot.get("buckets", 0) or 0)
        hk["top_k"] = int(snapshot.get("top_k", 0) or 0)
        hk["threshold_qps"] = float(snapshot.get("threshold_qps", 0.0) or 0.0)
        hk["sample_rate"] = float(snapshot.get("sample_rate", 1.0) or 1.0)
        hk["tracked_keys"] = int(snapshot.get("tracked_keys", 0) or 0)
        hk["hot_keys_current"] = int(snapshot.get("hot_keys_current", 0) or 0)
        hk["hot_keys_detected_total"] = int(snapshot.get("hot_keys_detected_total", 0) or 0)
        hk["top"] = list(snapshot.get("top", []) or [])

    def observe_heavy_hitters(self, snapshot: Dict[str, Any]) -> None:
        hh = self._metrics.setdefault(
            "heavy_hitters",
            {
                "enabled": False,
                "k": 0,
                "cms_width": 0,
                "cms_depth": 0,
                "threshold_count": 0,
                "tracked_keys": 0,
                "records": 0,
                "decays": 0,
                "evictions": 0,
                "detected_total": 0,
                "top": [],
            },
        )
        hh["enabled"] = bool(snapshot.get("enabled", False))
        hh["k"] = int(snapshot.get("k", 0) or 0)
        hh["cms_width"] = int(snapshot.get("cms_width", 0) or 0)
        hh["cms_depth"] = int(snapshot.get("cms_depth", 0) or 0)
        hh["threshold_count"] = int(snapshot.get("threshold_count", 0) or 0)
        hh["tracked_keys"] = int(snapshot.get("tracked_keys", 0) or 0)
        hh["records"] = int(snapshot.get("records", 0) or 0)
        hh["decays"] = int(snapshot.get("decays", 0) or 0)
        hh["evictions"] = int(snapshot.get("evictions", 0) or 0)
        hh["detected_total"] = int(snapshot.get("detected_total", 0) or 0)
        hh["top"] = list(snapshot.get("top", []) or [])

    def get_all(self) -> Dict[str, Any]:
        return self._metrics

registry = MetricsRegistry()
