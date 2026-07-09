#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hot key detection via a bucket-based sliding window.

The detector tracks per-key access counts across N time buckets covering
WINDOW_SECONDS seconds. Old buckets are evicted lazily on each record/query.
A key is "hot" when its access rate (sum / window) exceeds THRESHOLD_QPS.
"""

import time
from collections import defaultdict
from threading import RLock
from typing import Any, Callable, Dict, List, Optional


class HotKeyDetector:
    """Thread-safe sliding-window hot-key tracker."""

    def __init__(
        self,
        enabled: bool = False,
        window_seconds: float = 60.0,
        buckets: int = 60,
        top_k: int = 10,
        threshold_qps: float = 0.0,
        sample_rate: float = 1.0,
    ):
        if buckets < 1:
            buckets = 1
        if window_seconds <= 0:
            window_seconds = 1.0
        self._lock = RLock()
        self._enabled = bool(enabled)
        self._window_s = float(window_seconds)
        self._buckets_n = int(buckets)
        self._bucket_s = self._window_s / float(self._buckets_n)
        self._top_k = max(1, int(top_k))
        self._threshold_qps = max(0.0, float(threshold_qps))
        self._sample_rate = min(1.0, max(0.0, float(sample_rate)))
        self._buckets: List[Dict[Any, int]] = [defaultdict(int) for _ in range(self._buckets_n)]
        self._bucket_starts: List[float] = [0.0] * self._buckets_n
        self._sample_counter = 0
        self._detected_total = 0
        self._last_detected: Dict[Any, float] = {}

    def configure(
        self,
        enabled: Optional[bool] = None,
        window_seconds: Optional[float] = None,
        buckets: Optional[int] = None,
        top_k: Optional[int] = None,
        threshold_qps: Optional[float] = None,
        sample_rate: Optional[float] = None,
    ) -> None:
        with self._lock:
            structure_changed = False
            if enabled is not None:
                self._enabled = bool(enabled)
            if window_seconds is not None and float(window_seconds) > 0:
                self._window_s = float(window_seconds)
                structure_changed = True
            if buckets is not None and int(buckets) >= 1:
                self._buckets_n = int(buckets)
                structure_changed = True
            if top_k is not None:
                self._top_k = max(1, int(top_k))
            if threshold_qps is not None:
                self._threshold_qps = max(0.0, float(threshold_qps))
            if sample_rate is not None:
                self._sample_rate = min(1.0, max(0.0, float(sample_rate)))
            if structure_changed:
                self._bucket_s = self._window_s / float(self._buckets_n)
                self._buckets = [defaultdict(int) for _ in range(self._buckets_n)]
                self._bucket_starts = [0.0] * self._buckets_n
                self._last_detected.clear()

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def reset(self) -> None:
        with self._lock:
            self._buckets = [defaultdict(int) for _ in range(self._buckets_n)]
            self._bucket_starts = [0.0] * self._buckets_n
            self._sample_counter = 0
            self._detected_total = 0
            self._last_detected.clear()

    def _bucket_index(self, now: float) -> int:
        return int(now // self._bucket_s) % self._buckets_n

    def _rotate_locked(self, now: float) -> None:
        idx = self._bucket_index(now)
        start = now - (now % self._bucket_s)
        if self._bucket_starts[idx] != start:
            self._buckets[idx] = defaultdict(int)
            self._bucket_starts[idx] = start

    def _evict_stale_locked(self, now: float) -> None:
        cutoff = now - self._window_s
        for i in range(self._buckets_n):
            if self._bucket_starts[i] < cutoff:
                if self._buckets[i]:
                    self._buckets[i] = defaultdict(int)
                self._bucket_starts[i] = 0.0

    def record(self, key: Any, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        with self._lock:
            if self._sample_rate < 1.0:
                self._sample_counter += 1
                step = max(1, int(round(1.0 / self._sample_rate)))
                if self._sample_counter % step != 0:
                    return
            now = time.time()
            self._rotate_locked(now)
            idx = self._bucket_index(now)
            self._buckets[idx][key] += int(count)

    def record_many(self, keys, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        for k in keys:
            self.record(k, count)

    def forget(self, key: Any) -> None:
        with self._lock:
            for b in self._buckets:
                b.pop(key, None)
            self._last_detected.pop(key, None)

    def _aggregate_locked(self, now: float) -> Dict[Any, int]:
        self._evict_stale_locked(now)
        totals: Dict[Any, int] = defaultdict(int)
        for b in self._buckets:
            for k, c in b.items():
                totals[k] += c
        return totals

    def top_hot_keys(
        self,
        limit: Optional[int] = None,
        threshold_qps: Optional[float] = None,
        key_filter: Optional[Callable[[Any], bool]] = None,
        key_mapper: Optional[Callable[[Any], Any]] = None,
        detection_scope: str = "",
    ) -> List[Dict[str, Any]]:
        """Return up to `limit` (default top_k) hottest keys with stats.

        Each entry: {"key": str, "count": int, "qps": float, "hot": bool}.
        Sorted by count descending.
        """
        if not self._enabled:
            return []
        with self._lock:
            now = time.time()
            totals = self._aggregate_locked(now)
            if key_filter is not None:
                totals = {k: c for k, c in totals.items() if key_filter(k)}
            if not totals:
                return []
            lim = self._top_k if limit is None else max(1, int(limit))
            threshold = self._threshold_qps if threshold_qps is None else max(0.0, float(threshold_qps))
            ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:lim]
            out: List[Dict[str, Any]] = []
            for k, c in ranked:
                qps = float(c) / self._window_s if self._window_s > 0 else 0.0
                hot = threshold > 0.0 and qps >= threshold
                if hot:
                    detection_key = f"{detection_scope}:{repr(k)}" if detection_scope else k
                    if detection_key not in self._last_detected:
                        self._detected_total += 1
                    self._last_detected[detection_key] = now
                display_key = key_mapper(k) if key_mapper is not None else k
                out.append({
                    "key": display_key if isinstance(display_key, str) else str(display_key),
                    "count": int(c),
                    "qps": qps,
                    "hot": hot,
                })
            return out

    def report(
        self,
        limit: Optional[int] = None,
        threshold_qps: Optional[float] = None,
        key_filter: Optional[Callable[[Any], bool]] = None,
        key_mapper: Optional[Callable[[Any], Any]] = None,
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a metrics-friendly report, optionally scoped to a key subset."""
        with self._lock:
            threshold = self._threshold_qps if threshold_qps is None else max(0.0, float(threshold_qps))
            lim = self._top_k if limit is None else max(1, int(limit))
            if not self._enabled:
                out = {
                    "enabled": False,
                    "window_seconds": self._window_s,
                    "buckets": self._buckets_n,
                    "top_k": lim,
                    "threshold_qps": threshold,
                    "sample_rate": self._sample_rate,
                    "tracked_keys": 0,
                    "hot_keys_current": 0,
                    "hot_keys_detected_total": int(self._detected_total),
                    "top": [],
                }
                if namespace is not None:
                    out["namespace"] = namespace
                return out

            now = time.time()
            totals = self._aggregate_locked(now)
            if key_filter is not None:
                totals = {k: c for k, c in totals.items() if key_filter(k)}
            ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:lim]
            top: List[Dict[str, Any]] = []
            scope = f"namespace:{namespace}" if namespace is not None else ""
            for k, c in ranked:
                qps = float(c) / self._window_s if self._window_s > 0 else 0.0
                hot = threshold > 0.0 and qps >= threshold
                if hot:
                    detection_key = f"{scope}:{repr(k)}" if scope else k
                    if detection_key not in self._last_detected:
                        self._detected_total += 1
                    self._last_detected[detection_key] = now
                display_key = key_mapper(k) if key_mapper is not None else k
                top.append({
                    "key": display_key if isinstance(display_key, str) else str(display_key),
                    "count": int(c),
                    "qps": qps,
                    "hot": hot,
                })
            out = {
                "enabled": True,
                "window_seconds": self._window_s,
                "buckets": self._buckets_n,
                "top_k": lim,
                "threshold_qps": threshold,
                "sample_rate": self._sample_rate,
                "tracked_keys": len(totals),
                "hot_keys_current": sum(1 for e in top if e.get("hot")),
                "hot_keys_detected_total": int(self._detected_total),
                "top": top,
            }
            if namespace is not None:
                out["namespace"] = namespace
            return out

    def snapshot(self) -> Dict[str, Any]:
        """Return a metrics-friendly snapshot of detector state."""
        with self._lock:
            if not self._enabled:
                return {
                    "enabled": False,
                    "window_seconds": self._window_s,
                    "buckets": self._buckets_n,
                    "top_k": self._top_k,
                    "threshold_qps": self._threshold_qps,
                    "sample_rate": self._sample_rate,
                    "tracked_keys": 0,
                    "hot_keys_current": 0,
                    "hot_keys_detected_total": int(self._detected_total),
                    "top": [],
                }
            return self.report(limit=self._top_k)
