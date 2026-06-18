#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Cold key eviction hints.

Maintains a sliding-window per-key access counter that eviction policies use to
bias victim selection toward keys that have not been touched recently. This is
distinct from the underlying LRU/LFU bookkeeping: it reflects observed access
*frequency* over a configurable window, independent of recency or long-term
counters.

Usage:

* ``record(key)`` on every read/write.
* ``forget(key)`` on delete.
* LRU eviction calls ``pick_lru_victim`` to choose among the N least-recently-
  used candidates; the candidate with the lowest hint count wins.
* LFU eviction calls ``adjusted_lfu_score`` to bias scoring so keys with recent
  hint-window activity are protected even when their long-term frequency is
  similar to other candidates.

The hint window is a bucketed counter (same shape as ``HotKeyDetector``) that
ages out automatically.
"""

import time
from collections import defaultdict
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


class ColdKeyEvictionHints:
    """Thread-safe sliding-window access counter that biases eviction toward cold keys."""

    def __init__(
        self,
        enabled: bool = False,
        window_seconds: float = 300.0,
        buckets: int = 10,
        scan_candidates: int = 8,
        cold_threshold_count: int = 1,
        max_tracked_keys: int = 100000,
    ) -> None:
        if buckets < 1:
            buckets = 1
        if window_seconds <= 0:
            window_seconds = 1.0
        self._lock = RLock()
        self._enabled = bool(enabled)
        self._window_s = float(window_seconds)
        self._buckets_n = int(buckets)
        self._bucket_s = self._window_s / float(self._buckets_n)
        self._scan_candidates = max(1, int(scan_candidates))
        self._cold_threshold = max(0, int(cold_threshold_count))
        self._max_tracked = max(0, int(max_tracked_keys))

        self._buckets: List[Dict[Any, int]] = [defaultdict(int) for _ in range(self._buckets_n)]
        self._bucket_starts: List[float] = [0.0] * self._buckets_n
        self._tracked: "Dict[Any, None]" = {}

        self._victims_picked = 0
        self._victims_redirected = 0
        self._lfu_adjusted = 0

    def configure(
        self,
        enabled: Optional[bool] = None,
        window_seconds: Optional[float] = None,
        buckets: Optional[int] = None,
        scan_candidates: Optional[int] = None,
        cold_threshold_count: Optional[int] = None,
        max_tracked_keys: Optional[int] = None,
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
            if scan_candidates is not None:
                self._scan_candidates = max(1, int(scan_candidates))
            if cold_threshold_count is not None:
                self._cold_threshold = max(0, int(cold_threshold_count))
            if max_tracked_keys is not None:
                self._max_tracked = max(0, int(max_tracked_keys))
                self._evict_overflow_locked()
            if structure_changed:
                self._bucket_s = self._window_s / float(self._buckets_n)
                self._buckets = [defaultdict(int) for _ in range(self._buckets_n)]
                self._bucket_starts = [0.0] * self._buckets_n
                self._tracked.clear()

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def reset(self) -> None:
        with self._lock:
            self._buckets = [defaultdict(int) for _ in range(self._buckets_n)]
            self._bucket_starts = [0.0] * self._buckets_n
            self._tracked.clear()
            self._victims_picked = 0
            self._victims_redirected = 0
            self._lfu_adjusted = 0

    def scan_candidates(self) -> int:
        with self._lock:
            return self._scan_candidates

    def _bucket_index(self, now: float) -> int:
        return int(now // self._bucket_s) % self._buckets_n

    def _rotate_locked(self, now: float) -> None:
        idx = self._bucket_index(now)
        start = now - (now % self._bucket_s)
        if self._bucket_starts[idx] != start:
            evicted_keys = list(self._buckets[idx].keys())
            self._buckets[idx] = defaultdict(int)
            self._bucket_starts[idx] = start
            for k in evicted_keys:
                if not any(k in b for b in self._buckets):
                    self._tracked.pop(k, None)

    def _evict_stale_locked(self, now: float) -> None:
        cutoff = now - self._window_s
        for i in range(self._buckets_n):
            if self._bucket_starts[i] != 0.0 and self._bucket_starts[i] < cutoff:
                for k in list(self._buckets[i].keys()):
                    if not any(k in b for j, b in enumerate(self._buckets) if j != i):
                        self._tracked.pop(k, None)
                self._buckets[i] = defaultdict(int)
                self._bucket_starts[i] = 0.0

    def _evict_overflow_locked(self) -> None:
        if self._max_tracked <= 0:
            self._tracked.clear()
            for b in self._buckets:
                b.clear()
            return
        while len(self._tracked) > self._max_tracked:
            try:
                k = next(iter(self._tracked))
                self._tracked.pop(k, None)
                for b in self._buckets:
                    b.pop(k, None)
            except StopIteration:
                break

    def record(self, key: Any, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        with self._lock:
            now = time.time()
            self._rotate_locked(now)
            idx = self._bucket_index(now)
            self._buckets[idx][key] += int(count)
            if key in self._tracked:
                self._tracked.pop(key, None)
            self._tracked[key] = None
            self._evict_overflow_locked()

    def record_many(self, keys: Iterable[Any], count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        for k in keys:
            self.record(k, count)

    def forget(self, key: Any) -> None:
        with self._lock:
            for b in self._buckets:
                b.pop(key, None)
            self._tracked.pop(key, None)

    def access_count(self, key: Any) -> int:
        """Return the total access count for ``key`` across the active window."""
        with self._lock:
            if not self._enabled:
                return 0
            now = time.time()
            self._evict_stale_locked(now)
            total = 0
            for b in self._buckets:
                total += b.get(key, 0)
            return total

    def is_cold(self, key: Any) -> bool:
        """A key is cold when its window access count is at or below the threshold."""
        return self.access_count(key) <= self._cold_threshold

    def pick_lru_victim(
        self,
        candidate_keys: List[Any],
    ) -> Optional[int]:
        """Pick the coldest key among ``candidate_keys`` (head-of-LRU order).

        Returns the index of the chosen victim within ``candidate_keys``. The
        caller passes keys in order from least-recently-used outward; if hints
        are disabled or no candidate has hint data, index 0 (the strict LRU
        head) wins. Otherwise we pick the candidate with the lowest access
        count, breaking ties by preferring lower indices (older LRU position).
        """
        if not candidate_keys:
            return None
        with self._lock:
            if not self._enabled:
                self._victims_picked += 1
                return 0
            now = time.time()
            self._evict_stale_locked(now)
            best_idx = 0
            best_count: Optional[int] = None
            for idx, k in enumerate(candidate_keys[: self._scan_candidates]):
                c = 0
                for b in self._buckets:
                    c += b.get(k, 0)
                if best_count is None or c < best_count:
                    best_count = c
                    best_idx = idx
                    if c == 0:
                        break
            self._victims_picked += 1
            if best_idx != 0:
                self._victims_redirected += 1
            return best_idx

    def adjusted_lfu_score(
        self,
        key: Any,
        base_score: Tuple[int, int],
    ) -> Tuple[int, int, int]:
        """Augment an LFU ``(freq, last_access_seq)`` score with hint frequency.

        The returned tuple ``(freq, hint_count, last_access_seq)`` keeps the
        long-term freq as the primary key (so LFU semantics are preserved when
        all candidates are similarly hot or similarly cold) and uses the hint
        count as a tiebreaker before recency. A key with no hint activity will
        be evicted before one with recent accesses, biasing eviction toward
        truly cold keys.
        """
        freq, last = base_score
        if not self._enabled:
            return (freq, 0, last)
        with self._lock:
            now = time.time()
            self._evict_stale_locked(now)
            c = 0
            for b in self._buckets:
                c += b.get(key, 0)
            self._lfu_adjusted += 1
            return (freq, c, last)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            if not self._enabled:
                return {
                    "enabled": False,
                    "window_seconds": self._window_s,
                    "buckets": self._buckets_n,
                    "scan_candidates": self._scan_candidates,
                    "cold_threshold_count": self._cold_threshold,
                    "max_tracked_keys": self._max_tracked,
                    "tracked_keys": 0,
                    "victims_picked": int(self._victims_picked),
                    "victims_redirected": int(self._victims_redirected),
                    "lfu_adjusted": int(self._lfu_adjusted),
                }
            now = time.time()
            self._evict_stale_locked(now)
            tracked = len({k for b in self._buckets for k in b.keys()})
            return {
                "enabled": True,
                "window_seconds": self._window_s,
                "buckets": self._buckets_n,
                "scan_candidates": self._scan_candidates,
                "cold_threshold_count": self._cold_threshold,
                "max_tracked_keys": self._max_tracked,
                "tracked_keys": tracked,
                "victims_picked": int(self._victims_picked),
                "victims_redirected": int(self._victims_redirected),
                "lfu_adjusted": int(self._lfu_adjusted),
            }
