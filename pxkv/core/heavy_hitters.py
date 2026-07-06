#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Top-K heavy hitters via Count-Min Sketch.

A bounded-memory approximation of the existing :class:`HotKeyDetector`. The
sketch tracks per-key access counts in a fixed-size 2D array, so memory is
``O(width * depth)`` regardless of how many distinct keys are seen. On every
record we also maintain a top-K dictionary of the heaviest hitters using a
Space-Saving / Stream-Summary style replacement: a new key replaces the
current minimum if its CMS-estimated count exceeds the min.

This is intended for workloads where the cardinality of accessed keys
exceeds the per-bucket map size of :class:`HotKeyDetector`. CMS counts are
upper-bound estimates -- they never under-count, but may over-count by a
factor related to hash collisions.

Optional periodic decay halves all counts so the tracker adapts to shifting
hot sets over time. Decay is lazy: the first record after the decay interval
elapses triggers a halving pass.
"""

import time
import hashlib
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional


_PRIMES = (
    0x9E3779B97F4A7C15,
    0xBF58476D1CE4E5B9,
    0x94D049BB133111EB,
    0xD1342543DE82EF95,
    0xC2B2AE3D27D4EB4F,
    0x165667B19E3779F9,
    0x85EBCA77C2B2AE63,
    0xCC9E2D51AB1C5ED5,
)


class CountMinSketch:
    """Simple integer Count-Min Sketch with ``depth`` rows of ``width`` columns."""

    def __init__(self, width: int, depth: int) -> None:
        if width < 1:
            width = 1
        if depth < 1:
            depth = 1
        self._width = int(width)
        self._depth = min(int(depth), len(_PRIMES))
        self._rows: List[List[int]] = [[0] * self._width for _ in range(self._depth)]
        self._seeds: List[int] = list(_PRIMES[: self._depth])

    @property
    def width(self) -> int:
        return self._width

    @property
    def depth(self) -> int:
        return self._depth

    def _index(self, row: int, key: Any) -> int:
        if isinstance(key, str):
            raw = key.encode("utf-8", errors="replace")
        elif isinstance(key, bytes):
            raw = key
        else:
            raw = str(key).encode("utf-8", errors="replace")
        seed = self._seeds[row].to_bytes(8, "little", signed=False)
        h = int.from_bytes(hashlib.blake2b(raw, digest_size=8, key=seed).digest(), "little")
        return h % self._width

    def add(self, key: Any, count: int = 1) -> int:
        """Add ``count`` to ``key`` and return its estimated total."""
        if count <= 0:
            return self.estimate(key)
        est: Optional[int] = None
        for r in range(self._depth):
            col = self._index(r, key)
            self._rows[r][col] += int(count)
            v = self._rows[r][col]
            if est is None or v < est:
                est = v
        return int(est or 0)

    def estimate(self, key: Any) -> int:
        est: Optional[int] = None
        for r in range(self._depth):
            col = self._index(r, key)
            v = self._rows[r][col]
            if est is None or v < est:
                est = v
        return int(est or 0)

    def decay(self, factor: float) -> None:
        """Multiply every cell by ``factor`` (truncated to int).

        ``factor=0.5`` halves all counts; ``factor=0`` clears the sketch.
        """
        f = max(0.0, min(1.0, float(factor)))
        if f >= 1.0:
            return
        if f <= 0.0:
            self.clear()
            return
        for r in range(self._depth):
            row = self._rows[r]
            for i in range(self._width):
                row[i] = int(row[i] * f)

    def clear(self) -> None:
        for r in range(self._depth):
            row = self._rows[r]
            for i in range(self._width):
                row[i] = 0


class TopKHeavyHitters:
    """Thread-safe bounded-memory top-K hot-key tracker backed by CMS."""

    def __init__(
        self,
        enabled: bool = False,
        k: int = 10,
        cms_width: int = 2048,
        cms_depth: int = 4,
        decay_interval_seconds: float = 60.0,
        decay_factor: float = 0.5,
        threshold_count: int = 0,
    ) -> None:
        self._lock = RLock()
        self._enabled = bool(enabled)
        self._k = max(1, int(k))
        self._cms = CountMinSketch(int(cms_width), int(cms_depth))
        self._decay_interval_s = max(0.0, float(decay_interval_seconds))
        self._decay_factor = max(0.0, min(1.0, float(decay_factor)))
        self._threshold_count = max(0, int(threshold_count))

        # Tracked top-K candidates: key -> estimated count snapshot.
        self._tracked: Dict[Any, int] = {}
        self._last_decay_ts: float = time.time()

        self._records = 0
        self._decays = 0
        self._evictions = 0
        self._detected_total = 0
        self._last_detected: Dict[Any, float] = {}

    def configure(
        self,
        enabled: Optional[bool] = None,
        k: Optional[int] = None,
        cms_width: Optional[int] = None,
        cms_depth: Optional[int] = None,
        decay_interval_seconds: Optional[float] = None,
        decay_factor: Optional[float] = None,
        threshold_count: Optional[int] = None,
    ) -> None:
        with self._lock:
            if enabled is not None:
                self._enabled = bool(enabled)
            if k is not None:
                self._k = max(1, int(k))
                self._trim_tracked_locked()
            sketch_changed = False
            if cms_width is not None and int(cms_width) >= 1:
                sketch_changed = sketch_changed or (int(cms_width) != self._cms.width)
                width = int(cms_width)
            else:
                width = self._cms.width
            if cms_depth is not None and int(cms_depth) >= 1:
                sketch_changed = sketch_changed or (int(cms_depth) != self._cms.depth)
                depth = int(cms_depth)
            else:
                depth = self._cms.depth
            if sketch_changed:
                self._cms = CountMinSketch(width, depth)
                self._tracked.clear()
                self._last_decay_ts = time.time()
            if decay_interval_seconds is not None:
                self._decay_interval_s = max(0.0, float(decay_interval_seconds))
            if decay_factor is not None:
                self._decay_factor = max(0.0, min(1.0, float(decay_factor)))
            if threshold_count is not None:
                self._threshold_count = max(0, int(threshold_count))

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def reset(self) -> None:
        with self._lock:
            self._cms.clear()
            self._tracked.clear()
            self._last_decay_ts = time.time()
            self._records = 0
            self._decays = 0
            self._evictions = 0
            self._detected_total = 0
            self._last_detected.clear()

    def forget(self, key: Any) -> None:
        with self._lock:
            self._tracked.pop(key, None)
            self._last_detected.pop(key, None)

    def _trim_tracked_locked(self) -> None:
        while len(self._tracked) > self._k:
            victim = min(self._tracked.items(), key=lambda kv: kv[1])[0]
            self._tracked.pop(victim, None)
            self._evictions += 1

    def _maybe_decay_locked(self, now: float) -> None:
        if self._decay_interval_s <= 0.0:
            return
        if now - self._last_decay_ts < self._decay_interval_s:
            return
        if self._decay_factor >= 1.0:
            self._last_decay_ts = now
            return
        self._cms.decay(self._decay_factor)
        if self._decay_factor <= 0.0:
            self._tracked.clear()
        else:
            decayed: Dict[Any, int] = {}
            for k, v in self._tracked.items():
                nv = int(v * self._decay_factor)
                if nv > 0:
                    decayed[k] = nv
            self._tracked = decayed
        self._last_decay_ts = now
        self._decays += 1

    def record(self, key: Any, count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        with self._lock:
            now = time.time()
            self._maybe_decay_locked(now)
            est = self._cms.add(key, count)
            self._records += 1
            if key in self._tracked:
                self._tracked[key] = est
                return
            if len(self._tracked) < self._k:
                self._tracked[key] = est
                return
            # At capacity: replace minimum if our estimate beats it.
            min_key, min_count = min(self._tracked.items(), key=lambda kv: kv[1])
            if est > min_count:
                self._tracked.pop(min_key, None)
                self._tracked[key] = est
                self._evictions += 1

    def record_many(self, keys: Iterable[Any], count: int = 1) -> None:
        if not self._enabled or count <= 0:
            return
        for k in keys:
            self.record(k, count)

    def estimate(self, key: Any) -> int:
        with self._lock:
            if not self._enabled:
                return 0
            return self._cms.estimate(key)

    def is_hot(self, key: Any) -> bool:
        with self._lock:
            if not self._enabled or self._threshold_count <= 0:
                return False
            return self._cms.estimate(key) >= self._threshold_count

    def top_k(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return top-K observed heavy hitters with refreshed CMS estimates."""
        if not self._enabled:
            return []
        with self._lock:
            now = time.time()
            self._maybe_decay_locked(now)
            lim = self._k if limit is None else max(1, int(limit))
            refreshed: List[tuple] = []
            for k in list(self._tracked.keys()):
                est = self._cms.estimate(k)
                self._tracked[k] = est
                refreshed.append((k, est))
            refreshed.sort(key=lambda kv: kv[1], reverse=True)
            out: List[Dict[str, Any]] = []
            for k, c in refreshed[:lim]:
                hot = self._threshold_count > 0 and c >= self._threshold_count
                if hot:
                    if k not in self._last_detected:
                        self._detected_total += 1
                    self._last_detected[k] = now
                out.append({
                    "key": k if isinstance(k, str) else str(k),
                    "estimated_count": int(c),
                    "hot": hot,
                })
            return out

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            base = {
                "enabled": self._enabled,
                "k": self._k,
                "cms_width": self._cms.width,
                "cms_depth": self._cms.depth,
                "decay_interval_seconds": self._decay_interval_s,
                "decay_factor": self._decay_factor,
                "threshold_count": self._threshold_count,
                "tracked_keys": len(self._tracked),
                "records": int(self._records),
                "decays": int(self._decays),
                "evictions": int(self._evictions),
                "detected_total": int(self._detected_total),
            }
            if not self._enabled:
                base["top"] = []
                return base
            base["top"] = self.top_k(limit=self._k)
            return base
