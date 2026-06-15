#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Adaptive TTL controller.

Per-key TTLs are tuned from observed access patterns to maximize cache hit
ratio:

* Each read records a hit or miss against the key's running stats.
* Each write asks ``suggest_ttl`` for a tuned TTL based on the key's
  hit-rate and recency score.
* Frequently accessed, recently touched keys earn longer TTLs (up to a
  ceiling); rarely accessed or stale keys are shrunk toward a floor.

The controller is intentionally lightweight: an in-memory bounded dict of
per-key counters with insertion-order eviction once we exceed
``max_tracked_keys``. No state is persisted -- this is a runtime heuristic
that converges from live traffic.
"""

import math
import time
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional


class _KeyStat:
    __slots__ = ("hits", "misses", "last_access_ts", "last_set_ts", "last_tuned_ttl")

    def __init__(self, now: float) -> None:
        self.hits: int = 0
        self.misses: int = 0
        self.last_access_ts: float = now
        self.last_set_ts: float = now
        self.last_tuned_ttl: Optional[float] = None


class AdaptiveTTLController:
    """Thread-safe per-key TTL tuner driven by access frequency + recency."""

    def __init__(
        self,
        enabled: bool = False,
        min_ttl_seconds: float = 1.0,
        max_ttl_seconds: float = 86400.0,
        default_base_ttl_seconds: float = 60.0,
        hit_extend_factor: float = 2.0,
        miss_shrink_factor: float = 0.5,
        recency_half_life_seconds: float = 300.0,
        max_tracked_keys: int = 10000,
    ) -> None:
        self._lock = RLock()
        self._enabled = bool(enabled)
        self._min_ttl = max(0.0, float(min_ttl_seconds))
        self._max_ttl = max(self._min_ttl, float(max_ttl_seconds))
        self._default_base_ttl = max(0.0, float(default_base_ttl_seconds))
        self._hit_extend = max(1.0, float(hit_extend_factor))
        self._miss_shrink = max(0.0, min(1.0, float(miss_shrink_factor)))
        self._half_life_s = max(1e-6, float(recency_half_life_seconds))
        self._max_tracked = max(0, int(max_tracked_keys))

        self._stats: Dict[Any, _KeyStat] = {}
        self._total_hits = 0
        self._total_misses = 0
        self._suggestions = 0
        self._extensions = 0
        self._shrinks = 0
        self._evictions = 0

    def configure(
        self,
        enabled: Optional[bool] = None,
        min_ttl_seconds: Optional[float] = None,
        max_ttl_seconds: Optional[float] = None,
        default_base_ttl_seconds: Optional[float] = None,
        hit_extend_factor: Optional[float] = None,
        miss_shrink_factor: Optional[float] = None,
        recency_half_life_seconds: Optional[float] = None,
        max_tracked_keys: Optional[int] = None,
    ) -> None:
        with self._lock:
            if enabled is not None:
                was_enabled = self._enabled
                self._enabled = bool(enabled)
                if was_enabled and not self._enabled:
                    self._stats.clear()
            if min_ttl_seconds is not None:
                self._min_ttl = max(0.0, float(min_ttl_seconds))
            if max_ttl_seconds is not None:
                self._max_ttl = max(self._min_ttl, float(max_ttl_seconds))
            if self._max_ttl < self._min_ttl:
                self._max_ttl = self._min_ttl
            if default_base_ttl_seconds is not None:
                self._default_base_ttl = max(0.0, float(default_base_ttl_seconds))
            if hit_extend_factor is not None:
                self._hit_extend = max(1.0, float(hit_extend_factor))
            if miss_shrink_factor is not None:
                self._miss_shrink = max(0.0, min(1.0, float(miss_shrink_factor)))
            if recency_half_life_seconds is not None:
                self._half_life_s = max(1e-6, float(recency_half_life_seconds))
            if max_tracked_keys is not None:
                self._max_tracked = max(0, int(max_tracked_keys))
                self._evict_overflow_locked()

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def reset(self) -> None:
        with self._lock:
            self._stats.clear()
            self._total_hits = 0
            self._total_misses = 0
            self._suggestions = 0
            self._extensions = 0
            self._shrinks = 0
            self._evictions = 0

    def forget(self, key: Any) -> None:
        with self._lock:
            self._stats.pop(key, None)

    def forget_many(self, keys: Iterable[Any]) -> None:
        with self._lock:
            for k in keys:
                self._stats.pop(k, None)

    def _evict_overflow_locked(self) -> None:
        if self._max_tracked <= 0:
            if self._stats:
                self._evictions += len(self._stats)
                self._stats.clear()
            return
        while len(self._stats) > self._max_tracked:
            try:
                self._stats.pop(next(iter(self._stats)))
                self._evictions += 1
            except StopIteration:
                break

    def _stat_locked(self, key: Any, now: float, *, create: bool) -> Optional[_KeyStat]:
        st = self._stats.get(key)
        if st is None:
            if not create:
                return None
            st = _KeyStat(now)
            self._stats[key] = st
            self._evict_overflow_locked()
            return self._stats.get(key)
        # Refresh insertion order so recently touched stays "warm" against eviction.
        try:
            self._stats.move_to_end(key)  # type: ignore[attr-defined]
        except AttributeError:
            self._stats.pop(key, None)
            self._stats[key] = st
        return st

    def record_hit(self, key: Any) -> None:
        if not self._enabled:
            return
        with self._lock:
            now = time.time()
            st = self._stat_locked(key, now, create=True)
            if st is None:
                return
            st.hits += 1
            st.last_access_ts = now
            self._total_hits += 1

    def record_miss(self, key: Any) -> None:
        if not self._enabled:
            return
        with self._lock:
            now = time.time()
            st = self._stat_locked(key, now, create=True)
            if st is None:
                return
            st.misses += 1
            st.last_access_ts = now
            self._total_misses += 1

    def record_set(self, key: Any, tuned_ttl: Optional[float]) -> None:
        if not self._enabled:
            return
        with self._lock:
            now = time.time()
            st = self._stat_locked(key, now, create=True)
            if st is None:
                return
            st.last_set_ts = now
            st.last_tuned_ttl = tuned_ttl

    def _score_locked(self, st: _KeyStat, now: float) -> float:
        total = st.hits + st.misses
        if total <= 0:
            hit_rate = 0.0
        else:
            hit_rate = st.hits / float(total)
        elapsed = max(0.0, now - st.last_access_ts)
        recency = math.exp(-elapsed * math.log(2.0) / self._half_life_s)
        return max(0.0, min(1.0, hit_rate * recency))

    def suggest_ttl(self, key: Any, base_ttl: Optional[float]) -> Optional[float]:
        """Return a tuned TTL for ``key`` given a caller-supplied ``base_ttl``.

        Behavior:
        * Disabled / no base: returns ``base_ttl`` unchanged (no clamping).
        * No observed stats: returns ``base_ttl`` clamped to [min, max].
        * Otherwise applies factor = lerp(miss_shrink, hit_extend, score),
          clamped to [min_ttl, max_ttl].
        """
        if not self._enabled:
            return base_ttl
        b = base_ttl if base_ttl is not None else self._default_base_ttl
        if b <= 0:
            return base_ttl
        with self._lock:
            self._suggestions += 1
            now = time.time()
            st = self._stats.get(key)
            if st is None:
                tuned = max(self._min_ttl, min(self._max_ttl, float(b)))
                return tuned
            score = self._score_locked(st, now)
            factor = self._miss_shrink + (self._hit_extend - self._miss_shrink) * score
            tuned = float(b) * factor
            tuned = max(self._min_ttl, min(self._max_ttl, tuned))
            if tuned > float(b):
                self._extensions += 1
            elif tuned < float(b):
                self._shrinks += 1
            st.last_tuned_ttl = tuned
            return tuned

    def key_stats(self, key: Any) -> Optional[Dict[str, Any]]:
        with self._lock:
            st = self._stats.get(key)
            if st is None:
                return None
            now = time.time()
            return {
                "key": key if isinstance(key, str) else str(key),
                "hits": int(st.hits),
                "misses": int(st.misses),
                "hit_rate": (st.hits / float(st.hits + st.misses)) if (st.hits + st.misses) > 0 else 0.0,
                "last_access_age_seconds": max(0.0, now - st.last_access_ts),
                "last_set_age_seconds": max(0.0, now - st.last_set_ts),
                "last_tuned_ttl": st.last_tuned_ttl,
                "score": self._score_locked(st, now),
            }

    def top_tracked(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            now = time.time()
            ranked = sorted(
                self._stats.items(),
                key=lambda kv: (kv[1].hits + kv[1].misses, kv[1].last_access_ts),
                reverse=True,
            )[: max(1, int(limit))]
            out: List[Dict[str, Any]] = []
            for k, st in ranked:
                total = st.hits + st.misses
                hit_rate = (st.hits / float(total)) if total > 0 else 0.0
                out.append({
                    "key": k if isinstance(k, str) else str(k),
                    "hits": int(st.hits),
                    "misses": int(st.misses),
                    "hit_rate": hit_rate,
                    "last_access_age_seconds": max(0.0, now - st.last_access_ts),
                    "last_tuned_ttl": st.last_tuned_ttl,
                    "score": self._score_locked(st, now),
                })
            return out

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            total_acc = self._total_hits + self._total_misses
            overall_hit_rate = (self._total_hits / float(total_acc)) if total_acc > 0 else 0.0
            return {
                "enabled": self._enabled,
                "min_ttl_seconds": self._min_ttl,
                "max_ttl_seconds": self._max_ttl,
                "default_base_ttl_seconds": self._default_base_ttl,
                "hit_extend_factor": self._hit_extend,
                "miss_shrink_factor": self._miss_shrink,
                "recency_half_life_seconds": self._half_life_s,
                "max_tracked_keys": self._max_tracked,
                "tracked_keys": len(self._stats),
                "total_hits": int(self._total_hits),
                "total_misses": int(self._total_misses),
                "overall_hit_rate": overall_hit_rate,
                "suggestions": int(self._suggestions),
                "extensions": int(self._extensions),
                "shrinks": int(self._shrinks),
                "evictions": int(self._evictions),
            }
