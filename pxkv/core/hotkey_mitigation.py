#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hot key mitigation: per-key caching + request coalescing for detected hot keys.

This layer sits in front of the underlying shard for read operations. For keys
the HotKeyDetector marks as hot we:

* Cache the value with a short TTL so subsequent reads skip the shard entirely
  (a local "replication" of hot keys onto the read path).
* Single-flight concurrent misses: only one thread fetches from the shard;
  other threads waiting on the same key receive the same result.

Writes invalidate the cached entry. Cold keys flow through untouched.
"""

import time
from threading import Event, RLock
from typing import Any, Callable, Dict, Iterable, Optional, Tuple


_MISSING = object()


class _Inflight:
    __slots__ = ("event", "value", "etag", "error")

    def __init__(self) -> None:
        self.event = Event()
        self.value: Any = _MISSING
        self.etag: Optional[str] = None
        self.error: Optional[BaseException] = None


class HotKeyMitigator:
    """Per-key cache + single-flight coalescing for keys flagged hot."""

    def __init__(
        self,
        detector: Any,
        enabled: bool = False,
        cache_ttl_seconds: float = 1.0,
        max_entries: int = 1024,
        refresh_interval_seconds: float = 1.0,
    ) -> None:
        self._lock = RLock()
        self._detector = detector
        self._enabled = bool(enabled)
        self._cache_ttl_s = max(0.0, float(cache_ttl_seconds))
        self._max_entries = max(0, int(max_entries))
        self._refresh_interval_s = max(0.0, float(refresh_interval_seconds))

        # key -> (value, etag_or_None, expires_at)
        self._cache: Dict[Any, Tuple[Any, Optional[str], float]] = {}
        self._inflight: Dict[Any, _Inflight] = {}
        self._hot_set: set = set()
        self._hot_set_refreshed_at: float = 0.0

        self._cache_hits = 0
        self._cache_misses = 0
        self._coalesced_waiters = 0
        self._invalidations = 0
        self._evictions = 0

    def configure(
        self,
        enabled: Optional[bool] = None,
        cache_ttl_seconds: Optional[float] = None,
        max_entries: Optional[int] = None,
        refresh_interval_seconds: Optional[float] = None,
    ) -> None:
        with self._lock:
            if enabled is not None:
                self._enabled = bool(enabled)
                if not self._enabled:
                    self._cache.clear()
                    self._hot_set.clear()
                    self._hot_set_refreshed_at = 0.0
            if cache_ttl_seconds is not None:
                self._cache_ttl_s = max(0.0, float(cache_ttl_seconds))
            if max_entries is not None:
                self._max_entries = max(0, int(max_entries))
                self._evict_overflow_locked()
            if refresh_interval_seconds is not None:
                self._refresh_interval_s = max(0.0, float(refresh_interval_seconds))

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def reset(self) -> None:
        with self._lock:
            self._cache.clear()
            self._inflight.clear()
            self._hot_set.clear()
            self._hot_set_refreshed_at = 0.0
            self._cache_hits = 0
            self._cache_misses = 0
            self._coalesced_waiters = 0
            self._invalidations = 0
            self._evictions = 0

    @staticmethod
    def _key_repr(key: Any) -> str:
        return key if isinstance(key, str) else str(key)

    def _refresh_hot_set_locked(self, now: float) -> None:
        if self._refresh_interval_s <= 0.0:
            return
        if now - self._hot_set_refreshed_at < self._refresh_interval_s:
            return
        detector = self._detector
        if detector is None or not detector.is_enabled():
            self._hot_set.clear()
            self._hot_set_refreshed_at = now
            return
        try:
            top = detector.top_hot_keys()
        except Exception:
            top = []
        new_set = {entry["key"] for entry in top if entry.get("hot")}
        if new_set != self._hot_set:
            cooled = self._hot_set - new_set
            for k in cooled:
                self._cache.pop(k, None)
        self._hot_set = new_set
        self._hot_set_refreshed_at = now

    def is_hot(self, key: Any) -> bool:
        with self._lock:
            if not self._enabled:
                return False
            self._refresh_hot_set_locked(time.time())
            return self._key_repr(key) in self._hot_set

    def _evict_overflow_locked(self) -> None:
        if self._max_entries <= 0:
            self._cache.clear()
            return
        while len(self._cache) > self._max_entries:
            try:
                self._cache.pop(next(iter(self._cache)))
                self._evictions += 1
            except StopIteration:
                break

    def invalidate(self, key: Any) -> None:
        with self._lock:
            if self._cache.pop(key, None) is not None:
                self._invalidations += 1

    def invalidate_many(self, keys: Iterable[Any]) -> None:
        with self._lock:
            for k in keys:
                if self._cache.pop(k, None) is not None:
                    self._invalidations += 1

    def store(self, key: Any, value: Any, etag: Optional[str] = None) -> None:
        if not self._enabled or self._cache_ttl_s <= 0.0 or self._max_entries <= 0:
            return
        with self._lock:
            self._cache.pop(key, None)
            self._cache[key] = (value, etag, time.time() + self._cache_ttl_s)
            self._evict_overflow_locked()

    def read_through(
        self,
        key: Any,
        loader: Callable[[], Any],
    ) -> Tuple[Any, bool]:
        """Return (value, served_from_cache). Falls through to ``loader`` for cold keys."""
        value, _etag, served = self._read_through_inner(key, loader, want_etag=False)
        return value, served

    def read_through_with_etag(
        self,
        key: Any,
        loader: Callable[[], Tuple[Any, str]],
    ) -> Tuple[Any, str, bool]:
        value, etag, served = self._read_through_inner(key, loader, want_etag=True)
        return value, etag or "", served

    def _call_loader(
        self, loader: Callable[[], Any], want_etag: bool
    ) -> Tuple[Any, Optional[str]]:
        result = loader()
        if want_etag:
            value, etag = result
            return value, etag
        return result, None

    def _read_through_inner(
        self,
        key: Any,
        loader: Callable[[], Any],
        want_etag: bool,
    ) -> Tuple[Any, Optional[str], bool]:
        if not self._enabled:
            value, etag = self._call_loader(loader, want_etag)
            return value, etag, False

        leader = False
        inflight: Optional[_Inflight] = None
        with self._lock:
            now = time.time()
            self._refresh_hot_set_locked(now)
            if self._key_repr(key) not in self._hot_set:
                # cold path: skip cache + coalescing entirely
                pass
            else:
                entry = self._cache.get(key)
                if entry is not None:
                    cached_value, cached_etag, expires_at = entry
                    if expires_at > now:
                        self._cache_hits += 1
                        return cached_value, cached_etag, True
                    self._cache.pop(key, None)

                self._cache_misses += 1
                inflight = self._inflight.get(key)
                if inflight is None:
                    inflight = _Inflight()
                    self._inflight[key] = inflight
                    leader = True
                else:
                    self._coalesced_waiters += 1

        if inflight is None:
            # cold key: call loader without coalescing
            value, etag = self._call_loader(loader, want_etag)
            return value, etag, False

        if leader:
            try:
                value, etag = self._call_loader(loader, want_etag)
            except BaseException as exc:
                with self._lock:
                    inflight.error = exc
                    self._inflight.pop(key, None)
                inflight.event.set()
                raise
            with self._lock:
                inflight.value = value
                inflight.etag = etag
                if self._cache_ttl_s > 0.0 and self._max_entries > 0:
                    self._cache.pop(key, None)
                    self._cache[key] = (value, etag, time.time() + self._cache_ttl_s)
                    self._evict_overflow_locked()
                self._inflight.pop(key, None)
            inflight.event.set()
            return value, etag, False

        # follower: wait for leader's result
        inflight.event.wait()
        if inflight.error is not None:
            raise inflight.error
        if inflight.value is _MISSING:
            value, etag = self._call_loader(loader, want_etag)
            return value, etag, False
        return inflight.value, inflight.etag, True

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self._enabled,
                "cache_ttl_seconds": self._cache_ttl_s,
                "max_entries": self._max_entries,
                "refresh_interval_seconds": self._refresh_interval_s,
                "cache_size": len(self._cache),
                "hot_set_size": len(self._hot_set),
                "inflight": len(self._inflight),
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
                "coalesced_waiters": self._coalesced_waiters,
                "invalidations": self._invalidations,
                "evictions": self._evictions,
            }
