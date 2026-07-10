#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bounded query plan cache for repeated Redis commands and scan requests.

The cache stores parsed command metadata, not command results. It is safe for
mutating Redis commands because values are still read from the request and all
store operations execute normally.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable, Dict, Optional, Tuple

from ..config.settings import settings
from ..namespaces import namespace_manager


@dataclass(frozen=True)
class RedisCommandPlan:
    cmd: str
    argc: int
    str_args: Tuple[str, ...]


@dataclass(frozen=True)
class ScanPlan:
    namespace: str
    prefix: Optional[str]
    start_after: Optional[str]
    cursor: Optional[str]
    limit: int
    storage_prefix: Optional[str]
    storage_start_after: Optional[str]
    cursor_mode: bool


class _LRUCache:
    def __init__(self, max_entries: int) -> None:
        self.max_entries = max(1, int(max_entries))
        self._data: OrderedDict[Any, Any] = OrderedDict()

    def resize(self, max_entries: int) -> None:
        self.max_entries = max(1, int(max_entries))
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)

    def get(self, key: Any) -> Any:
        try:
            value = self._data.pop(key)
        except KeyError:
            return None
        self._data[key] = value
        return value

    def put(self, key: Any, value: Any) -> None:
        if key in self._data:
            self._data.pop(key, None)
        self._data[key] = value
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)

    def clear(self) -> None:
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)


class QueryPlanCache:
    def __init__(self) -> None:
        self._lock = RLock()
        self._enabled = bool(getattr(settings, "QUERY_PLAN_CACHE_ENABLED", False))
        max_entries = int(getattr(settings, "QUERY_PLAN_CACHE_MAX_ENTRIES", 1024) or 1024)
        self._redis = _LRUCache(max_entries)
        self._scan = _LRUCache(max_entries)
        self._redis_hits = 0
        self._redis_misses = 0
        self._scan_hits = 0
        self._scan_misses = 0
        self._redis_cached = 0
        self._scan_cached = 0

    def configure(self, enabled: Optional[bool] = None, max_entries: Optional[int] = None) -> None:
        with self._lock:
            if enabled is not None:
                self._enabled = bool(enabled)
            if max_entries is not None:
                size = max(1, int(max_entries))
                self._redis.resize(size)
                self._scan.resize(size)
            if not self._enabled:
                self._redis.clear()
                self._scan.clear()

    def reset(self) -> None:
        with self._lock:
            self._redis.clear()
            self._scan.clear()
            self._redis_hits = 0
            self._redis_misses = 0
            self._scan_hits = 0
            self._scan_misses = 0
            self._redis_cached = 0
            self._scan_cached = 0

    def redis_plan(self, args: list[bytes]) -> RedisCommandPlan:
        if not args:
            return RedisCommandPlan("", 0, ())
        key = tuple(args)
        with self._lock:
            if self._enabled:
                cached = self._redis.get(key)
                if cached is not None:
                    self._redis_hits += 1
                    return cached
                self._redis_misses += 1
            plan = RedisCommandPlan(
                cmd=args[0].decode("utf-8", errors="replace").upper(),
                argc=len(args),
                str_args=tuple(a.decode("utf-8", errors="replace") for a in args),
            )
            if self._enabled:
                self._redis.put(key, plan)
                self._redis_cached += 1
            return plan

    def scan_plan(
        self,
        *,
        namespace: str,
        prefix: Optional[str],
        start_after: Optional[str],
        cursor: Optional[str],
        limit: int,
        cursor_mode: bool,
        ns_prefix_fn: Callable[[str, Optional[str]], Optional[str]],
        ns_key_fn: Callable[[str, Any], Any],
    ) -> ScanPlan:
        lim = int(limit)
        scope_key = (bool(namespace_manager.enabled()), namespace, prefix, start_after, cursor, lim, bool(cursor_mode))
        with self._lock:
            if self._enabled:
                cached = self._scan.get(scope_key)
                if cached is not None:
                    self._scan_hits += 1
                    return cached
                self._scan_misses += 1
            plan = ScanPlan(
                namespace=namespace,
                prefix=prefix,
                start_after=start_after,
                cursor=cursor,
                limit=lim,
                storage_prefix=ns_prefix_fn(namespace, prefix),
                storage_start_after=ns_key_fn(namespace, start_after) if start_after else None,
                cursor_mode=bool(cursor_mode),
            )
            if self._enabled:
                self._scan.put(scope_key, plan)
                self._scan_cached += 1
            return plan

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            redis_total = self._redis_hits + self._redis_misses
            scan_total = self._scan_hits + self._scan_misses
            return {
                "enabled": self._enabled,
                "max_entries": self._redis.max_entries,
                "redis_entries": len(self._redis),
                "scan_entries": len(self._scan),
                "redis_hits": int(self._redis_hits),
                "redis_misses": int(self._redis_misses),
                "redis_cached": int(self._redis_cached),
                "redis_hit_ratio": float(self._redis_hits) / float(redis_total) if redis_total else 0.0,
                "scan_hits": int(self._scan_hits),
                "scan_misses": int(self._scan_misses),
                "scan_cached": int(self._scan_cached),
                "scan_hit_ratio": float(self._scan_hits) / float(scan_total) if scan_total else 0.0,
            }


query_plan_cache = QueryPlanCache()
