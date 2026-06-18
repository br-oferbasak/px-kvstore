#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import hashlib
import json
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from threading import Lock
from .base import _SortedStringKeyIndex
from .cold_eviction import ColdKeyEvictionHints
from ..tiering.base import TieringBackend

try:
    import jsonpatch
    HAS_JSONPATCH = True
except ImportError:
    HAS_JSONPATCH = False

class _Node(object):
    def __init__(self, key: Any, value: Any):
        self.key = key
        self.value = value
        self.prev: Optional["_Node"] = None
        self.next: Optional["_Node"] = None

class LRUKeyValueStore(object):
    """
    Thread-safe in-memory LRU key-value store with optional TTL per key.
    """

    def __init__(
        self,
        max_size: Optional[int] = None,
        tiering: Optional[TieringBackend] = None,
        eviction_hints: Optional[ColdKeyEvictionHints] = None,
    ):
        self._lock = Lock()
        self._max = max_size
        self._map: Dict[Any, _Node] = {}
        self._ttl: Dict[Any, Optional[float]] = {}
        self._xmeta: Dict[Any, Dict[str, Any]] = {}
        self._skeys = _SortedStringKeyIndex()
        self._tiering = tiering
        self._hints = eviction_hints

        self._head = _Node(None, None)
        self._tail = _Node(None, None)
        self._head.next = self._tail
        self._tail.prev = self._head

    def _append(self, node: _Node) -> None:
        last = self._tail.prev
        if last is None:
            return
        last.next = node
        node.prev = last
        node.next = self._tail
        self._tail.prev = node

    def _remove(self, node: _Node) -> None:
        if node.prev is not None:
            node.prev.next = node.next
        if node.next is not None:
            node.next.prev = node.prev
        node.prev = node.next = None

    def _move_to_tail(self, node: _Node) -> None:
        self._remove(node)
        self._append(node)

    def _purge_expired_keys(self) -> List[Any]:
        now = time.time()

        expired = []
        for k, ts in self._ttl.items():
            if ts is not None and ts <= now:
                expired.append(k)

        for k in expired:
            self._delete(k)
        return expired

    def _purge_expired(self) -> None:
        _ = self._purge_expired_keys()

    def _pick_victim(self) -> Optional[_Node]:
        head_next = self._head.next
        if head_next is None or head_next is self._tail:
            return None
        if self._hints is None or not self._hints.is_enabled():
            return head_next
        scan_n = self._hints.scan_candidates()
        candidates: List[_Node] = []
        cur = head_next
        while cur is not None and cur is not self._tail and len(candidates) < scan_n:
            candidates.append(cur)
            cur = cur.next
        if not candidates:
            return head_next
        idx = self._hints.pick_lru_victim([n.key for n in candidates])
        if idx is None:
            return head_next
        return candidates[idx]

    def _evict_if_needed(self) -> None:
        while self._max and len(self._map) > self._max:
            lru = self._pick_victim()
            if lru is None:
                break
            if self._tiering is not None and lru.key is not None:
                now = time.time()
                ts = self._ttl.get(lru.key)
                ttl_remaining = None if ts is None else max(0.0, ts - now)
                if ttl_remaining is None or ttl_remaining > 0:
                    try:
                        self._tiering.put(lru.key, lru.value, ttl_remaining)
                    except Exception:
                        pass
            self._delete(lru.key)

    def _delete(self, key: Any) -> None:
        node = self._map.pop(key, None)
        if node:
            self._remove(node)
        self._ttl.pop(key, None)
        self._xmeta.pop(key, None)
        self._skeys.discard(key)

    def get_xmeta(self, key: Any) -> Optional[Dict[str, Any]]:
        """Get cross-cluster metadata for a key (origin_cluster_id, origin_ts)."""
        with self._lock:
            return dict(self._xmeta[key]) if key in self._xmeta else None

    def set_xmeta(self, key: Any, meta: Dict[str, Any]) -> None:
        """Set cross-cluster metadata for a key."""
        with self._lock:
            self._xmeta[key] = dict(meta)

    def resolve_conflict(
        self,
        key: Any,
        new_value: Any,
        new_ttl: Optional[float],
        new_origin_cluster_id: Optional[str] = None,
        new_origin_ts: Optional[float] = None,
        policy: str = "last_write_wins",
    ) -> Tuple[bool, Any, Optional[float]]:
        """
        Resolve cross-cluster conflict.
        Returns (should_apply, resolved_value, resolved_ttl).
        """
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            current_meta = self._xmeta.get(key, {})
            current_origin_ts = current_meta.get("origin_ts", 0.0)
            current_origin_cluster = current_meta.get("origin_cluster_id", "")

            # If key doesn't exist locally, always apply
            if node is None:
                return True, new_value, new_ttl

            if policy == "last_write_wins":
                if new_origin_ts and new_origin_ts > current_origin_ts:
                    return True, new_value, new_ttl
                elif new_origin_ts and new_origin_ts == current_origin_ts:
                    # Tiebreaker: cluster ID lex order
                    if new_origin_cluster_id and new_origin_cluster_id > current_origin_cluster:
                        return True, new_value, new_ttl
                    else:
                        return False, node.value, self._ttl.get(key)
                else:
                    return False, node.value, self._ttl.get(key)

            # Default: always apply (like before)
            return True, new_value, new_ttl

    def purge_expired(self) -> None:
        with self._lock:
            self._purge_expired()

    def purge_expired_keys(self) -> List[Any]:
        with self._lock:
            return self._purge_expired_keys()

    def create(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key in self._map:
                raise KeyError(key)
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
            node = _Node(key, value)
            self._map[key] = node
            self._append(node)
            self._skeys.add(key)
            if ttl is not None:
                expire_ts = time.time() + ttl
            else:
                expire_ts = None
            self._ttl[key] = expire_ts

            self._evict_if_needed()

    def _compute_etag(self, value: Any, ttl: Optional[float]) -> str:
        """Compute an ETag from the value and TTL timestamp."""
        def _default(obj):
            if isinstance(obj, (bytes, bytearray)):
                return obj.decode("utf-8", errors="replace")
            raise TypeError
        try:
            payload = json.dumps({"value": value, "ttl": ttl}, default=_default, sort_keys=True, ensure_ascii=False).encode("utf-8")
        except TypeError:
            payload = json.dumps({"value": str(value), "ttl": ttl}, sort_keys=True, ensure_ascii=False).encode("utf-8")
        h = hashlib.sha256(payload).hexdigest()
        return f'"{h}"'

    def read(self, key: Any) -> Any:
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                if self._tiering is None:
                    raise KeyError(key)
                try:
                    hit = self._tiering.get(key)
                except Exception:
                    hit = None
                if hit is None:
                    raise KeyError(key)
                node = _Node(key, hit.value)
                self._map[key] = node
                self._append(node)
                self._skeys.add(key)
                if hit.ttl_remaining is None:
                    self._ttl[key] = None
                else:
                    self._ttl[key] = time.time() + float(hit.ttl_remaining)
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
                self._evict_if_needed()
            self._move_to_tail(node)
            return node.value

    def read_with_etag(self, key: Any) -> Tuple[Any, str]:
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                if self._tiering is None:
                    raise KeyError(key)
                try:
                    hit = self._tiering.get(key)
                except Exception:
                    hit = None
                if hit is None:
                    raise KeyError(key)
                node = _Node(key, hit.value)
                self._map[key] = node
                self._append(node)
                self._skeys.add(key)
                if hit.ttl_remaining is None:
                    self._ttl[key] = None
                else:
                    self._ttl[key] = time.time() + float(hit.ttl_remaining)
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
                self._evict_if_needed()
            self._move_to_tail(node)
            ttl = self._ttl.get(key)
            etag = self._compute_etag(node.value, ttl)
            return node.value, etag

    def update(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                raise KeyError(key)
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
            node.value = value
            self._move_to_tail(node)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._evict_if_needed()

    def patch(self, key: Any, patches: List[Dict[str, Any]], ttl: Optional[float] = None) -> Tuple[Any, str]:
        """
        Apply JSON Patch to a key.

        Args:
            key: The key to patch
            patches: List of JSON Patch operations
            ttl: Optional new TTL for the key

        Returns:
            Tuple of (new_value, new_etag)

        Raises:
            KeyError: If the key doesn't exist
            ValueError: If jsonpatch library is not installed
            jsonpatch.JsonPatchException: If patch application fails
        """
        if not HAS_JSONPATCH:
            raise ValueError("jsonpatch library not installed")
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                if self._tiering is not None:
                    try:
                        hit = self._tiering.get(key)
                        if hit is not None:
                            node = _Node(key, hit.value)
                            self._map[key] = node
                            self._append(node)
                            self._skeys.add(key)
                            if hit.ttl_remaining is None:
                                self._ttl[key] = None
                            else:
                                self._ttl[key] = time.time() + float(hit.ttl_remaining)
                            try:
                                self._tiering.delete(key)
                            except Exception:
                                pass
                            self._evict_if_needed()
                    except Exception:
                        pass
                if node is None:
                    raise KeyError(key)
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
            new_value = jsonpatch.apply_patch(node.value, patches)
            node.value = new_value
            self._move_to_tail(node)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._evict_if_needed()
            current_ttl = self._ttl.get(key)
            etag = self._compute_etag(new_value, current_ttl)
            return new_value, etag

    def delete(self, key: Any) -> None:
        with self._lock:
            if key not in self._map:
                raise KeyError(key)
            self._delete(key)
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            for k, v in items.items():
                if self._tiering is not None:
                    try:
                        self._tiering.delete(k)
                    except Exception:
                        pass
                node = self._map.get(k)
                if node:
                    node.value = v
                    self._move_to_tail(node)
                else:
                    node = _Node(k, v)
                    self._map[k] = node
                    self._append(node)
                    self._skeys.add(k)
                if ttl is not None:
                    expire_ts = time.time() + ttl
                else:
                    expire_ts = None
                self._ttl[k] = expire_ts
            self._evict_if_needed()

    def mget(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        with self._lock:
            self._purge_expired()
            out: Dict[Any, Any] = {}
            if self._tiering is not None and hasattr(self._tiering, "prefetch"):
                missing: List[Any] = []
                for k in keys:
                    if k not in self._map:
                        missing.append(k)
                if missing:
                    try:
                        self._tiering.prefetch(missing)  # type: ignore[attr-defined]
                    except Exception:
                        pass
            for k in keys:
                node = self._map.get(k)
                if node is None and self._tiering is not None:
                    try:
                        hit = self._tiering.get(k)
                    except Exception:
                        hit = None
                    if hit is not None:
                        node = _Node(k, hit.value)
                        self._map[k] = node
                        self._append(node)
                        self._skeys.add(k)
                        if hit.ttl_remaining is None:
                            self._ttl[k] = None
                        else:
                            self._ttl[k] = time.time() + float(hit.ttl_remaining)
                        try:
                            self._tiering.delete(k)
                        except Exception:
                            pass
                        self._evict_if_needed()
                if node:
                    self._move_to_tail(node)
                    out[k] = node.value
            return out

    def incr(self, key: Any, delta: float = 1, ttl: Optional[float] = None) -> float:
        with self._lock:
            self._purge_expired()
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
            node = self._map.get(key)
            if node is None:
                current = 0.0
                node = _Node(key, current)
                self._map[key] = node
                self._append(node)
                self._skeys.add(key)
            else:
                current = node.value
                if not isinstance(current, (int, float)):
                    try:
                        current = float(current)
                    except (ValueError, TypeError):
                        raise TypeError("INCR target must be numeric")
            new_val = float(current) + float(delta)
            node.value = new_val
            self._move_to_tail(node)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._evict_if_needed()
            return new_val

    def keys(self) -> List[Any]:
        with self._lock:
            self._purge_expired()
            return list(self._map.keys())

    def get_ttl(self, key: Any) -> Optional[float]:
        """Remaining TTL in seconds. Raises KeyError if key is absent;
        returns None if key has no TTL set; otherwise float seconds (>= 0)."""
        with self._lock:
            self._purge_expired()
            if key not in self._map:
                raise KeyError(key)
            ts = self._ttl.get(key)
            if ts is None:
                return None
            return max(0.0, ts - time.time())

    def persist(self, key: Any) -> bool:
        """Clear the TTL on a key. Raises KeyError if absent.
        Returns True if a TTL was cleared, False if the key had none."""
        with self._lock:
            self._purge_expired()
            if key not in self._map:
                raise KeyError(key)
            had_ttl = self._ttl.get(key) is not None
            self._ttl[key] = None
            return had_ttl

    def iter_string_keys_sorted(
        self, *, prefix: Optional[str] = None, start_after: Optional[str] = None
    ) -> Iterator[str]:
        with self._lock:
            self._purge_expired()
            yield from self._skeys.iter_from(prefix=prefix, start_after=start_after)

    def dump_state(self) -> Dict[str, Any]:
        with self._lock:
            self._purge_expired()
            now = time.time()
            data: Dict[str, Any] = {}
            for k, node in self._map.items():
                ts = self._ttl.get(k)
                if ts is not None and ts <= now:
                    continue
                if ts is None:
                    ttl_remaining = None
                else:
                    ttl_remaining = max(0.0, ts - now)
                entry = {"value": node.value, "ttl": ttl_remaining}
                xmeta = self._xmeta.get(k)
                if xmeta:
                    entry["xmeta"] = dict(xmeta)
                data[k] = entry
            return data

    def load_state(self, data: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            self._map.clear()
            self._ttl.clear()
            self._xmeta.clear()
            self._skeys.clear()
            now = time.time()
            for k, entry in data.items():
                ttl_remaining = entry.get("ttl")
                value = entry.get("value")
                node = _Node(k, value)
                self._map[k] = node
                self._append(node)
                self._skeys.add(k)
                if ttl_remaining is None:
                    expire_ts = None
                else:
                    expire_ts = now + float(ttl_remaining)
                self._ttl[k] = expire_ts
                xmeta = entry.get("xmeta")
                if xmeta:
                    self._xmeta[k] = dict(xmeta)
