#!/usr/bin/env python
# -*- coding: utf-8 -*-

import heapq
import math
import random
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


Vector = Tuple[float, ...]


def normalize_vector(vector: Iterable[Any]) -> Vector:
    out: List[float] = []
    for item in vector:
        try:
            value = float(item)
        except (TypeError, ValueError):
            raise ValueError("vector values must be numeric")
        if not math.isfinite(value):
            raise ValueError("vector values must be finite")
        out.append(value)
    if not out:
        raise ValueError("vector must not be empty")
    return tuple(out)


class HNSWVectorIndex:
    """
    Small dependency-free HNSW index for in-memory embedding search.

    It keeps tombstones for deletes and rebuilds links on demand after updates.
    Search results are ranked by the configured metric and can be validated by
    callers against their source KV records.
    """

    def __init__(
        self,
        *,
        metric: str = "cosine",
        m: int = 16,
        ef_construction: int = 64,
        ef_search: int = 64,
        seed: int = 13,
    ) -> None:
        metric = (metric or "cosine").strip().lower()
        if metric not in ("cosine", "l2", "dot"):
            raise ValueError("metric must be one of: cosine, l2, dot")
        self.metric = metric
        self.m = max(2, int(m))
        self.ef_construction = max(self.m, int(ef_construction))
        self.ef_search = max(1, int(ef_search))
        self._vectors: Dict[Any, Vector] = {}
        self._norms: Dict[Any, float] = {}
        self._levels: Dict[Any, int] = {}
        self._links: Dict[Any, Dict[int, Set[Any]]] = {}
        self._entry: Any = None
        self._max_level = -1
        self._rng = random.Random(seed)
        self._dim: Optional[int] = None

    def __len__(self) -> int:
        return len(self._vectors)

    @property
    def dimension(self) -> Optional[int]:
        return self._dim

    def clear(self) -> None:
        self._vectors.clear()
        self._norms.clear()
        self._levels.clear()
        self._links.clear()
        self._entry = None
        self._max_level = -1
        self._dim = None

    def _random_level(self) -> int:
        level = 0
        while self._rng.random() < (1.0 / self.m):
            level += 1
        return level

    def _validate_dim(self, vector: Vector) -> None:
        if self._dim is None:
            self._dim = len(vector)
        elif len(vector) != self._dim:
            raise ValueError(f"vector dimension mismatch: expected {self._dim}, got {len(vector)}")

    def _distance_to_vector(self, key: Any, query: Vector, query_norm: Optional[float] = None) -> float:
        vec = self._vectors[key]
        if self.metric == "l2":
            return sum((a - b) * (a - b) for a, b in zip(vec, query))
        if self.metric == "dot":
            return -sum(a * b for a, b in zip(vec, query))
        norm = self._norms.get(key, 0.0)
        q_norm = query_norm if query_norm is not None else math.sqrt(sum(v * v for v in query))
        if norm <= 0.0 or q_norm <= 0.0:
            return 1.0
        dot = sum(a * b for a, b in zip(vec, query))
        return 1.0 - (dot / (norm * q_norm))

    def _score_from_distance(self, distance: float) -> float:
        if self.metric == "l2":
            return 1.0 / (1.0 + distance)
        if self.metric == "dot":
            return -distance
        return 1.0 - distance

    def _search_layer(self, query: Vector, entry: Any, ef: int, level: int) -> List[Any]:
        q_norm = math.sqrt(sum(v * v for v in query)) if self.metric == "cosine" else None
        visited = {entry}
        candidates: List[Tuple[float, Any]] = [(self._distance_to_vector(entry, query, q_norm), entry)]
        best: List[Tuple[float, Any]] = list(candidates)
        heapq.heapify(candidates)
        heapq.heapify(best)

        while candidates:
            cur_dist, cur = heapq.heappop(candidates)
            worst = max(best)[0] if best else float("inf")
            if cur_dist > worst and len(best) >= ef:
                break
            for nb in self._links.get(cur, {}).get(level, set()):
                if nb in visited or nb not in self._vectors:
                    continue
                visited.add(nb)
                dist = self._distance_to_vector(nb, query, q_norm)
                if len(best) < ef or dist < max(best)[0]:
                    heapq.heappush(candidates, (dist, nb))
                    heapq.heappush(best, (dist, nb))
                    if len(best) > ef:
                        worst_item = max(best)
                        best.remove(worst_item)
                        heapq.heapify(best)
        return [key for _, key in sorted(best)]

    def _select_neighbors(self, query: Vector, candidates: Iterable[Any], limit: int) -> List[Any]:
        q_norm = math.sqrt(sum(v * v for v in query)) if self.metric == "cosine" else None
        ranked = sorted(
            ((self._distance_to_vector(k, query, q_norm), k) for k in candidates if k in self._vectors),
            key=lambda item: item[0],
        )
        return [k for _, k in ranked[:limit]]

    def _recompute_entry(self) -> None:
        if not self._vectors:
            self._entry = None
            self._max_level = -1
            self._dim = None
            return
        self._entry = max(self._vectors.keys(), key=lambda k: self._levels.get(k, 0))
        self._max_level = self._levels.get(self._entry, 0)

    def upsert(self, key: Any, vector: Iterable[Any]) -> None:
        vec = normalize_vector(vector)
        self._validate_dim(vec)
        if key in self._vectors:
            self.delete(key)
            self._validate_dim(vec)

        level = self._random_level()
        self._vectors[key] = vec
        self._norms[key] = math.sqrt(sum(v * v for v in vec))
        self._levels[key] = level
        self._links[key] = {lv: set() for lv in range(level + 1)}

        if self._entry is None:
            self._entry = key
            self._max_level = level
            return

        entry = self._entry
        for lv in range(self._max_level, level, -1):
            nearest = self._search_layer(vec, entry, 1, lv)
            if nearest:
                entry = nearest[0]

        for lv in range(min(level, self._max_level), -1, -1):
            candidates = self._search_layer(vec, entry, self.ef_construction, lv)
            selected = self._select_neighbors(vec, candidates, self.m)
            for nb in selected:
                self._links[key].setdefault(lv, set()).add(nb)
                self._links.setdefault(nb, {}).setdefault(lv, set()).add(key)
                if len(self._links[nb][lv]) > self.m:
                    nb_vec = self._vectors[nb]
                    keep = self._select_neighbors(nb_vec, self._links[nb][lv], self.m)
                    self._links[nb][lv] = set(keep)
            if selected:
                entry = selected[0]

        if level > self._max_level:
            self._entry = key
            self._max_level = level

    def delete(self, key: Any) -> bool:
        if key not in self._vectors:
            return False
        links = self._links.pop(key, {})
        for lv, neighbors in links.items():
            for nb in neighbors:
                self._links.get(nb, {}).get(lv, set()).discard(key)
        self._vectors.pop(key, None)
        self._norms.pop(key, None)
        self._levels.pop(key, None)
        if key == self._entry:
            self._recompute_entry()
        return True

    def get(self, key: Any) -> Optional[Vector]:
        vec = self._vectors.get(key)
        return tuple(vec) if vec is not None else None

    def search(self, query: Iterable[Any], *, k: int = 10, ef: Optional[int] = None) -> List[Dict[str, Any]]:
        vec = normalize_vector(query)
        limit = max(0, int(k))
        if limit == 0 or self._entry is None:
            return []
        self._validate_dim(vec)

        ef_value = max(limit, int(ef or self.ef_search))
        entry = self._entry
        for lv in range(self._max_level, 0, -1):
            nearest = self._search_layer(vec, entry, 1, lv)
            if nearest:
                entry = nearest[0]
        candidates = self._search_layer(vec, entry, ef_value, 0)
        if len(candidates) < min(ef_value, len(self._vectors)):
            candidates = list(self._vectors.keys())

        q_norm = math.sqrt(sum(v * v for v in vec)) if self.metric == "cosine" else None
        ranked = sorted(
            ((self._distance_to_vector(key, vec, q_norm), key) for key in candidates if key in self._vectors),
            key=lambda item: item[0],
        )
        return [
            {"key": key, "score": self._score_from_distance(distance), "distance": distance}
            for distance, key in ranked[:limit]
        ]

    def dump(self) -> Dict[str, Any]:
        return {
            "metric": self.metric,
            "m": self.m,
            "ef_construction": self.ef_construction,
            "ef_search": self.ef_search,
            "dimension": self._dim,
            "vectors": {str(k): list(v) for k, v in self._vectors.items()},
        }

    def load(self, data: Dict[str, Any]) -> None:
        self.clear()
        vectors = data.get("vectors", {}) if isinstance(data, dict) else {}
        if not isinstance(vectors, dict):
            return
        for key, vector in vectors.items():
            self.upsert(key, vector)

    def stats(self) -> Dict[str, Any]:
        edges = 0
        for levels in self._links.values():
            for neighbors in levels.values():
                edges += len(neighbors)
        return {
            "metric": self.metric,
            "dimension": self._dim,
            "vectors": len(self._vectors),
            "max_level": self._max_level,
            "m": self.m,
            "ef_construction": self.ef_construction,
            "ef_search": self.ef_search,
            "edges": edges,
        }
