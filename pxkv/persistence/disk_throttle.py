#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
import threading
import time
from typing import Any, Dict, List

from ..config.settings import settings


class DiskUsageThrottler:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._last_checked_at = 0.0
        self._cache_ttl_s = 0.25
        self._last_sample: Dict[str, Any] = {}

    def _configured_paths(self) -> List[str]:
        raw_paths = list(getattr(settings, "DISK_THROTTLE_PATHS", []) or [])
        if not raw_paths:
            raw_paths = [
                getattr(settings, "WAL_FILE", ""),
                getattr(settings, "SNAPSHOT_FILE", ""),
                getattr(settings, "TIERING_DIR", ""),
                getattr(settings, "PITR_WAL_ARCHIVE_DIR", ""),
                os.getcwd(),
            ]

        out: List[str] = []
        seen = set()
        for path in raw_paths:
            if not path:
                continue
            abs_path = os.path.abspath(path)
            candidate = abs_path if os.path.isdir(abs_path) else (os.path.dirname(abs_path) or abs_path)
            if candidate in seen:
                continue
            seen.add(candidate)
            out.append(candidate)
        return out

    def _existing_path(self, path: str) -> str:
        candidate = os.path.abspath(path)
        while candidate and not os.path.exists(candidate):
            parent = os.path.dirname(candidate)
            if not parent or parent == candidate:
                break
            candidate = parent
        if candidate and os.path.exists(candidate):
            return candidate
        return os.getcwd()

    def _sample_path(self, path: str) -> Dict[str, Any]:
        checked = self._existing_path(path)
        usage = shutil.disk_usage(checked)
        total = int(getattr(usage, "total", 0) or 0)
        used = int(getattr(usage, "used", 0) or 0)
        free = int(getattr(usage, "free", 0) or 0)
        used_percent = (float(used) * 100.0 / float(total)) if total > 0 else 0.0
        return {
            "path": checked,
            "total_bytes": total,
            "used_bytes": used,
            "free_bytes": free,
            "used_percent": used_percent,
        }

    def _build_reason(self, sample: Dict[str, Any], prefix: str) -> str:
        reasons: List[str] = []
        hard_pct = float(getattr(settings, "DISK_THROTTLE_HARD_PERCENT", 0.0) or 0.0)
        hard_used = int(getattr(settings, "DISK_THROTTLE_HARD_USED_BYTES", 0) or 0)
        soft_pct = float(getattr(settings, "DISK_THROTTLE_SOFT_PERCENT", 0.0) or 0.0)
        soft_used = int(getattr(settings, "DISK_THROTTLE_SOFT_USED_BYTES", 0) or 0)

        if prefix == "hard":
            if hard_pct > 0 and float(sample["used_percent"]) >= hard_pct:
                reasons.append(f"used_percent={sample['used_percent']:.2f} >= hard_percent={hard_pct:.2f}")
            if hard_used > 0 and int(sample["used_bytes"]) >= hard_used:
                reasons.append(f"used_bytes={int(sample['used_bytes'])} >= hard_used_bytes={hard_used}")
        else:
            if soft_pct > 0 and float(sample["used_percent"]) >= soft_pct:
                reasons.append(f"used_percent={sample['used_percent']:.2f} >= soft_percent={soft_pct:.2f}")
            if soft_used > 0 and int(sample["used_bytes"]) >= soft_used:
                reasons.append(f"used_bytes={int(sample['used_bytes'])} >= soft_used_bytes={soft_used}")

        return ", ".join(reasons)

    def sample(self, force: bool = False) -> Dict[str, Any]:
        enabled = bool(getattr(settings, "DISK_THROTTLE_ENABLED", False))
        paths = self._configured_paths()
        now = time.time()
        with self._lock:
            if not force and self._last_sample and (now - self._last_checked_at) < self._cache_ttl_s:
                return dict(self._last_sample)

            base: Dict[str, Any] = {
                "enabled": enabled,
                "paths": paths,
                "last_path": "",
                "total_bytes": 0,
                "used_bytes": 0,
                "free_bytes": 0,
                "used_percent": 0.0,
                "soft_exceeded": False,
                "hard_exceeded": False,
                "delay_ms": 0.0,
                "reason": "",
                "last_checked_at": now,
            }
            if not enabled:
                self._last_checked_at = now
                self._last_sample = base
                return dict(base)

            selected = None
            for path in paths or [os.getcwd()]:
                sample = self._sample_path(path)
                if selected is None or float(sample["used_percent"]) >= float(selected["used_percent"]):
                    selected = sample
            if selected is None:
                selected = self._sample_path(os.getcwd())

            soft_pct = float(getattr(settings, "DISK_THROTTLE_SOFT_PERCENT", 0.0) or 0.0)
            hard_pct = float(getattr(settings, "DISK_THROTTLE_HARD_PERCENT", 0.0) or 0.0)
            soft_used = int(getattr(settings, "DISK_THROTTLE_SOFT_USED_BYTES", 0) or 0)
            hard_used = int(getattr(settings, "DISK_THROTTLE_HARD_USED_BYTES", 0) or 0)
            delay_ms = float(getattr(settings, "DISK_THROTTLE_DELAY_MS", 0.0) or 0.0)

            hard_exceeded = False
            soft_exceeded = False
            if hard_pct > 0 and float(selected["used_percent"]) >= hard_pct:
                hard_exceeded = True
            if hard_used > 0 and int(selected["used_bytes"]) >= hard_used:
                hard_exceeded = True
            if soft_pct > 0 and float(selected["used_percent"]) >= soft_pct:
                soft_exceeded = True
            if soft_used > 0 and int(selected["used_bytes"]) >= soft_used:
                soft_exceeded = True

            if hard_exceeded:
                reason = self._build_reason(selected, "hard")
            elif soft_exceeded:
                reason = self._build_reason(selected, "soft")
            else:
                reason = ""

            base.update(selected)
            base["soft_exceeded"] = bool(soft_exceeded)
            base["hard_exceeded"] = bool(hard_exceeded)
            base["delay_ms"] = delay_ms if soft_exceeded and not hard_exceeded else 0.0
            base["reason"] = reason

            self._last_checked_at = now
            self._last_sample = base
            return dict(base)

    def gate_write(self) -> Dict[str, Any]:
        sample = self.sample()
        if sample.get("enabled") and sample.get("soft_exceeded") and not sample.get("hard_exceeded"):
            delay_ms = float(sample.get("delay_ms", 0.0) or 0.0)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
        out = dict(sample)
        out["rejected"] = bool(out.get("hard_exceeded"))
        return out


disk_throttler = DiskUsageThrottler()
