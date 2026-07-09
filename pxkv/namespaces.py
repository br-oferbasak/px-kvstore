#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .auth import best_role_for_secret
from .config.settings import settings

NAMESPACE_HEADER = "X-PXKV-Namespace"
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class NamespaceManager:
    def enabled(self) -> bool:
        return bool(getattr(settings, "NAMESPACE_ENABLED", False))

    def default(self) -> str:
        name = str(getattr(settings, "NAMESPACE_DEFAULT", "default") or "default").strip()
        return name if self.is_valid(name) else "default"

    def is_valid(self, name: Optional[str]) -> bool:
        if name is None:
            return False
        return bool(_NAME_RE.match(str(name).strip()))

    def resolve(self, name: Optional[str]) -> Optional[str]:
        if not self.enabled():
            return self.default()
        if name is None or str(name).strip() == "":
            return self.default()
        value = str(name).strip()
        if not self.is_valid(value):
            return None
        return value

    def prefix(self, namespace: str) -> str:
        return f"ns:{namespace}:"

    def key(self, namespace: str, key: Any) -> Any:
        if not self.enabled():
            return key
        if isinstance(key, bytes):
            return self.prefix(namespace).encode("utf-8") + key
        return f"{self.prefix(namespace)}{key}"

    def belongs(self, namespace: str, key: Any) -> bool:
        if not self.enabled():
            return True
        raw = key.decode("utf-8", errors="replace") if isinstance(key, (bytes, bytearray)) else str(key)
        return raw.startswith(self.prefix(namespace))

    def strip(self, namespace: str, key: Any) -> Any:
        if not self.enabled():
            return key
        if isinstance(key, bytes):
            pref = self.prefix(namespace).encode("utf-8")
            return key[len(pref):] if key.startswith(pref) else key
        raw = str(key)
        pref = self.prefix(namespace)
        return raw[len(pref):] if raw.startswith(pref) else raw

    def user_prefix(self, namespace: str, prefix: Optional[str]) -> Optional[str]:
        if prefix is None:
            return self.prefix(namespace) if self.enabled() else None
        return self.key(namespace, prefix)

    def config(self, namespace: str) -> Dict[str, Any]:
        cfg = getattr(settings, "NAMESPACE_CONFIGS", {}) or {}
        if not isinstance(cfg, dict):
            return {}
        item = cfg.get(namespace, {}) or {}
        return item if isinstance(item, dict) else {}

    def auth_secrets(self, namespace: str) -> Dict[str, str]:
        cfg = self.config(namespace)
        auth = cfg.get("auth", {}) or {}
        if not isinstance(auth, dict):
            auth = {}
        return {
            "admin_token": str(auth.get("admin_token", getattr(settings, "AUTH_ADMIN_TOKEN", "")) or ""),
            "writer_token": str(auth.get("writer_token", getattr(settings, "AUTH_WRITER_TOKEN", "")) or ""),
            "reader_token": str(auth.get("reader_token", getattr(settings, "AUTH_READER_TOKEN", "")) or ""),
            "admin_password": str(auth.get("admin_password", getattr(settings, "AUTH_ADMIN_PASSWORD", "")) or ""),
            "writer_password": str(auth.get("writer_password", getattr(settings, "AUTH_WRITER_PASSWORD", "")) or ""),
            "reader_password": str(auth.get("reader_password", getattr(settings, "AUTH_READER_PASSWORD", "")) or ""),
        }

    def auth_enabled(self, namespace: Optional[str]) -> bool:
        ns = self.resolve(namespace)
        if ns is None:
            return True
        secrets = self.auth_secrets(ns)
        return any(secrets.values())

    def role_for_secret(self, namespace: Optional[str], secret: str) -> Optional[str]:
        ns = self.resolve(namespace)
        if ns is None:
            return None
        return best_role_for_secret(secret, **self.auth_secrets(ns))

    def rate_limit_default(self, namespace: str) -> Dict[str, Any]:
        cfg = self.config(namespace)
        pol = cfg.get("rate_limit_default")
        if isinstance(pol, dict):
            return pol
        return getattr(settings, "RATE_LIMIT_DEFAULT", None) or {"rps": 0.0, "burst": 0, "per_ip": True}

    def rate_limit_routes(self, namespace: str) -> Dict[str, Any]:
        cfg = self.config(namespace)
        pol = cfg.get("rate_limit_routes")
        if isinstance(pol, dict):
            return pol
        return getattr(settings, "RATE_LIMIT_ROUTES", None) or {}

    def hot_key_config(self, namespace: str) -> Dict[str, Any]:
        cfg = self.config(namespace)
        nested = cfg.get("hot_keys", {}) or {}
        out = dict(nested) if isinstance(nested, dict) else {}
        for src, dst in (
            ("hot_key_top_k", "top_k"),
            ("hot_key_threshold_qps", "threshold_qps"),
            ("hot_key_window_seconds", "window_seconds"),
        ):
            if src in cfg:
                out[dst] = cfg[src]
        return out

    def hot_key_top_k(self, namespace: str) -> int:
        cfg = self.hot_key_config(namespace)
        try:
            return max(1, int(cfg.get("top_k", getattr(settings, "HOT_KEY_TOP_K", 10)) or 10))
        except (TypeError, ValueError):
            return max(1, int(getattr(settings, "HOT_KEY_TOP_K", 10) or 10))

    def hot_key_threshold_qps(self, namespace: str) -> float:
        cfg = self.hot_key_config(namespace)
        try:
            return max(0.0, float(cfg.get("threshold_qps", getattr(settings, "HOT_KEY_THRESHOLD_QPS", 0.0)) or 0.0))
        except (TypeError, ValueError):
            return max(0.0, float(getattr(settings, "HOT_KEY_THRESHOLD_QPS", 0.0) or 0.0))

    def known_namespaces(self) -> List[str]:
        names = [self.default()]
        cfg = getattr(settings, "NAMESPACE_CONFIGS", {}) or {}
        if isinstance(cfg, dict):
            for name in cfg.keys():
                value = str(name).strip()
                if self.is_valid(value) and value not in names:
                    names.append(value)
        return names

    def scope(self, namespace: Optional[str]) -> str:
        ns = self.resolve(namespace)
        if ns is None:
            return "invalid"
        return f"ns:{ns}" if self.enabled() else "global"


namespace_manager = NamespaceManager()
