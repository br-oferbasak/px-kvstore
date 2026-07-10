#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import json
from threading import RLock

class Settings:
    def __init__(self):
        self._lock = RLock()
        self.reload()

    def reload(self):
        """Reload settings from environment variables."""
        with self._lock:
            self.CONFIG_FILE = os.getenv("PXKV_CONFIG_FILE", "")

            self.HOST = os.getenv("PXKV_HOST", "0.0.0.0")
            self.PORT = int(os.getenv("PXKV_PORT", "8000"))
            self.HTTP_TLS_ENABLED = os.getenv("PXKV_HTTP_TLS_ENABLED", "false").lower() == "true"
            self.HTTPS_PORT = int(os.getenv("PXKV_HTTPS_PORT", "8443") or "8443")
            self.TLS_CERT_FILE = os.getenv("PXKV_TLS_CERT_FILE", "")
            self.TLS_KEY_FILE = os.getenv("PXKV_TLS_KEY_FILE", "")
            self.SHARDS = int(os.getenv("PXKV_SHARD_COUNT", os.getenv("SHARD_COUNT", "4")))
            self.PER_SHARD_MAX = int(os.getenv("PXKV_PER_SHARD_MAX", "1000"))
            self.EVICTION_POLICY = os.getenv("PXKV_EVICTION_POLICY", "lru")

            self.FAULT_LATENCY_MS = float(os.getenv("PXKV_FAULT_LATENCY_MS", "0") or "0")
            self.FAULT_LATENCY_JITTER_MS = float(os.getenv("PXKV_FAULT_LATENCY_JITTER_MS", "0") or "0")

            self.SNAPSHOT_FILE = os.getenv("PXKV_SNAPSHOT_FILE", "")
            self.SNAPSHOT_INTERVAL = float(os.getenv("PXKV_SNAPSHOT_INTERVAL", "0"))
            self.WAL_FILE = os.getenv("PXKV_WAL_FILE", "")
            self.WAL_ROTATE_ENABLED = os.getenv("PXKV_WAL_ROTATE_ENABLED", "false").lower() == "true"
            self.WAL_ROTATE_KEEP = int(os.getenv("PXKV_WAL_ROTATE_KEEP", "0") or "0")

            self.REDIS_HOST = os.getenv("PXKV_REDIS_HOST", "0.0.0.0")
            self.REDIS_PORT = int(os.getenv("PXKV_REDIS_PORT", "6379"))
            self.REDIS_ENABLED = os.getenv("PXKV_REDIS_ENABLED", "true").lower() == "true"
            self.REDIS_TLS_ENABLED = os.getenv("PXKV_REDIS_TLS_ENABLED", "false").lower() == "true"
            self.REDIS_TLS_PORT = int(os.getenv("PXKV_REDIS_TLS_PORT", "6380") or "6380")
            self.REDIS_TLS_CERT_FILE = os.getenv("PXKV_REDIS_TLS_CERT_FILE", self.TLS_CERT_FILE)
            self.REDIS_TLS_KEY_FILE = os.getenv("PXKV_REDIS_TLS_KEY_FILE", self.TLS_KEY_FILE)

            self.TIERING_DIR = os.getenv("PXKV_TIERING_DIR", "")
            self.TIERING_BACKEND = os.getenv("PXKV_TIERING_BACKEND", "file" if self.TIERING_DIR else "").lower()
            self.TIERING_HTTP_BASE_URL = os.getenv("PXKV_TIERING_HTTP_BASE_URL", "")
            self.TIERING_HTTP_TIMEOUT = float(os.getenv("PXKV_TIERING_HTTP_TIMEOUT", "2.0") or "2.0")
            self.TIERING_S3_BUCKET = os.getenv("PXKV_TIERING_S3_BUCKET", "")
            self.TIERING_S3_PREFIX = os.getenv("PXKV_TIERING_S3_PREFIX", "")
            self.TIERING_S3_REGION = os.getenv("PXKV_TIERING_S3_REGION", "")
            self.TIERING_S3_ENDPOINT_URL = os.getenv("PXKV_TIERING_S3_ENDPOINT_URL", "")
            self.TIERING_PREFETCH_ENABLED = os.getenv("PXKV_TIERING_PREFETCH_ENABLED", "true").lower() == "true"
            self.TIERING_PREFETCH_WORKERS = int(os.getenv("PXKV_TIERING_PREFETCH_WORKERS", "4") or "4")
            self.TIERING_PREFETCH_WAIT_MS = float(os.getenv("PXKV_TIERING_PREFETCH_WAIT_MS", "25") or "25")
            self.TIERING_PREFETCH_CACHE_MAX = int(os.getenv("PXKV_TIERING_PREFETCH_CACHE_MAX", "4096") or "4096")

            self.AUTH_ADMIN_TOKEN = os.getenv("PXKV_AUTH_ADMIN_TOKEN", "")
            self.AUTH_WRITER_TOKEN = os.getenv("PXKV_AUTH_WRITER_TOKEN", "")
            self.AUTH_READER_TOKEN = os.getenv("PXKV_AUTH_READER_TOKEN", "")
            self.AUTH_ADMIN_PASSWORD = os.getenv("PXKV_AUTH_ADMIN_PASSWORD", "")
            self.AUTH_WRITER_PASSWORD = os.getenv("PXKV_AUTH_WRITER_PASSWORD", "")
            self.AUTH_READER_PASSWORD = os.getenv("PXKV_AUTH_READER_PASSWORD", "")

            self.NAMESPACE_ENABLED = os.getenv("PXKV_NAMESPACE_ENABLED", "false").lower() == "true"
            self.NAMESPACE_DEFAULT = os.getenv("PXKV_NAMESPACE_DEFAULT", "default")
            self.NAMESPACE_CONFIGS = {}
            namespace_json = os.getenv("PXKV_NAMESPACE_CONFIGS", "") or ""
            if namespace_json.strip():
                try:
                    parsed = json.loads(namespace_json)
                    if isinstance(parsed, dict):
                        self.NAMESPACE_CONFIGS = parsed
                except Exception as e:
                    logging.warning("Failed to parse PXKV_NAMESPACE_CONFIGS: %s", e)

            self.RATE_LIMIT_ENABLED = os.getenv("PXKV_RATE_LIMIT_ENABLED", "false").lower() == "true"
            self.RATE_LIMIT_DEFAULT = {
                "rps": float(os.getenv("PXKV_RATE_LIMIT_DEFAULT_RPS", "0") or "0"),
                "burst": int(os.getenv("PXKV_RATE_LIMIT_DEFAULT_BURST", "0") or "0"),
                "per_ip": os.getenv("PXKV_RATE_LIMIT_DEFAULT_PER_IP", "true").lower() == "true",
            }
            self.RATE_LIMIT_ROUTES = {}
            routes_json = os.getenv("PXKV_RATE_LIMIT_ROUTES", "") or ""
            if routes_json.strip():
                try:
                    parsed = json.loads(routes_json)
                    if isinstance(parsed, dict):
                        self.RATE_LIMIT_ROUTES = parsed
                except Exception as e:
                    logging.warning("Failed to parse PXKV_RATE_LIMIT_ROUTES: %s", e)

            self.FOLLOWER_READ_ENABLED = os.getenv("PXKV_FOLLOWER_READ_ENABLED", "false").lower() == "true"
            self.FOLLOWER_READ_MAX_LAG_LSN = int(os.getenv("PXKV_FOLLOWER_READ_MAX_LAG_LSN", "0") or "0")
            self.FOLLOWER_READ_MAX_AGE_MS = float(os.getenv("PXKV_FOLLOWER_READ_MAX_AGE_MS", "0") or "0")
            self.FOLLOWER_READ_STRATEGY = os.getenv("PXKV_FOLLOWER_READ_STRATEGY", "least_lag").lower()

            self.TRACING_ENABLED = os.getenv("PXKV_TRACING_ENABLED", "false").lower() == "true"
            self.TRACING_SERVICE_NAME = os.getenv("PXKV_TRACING_SERVICE_NAME", "pxkv")
            self.TRACING_EXPORTER = os.getenv("PXKV_TRACING_EXPORTER", "console").lower()
            self.TRACING_OTLP_ENDPOINT = os.getenv("PXKV_TRACING_OTLP_ENDPOINT", "")

            self.REPLICATION_ROLE = os.getenv("PXKV_REPLICATION_ROLE", "leader").lower()
            self.REPLICATION_LEADER_ADDR = os.getenv("PXKV_REPLICATION_LEADER_ADDR", "127.0.0.1:8000")
            self.REPLICATION_FOLLOWERS = [f for f in os.getenv("PXKV_REPLICATION_FOLLOWERS", "").split(",") if f]
            self.REPLICATION_SYNC_INTERVAL = float(os.getenv("PXKV_REPLICATION_SYNC_INTERVAL", "1.0"))
            self.REPLICATION_QUEUE_MAX = int(os.getenv("PXKV_REPLICATION_QUEUE_MAX", "10000") or "10000")
            self.REPLICATION_SHED_POLICY = os.getenv("PXKV_REPLICATION_SHED_POLICY", "drop_newest").lower()

            self.ANTI_ENTROPY_ENABLED = os.getenv("PXKV_ANTI_ENTROPY_ENABLED", "true").lower() == "true"
            self.ANTI_ENTROPY_INTERVAL = float(os.getenv("PXKV_ANTI_ENTROPY_INTERVAL", "60.0") or "60.0")
            self.ANTI_ENTROPY_MAX_LAG_LSN = int(os.getenv("PXKV_ANTI_ENTROPY_MAX_LAG_LSN", "100000") or "100000")
            self.ANTI_ENTROPY_MAX_AGE_MS = float(os.getenv("PXKV_ANTI_ENTROPY_MAX_AGE_MS", "300000.0") or "300000.0")

            self.CROSS_CLUSTER_ENABLED = os.getenv("PXKV_CROSS_CLUSTER_ENABLED", "false").lower() == "true"
            self.CLUSTER_ID = os.getenv("PXKV_CLUSTER_ID", "local")
            self.CROSS_CLUSTER_PEERS = [f for f in os.getenv("PXKV_CROSS_CLUSTER_PEERS", "").split(",") if f]
            self.CROSS_CLUSTER_CONFLICT_POLICY = os.getenv("PXKV_CROSS_CLUSTER_CONFLICT_POLICY", "last_write_wins").lower()

            self.RESHARD_ENABLED = os.getenv("PXKV_RESHARD_ENABLED", "true").lower() == "true"
            self.PITR_ENABLED = os.getenv("PXKV_PITR_ENABLED", "true").lower() == "true"
            self.PITR_SNAPSHOT_KEEP = int(os.getenv("PXKV_PITR_SNAPSHOT_KEEP", "5") or "5")
            self.PITR_WAL_ARCHIVE_DIR = os.getenv("PXKV_PITR_WAL_ARCHIVE_DIR", "")
            
            self.COMPRESSION_ENABLED = os.getenv("PXKV_COMPRESSION_ENABLED", "false").lower() == "true"
            self.COMPRESSION_ALGORITHM = os.getenv("PXKV_COMPRESSION_ALGORITHM", "gzip").lower()
            self.COMPRESSION_LEVEL = int(os.getenv("PXKV_COMPRESSION_LEVEL", "6") or "6")

            self.DISK_THROTTLE_ENABLED = os.getenv("PXKV_DISK_THROTTLE_ENABLED", "false").lower() == "true"
            self.DISK_THROTTLE_PATHS = [p.strip() for p in os.getenv("PXKV_DISK_THROTTLE_PATHS", "").split(",") if p.strip()]
            self.DISK_THROTTLE_SOFT_PERCENT = float(os.getenv("PXKV_DISK_THROTTLE_SOFT_PERCENT", "0") or "0")
            self.DISK_THROTTLE_HARD_PERCENT = float(os.getenv("PXKV_DISK_THROTTLE_HARD_PERCENT", "0") or "0")
            self.DISK_THROTTLE_SOFT_USED_BYTES = int(os.getenv("PXKV_DISK_THROTTLE_SOFT_USED_BYTES", "0") or "0")
            self.DISK_THROTTLE_HARD_USED_BYTES = int(os.getenv("PXKV_DISK_THROTTLE_HARD_USED_BYTES", "0") or "0")
            self.DISK_THROTTLE_DELAY_MS = float(os.getenv("PXKV_DISK_THROTTLE_DELAY_MS", "0") or "0")
            
            self.HOT_KEY_DETECTION_ENABLED = os.getenv("PXKV_HOT_KEY_DETECTION_ENABLED", "false").lower() == "true"
            self.HOT_KEY_WINDOW_SECONDS = float(os.getenv("PXKV_HOT_KEY_WINDOW_SECONDS", "60.0") or "60.0")
            self.HOT_KEY_BUCKETS = int(os.getenv("PXKV_HOT_KEY_BUCKETS", "60") or "60")
            self.HOT_KEY_TOP_K = int(os.getenv("PXKV_HOT_KEY_TOP_K", "10") or "10")
            self.HOT_KEY_THRESHOLD_QPS = float(os.getenv("PXKV_HOT_KEY_THRESHOLD_QPS", "0") or "0")
            self.HOT_KEY_SAMPLE_RATE = float(os.getenv("PXKV_HOT_KEY_SAMPLE_RATE", "1.0") or "1.0")

            self.HOT_KEY_MITIGATION_ENABLED = os.getenv("PXKV_HOT_KEY_MITIGATION_ENABLED", "false").lower() == "true"
            self.HOT_KEY_MITIGATION_CACHE_TTL_SECONDS = float(os.getenv("PXKV_HOT_KEY_MITIGATION_CACHE_TTL_SECONDS", "1.0") or "1.0")
            self.HOT_KEY_MITIGATION_MAX_ENTRIES = int(os.getenv("PXKV_HOT_KEY_MITIGATION_MAX_ENTRIES", "1024") or "1024")
            self.HOT_KEY_MITIGATION_REFRESH_INTERVAL_SECONDS = float(os.getenv("PXKV_HOT_KEY_MITIGATION_REFRESH_INTERVAL_SECONDS", "1.0") or "1.0")

            self.QUERY_PLAN_CACHE_ENABLED = os.getenv("PXKV_QUERY_PLAN_CACHE_ENABLED", "false").lower() == "true"
            self.QUERY_PLAN_CACHE_MAX_ENTRIES = int(os.getenv("PXKV_QUERY_PLAN_CACHE_MAX_ENTRIES", "1024") or "1024")

            self.ADAPTIVE_TTL_ENABLED = os.getenv("PXKV_ADAPTIVE_TTL_ENABLED", "false").lower() == "true"
            self.ADAPTIVE_TTL_MIN_SECONDS = float(os.getenv("PXKV_ADAPTIVE_TTL_MIN_SECONDS", "1.0") or "1.0")
            self.ADAPTIVE_TTL_MAX_SECONDS = float(os.getenv("PXKV_ADAPTIVE_TTL_MAX_SECONDS", "86400.0") or "86400.0")
            self.ADAPTIVE_TTL_DEFAULT_BASE_SECONDS = float(os.getenv("PXKV_ADAPTIVE_TTL_DEFAULT_BASE_SECONDS", "60.0") or "60.0")
            self.ADAPTIVE_TTL_HIT_EXTEND_FACTOR = float(os.getenv("PXKV_ADAPTIVE_TTL_HIT_EXTEND_FACTOR", "2.0") or "2.0")
            self.ADAPTIVE_TTL_MISS_SHRINK_FACTOR = float(os.getenv("PXKV_ADAPTIVE_TTL_MISS_SHRINK_FACTOR", "0.5") or "0.5")
            self.ADAPTIVE_TTL_RECENCY_HALF_LIFE_SECONDS = float(os.getenv("PXKV_ADAPTIVE_TTL_RECENCY_HALF_LIFE_SECONDS", "300.0") or "300.0")
            self.ADAPTIVE_TTL_MAX_TRACKED_KEYS = int(os.getenv("PXKV_ADAPTIVE_TTL_MAX_TRACKED_KEYS", "10000") or "10000")

            self.HEAVY_HITTERS_ENABLED = os.getenv("PXKV_HEAVY_HITTERS_ENABLED", "false").lower() == "true"
            self.HEAVY_HITTERS_K = int(os.getenv("PXKV_HEAVY_HITTERS_K", "10") or "10")
            self.HEAVY_HITTERS_CMS_WIDTH = int(os.getenv("PXKV_HEAVY_HITTERS_CMS_WIDTH", "2048") or "2048")
            self.HEAVY_HITTERS_CMS_DEPTH = int(os.getenv("PXKV_HEAVY_HITTERS_CMS_DEPTH", "4") or "4")
            self.HEAVY_HITTERS_DECAY_INTERVAL_SECONDS = float(os.getenv("PXKV_HEAVY_HITTERS_DECAY_INTERVAL_SECONDS", "60.0") or "60.0")
            self.HEAVY_HITTERS_DECAY_FACTOR = float(os.getenv("PXKV_HEAVY_HITTERS_DECAY_FACTOR", "0.5") or "0.5")
            self.HEAVY_HITTERS_THRESHOLD_COUNT = int(os.getenv("PXKV_HEAVY_HITTERS_THRESHOLD_COUNT", "0") or "0")

            self.COLD_KEY_HINTS_ENABLED = os.getenv("PXKV_COLD_KEY_HINTS_ENABLED", "false").lower() == "true"
            self.COLD_KEY_HINTS_WINDOW_SECONDS = float(os.getenv("PXKV_COLD_KEY_HINTS_WINDOW_SECONDS", "300.0") or "300.0")
            self.COLD_KEY_HINTS_BUCKETS = int(os.getenv("PXKV_COLD_KEY_HINTS_BUCKETS", "10") or "10")
            self.COLD_KEY_HINTS_SCAN_CANDIDATES = int(os.getenv("PXKV_COLD_KEY_HINTS_SCAN_CANDIDATES", "8") or "8")
            self.COLD_KEY_HINTS_COLD_THRESHOLD_COUNT = int(os.getenv("PXKV_COLD_KEY_HINTS_COLD_THRESHOLD_COUNT", "1") or "1")
            self.COLD_KEY_HINTS_MAX_TRACKED_KEYS = int(os.getenv("PXKV_COLD_KEY_HINTS_MAX_TRACKED_KEYS", "100000") or "100000")

            self.GOSSIP_ENABLED = os.getenv("PXKV_GOSSIP_ENABLED", "false").lower() == "true"
            self.GOSSIP_INTERVAL = float(os.getenv("PXKV_GOSSIP_INTERVAL", "1.0") or "1.0")
            self.GOSSIP_FAILURE_TIMEOUT = float(os.getenv("PXKV_GOSSIP_FAILURE_TIMEOUT", "5.0") or "5.0")
            self.GOSSIP_SEED_PEERS = [p.strip() for p in os.getenv("PXKV_GOSSIP_SEED_PEERS", "").split(",") if p.strip()]

            if self.CONFIG_FILE:
                try:
                    if os.path.exists(self.CONFIG_FILE):
                        with open(self.CONFIG_FILE, "r") as f:
                            data = json.load(f) or {}
                        self.update(data)
                except Exception as e:
                    logging.warning("Failed to load config file %s: %s", self.CONFIG_FILE, e)
            logging.info("Settings reloaded from environment.")

    def update(self, new_settings: dict):
        """Update specific settings dynamically."""
        with self._lock:
            def _to_bool(v):
                if isinstance(v, bool):
                    return v
                return str(v).lower() == "true"

            def _policy_merge(base: dict, patch: dict) -> dict:
                out = dict(base or {})
                if not isinstance(patch, dict):
                    raise TypeError("policy must be an object")
                if "rps" in patch:
                    out["rps"] = float(patch["rps"])
                if "burst" in patch:
                    out["burst"] = int(patch["burst"])
                if "per_ip" in patch:
                    out["per_ip"] = _to_bool(patch["per_ip"])
                return out

            updatable = {
                "FAULT_LATENCY_MS": float,
                "FAULT_LATENCY_JITTER_MS": float,
                "SNAPSHOT_INTERVAL": float,
                "REPLICATION_SYNC_INTERVAL": float,
                "REDIS_ENABLED": _to_bool,
                "RATE_LIMIT_ENABLED": _to_bool,
                "NAMESPACE_ENABLED": _to_bool,
                "NAMESPACE_DEFAULT": str,
                "DISK_THROTTLE_ENABLED": _to_bool,
                "DISK_THROTTLE_SOFT_PERCENT": float,
                "DISK_THROTTLE_HARD_PERCENT": float,
                "DISK_THROTTLE_SOFT_USED_BYTES": int,
                "DISK_THROTTLE_HARD_USED_BYTES": int,
                "DISK_THROTTLE_DELAY_MS": float,
                "HOT_KEY_DETECTION_ENABLED": _to_bool,
                "HOT_KEY_WINDOW_SECONDS": float,
                "HOT_KEY_BUCKETS": int,
                "HOT_KEY_TOP_K": int,
                "HOT_KEY_THRESHOLD_QPS": float,
                "HOT_KEY_SAMPLE_RATE": float,
                "QUERY_PLAN_CACHE_ENABLED": _to_bool,
                "QUERY_PLAN_CACHE_MAX_ENTRIES": int,
                "ADAPTIVE_TTL_ENABLED": _to_bool,
                "ADAPTIVE_TTL_MIN_SECONDS": float,
                "ADAPTIVE_TTL_MAX_SECONDS": float,
                "ADAPTIVE_TTL_DEFAULT_BASE_SECONDS": float,
                "ADAPTIVE_TTL_HIT_EXTEND_FACTOR": float,
                "ADAPTIVE_TTL_MISS_SHRINK_FACTOR": float,
                "ADAPTIVE_TTL_RECENCY_HALF_LIFE_SECONDS": float,
                "ADAPTIVE_TTL_MAX_TRACKED_KEYS": int,
                "HEAVY_HITTERS_ENABLED": _to_bool,
                "HEAVY_HITTERS_K": int,
                "HEAVY_HITTERS_CMS_WIDTH": int,
                "HEAVY_HITTERS_CMS_DEPTH": int,
                "HEAVY_HITTERS_DECAY_INTERVAL_SECONDS": float,
                "HEAVY_HITTERS_DECAY_FACTOR": float,
                "HEAVY_HITTERS_THRESHOLD_COUNT": int,
                "COLD_KEY_HINTS_ENABLED": _to_bool,
                "COLD_KEY_HINTS_WINDOW_SECONDS": float,
                "COLD_KEY_HINTS_BUCKETS": int,
                "COLD_KEY_HINTS_SCAN_CANDIDATES": int,
                "COLD_KEY_HINTS_COLD_THRESHOLD_COUNT": int,
                "COLD_KEY_HINTS_MAX_TRACKED_KEYS": int,
            }
            for key, val in new_settings.items():
                if key in updatable:
                    try:
                        typed_val = updatable[key](val)
                        setattr(self, key, typed_val)
                        logging.info("Setting %s updated to %s", key, typed_val)
                    except (ValueError, TypeError) as e:
                        logging.warning("Failed to update setting %s: %s", key, e)
                elif key == "RATE_LIMIT_DEFAULT":
                    try:
                        if isinstance(val, str):
                            val = json.loads(val)
                        self.RATE_LIMIT_DEFAULT = _policy_merge(self.RATE_LIMIT_DEFAULT, val)
                        logging.info("Setting RATE_LIMIT_DEFAULT updated to %s", self.RATE_LIMIT_DEFAULT)
                    except Exception as e:
                        logging.warning("Failed to update setting RATE_LIMIT_DEFAULT: %s", e)
                elif key == "RATE_LIMIT_ROUTES":
                    try:
                        if isinstance(val, str):
                            val = json.loads(val)
                        if not isinstance(val, dict):
                            raise TypeError("RATE_LIMIT_ROUTES must be an object")
                        merged = dict(self.RATE_LIMIT_ROUTES or {})
                        for route, pol in val.items():
                            if pol is None:
                                merged.pop(route, None)
                                continue
                            merged[route] = _policy_merge(self.RATE_LIMIT_DEFAULT, pol)
                        self.RATE_LIMIT_ROUTES = merged
                        logging.info("Setting RATE_LIMIT_ROUTES updated (%d routes)", len(self.RATE_LIMIT_ROUTES))
                    except Exception as e:
                        logging.warning("Failed to update setting RATE_LIMIT_ROUTES: %s", e)
                elif key == "DISK_THROTTLE_PATHS":
                    try:
                        if isinstance(val, str):
                            val = [p.strip() for p in val.split(",") if p.strip()]
                        if not isinstance(val, list):
                            raise TypeError("DISK_THROTTLE_PATHS must be an array")
                        self.DISK_THROTTLE_PATHS = [str(p).strip() for p in val if str(p).strip()]
                        logging.info("Setting DISK_THROTTLE_PATHS updated (%d paths)", len(self.DISK_THROTTLE_PATHS))
                    except Exception as e:
                        logging.warning("Failed to update setting DISK_THROTTLE_PATHS: %s", e)
                elif key == "NAMESPACE_CONFIGS":
                    try:
                        if isinstance(val, str):
                            val = json.loads(val)
                        if not isinstance(val, dict):
                            raise TypeError("NAMESPACE_CONFIGS must be an object")
                        self.NAMESPACE_CONFIGS = val
                        logging.info("Setting NAMESPACE_CONFIGS updated (%d namespaces)", len(self.NAMESPACE_CONFIGS))
                    except Exception as e:
                        logging.warning("Failed to update setting NAMESPACE_CONFIGS: %s", e)

    def to_dict(self):
        """Return current settings as a dictionary."""
        with self._lock:
            return {k: v for k, v in self.__dict__.items() if k.isupper()}

settings = Settings()
