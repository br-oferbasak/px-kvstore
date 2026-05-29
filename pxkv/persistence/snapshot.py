#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import threading
import time
import glob
import gzip
from typing import Optional

from ..config.settings import settings

def load_snapshot(store, path: str) -> bool:
    if not path:
        return False
    file_to_read = path
    if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
        if not file_to_read.endswith(".gz"):
            file_to_read = file_to_read + ".gz"
        if not os.path.exists(file_to_read):
            file_to_read = path
    if not os.path.exists(file_to_read):
        return False
    try:
        if file_to_read.endswith(".gz"):
            with gzip.open(file_to_read, "rt") as f:
                data = json.load(f)
        else:
            with open(file_to_read, "r") as f:
                data = json.load(f)
        if isinstance(data, dict) and "_lsn" in data:
            try:
                store._wal._lsn = max(int(store._wal._lsn), int(data.get("_lsn", 0) or 0))
            except Exception:
                pass
            try:
                data = dict(data)
                data.pop("_lsn", None)
            except Exception:
                pass
        store.load(data)
        logging.info("Restored state from %s", file_to_read)
        return True
    except Exception as e:
        logging.error("Failed to load snapshot from %s: %s", file_to_read, e)
        return False


def list_snapshot_archives(base_path: str) -> list:
    """List all snapshot archives, sorted by LSN descending."""
    archives = []
    if not base_path:
        return archives
    pattern = f"{base_path}.*.archive*"
    files = glob.glob(pattern)
    for fpath in files:
        try:
            parts = fpath.split(".")
            lsn_idx = None
            for i, part in enumerate(parts):
                if part == "archive":
                    lsn_idx = i - 1
                    break
            if lsn_idx is not None and lsn_idx >= 0 and parts[lsn_idx].isdigit():
                lsn = int(parts[lsn_idx])
                archives.append((fpath, lsn))
        except Exception:
            pass
    archives.sort(key=lambda x: x[1], reverse=True)
    return archives


def prune_snapshot_archives(base_path: str, keep: int) -> None:
    """Prune snapshot archives to keep only the N most recent ones."""
    if keep <= 0 or not base_path:
        return
    archives = list_snapshot_archives(base_path)
    if len(archives) > keep:
        for fpath, _ in archives[keep:]:
            try:
                os.remove(fpath)
                logging.info("Pruned old snapshot archive: %s", fpath)
            except Exception as e:
                logging.warning("Failed to prune snapshot archive %s: %s", fpath, e)


def find_snapshot_for_lsn(base_path: str, target_lsn: int) -> Optional[str]:
    """Find the most recent snapshot whose LSN <= target_lsn."""
    archives = list_snapshot_archives(base_path)
    for fpath, lsn in archives:
        if lsn <= target_lsn:
            return fpath
    # If no archive matches, check the main snapshot
    file_to_check = base_path
    if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
        if not file_to_check.endswith(".gz"):
            file_to_check = file_to_check + ".gz"
        if not os.path.exists(file_to_check):
            file_to_check = base_path
    if os.path.exists(file_to_check):
        try:
            if file_to_check.endswith(".gz"):
                with gzip.open(file_to_check, "rt") as f:
                    data = json.load(f)
            else:
                with open(file_to_check, "r") as f:
                    data = json.load(f)
            lsn = int(data.get("_lsn", 0) or 0)
            if lsn <= target_lsn:
                return file_to_check
        except Exception:
            pass
    return None


class SnapshotManager(threading.Thread):
    def __init__(self, store, path: str, interval: float):
        super().__init__(daemon=True)
        self.store = store
        self.path = path
        self.interval = interval
        self._stop_event = threading.Event()

    def snapshot_once(self) -> None:
        if not self.path:
            return
        tmp = f"{self.path}.tmp"
        try:
            lsn, data = self.store.dump_with_lsn()
            payload = dict(data)
            payload["_lsn"] = int(lsn)
            payload["_ts"] = time.time()
            
            if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
                tmp_gz = f"{self.path}.tmp.gz"
                with gzip.open(tmp_gz, "wt", compresslevel=int(getattr(settings, "COMPRESSION_LEVEL", 6))) as f:
                    json.dump(payload, f)
                os.replace(tmp_gz, self.path + ".gz")
                logging.info("Saved compressed snapshot to %s.gz", self.path)
            else:
                with open(tmp, "w") as f:
                    json.dump(payload, f)
                os.replace(tmp, self.path)
                logging.info("Saved snapshot to %s", self.path)
            
            # Also save an archived version if PITR is enabled
            if getattr(settings, "PITR_ENABLED", True):
                try:
                    ts = int(time.time())
                    archive_path = f"{self.path}.{lsn}.{ts}.archive"
                    if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
                        archive_path_gz = archive_path + ".gz"
                        with gzip.open(archive_path_gz, "wt", compresslevel=int(getattr(settings, "COMPRESSION_LEVEL", 6))) as f:
                            json.dump(payload, f)
                        logging.info("Saved compressed snapshot archive to %s", archive_path_gz)
                    else:
                        with open(archive_path, "w") as f:
                            json.dump(payload, f)
                        logging.info("Saved snapshot archive to %s", archive_path)
                    keep = int(getattr(settings, "PITR_SNAPSHOT_KEEP", 5))
                    prune_snapshot_archives(self.path, keep)
                except Exception as e:
                    logging.warning("Failed to save snapshot archive: %s", e)
            
            if settings.WAL_ROTATE_ENABLED and getattr(self.store, "_wal", None) is not None:
                try:
                    self.store._wal.rotate_after_snapshot(int(lsn), keep=int(settings.WAL_ROTATE_KEEP))
                    logging.info("Rotated WAL after snapshot (lsn=%d)", int(lsn))
                except Exception as e:
                    logging.warning("WAL rotation failed: %s", e)
        except Exception as e:
            logging.error("Failed to save snapshot: %s", e)
            for tmp_file in [tmp, f"{self.path}.tmp.gz"]:
                if os.path.exists(tmp_file):
                    try:
                        os.remove(tmp_file)
                    except:
                        pass

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logging.info("Snapshot manager started (interval=%.1fs)", settings.SNAPSHOT_INTERVAL)
        while not self._stop_event.is_set():
            interval = settings.SNAPSHOT_INTERVAL
            if interval <= 0:
                time.sleep(1.0)
                continue
            
            time.sleep(interval)
            if self._stop_event.is_set():
                break
            self.snapshot_once()


def recover_to_lsn(store, target_lsn: int, snapshot_path: str, wal_path: str) -> bool:
    """
    Recover the store to a specific LSN using snapshots and WAL.
    Returns True on success.
    """
    from .wal import WAL

    logging.info("Starting PITR recovery to LSN %d", target_lsn)

    # Find and load the appropriate snapshot
    snapshot_file = find_snapshot_for_lsn(snapshot_path, target_lsn)
    if snapshot_file:
        logging.info("Loading snapshot for PITR: %s", snapshot_file)
        if not load_snapshot(store, snapshot_file):
            return False
    else:
        logging.info("No suitable snapshot found, starting from empty")
        try:
            store.load({})
        except Exception:
            pass

    # Now replay WAL up to target_lsn
    if wal_path and os.path.exists(wal_path):
        wal = WAL(wal_path)
        try:
            entries = []
            with open(wal_path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("type") == "meta":
                            continue
                        entry_lsn = entry.get("lsn", 0)
                        if entry_lsn > target_lsn:
                            break
                        entries.append(entry)
                    except Exception:
                        continue

            # Apply the entries
            max_applied_lsn = 0
            for entry in entries:
                lsn = entry.get("lsn", 0)
                if lsn <= max_applied_lsn:
                    continue
                op = entry.get("op")
                key = entry.get("key")
                val = entry.get("value")
                ttl = entry.get("ttl")
                try:
                    if op == "create":
                        try:
                            store.create(key, val, ttl, skip_wal=True, skip_replication=True)
                        except KeyError:
                            store.update(key, val, ttl, skip_wal=True, skip_replication=True)
                    elif op == "update":
                        store.update(key, val, ttl, skip_wal=True, skip_replication=True)
                    elif op == "delete":
                        try:
                            store.delete(key, skip_wal=True, skip_replication=True)
                        except KeyError:
                            pass
                    elif op == "mset":
                        store.mset(key, ttl, skip_wal=True, skip_replication=True)
                    elif op == "incr":
                        store.incr(key, val, ttl, skip_wal=True, skip_replication=True)
                    elif op == "persist":
                        try:
                            store.persist(key, skip_wal=True, skip_replication=True)
                        except KeyError:
                            pass
                    max_applied_lsn = max(max_applied_lsn, lsn)
                except Exception as e:
                    logging.warning("PITR failed to apply entry LSN %d: %s", lsn, e)
            store._wal._lsn = max_applied_lsn
            logging.info("PITR recovery completed, applied up to LSN %d", max_applied_lsn)
            return True
        except Exception as e:
            logging.error("PITR recovery failed: %s", e)
            return False
    return True


def recover_to_timestamp(store, target_ts: float, snapshot_path: str, wal_path: str) -> bool:
    """
    Recover the store to a specific timestamp (seconds since epoch) using snapshots and WAL.
    Returns True on success.
    """
    from .wal import WAL

    logging.info("Starting PITR recovery to timestamp %f", target_ts)

    # First pass: find max LSN with ts <= target_ts from WAL
    target_lsn = None
    if wal_path and os.path.exists(wal_path):
        try:
            with open(wal_path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("type") == "meta":
                            continue
                        entry_ts = entry.get("ts", 0.0)
                        entry_lsn = entry.get("lsn", 0)
                        if entry_ts <= target_ts:
                            target_lsn = entry_lsn
                        else:
                            break
                    except Exception:
                        continue
        except Exception:
            pass

    # If we found a target LSN, use recover_to_lsn
    if target_lsn is not None:
        return recover_to_lsn(store, target_lsn, snapshot_path, wal_path)
    else:
        # If no WAL, try to find the best snapshot
        if snapshot_path and os.path.exists(snapshot_path):
            try:
                with open(snapshot_path, "r") as f:
                    data = json.load(f)
                snapshot_ts = data.get("_ts", 0.0)
                if snapshot_ts <= target_ts:
                    load_snapshot(store, snapshot_path)
                    return True
            except Exception:
                pass
        # List archives and find the best one
        archives = list_snapshot_archives(snapshot_path)
        for fpath, _ in archives:
            try:
                with open(fpath, "r") as f:
                    data = json.load(f)
                snapshot_ts = data.get("_ts", 0.0)
                if snapshot_ts <= target_ts:
                    load_snapshot(store, fpath)
                    return True
            except Exception:
                continue
        return False
