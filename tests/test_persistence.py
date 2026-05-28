import pytest
import os
import json
from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.persistence.snapshot import SnapshotManager, load_snapshot
from pxkv.persistence.wal import recover_from_wal
from pxkv.config.settings import settings

@pytest.fixture
def store():
    return ShardedKeyValueStore(shards=2, per_shard_max=10)

def test_snapshot_restore(store, tmp_path):
    snapshot_path = str(tmp_path / "test.json")
    store.create("k1", "v1", ttl=3600)
    store.create("k2", {"complex": "data"})
    
    manager = SnapshotManager(store, snapshot_path, 60)
    manager.snapshot_once()
    
    assert os.path.exists(snapshot_path)
    
    new_store = ShardedKeyValueStore(shards=2, per_shard_max=10)
    load_snapshot(new_store, snapshot_path)
    
    assert new_store.read("k1") == "v1"
    assert new_store.read("k2") == {"complex": "data"}

def test_snapshot_invalid_path():
    store = ShardedKeyValueStore()
    assert load_snapshot(store, "/nonexistent/path") is False

def test_wal_rotation_after_snapshot(tmp_path, monkeypatch):
    wal_path = str(tmp_path / "wal.log")
    snap_path = str(tmp_path / "snap.json")

    monkeypatch.setattr(settings, "WAL_ROTATE_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "WAL_ROTATE_KEEP", 0, raising=False)

    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path)
    store.incr("c1", 1)
    store.incr("c1", 2)

    manager = SnapshotManager(store, snap_path, 60)
    manager.snapshot_once()
    assert os.path.exists(snap_path)

    snapshot_payload = json.loads(open(snap_path, "r").read())
    snapshot_lsn = int(snapshot_payload.get("_lsn", 0))
    assert snapshot_lsn > 0
    assert store._wal.get_oldest_lsn() == snapshot_lsn + 1

    store.incr("c1", 3)
    assert store._wal._lsn == snapshot_lsn + 1

    new_store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path)
    assert load_snapshot(new_store, snap_path) is True
    recover_from_wal(new_store, new_store._wal)
    assert new_store.read("c1") == 6.0


def test_pitr_recover_to_lsn(tmp_path, monkeypatch):
    from pxkv.persistence.snapshot import recover_to_lsn, recover_to_timestamp, SnapshotManager

    wal_path = str(tmp_path / "wal.log")
    snap_path = str(tmp_path / "snap.json")

    monkeypatch.setattr(settings, "PITR_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "PITR_SNAPSHOT_KEEP", 3, raising=False)

    store = ShardedKeyValueStore(shards=2, per_shard_max=100, wal_path=wal_path)

    # Initial writes
    store.create("k1", "v1")
    store.create("k2", "v2")

    # Save snapshot 1
    manager = SnapshotManager(store, snap_path, 60)
    manager.snapshot_once()

    lsn_after_snap1 = store._wal._lsn

    # More writes after snapshot 1
    store.create("k3", "v3")
    store.update("k1", "v1-updated")
    store.delete("k2")

    lsn_final = store._wal._lsn

    # Recover to final LSN
    store1 = ShardedKeyValueStore(shards=2, per_shard_max=100, wal_path=wal_path)
    ok = recover_to_lsn(store1, lsn_final, snap_path, wal_path)
    assert ok
    assert store1.read("k1") == "v1-updated"
    try:
        store1.read("k2")
        assert False, "k2 should be deleted"
    except KeyError:
        pass
    assert store1.read("k3") == "v3"

    # Recover to snapshot 1 LSN
    store2 = ShardedKeyValueStore(shards=2, per_shard_max=100, wal_path=wal_path)
    ok2 = recover_to_lsn(store2, lsn_after_snap1, snap_path, wal_path)
    assert ok2
    assert store2.read("k1") == "v1"
    assert store2.read("k2") == "v2"
    try:
        store2.read("k3")
        assert False, "k3 should not exist yet"
    except KeyError:
        pass
