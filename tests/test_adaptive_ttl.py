import time
import pytest

from pxkv.core.adaptive_ttl import AdaptiveTTLController
from pxkv.core.sharded import ShardedKeyValueStore


@pytest.fixture
def controller():
    return AdaptiveTTLController(
        enabled=True,
        min_ttl_seconds=1.0,
        max_ttl_seconds=1000.0,
        default_base_ttl_seconds=10.0,
        hit_extend_factor=4.0,
        miss_shrink_factor=0.25,
        recency_half_life_seconds=10.0,
        max_tracked_keys=100,
    )


class TestAdaptiveTTLController:
    def test_disabled_passthrough(self):
        c = AdaptiveTTLController(enabled=False)
        assert c.suggest_ttl("k", 30.0) == 30.0
        c.record_hit("k")
        c.record_miss("k")
        assert c.snapshot()["tracked_keys"] == 0

    def test_no_stats_returns_clamped_base(self, controller):
        assert controller.suggest_ttl("fresh", 5.0) == 5.0

    def test_clamps_to_min_and_max(self):
        c = AdaptiveTTLController(
            enabled=True, min_ttl_seconds=5.0, max_ttl_seconds=8.0, hit_extend_factor=10.0
        )
        assert c.suggest_ttl("k", 1.0) == 5.0  # raised to min
        assert c.suggest_ttl("k", 100.0) == 8.0  # capped to max

    def test_hits_extend_ttl(self, controller):
        for _ in range(20):
            controller.record_hit("hot")
        tuned = controller.suggest_ttl("hot", 10.0)
        assert tuned > 10.0
        assert tuned <= 1000.0

    def test_misses_shrink_ttl(self, controller):
        for _ in range(20):
            controller.record_miss("cold")
        tuned = controller.suggest_ttl("cold", 10.0)
        assert tuned < 10.0
        assert tuned >= 1.0

    def test_recency_decays_score(self):
        c = AdaptiveTTLController(
            enabled=True,
            min_ttl_seconds=0.1,
            max_ttl_seconds=1000.0,
            hit_extend_factor=4.0,
            miss_shrink_factor=0.25,
            recency_half_life_seconds=0.05,
        )
        for _ in range(10):
            c.record_hit("k")
        fresh = c.suggest_ttl("k", 10.0)
        time.sleep(0.2)
        stale = c.suggest_ttl("k", 10.0)
        assert stale < fresh

    def test_forget(self, controller):
        controller.record_hit("k")
        assert controller.key_stats("k") is not None
        controller.forget("k")
        assert controller.key_stats("k") is None

    def test_max_tracked_evicts_oldest(self):
        c = AdaptiveTTLController(enabled=True, max_tracked_keys=3)
        for i in range(5):
            c.record_hit(f"k{i}")
        snap = c.snapshot()
        assert snap["tracked_keys"] == 3
        assert snap["evictions"] >= 2

    def test_snapshot_counters(self, controller):
        controller.record_hit("a")
        controller.record_miss("b")
        controller.suggest_ttl("a", 10.0)
        controller.suggest_ttl("b", 10.0)
        snap = controller.snapshot()
        assert snap["total_hits"] == 1
        assert snap["total_misses"] == 1
        assert snap["suggestions"] == 2
        assert snap["enabled"] is True

    def test_top_tracked(self, controller):
        for _ in range(3):
            controller.record_hit("a")
        for _ in range(1):
            controller.record_hit("b")
        top = controller.top_tracked(limit=2)
        assert top[0]["key"] == "a"
        assert top[0]["hits"] == 3


class TestShardedAdaptiveTTLIntegration:
    @pytest.fixture
    def store(self):
        s = ShardedKeyValueStore(shards=2, per_shard_max=100)
        s._adaptive_ttl.configure(
            enabled=True,
            min_ttl_seconds=1.0,
            max_ttl_seconds=10000.0,
            hit_extend_factor=4.0,
            miss_shrink_factor=0.25,
            recency_half_life_seconds=60.0,
        )
        yield s

    def test_read_records_hit(self, store):
        store.create("a", 1, ttl=100.0)
        store.read("a")
        store.read("a")
        stats = store._adaptive_ttl.key_stats("a")
        assert stats is not None
        assert stats["hits"] == 2
        assert stats["misses"] == 0

    def test_read_missing_records_miss(self, store):
        with pytest.raises(KeyError):
            store.read("ghost")
        stats = store._adaptive_ttl.key_stats("ghost")
        assert stats is not None
        assert stats["misses"] == 1
        assert stats["hits"] == 0

    def test_create_tunes_ttl_for_warm_key(self, store):
        # Warm up "a" via misses then writes/hits
        for _ in range(10):
            store._adaptive_ttl.record_hit("a")
        # Now create the key - TTL should be extended above 10s
        store.create("a", 1, ttl=10.0)
        actual_ttl = store.get_ttl("a")
        assert actual_ttl is not None
        assert actual_ttl > 10.0

    def test_update_tunes_ttl_for_cold_key(self, store):
        store.create("a", 1, ttl=100.0)
        for _ in range(20):
            store._adaptive_ttl.record_miss("a")
        store.update("a", 2, ttl=100.0)
        actual_ttl = store.get_ttl("a")
        assert actual_ttl is not None
        assert actual_ttl < 100.0

    def test_delete_forgets_key(self, store):
        store.create("a", 1, ttl=10.0)
        store.read("a")
        assert store._adaptive_ttl.key_stats("a") is not None
        store.delete("a")
        assert store._adaptive_ttl.key_stats("a") is None

    def test_mget_records_hits_and_misses(self, store):
        store.mset({"a": 1, "b": 2})
        store.mget(["a", "b", "missing"])
        assert store._adaptive_ttl.key_stats("a")["hits"] == 1
        assert store._adaptive_ttl.key_stats("b")["hits"] == 1
        assert store._adaptive_ttl.key_stats("missing")["misses"] == 1

    def test_disabled_does_not_change_ttl(self):
        s = ShardedKeyValueStore(shards=2, per_shard_max=100)
        s._adaptive_ttl.configure(enabled=False)
        for _ in range(50):
            s._adaptive_ttl.record_hit("a")
        s.create("a", 1, ttl=10.0)
        actual_ttl = s.get_ttl("a")
        assert actual_ttl is not None
        assert pytest.approx(actual_ttl, abs=1.0) == 10.0
