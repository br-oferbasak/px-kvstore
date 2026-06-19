import time

import pytest

from pxkv.core.heavy_hitters import CountMinSketch, TopKHeavyHitters
from pxkv.core.sharded import ShardedKeyValueStore


class TestCountMinSketch:
    def test_add_returns_estimate_at_least_count(self):
        cms = CountMinSketch(width=64, depth=3)
        for _ in range(7):
            cms.add("a")
        assert cms.estimate("a") >= 7

    def test_estimate_unseen_is_zero(self):
        cms = CountMinSketch(width=64, depth=3)
        cms.add("a", 5)
        assert cms.estimate("ghost") == 0

    def test_never_underestimates(self):
        cms = CountMinSketch(width=128, depth=4)
        for k in ["a", "b", "c", "d", "e"]:
            for _ in range(10):
                cms.add(k)
        for k in ["a", "b", "c", "d", "e"]:
            assert cms.estimate(k) >= 10

    def test_clear(self):
        cms = CountMinSketch(width=64, depth=3)
        cms.add("a", 100)
        cms.clear()
        assert cms.estimate("a") == 0

    def test_decay_halves_counts(self):
        cms = CountMinSketch(width=64, depth=3)
        cms.add("a", 100)
        before = cms.estimate("a")
        cms.decay(0.5)
        after = cms.estimate("a")
        assert after <= before // 2 + 1

    def test_decay_zero_clears(self):
        cms = CountMinSketch(width=64, depth=3)
        cms.add("a", 50)
        cms.decay(0.0)
        assert cms.estimate("a") == 0

    def test_handles_various_key_types(self):
        cms = CountMinSketch(width=64, depth=3)
        cms.add("string_key", 3)
        cms.add(b"bytes_key", 2)
        cms.add(42, 1)
        assert cms.estimate("string_key") >= 3
        assert cms.estimate(b"bytes_key") >= 2
        assert cms.estimate(42) >= 1


class TestTopKHeavyHitters:
    def test_disabled_passthrough(self):
        h = TopKHeavyHitters(enabled=False, k=5)
        h.record("a")
        assert h.estimate("a") == 0
        assert h.top_k() == []
        assert h.snapshot()["enabled"] is False

    def test_basic_top_k(self):
        h = TopKHeavyHitters(enabled=True, k=3, cms_width=256, cms_depth=4)
        for _ in range(100):
            h.record("hot")
        for _ in range(50):
            h.record("warm")
        for _ in range(10):
            h.record("cool")
        for _ in range(2):
            h.record("cold")
        top = h.top_k()
        keys = [e["key"] for e in top]
        assert "hot" in keys
        assert "warm" in keys
        assert keys[0] == "hot"

    def test_replaces_minimum_when_new_key_exceeds(self):
        h = TopKHeavyHitters(enabled=True, k=2, cms_width=256, cms_depth=4)
        for _ in range(5):
            h.record("a")
        for _ in range(3):
            h.record("b")
        # Fill the tracked set with a and b. Now bring in c with higher count.
        for _ in range(10):
            h.record("c")
        keys = {e["key"] for e in h.top_k()}
        assert "c" in keys
        # b had the lowest count, should be evicted
        assert "b" not in keys

    def test_bounded_memory_under_high_cardinality(self):
        h = TopKHeavyHitters(enabled=True, k=10, cms_width=512, cms_depth=4)
        # Insert 10000 unique keys, each once, plus a single very-hot key.
        for i in range(10000):
            h.record(f"k{i}")
        for _ in range(5000):
            h.record("super_hot")
        snap = h.snapshot()
        # Tracked set is bounded by K.
        assert snap["tracked_keys"] <= 10
        top = h.top_k()
        assert top[0]["key"] == "super_hot"

    def test_threshold_marks_hot(self):
        h = TopKHeavyHitters(enabled=True, k=5, threshold_count=10)
        for _ in range(15):
            h.record("hot")
        for _ in range(3):
            h.record("warm")
        assert h.is_hot("hot") is True
        assert h.is_hot("warm") is False
        top = h.top_k()
        hot_entry = next(e for e in top if e["key"] == "hot")
        warm_entry = next(e for e in top if e["key"] == "warm")
        assert hot_entry["hot"] is True
        assert warm_entry["hot"] is False

    def test_threshold_disabled_when_zero(self):
        h = TopKHeavyHitters(enabled=True, k=5, threshold_count=0)
        for _ in range(100):
            h.record("hot")
        assert h.is_hot("hot") is False

    def test_forget_drops_tracked_entry(self):
        h = TopKHeavyHitters(enabled=True, k=5)
        for _ in range(10):
            h.record("a")
        assert any(e["key"] == "a" for e in h.top_k())
        h.forget("a")
        assert not any(e["key"] == "a" for e in h.top_k())

    def test_reset_clears_everything(self):
        h = TopKHeavyHitters(enabled=True, k=5)
        for _ in range(20):
            h.record("a")
        h.reset()
        assert h.estimate("a") == 0
        assert h.top_k() == []
        snap = h.snapshot()
        assert snap["records"] == 0
        assert snap["tracked_keys"] == 0

    def test_lazy_decay_on_interval(self):
        h = TopKHeavyHitters(
            enabled=True,
            k=5,
            decay_interval_seconds=0.05,
            decay_factor=0.5,
        )
        for _ in range(100):
            h.record("a")
        before = h.estimate("a")
        time.sleep(0.07)
        # Next record triggers the decay pass before adding.
        h.record("a")
        after = h.estimate("a")
        assert after < before  # halved then incremented by 1
        snap = h.snapshot()
        assert snap["decays"] >= 1

    def test_no_decay_when_interval_zero(self):
        h = TopKHeavyHitters(
            enabled=True, k=5, decay_interval_seconds=0.0, decay_factor=0.5
        )
        for _ in range(50):
            h.record("a")
        before = h.estimate("a")
        time.sleep(0.05)
        h.record("a")
        after = h.estimate("a")
        assert after >= before  # no decay applied

    def test_configure_resizes_sketch(self):
        h = TopKHeavyHitters(enabled=True, k=5, cms_width=128, cms_depth=2)
        for _ in range(10):
            h.record("a")
        assert h.estimate("a") >= 10
        h.configure(cms_width=256, cms_depth=3)
        # Resize wipes state.
        assert h.estimate("a") == 0
        assert h.snapshot()["cms_width"] == 256
        assert h.snapshot()["cms_depth"] == 3

    def test_configure_shrinking_k_trims_tracked(self):
        h = TopKHeavyHitters(enabled=True, k=5)
        for k in ["a", "b", "c", "d", "e"]:
            for _ in range(int(k.encode()[0])):  # different counts
                h.record(k)
        assert h.snapshot()["tracked_keys"] == 5
        h.configure(k=2)
        assert h.snapshot()["tracked_keys"] <= 2

    def test_top_k_sorted_by_estimate(self):
        h = TopKHeavyHitters(enabled=True, k=10, cms_width=256, cms_depth=4)
        for _ in range(100):
            h.record("first")
        for _ in range(50):
            h.record("second")
        for _ in range(25):
            h.record("third")
        top = h.top_k()
        counts = [e["estimated_count"] for e in top]
        assert counts == sorted(counts, reverse=True)
        assert top[0]["key"] == "first"

    def test_snapshot_includes_top(self):
        h = TopKHeavyHitters(enabled=True, k=3)
        for _ in range(5):
            h.record("a")
        snap = h.snapshot()
        assert "top" in snap
        assert len(snap["top"]) >= 1
        assert snap["enabled"] is True


class TestShardedIntegration:
    @pytest.fixture
    def store(self):
        s = ShardedKeyValueStore(shards=2, per_shard_max=100)
        s._heavy_hitters.configure(
            enabled=True,
            k=5,
            cms_width=512,
            cms_depth=4,
            decay_interval_seconds=0.0,
            threshold_count=5,
        )
        yield s

    def test_reads_record_into_heavy_hitters(self, store):
        store.create("a", 1)
        store.create("b", 2)
        for _ in range(10):
            store.read("a")
        for _ in range(2):
            store.read("b")
        top = store._heavy_hitters.top_k()
        keys = [e["key"] for e in top]
        assert "a" in keys
        a_entry = next(e for e in top if e["key"] == "a")
        assert a_entry["estimated_count"] >= 10

    def test_delete_forgets_heavy_hitter(self, store):
        store.create("a", 1)
        for _ in range(20):
            store.read("a")
        assert any(e["key"] == "a" for e in store._heavy_hitters.top_k())
        store.delete("a")
        assert not any(e["key"] == "a" for e in store._heavy_hitters.top_k())

    def test_mget_records_into_heavy_hitters(self, store):
        store.mset({"a": 1, "b": 2})
        for _ in range(5):
            store.mget(["a", "b"])
        keys = [e["key"] for e in store._heavy_hitters.top_k()]
        assert "a" in keys
        assert "b" in keys

    def test_disabled_does_not_track(self):
        s = ShardedKeyValueStore(shards=1, per_shard_max=10)
        s._heavy_hitters.configure(enabled=False)
        s.create("a", 1)
        for _ in range(20):
            s.read("a")
        assert s._heavy_hitters.top_k() == []
        assert s._heavy_hitters.estimate("a") == 0

    def test_threshold_marks_hot_via_sharded(self, store):
        store.create("hot", 1)
        for _ in range(20):
            store.read("hot")
        assert store._heavy_hitters.is_hot("hot") is True
        store.create("cool", 1)
        store.read("cool")
        assert store._heavy_hitters.is_hot("cool") is False
