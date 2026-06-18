import pytest

from pxkv.core.cold_eviction import ColdKeyEvictionHints
from pxkv.core.lfu import LFUKeyValueStore
from pxkv.core.lru import LRUKeyValueStore
from pxkv.core.sharded import ShardedKeyValueStore


@pytest.fixture
def hints():
    return ColdKeyEvictionHints(
        enabled=True,
        window_seconds=60.0,
        buckets=6,
        scan_candidates=8,
        cold_threshold_count=1,
        max_tracked_keys=1000,
    )


class TestColdKeyEvictionHints:
    def test_disabled_returns_zero(self):
        h = ColdKeyEvictionHints(enabled=False)
        h.record("a")
        assert h.access_count("a") == 0
        assert h.is_cold("a") is True

    def test_record_and_count(self, hints):
        hints.record("a")
        hints.record("a")
        hints.record("a")
        hints.record("b")
        assert hints.access_count("a") == 3
        assert hints.access_count("b") == 1
        assert hints.access_count("missing") == 0

    def test_is_cold_threshold(self):
        h = ColdKeyEvictionHints(enabled=True, cold_threshold_count=2)
        h.record("warm")
        h.record("warm")
        h.record("warm")
        h.record("cold")
        h.record("cold")
        assert h.is_cold("warm") is False
        assert h.is_cold("cold") is True
        assert h.is_cold("ghost") is True

    def test_forget(self, hints):
        hints.record("a")
        hints.record("a")
        assert hints.access_count("a") == 2
        hints.forget("a")
        assert hints.access_count("a") == 0

    def test_pick_lru_victim_disabled_returns_head(self):
        h = ColdKeyEvictionHints(enabled=False)
        assert h.pick_lru_victim(["a", "b", "c"]) == 0

    def test_pick_lru_victim_empty(self, hints):
        assert hints.pick_lru_victim([]) is None

    def test_pick_lru_victim_picks_coldest(self, hints):
        # head ('a') is warm; 'b' is cold -> should redirect eviction to b
        for _ in range(10):
            hints.record("a")
        for _ in range(5):
            hints.record("c")
        # b never recorded -> count 0
        idx = hints.pick_lru_victim(["a", "b", "c"])
        assert idx == 1

    def test_pick_lru_victim_falls_back_to_head_when_all_equally_cold(self, hints):
        # no key recorded -> all candidates have count 0; tiebreaker = head
        idx = hints.pick_lru_victim(["a", "b", "c"])
        assert idx == 0

    def test_pick_lru_victim_respects_scan_window(self):
        h = ColdKeyEvictionHints(enabled=True, scan_candidates=2)
        for _ in range(5):
            h.record("a")
            h.record("b")
        # 'c' is cold, but scan window only covers a, b -> pick from those
        idx = h.pick_lru_victim(["a", "b", "c"])
        assert idx in (0, 1)

    def test_adjusted_lfu_score_keeps_freq_primary(self, hints):
        # freq=5, hint warm -> still beats freq=2 with no hint
        for _ in range(10):
            hints.record("warm")
        s_warm = hints.adjusted_lfu_score("warm", (5, 100))
        s_cold = hints.adjusted_lfu_score("cold", (2, 50))
        assert s_cold < s_warm  # cold has lower primary freq -> evicted first

    def test_adjusted_lfu_score_breaks_freq_ties_by_hint(self, hints):
        # Same freq; warm has more hint activity so it's protected
        for _ in range(10):
            hints.record("warm")
        s_warm = hints.adjusted_lfu_score("warm", (5, 100))
        s_cold = hints.adjusted_lfu_score("cold", (5, 200))
        assert s_cold < s_warm

    def test_adjusted_lfu_score_disabled_passes_through(self):
        h = ColdKeyEvictionHints(enabled=False)
        score = h.adjusted_lfu_score("a", (3, 10))
        assert score == (3, 0, 10)

    def test_snapshot_counts_redirects(self, hints):
        for _ in range(5):
            hints.record("warm")
        # warm at head, cold (no record) after -> should redirect
        hints.pick_lru_victim(["warm", "cold"])
        snap = hints.snapshot()
        assert snap["victims_picked"] >= 1
        assert snap["victims_redirected"] >= 1

    def test_max_tracked_evicts(self):
        h = ColdKeyEvictionHints(enabled=True, max_tracked_keys=3)
        for i in range(6):
            h.record(f"k{i}")
        snap = h.snapshot()
        assert snap["tracked_keys"] <= 3

    def test_configure_resets_window_on_structure_change(self, hints):
        hints.record("a")
        assert hints.access_count("a") == 1
        hints.configure(window_seconds=30.0, buckets=3)
        assert hints.access_count("a") == 0  # reset by structure change

    def test_reset_clears_state(self, hints):
        hints.record("a")
        hints.pick_lru_victim(["a"])
        hints.reset()
        snap = hints.snapshot()
        assert snap["tracked_keys"] == 0
        assert snap["victims_picked"] == 0


class TestLRUEvictionBias:
    def test_lru_evicts_cold_over_head_when_head_warm(self):
        hints = ColdKeyEvictionHints(
            enabled=True, window_seconds=60.0, buckets=2, scan_candidates=4
        )
        store = LRUKeyValueStore(max_size=3, eviction_hints=hints)
        # Insert a, b, c. Strict LRU head is 'a'.
        store.create("a", 1)
        store.create("b", 2)
        store.create("c", 3)
        # Mark 'a' as warm; leave 'b' cold.
        hints.record("a", count=20)
        hints.record("c", count=5)
        # Insert 'd' -> eviction triggers. Strict LRU would drop 'a'; hints
        # should redirect to 'b'.
        store.create("d", 4)
        assert "a" in store._map
        assert "b" not in store._map
        assert "c" in store._map
        assert "d" in store._map

    def test_lru_falls_back_to_head_when_hints_disabled(self):
        hints = ColdKeyEvictionHints(enabled=False)
        store = LRUKeyValueStore(max_size=3, eviction_hints=hints)
        store.create("a", 1)
        store.create("b", 2)
        store.create("c", 3)
        store.create("d", 4)
        assert "a" not in store._map  # strict LRU evicts head

    def test_lru_without_hints_param_works(self):
        store = LRUKeyValueStore(max_size=2)
        store.create("a", 1)
        store.create("b", 2)
        store.create("c", 3)
        assert "a" not in store._map


class TestLFUEvictionBias:
    def test_lfu_uses_hint_as_tiebreaker(self):
        hints = ColdKeyEvictionHints(
            enabled=True, window_seconds=60.0, buckets=2, scan_candidates=8
        )
        store = LFUKeyValueStore(max_size=3, eviction_hints=hints)
        store.create("a", 1)
        store.create("b", 2)
        store.create("c", 3)
        # All keys have freq=1 from create's _touch. Mark 'a' and 'c' warm.
        hints.record("a", count=20)
        hints.record("c", count=20)
        # 'b' is cold by hint -> should be evicted when we add 'd'.
        store.create("d", 4)
        assert "a" in store._map
        assert "b" not in store._map
        assert "c" in store._map

    def test_lfu_respects_long_term_freq_over_hint(self):
        hints = ColdKeyEvictionHints(enabled=True, window_seconds=60.0, buckets=2)
        store = LFUKeyValueStore(max_size=3, eviction_hints=hints)
        store.create("rare", 1)
        store.create("common", 2)
        # Read 'common' many times so its long-term freq is far higher.
        for _ in range(20):
            store.read("common")
        store.create("medium", 3)
        # Hint warms 'rare' a bit, but 'common's freq dominates.
        hints.record("rare", count=50)
        # Add a new key forcing eviction.
        store.create("new", 4)
        # 'common' should survive; 'rare' or 'medium' goes.
        assert "common" in store._map

    def test_lfu_without_hints_param_works(self):
        store = LFUKeyValueStore(max_size=2)
        store.create("a", 1)
        store.create("b", 2)
        store.create("c", 3)
        assert len(store._map) == 2


class TestShardedIntegration:
    @pytest.fixture
    def store(self):
        s = ShardedKeyValueStore(shards=1, per_shard_max=3, eviction_policy="lru")
        s._cold_eviction_hints.configure(
            enabled=True,
            window_seconds=60.0,
            buckets=4,
            scan_candidates=8,
            cold_threshold_count=1,
        )
        yield s

    def test_writes_record_hints(self, store):
        store.create("a", 1)
        store.create("b", 2)
        assert store._cold_eviction_hints.access_count("a") >= 1
        assert store._cold_eviction_hints.access_count("b") >= 1

    def test_reads_record_hints(self, store):
        store.create("a", 1)
        before = store._cold_eviction_hints.access_count("a")
        store.read("a")
        store.read("a")
        after = store._cold_eviction_hints.access_count("a")
        assert after >= before + 2

    def test_delete_forgets_hints(self, store):
        store.create("a", 1)
        assert store._cold_eviction_hints.access_count("a") >= 1
        store.delete("a")
        assert store._cold_eviction_hints.access_count("a") == 0

    def test_eviction_redirects_to_cold_key(self):
        s = ShardedKeyValueStore(shards=1, per_shard_max=3, eviction_policy="lru")
        s._cold_eviction_hints.configure(
            enabled=True, window_seconds=60.0, buckets=4, scan_candidates=8
        )
        s.create("a", 1)
        s.create("b", 2)
        s.create("c", 3)
        # Warm a and c by reading them many times; b stays cold.
        for _ in range(15):
            s.read("a")
            s.read("c")
        # Adding 'd' forces eviction. Strict LRU would drop 'a'; cold-key
        # hints should redirect to 'b'.
        s.create("d", 4)
        keys = set(s.keys())
        assert "a" in keys
        assert "b" not in keys
        assert "c" in keys
        assert "d" in keys

    def test_lfu_eviction_redirects_to_cold_key(self):
        s = ShardedKeyValueStore(shards=1, per_shard_max=3, eviction_policy="lfu")
        s._cold_eviction_hints.configure(
            enabled=True, window_seconds=60.0, buckets=4, scan_candidates=8
        )
        s.create("a", 1)
        s.create("b", 2)
        s.create("c", 3)
        # All have freq=1. Warm 'a' and 'c' via hints only (not via read so
        # LFU freq stays tied).
        s._cold_eviction_hints.record("a", count=20)
        s._cold_eviction_hints.record("c", count=20)
        s.create("d", 4)
        keys = set(s.keys())
        assert "b" not in keys

    def test_disabled_does_not_affect_eviction(self):
        s = ShardedKeyValueStore(shards=1, per_shard_max=3, eviction_policy="lru")
        s._cold_eviction_hints.configure(enabled=False)
        s.create("a", 1)
        s.create("b", 2)
        s.create("c", 3)
        s.create("d", 4)
        keys = set(s.keys())
        assert "a" not in keys  # strict LRU: oldest goes
