from pxkv.api.redis_server import RedisServer
from pxkv.core.query_plan_cache import query_plan_cache
from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.metrics.prometheus import registry_to_prometheus
from pxkv.metrics.registry import MetricsRegistry
from pxkv.namespaces import namespace_manager


def setup_function():
    query_plan_cache.configure(enabled=True, max_entries=8)
    query_plan_cache.reset()


def teardown_function():
    query_plan_cache.configure(enabled=False, max_entries=1024)
    query_plan_cache.reset()


def test_redis_command_plan_cache_hits_repeated_command():
    args = [b"GET", b"hot-key"]

    first = query_plan_cache.redis_plan(args)
    second = query_plan_cache.redis_plan(args)
    snap = query_plan_cache.snapshot()

    assert first is second
    assert second.cmd == "GET"
    assert second.str_args == ("GET", "hot-key")
    assert snap["redis_misses"] == 1
    assert snap["redis_hits"] == 1


def test_redis_server_uses_cached_command_plan():
    store = ShardedKeyValueStore(shards=1, per_shard_max=10)
    server = RedisServer(store)

    resp, role, namespace = server.handle_command([b"SET", b"a", b"1"], None, namespace_manager.default())
    assert resp == b"+OK\r\n"
    resp, role, namespace = server.handle_command([b"GET", b"a"], role, namespace)
    assert resp == b"$1\r\n1\r\n"
    resp, role, namespace = server.handle_command([b"GET", b"a"], role, namespace)
    assert resp == b"$1\r\n1\r\n"

    snap = query_plan_cache.snapshot()
    assert snap["redis_hits"] >= 1
    assert snap["redis_entries"] >= 2


def test_scan_plan_cache_hits_repeated_namespace_scan():
    plan1 = query_plan_cache.scan_plan(
        namespace=namespace_manager.default(),
        prefix="fo",
        start_after="foo",
        cursor=None,
        limit=25,
        cursor_mode=False,
        ns_prefix_fn=namespace_manager.user_prefix,
        ns_key_fn=namespace_manager.key,
    )
    plan2 = query_plan_cache.scan_plan(
        namespace=namespace_manager.default(),
        prefix="fo",
        start_after="foo",
        cursor=None,
        limit=25,
        cursor_mode=False,
        ns_prefix_fn=namespace_manager.user_prefix,
        ns_key_fn=namespace_manager.key,
    )
    snap = query_plan_cache.snapshot()

    assert plan1 is plan2
    assert plan2.limit == 25
    assert snap["scan_misses"] == 1
    assert snap["scan_hits"] == 1


def test_query_plan_cache_prometheus_metrics():
    registry = MetricsRegistry()
    query_plan_cache.redis_plan([b"PING"])
    query_plan_cache.redis_plan([b"PING"])
    registry.observe_query_plan_cache(query_plan_cache.snapshot())

    text = registry_to_prometheus(registry.get_all())
    assert "pxkv_query_plan_cache_enabled 1" in text
    assert 'pxkv_query_plan_cache_hits_total{kind="redis"} 1' in text
