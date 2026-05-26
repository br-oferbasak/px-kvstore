import os
import json
import subprocess
import time
import urllib.request
import urllib.error

import socket

import pytest


def get_free_port() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return str(s.getsockname()[1])


def http_put(url: str, data: bytes) -> int:
    req = urllib.request.Request(url, data=data, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))


def http_post(url: str, data: bytes = b"") -> int:
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))


def http_get_json(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            raw = resp.read()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            return resp.status, body
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500)), {}


def http_get_json_with_headers(url: str) -> tuple[int, dict, dict]:
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            raw = resp.read()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            return resp.status, body, dict(resp.headers.items())
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500)), {}, {}

def stop_proc(proc: subprocess.Popen) -> None:
    try:
        proc.terminate()
    except Exception:
        return
    try:
        proc.wait(timeout=3.0)
        return
    except Exception:
        pass
    try:
        proc.kill()
    except Exception:
        return
    try:
        proc.wait(timeout=3.0)
    except Exception:
        pass

def wait_for_http_ready(base: str, timeout_s: float = 8.0) -> None:
    deadline = time.time() + timeout_s
    last_status = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                last_status = resp.status
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(0.2)
    raise AssertionError(f"server not ready: {base} (last_status={last_status})")

def wait_for_kv(base: str, key: str, expected: object, timeout_s: float = 8.0) -> dict:
    url = f"{base}/kv/{key}"
    deadline = time.time() + timeout_s
    last_status = None
    last_body: dict = {}
    while time.time() < deadline:
        status, body = http_get_json(url)
        last_status = status
        last_body = body
        if status == 200 and body.get("value") == expected:
            return body
        time.sleep(0.2)
    raise AssertionError(f"kv not replicated in time: {url} status={last_status} body={last_body}")


@pytest.fixture
def leader_follower_cluster():
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_WAL_FILE"] = "follower_wal.log"
    follower_env["PXKV_REDIS_ENABLED"] = "false"

    for f in ["leader_wal.log", "follower_wal.log"]:
        if os.path.exists(f): os.remove(f)

    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    leader_base = f"http://localhost:{leader_port}"
    wait_for_http_ready(leader_base, timeout_s=8.0)
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    follower_base = f"http://localhost:{follower_port}"
    wait_for_http_ready(follower_base, timeout_s=8.0)
    
    yield (leader_port, follower_port)
    
    stop_proc(leader_proc)
    stop_proc(follower_proc)

def test_replication_basic(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    leader_url = f"http://localhost:{leader_port}/kv/repl_key"
    status = http_put(leader_url, b"repl_value")
    assert status in [201, 204]

    follower_base = f"http://localhost:{follower_port}"
    wait_for_kv(follower_base, "repl_key", "repl_value", timeout_s=8.0)

    follower_url = f"{follower_base}/kv/repl_key"
    status, body, headers = http_get_json_with_headers(follower_url)
    assert status == 200 and body["value"] == "repl_value"
    assert headers.get("X-PXKV-Role") == "follower"
    assert int(headers.get("X-PXKV-Replication-Last-Applied-LSN", "0")) > 0

def test_replication_incr(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    leader_url = f"http://localhost:{leader_port}/kv/incr/c1"
    assert http_post(leader_url) == 200
    assert http_post(leader_url) == 200

    follower_base = f"http://localhost:{follower_port}"
    wait_for_kv(follower_base, "c1", 2.0, timeout_s=8.0)

def test_replication_full_sync():
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    leader_base = f"http://localhost:{leader_port}"
    wait_for_http_ready(leader_base, timeout_s=8.0)
    assert http_put(f"{leader_base}/kv/pre_existing", b"pre_value") in [201, 204]
    
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    follower_base = f"http://localhost:{follower_port}"
    wait_for_http_ready(follower_base, timeout_s=8.0)
    wait_for_kv(follower_base, "pre_existing", "pre_value", timeout_s=8.0)
    stop_proc(leader_proc)
    stop_proc(follower_proc)

def test_replication_catchup():
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "catchup_leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    if os.path.exists("catchup_leader_wal.log"): os.remove("catchup_leader_wal.log")

    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    leader_base = f"http://localhost:{leader_port}"
    wait_for_http_ready(leader_base, timeout_s=8.0)
    
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    follower_env["PXKV_REPLICATION_SYNC_INTERVAL"] = "1.0"
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    follower_base = f"http://localhost:{follower_port}"
    wait_for_http_ready(follower_base, timeout_s=8.0)
    
    assert http_put(f"{leader_base}/kv/k1", b"v1") in [201, 204]
    wait_for_kv(follower_base, "k1", "v1", timeout_s=8.0)
    stop_proc(follower_proc)
    
    assert http_put(f"{leader_base}/kv/k2", b"v2") in [201, 204]
    assert http_put(f"{leader_base}/kv/k3", b"v3") in [201, 204]
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    wait_for_http_ready(follower_base, timeout_s=8.0)
    wait_for_kv(follower_base, "k2", "v2", timeout_s=8.0)
    wait_for_kv(follower_base, "k3", "v3", timeout_s=8.0)
    stop_proc(leader_proc)
    stop_proc(follower_proc)


def test_replication_ack_and_lag_metrics(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    follower_name = f"localhost:{follower_port}"
    assert http_put(f"http://localhost:{leader_port}/kv/ack_probe", b"v1") in [201, 204]

    deadline = time.time() + 8.0
    while time.time() < deadline:
        status, body = http_get_json(f"http://localhost:{leader_port}/admin/metrics")
        assert status == 200
        repl = body.get("replication", {})
        followers = repl.get("followers", {})
        if follower_name in followers:
            item = followers[follower_name]
            if int(item.get("ack_lsn", 0)) > 0:
                assert int(item.get("lag_lsn", 0)) >= 0
                return
        time.sleep(0.2)

    raise AssertionError("replication ack metrics not updated in time")


def test_follower_http_readonly_rejects_writes(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    assert http_put(f"http://localhost:{leader_port}/kv/ro_seed", b"v1") in [201, 204]
    follower_base = f"http://localhost:{follower_port}"
    wait_for_kv(follower_base, "ro_seed", "v1", timeout_s=8.0)
    status = http_put(f"http://localhost:{follower_port}/kv/should_fail", b"nope")
    assert status == 403


def test_replication_anti_entropy_config():
    leader_port = get_free_port()
    follower_port = get_free_port()

    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REDIS_ENABLED"] = "false"

    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    leader_base = f"http://localhost:{leader_port}"
    wait_for_http_ready(leader_base, timeout_s=8.0)

    # Insert test keys
    for i in range(3):
        assert http_put(f"{leader_base}/kv/ae_key_{i}", f"ae_val_{i}".encode("utf-8")) in [201, 204]

    # Start follower with anti-entropy enabled and short interval
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    follower_env["PXKV_ANTI_ENTROPY_ENABLED"] = "true"
    follower_env["PXKV_ANTI_ENTROPY_INTERVAL"] = "1.0"
    follower_env["PXKV_ANTI_ENTROPY_MAX_LAG_LSN"] = "100"
    follower_env["PXKV_ANTI_ENTROPY_MAX_AGE_MS"] = "10000.0"

    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    follower_base = f"http://localhost:{follower_port}"
    wait_for_http_ready(follower_base, timeout_s=8.0)

    # Wait for normal full sync
    for i in range(3):
        wait_for_kv(follower_base, f"ae_key_{i}", f"ae_val_{i}", timeout_s=8.0)

    # Verify anti-entropy config is present (no crash)
    status, metrics = http_get_json(f"{follower_base}/admin/metrics")
    assert status == 200

    stop_proc(leader_proc)
    stop_proc(follower_proc)


def test_cross_cluster_conflict_resolution_last_write_wins():
    from pxkv.core.sharded import ShardedKeyValueStore

    # Create two stores with different cluster IDs
    store1 = ShardedKeyValueStore(shards=2, wal_path="", tiering_dir="")
    store2 = ShardedKeyValueStore(shards=2, wal_path="", tiering_dir="")

    # Write from store1 first
    ts_early = time.time() - 10.0
    store1.create("k1", "v1", origin_cluster_id="cluster-a", origin_ts=ts_early)

    # Write from store2 later (should win)
    ts_late = time.time()
    store2.create("k1", "v2", origin_cluster_id="cluster-b", origin_ts=ts_late)

    # Resolve conflict in store1
    should_apply, resolved_val, _ = store1.resolve_conflict(
        "k1", "v2", None,
        new_origin_cluster_id="cluster-b",
        new_origin_ts=ts_late,
    )
    assert should_apply is True

    # Now apply it
    store1.update("k1", "v2", origin_cluster_id="cluster-b", origin_ts=ts_late)
    assert store1.read("k1") == "v2"
    meta = store1.get_xmeta("k1")
    assert meta is not None
    assert meta["origin_cluster_id"] == "cluster-b"
    assert meta["origin_ts"] == ts_late

    # Test tie: same ts, use cluster ID lex order (cluster-b wins over cluster-a)
    ts_same = time.time()
    store1.create("k2", "va", origin_cluster_id="cluster-a", origin_ts=ts_same)
    should_apply2, _, _ = store1.resolve_conflict(
        "k2", "vb", None,
        new_origin_cluster_id="cluster-b",
        new_origin_ts=ts_same,
    )
    assert should_apply2 is True

    # Test older write: should not apply
    store1.create("k3", "v_newer", origin_cluster_id="cluster-c", origin_ts=time.time())
    should_apply3, _, _ = store1.resolve_conflict(
        "k3", "v_older", None,
        new_origin_cluster_id="cluster-d",
        new_origin_ts=time.time() - 100,
    )
    assert should_apply3 is False


def test_reshard():
    from pxkv.core.sharded import ShardedKeyValueStore

    store = ShardedKeyValueStore(shards=2, wal_path="", tiering_dir="")

    # Insert some test keys
    test_keys = []
    for i in range(20):
        k = f"key_{i:02d}"
        v = f"value_{i:02d}"
        store.create(k, v)
        test_keys.append((k, v))

    # Verify all keys are present
    for k, v in test_keys:
        assert store.read(k) == v

    # Reshard to 4 shards
    result = store.reshard(new_shards=4)
    assert result["old_shards"] == 2
    assert result["new_shards"] == 4
    assert result["keys_migrated"] == 20

    # Verify all keys are still present
    for k, v in test_keys:
        assert store.read(k) == v

    # Reshard back to 2 shards
    result2 = store.reshard(new_shards=2)
    assert result2["old_shards"] == 4
    assert result2["new_shards"] == 2
    assert result2["keys_migrated"] == 20

    # Verify all keys are still present
    for k, v in test_keys:
        assert store.read(k) == v
