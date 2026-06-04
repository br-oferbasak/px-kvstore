import os
import subprocess
import time
import json
import urllib.request
import urllib.error
import urllib.parse
import gzip
import threading
import ssl
import socket
import shutil

import pytest


def get_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

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


@pytest.fixture
def http_server():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    deadline = time.time() + 8.0
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                if resp.status == 200:
                    break
        except Exception:
            time.sleep(0.2)
    yield base
    stop_proc(proc)


def test_admin_health(http_server):
    with urllib.request.urlopen(f"{http_server}/admin/health", timeout=2.0) as resp:
        assert resp.status == 200
        body = json.loads(resp.read().decode("utf-8"))
    assert body["status"] == "ok"
    assert "uptime_seconds" in body


def test_metrics_prometheus(http_server):
    with urllib.request.urlopen(f"{http_server}/admin/metrics?format=prometheus", timeout=2.0) as resp:
        assert resp.status == 200
        text = resp.read().decode("utf-8", errors="replace")
    assert "pxkv_requests_total" in text
    assert "pxkv_replication_leader_lsn" in text


def test_replication_snapshot_ndjson_gzip(http_server):
    url = f"{http_server}/replication/snapshot?format=ndjson&compress=gzip"
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"}, method="GET")
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200
        raw = resp.read()
        body = raw
        if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
            body = gzip.decompress(raw)
    lines = body.decode("utf-8", errors="replace").splitlines()
    assert len(lines) >= 1
    meta = json.loads(lines[0])
    assert "_lsn" in meta
    assert "shards" in meta


def test_sse_keyspace_notifications(http_server):
    url = f"{http_server}/events/keyspace"
    got = {"payload": None}

    def _reader():
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            deadline = time.time() + 3.0
            while time.time() < deadline:
                line = resp.readline()
                if not line:
                    break
                if line.startswith(b"data: "):
                    got["payload"] = line[len(b"data: ") :].decode("utf-8", errors="replace").strip()
                    break

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    put = urllib.request.Request(
        f"{http_server}/kv/sse_test_key",
        data=json.dumps({"value": "v"}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(put, timeout=2.0) as resp:
        assert resp.status in (200, 201)

    t.join(timeout=3.0)
    assert got["payload"] is not None
    ev = json.loads(got["payload"])
    assert ev["op"] == "set"
    assert ev["key"] == "sse_test_key"


def test_rate_limiting_per_route_admin_configurable():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"
    env["PXKV_RATE_LIMIT_ENABLED"] = "true"
    env["PXKV_RATE_LIMIT_DEFAULT_RPS"] = "0"
    env["PXKV_RATE_LIMIT_DEFAULT_BURST"] = "0"
    env["PXKV_RATE_LIMIT_ROUTES"] = json.dumps(
        {"GET /admin/health": {"rps": 0.1, "burst": 1, "per_ip": False}}
    )

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/metrics", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        with urllib.request.urlopen(f"{base}/admin/health", timeout=2.0) as resp:
            assert resp.status == 200

        try:
            urllib.request.urlopen(f"{base}/admin/health", timeout=2.0)
            raise AssertionError("expected 429")
        except urllib.error.HTTPError as e:
            assert e.code == 429
            assert (e.headers.get("Retry-After", "") or "").strip() != ""

        with urllib.request.urlopen(f"{base}/admin/metrics", timeout=2.0) as resp:
            assert resp.status == 200
        with urllib.request.urlopen(f"{base}/admin/metrics", timeout=2.0) as resp:
            assert resp.status == 200

        data = json.dumps(
            {"RATE_LIMIT_ROUTES": {"GET /admin/health": {"rps": 100, "burst": 100, "per_ip": False}}}
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{base}/admin/config",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            assert resp.status == 200
            updated = json.loads(resp.read().decode("utf-8"))
        assert updated["config"]["RATE_LIMIT_ROUTES"]["GET /admin/health"]["rps"] == 100.0

        with urllib.request.urlopen(f"{base}/admin/health", timeout=2.0) as resp:
            assert resp.status == 200
    finally:
        stop_proc(proc)


def test_tls_https_and_rediss(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")

    cert_file = tmp_path / "cert.pem"
    key_file = tmp_path / "key.pem"
    try:
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key_file),
                "-out",
                str(cert_file),
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pytest.skip("failed to generate self-signed cert with openssl")

    http_port = get_free_port()
    https_port = get_free_port()
    rediss_port = get_free_port()

    env = os.environ.copy()
    env["PXKV_PORT"] = str(http_port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_HTTP_TLS_ENABLED"] = "true"
    env["PXKV_HTTPS_PORT"] = str(https_port)
    env["PXKV_TLS_CERT_FILE"] = str(cert_file)
    env["PXKV_TLS_KEY_FILE"] = str(key_file)
    env["PXKV_REDIS_TLS_ENABLED"] = "true"
    env["PXKV_REDIS_TLS_PORT"] = str(rediss_port)
    env["PXKV_REDIS_TLS_CERT_FILE"] = str(cert_file)
    env["PXKV_REDIS_TLS_KEY_FILE"] = str(key_file)

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    try:
        https_base = f"https://localhost:{https_port}"
        deadline = time.time() + 8.0
        ctx = ssl._create_unverified_context()
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{https_base}/admin/health", timeout=1.0, context=ctx) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        with urllib.request.urlopen(f"{https_base}/admin/health", timeout=2.0, context=ctx) as resp:
            assert resp.status == 200

        raw = b"*1\r\n$4\r\nPING\r\n"
        sock = socket.create_connection(("127.0.0.1", rediss_port), timeout=2.0)
        try:
            with ctx.wrap_socket(sock, server_hostname="localhost") as ssock:
                ssock.sendall(raw)
                data = ssock.recv(1024)
        finally:
            try:
                sock.close()
            except Exception:
                pass
        assert b"+PONG" in data
    finally:
        stop_proc(proc)


def test_scan_cursor():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        # Insert some keys
        test_keys = [f"key_{i:03d}" for i in range(25)]
        for k in test_keys:
            req = urllib.request.Request(
                f"{base}/kv/{k}",
                data=json.dumps({"value": f"v_{k}"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="PUT",
            )
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                assert resp.status in (201, 204)

        # Scan with small limit and collect all keys via cursor
        collected = []
        cursor = None
        while True:
            url = f"{base}/kv/scan-cursor?limit=5"
            if cursor:
                url += f"&cursor={urllib.parse.quote_plus(cursor)}"
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                assert resp.status == 200
                body = json.loads(resp.read().decode("utf-8"))
            collected.extend(body["keys"])
            cursor = body["cursor"]
            if cursor == "0":
                break

        # Verify we got all keys, sorted
        assert sorted(collected) == sorted(test_keys)

    finally:
        stop_proc(proc)


def test_etag_conditional_get():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        # Create a key
        test_key = "etag_test"
        test_value = {"data": "etag_value"}
        req = urllib.request.Request(
            f"{base}/kv/{test_key}",
            data=json.dumps(test_value).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            assert resp.status in (201, 204)

        # First GET should return 200 and ETag
        with urllib.request.urlopen(f"{base}/kv/{test_key}", timeout=2.0) as resp:
            assert resp.status == 200
            etag = resp.headers.get("ETag")
            assert etag is not None and etag != ""
            body = json.loads(resp.read().decode("utf-8"))
            assert body["key"] == test_key
            assert body["value"] == test_value

        # Conditional GET with If-None-Match should return 304
        req_304 = urllib.request.Request(
            f"{base}/kv/{test_key}",
            headers={"If-None-Match": etag},
        )
        try:
            with urllib.request.urlopen(req_304, timeout=2.0):
                raise AssertionError("Expected 304 Not Modified")
        except urllib.error.HTTPError as e:
            assert e.code == 304
            assert e.headers.get("ETag") == etag

        # Update the key
        updated_value = {"data": "updated_etag_value"}
        req_update = urllib.request.Request(
            f"{base}/kv/{test_key}",
            data=json.dumps(updated_value).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        with urllib.request.urlopen(req_update, timeout=2.0) as resp:
            assert resp.status in (201, 204)

        # Conditional GET with old ETag should now return 200
        with urllib.request.urlopen(req_304, timeout=2.0) as resp:
            assert resp.status == 200
            new_etag = resp.headers.get("ETag")
            assert new_etag != etag
            body = json.loads(resp.read().decode("utf-8"))
            assert body["value"] == updated_value

    finally:
        stop_proc(proc)


def test_json_patch():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        # Create a test key with initial value
        test_key = "patch_test"
        initial_value = {
            "name": "John",
            "age": 30,
            "skills": ["Python"]
        }
        req_put = urllib.request.Request(
            f"{base}/kv/{test_key}",
            data=json.dumps(initial_value).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        with urllib.request.urlopen(req_put, timeout=2.0) as resp:
            assert resp.status in (201, 204)

        # First, apply a simple patch to add a new key and update age
        patch = [
            {"op": "replace", "path": "/age", "value": 31},
            {"op": "add", "path": "/skills/-", "value": "Go"},
            {"op": "add", "path": "/email", "value": "john@example.com"}
        ]
        req_patch = urllib.request.Request(
            f"{base}/kv/{test_key}",
            data=json.dumps(patch).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PATCH",
        )
        with urllib.request.urlopen(req_patch, timeout=2.0) as resp:
            assert resp.status == 200
            etag_patch = resp.headers.get("ETag")
            assert etag_patch is not None
            patch_result = json.loads(resp.read().decode("utf-8"))
            assert patch_result["key"] == test_key
            assert patch_result["value"]["name"] == "John"
            assert patch_result["value"]["age"] == 31
            assert patch_result["value"]["skills"] == ["Python", "Go"]
            assert patch_result["value"]["email"] == "john@example.com"

        # Verify GET returns the same value
        with urllib.request.urlopen(f"{base}/kv/{test_key}", timeout=2.0) as resp:
            assert resp.status == 200
            get_result = json.loads(resp.read().decode("utf-8"))
            assert get_result["value"] == patch_result["value"]

        # Now apply a patch to remove a skill
        patch2 = [
            {"op": "remove", "path": "/skills/0"}
        ]
        req_patch2 = urllib.request.Request(
            f"{base}/kv/{test_key}",
            data=json.dumps(patch2).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PATCH",
        )
        with urllib.request.urlopen(req_patch2, timeout=2.0) as resp:
            assert resp.status == 200
            patch_result2 = json.loads(resp.read().decode("utf-8"))
            assert patch_result2["value"]["skills"] == ["Go"]

    finally:
        stop_proc(proc)


def test_disk_usage_throttling_blocks_writes(tmp_path):
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"
    env["PXKV_DISK_THROTTLE_ENABLED"] = "true"
    env["PXKV_DISK_THROTTLE_PATHS"] = str(tmp_path)
    env["PXKV_DISK_THROTTLE_HARD_USED_BYTES"] = "1"

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        req = urllib.request.Request(
            f"{base}/kv/disk_guard_key",
            data=json.dumps({"value": "blocked"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        try:
            urllib.request.urlopen(req, timeout=2.0)
            raise AssertionError("expected 507")
        except urllib.error.HTTPError as e:
            assert e.code == 507
            body = json.loads(e.read().decode("utf-8"))
            assert body["error"] == "disk_throttled"
            assert body["used_bytes"] >= 1

        with urllib.request.urlopen(f"{base}/admin/health", timeout=2.0) as resp:
            assert resp.status == 200
            health = json.loads(resp.read().decode("utf-8"))
        assert health["disk"]["enabled"] is True
        assert health["disk"]["hard_exceeded"] is True

        with urllib.request.urlopen(f"{base}/admin/metrics", timeout=2.0) as resp:
            assert resp.status == 200
            metrics = json.loads(resp.read().decode("utf-8"))
        assert metrics["disk"]["rejected_total"] >= 1
    finally:
        stop_proc(proc)


def test_namespace_isolation_auth_and_rate_limits():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"
    env["PXKV_NAMESPACE_ENABLED"] = "true"
    env["PXKV_NAMESPACE_DEFAULT"] = "tenant-a"
    env["PXKV_NAMESPACE_CONFIGS"] = json.dumps(
        {
            "tenant-a": {
                "auth": {"writer_token": "wa", "reader_token": "ra"},
                "rate_limit_routes": {"GET /kv/:key": {"rps": 0.1, "burst": 1, "per_ip": False}},
            },
            "tenant-b": {
                "auth": {"writer_token": "wb", "reader_token": "rb"},
            },
        }
    )

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        req_a = urllib.request.Request(
            f"{base}/kv/shared",
            data=json.dumps({"tenant": "a"}).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": "Bearer wa", "X-PXKV-Namespace": "tenant-a"},
            method="PUT",
        )
        with urllib.request.urlopen(req_a, timeout=2.0) as resp:
            assert resp.status in (201, 204)
            assert resp.headers.get("X-PXKV-Namespace") == "tenant-a"

        req_b = urllib.request.Request(
            f"{base}/kv/shared",
            data=json.dumps({"tenant": "b"}).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": "Bearer wb", "X-PXKV-Namespace": "tenant-b"},
            method="PUT",
        )
        with urllib.request.urlopen(req_b, timeout=2.0) as resp:
            assert resp.status in (201, 204)
            assert resp.headers.get("X-PXKV-Namespace") == "tenant-b"

        get_a = urllib.request.Request(
            f"{base}/kv/shared",
            headers={"Authorization": "Bearer ra", "X-PXKV-Namespace": "tenant-a"},
        )
        with urllib.request.urlopen(get_a, timeout=2.0) as resp:
            assert resp.status == 200
            body_a = json.loads(resp.read().decode("utf-8"))
        assert body_a["value"] == {"tenant": "a"}

        get_b = urllib.request.Request(
            f"{base}/kv/shared",
            headers={"Authorization": "Bearer rb", "X-PXKV-Namespace": "tenant-b"},
        )
        with urllib.request.urlopen(get_b, timeout=2.0) as resp:
            assert resp.status == 200
            body_b = json.loads(resp.read().decode("utf-8"))
        assert body_b["value"] == {"tenant": "b"}

        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    f"{base}/kv/shared",
                    headers={"Authorization": "Bearer ra", "X-PXKV-Namespace": "tenant-b"},
                ),
                timeout=2.0,
            )
            raise AssertionError("expected auth failure")
        except urllib.error.HTTPError as e:
            assert e.code == 401

        scan_req = urllib.request.Request(
            f"{base}/kv/scan?prefix=sh",
            headers={"Authorization": "Bearer rb", "X-PXKV-Namespace": "tenant-b"},
        )
        with urllib.request.urlopen(scan_req, timeout=2.0) as resp:
            assert resp.status == 200
            scan_body = json.loads(resp.read().decode("utf-8"))
        assert scan_body["keys"] == ["shared"]

        try:
            urllib.request.urlopen(get_a, timeout=2.0)
            raise AssertionError("expected namespace rate limit")
        except urllib.error.HTTPError as e:
            assert e.code == 429

        with urllib.request.urlopen(get_b, timeout=2.0) as resp:
            assert resp.status == 200
    finally:
        stop_proc(proc)
