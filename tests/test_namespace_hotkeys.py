from pxkv.config.settings import settings
from pxkv.core.hotkey import HotKeyDetector
from pxkv.metrics.prometheus import registry_to_prometheus
from pxkv.metrics.registry import MetricsRegistry
from pxkv.namespaces import namespace_manager


def test_namespace_hot_key_reports_use_scoped_keys_and_thresholds():
    old_enabled = settings.NAMESPACE_ENABLED
    old_default = settings.NAMESPACE_DEFAULT
    old_configs = settings.NAMESPACE_CONFIGS
    try:
        settings.NAMESPACE_ENABLED = True
        settings.NAMESPACE_DEFAULT = "tenant-a"
        settings.NAMESPACE_CONFIGS = {
            "tenant-a": {"hot_keys": {"top_k": 1, "threshold_qps": 0.05}},
            "tenant-b": {"hot_keys": {"top_k": 2, "threshold_qps": 10.0}},
        }

        detector = HotKeyDetector(enabled=True, window_seconds=60.0, buckets=2, top_k=5)
        key_a_hot = namespace_manager.key("tenant-a", "shared")
        key_a_warm = namespace_manager.key("tenant-a", "warm")
        key_b_hot = namespace_manager.key("tenant-b", "shared")
        for _ in range(5):
            detector.record(key_a_hot)
        for _ in range(3):
            detector.record(key_a_warm)
        for _ in range(5):
            detector.record(key_b_hot)

        report_a = detector.report(
            limit=namespace_manager.hot_key_top_k("tenant-a"),
            threshold_qps=namespace_manager.hot_key_threshold_qps("tenant-a"),
            key_filter=lambda k: namespace_manager.belongs("tenant-a", k),
            key_mapper=lambda k: namespace_manager.strip("tenant-a", k),
            namespace="tenant-a",
        )
        report_b = detector.report(
            limit=namespace_manager.hot_key_top_k("tenant-b"),
            threshold_qps=namespace_manager.hot_key_threshold_qps("tenant-b"),
            key_filter=lambda k: namespace_manager.belongs("tenant-b", k),
            key_mapper=lambda k: namespace_manager.strip("tenant-b", k),
            namespace="tenant-b",
        )

        assert report_a["namespace"] == "tenant-a"
        assert report_a["top_k"] == 1
        assert report_a["tracked_keys"] == 2
        assert report_a["top"] == [{"key": "shared", "count": 5, "qps": 5 / 60.0, "hot": True}]

        assert report_b["namespace"] == "tenant-b"
        assert report_b["top_k"] == 2
        assert report_b["tracked_keys"] == 1
        assert report_b["top"][0]["key"] == "shared"
        assert report_b["top"][0]["hot"] is False
    finally:
        settings.NAMESPACE_ENABLED = old_enabled
        settings.NAMESPACE_DEFAULT = old_default
        settings.NAMESPACE_CONFIGS = old_configs


def test_namespace_hot_key_reports_export_prometheus_labels():
    registry = MetricsRegistry()
    registry.observe_namespace_hot_keys(
        {
            "enabled": True,
            "namespace_count": 1,
            "namespaces": [
                {
                    "namespace": "tenant-a",
                    "tracked_keys": 1,
                    "hot_keys_current": 1,
                    "top": [{"key": "shared", "qps": 2.5, "hot": True}],
                }
            ],
        }
    )

    text = registry_to_prometheus(registry.get_all())
    assert 'pxkv_namespace_hot_keys_tracked{namespace="tenant-a"} 1' in text
    assert 'pxkv_namespace_hot_key_qps{namespace="tenant-a",key="shared"} 2.5' in text
