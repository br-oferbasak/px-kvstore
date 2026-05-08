#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Lightweight OpenTelemetry integration for PX-KVStore.

Provides trace spans for API requests and replication operations.
All functions degrade to no-ops when ``opentelemetry`` is not installed
or when ``settings.TRACING_ENABLED`` is false, so the rest of the codebase
can call into this module unconditionally.
"""

import contextlib
import logging
from typing import Any, Dict, Iterator, Optional

from ..config.settings import settings


def _otel_importable() -> bool:
    try:
        import opentelemetry  # noqa: F401
        return True
    except ImportError:
        return False


_OTEL_AVAILABLE = _otel_importable()
_tracer: Any = None


def init_tracing() -> bool:
    """Initialize the global tracer based on current settings.

    Returns True iff a real OpenTelemetry tracer was installed.
    """
    global _tracer
    if not getattr(settings, "TRACING_ENABLED", False):
        _tracer = None
        return False
    if not _OTEL_AVAILABLE:
        logging.info("PXKV_TRACING_ENABLED=true but opentelemetry is not installed; tracing disabled")
        _tracer = None
        return False
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        service_name = getattr(settings, "TRACING_SERVICE_NAME", "pxkv") or "pxkv"
        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)

        exporter = (getattr(settings, "TRACING_EXPORTER", "console") or "console").lower()
        if exporter == "otlp":
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
                endpoint = getattr(settings, "TRACING_OTLP_ENDPOINT", "") or None
                otlp_exporter = OTLPSpanExporter(endpoint=endpoint) if endpoint else OTLPSpanExporter()
                provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            except ImportError:
                logging.warning("OTLP exporter requested but exporter package missing; falling back to console")
                provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        elif exporter == "console":
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        elif exporter != "none":
            logging.warning("Unknown PXKV_TRACING_EXPORTER=%s; spans will not be exported", exporter)

        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer("pxkv", "0.1.0")
        logging.info("Tracing initialized: service=%s exporter=%s", service_name, exporter)
        return True
    except Exception as e:
        logging.warning("Failed to initialize tracing: %s", e)
        _tracer = None
        return False


def is_enabled() -> bool:
    return _tracer is not None


@contextlib.contextmanager
def start_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
    kind: str = "internal",
) -> Iterator[Any]:
    """Context manager that starts a span. Yields None when tracing is disabled."""
    if _tracer is None:
        yield None
        return
    from opentelemetry import trace as _trace
    span_kinds = {
        "server": _trace.SpanKind.SERVER,
        "client": _trace.SpanKind.CLIENT,
        "producer": _trace.SpanKind.PRODUCER,
        "consumer": _trace.SpanKind.CONSUMER,
        "internal": _trace.SpanKind.INTERNAL,
    }
    span_kind = span_kinds.get(kind, _trace.SpanKind.INTERNAL)
    with _tracer.start_as_current_span(name, kind=span_kind) as span:
        if attributes:
            for k, v in attributes.items():
                if v is None:
                    continue
                try:
                    span.set_attribute(k, v)
                except Exception:
                    pass
        try:
            yield span
        except Exception as e:
            try:
                span.record_exception(e)
                from opentelemetry.trace import Status, StatusCode
                span.set_status(Status(StatusCode.ERROR, str(e)))
            except Exception:
                pass
            raise


def set_attribute(span: Any, key: str, value: Any) -> None:
    if span is None or value is None:
        return
    try:
        span.set_attribute(key, value)
    except Exception:
        pass


def set_status_error(span: Any, description: str = "") -> None:
    if span is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode
        span.set_status(Status(StatusCode.ERROR, description))
    except Exception:
        pass


def add_event(span: Any, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
    if span is None:
        return
    try:
        span.add_event(name, attributes=attributes or {})
    except Exception:
        pass


def inject_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Inject the current trace context into an outgoing-headers dict (W3C traceparent etc.)."""
    if _tracer is None:
        return headers
    try:
        from opentelemetry.propagate import inject
        inject(headers)
    except Exception:
        pass
    return headers


def _carrier_from(headers: Any) -> Dict[str, str]:
    carrier: Dict[str, str] = {}
    if headers is None:
        return carrier
    try:
        if hasattr(headers, "items"):
            for k, v in headers.items():
                if v is None:
                    continue
                carrier[str(k).lower()] = str(v)
        elif isinstance(headers, dict):
            for k, v in headers.items():
                carrier[str(k).lower()] = str(v)
    except Exception:
        pass
    return carrier


@contextlib.contextmanager
def extract_context(headers: Any) -> Iterator[None]:
    """Extract a trace context from incoming HTTP headers and activate it for the block."""
    if _tracer is None:
        yield
        return
    try:
        from opentelemetry import context as otel_context
        from opentelemetry.propagate import extract
        ctx = extract(_carrier_from(headers))
        token = otel_context.attach(ctx)
        try:
            yield
        finally:
            try:
                otel_context.detach(token)
            except Exception:
                pass
    except Exception:
        yield
