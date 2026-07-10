# PX-KVStore Roadmap

This roadmap describes intended areas of development. It is not a delivery commitment.

## Reliability

- Expand WAL corruption and partial-write recovery tests.
- Add snapshot compatibility and migration tests.
- Improve graceful shutdown and recovery diagnostics.
- Add replication retry, backoff, and follower health reporting.

## Protocol compatibility

- Expand Redis command coverage.
- Add protocol conformance tests.
- Document command-level compatibility and known differences.

## Performance

- Add reproducible multi-platform benchmark scripts.
- Compare representative workloads with Redis and embedded KV stores.
- Add sustained-load and tail-latency benchmarks.
- Add memory-usage reporting.

## Operability

- Add structured configuration validation.
- Improve Prometheus metrics and alerting examples.
- Add container health checks.
- Publish deployment and troubleshooting guides.

## Community

- Label beginner-friendly issues.
- Add architecture decision records.
- Publish benchmark reproduction reports.
- Encourage external integrations and usage reports.
