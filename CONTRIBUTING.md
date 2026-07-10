# Contributing to PX-KVStore

Thank you for considering a contribution to PX-KVStore.

PX-KVStore is a lightweight key-value storage engine focused on low-latency access, deterministic LLM caching, durability, observability, and distributed-systems experimentation.

## Ways to contribute

You can help by:

- reporting reproducible bugs;
- improving tests and documentation;
- reproducing benchmarks on different systems;
- fixing protocol compatibility gaps;
- improving WAL, snapshot, replication, or concurrency behavior;
- proposing integrations and realistic usage examples.

## Development setup

```bash
git clone https://github.com/cchenax/px-kvstore.git
cd px-kvstore
python -m venv .venv
source .venv/bin/activate
pip install -e .
pytest
```

## Before opening an issue

Please:

1. Search existing issues.
2. Test against the latest `main` branch.
3. Include your operating system, Python version, configuration, logs, and exact reproduction steps.
4. Remove secrets and private data.

## Pull request workflow

1. Fork the repository.
2. Create a focused branch:
   ```bash
   git checkout -b fix/wal-recovery-edge-case
   ```
3. Add or update tests.
4. Run:
   ```bash
   pytest
   ```
5. Keep commits focused and explain the design trade-offs in the pull request.
6. Link the relevant issue when one exists.

## Pull request expectations

A strong pull request should include:

- a clear problem statement;
- a minimal, reviewable change;
- tests covering the new behavior;
- documentation updates where needed;
- benchmark data for performance-sensitive changes;
- no unrelated formatting or refactoring.

## Benchmark contributions

Benchmark reports should include:

- CPU and memory;
- operating system;
- Python version;
- shard count;
- client concurrency;
- command used;
- warm-up procedure;
- raw output or reproducible script.

## Code of conduct

Be respectful, technical, and constructive. Harassment, personal attacks, and bad-faith participation are not accepted.
