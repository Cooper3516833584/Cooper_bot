# Testing Guide

This project uses `pytest` with a unified launcher at the repository root:

- `test_client.py`

All test Python files (except `test_client.py`) live under `test/`.

## 1) Test Structure

```text
project/
├─ test_client.py
├─ pytest.ini
├─ requirements-dev.txt
└─ test/
   ├─ conftest.py
   ├─ helpers/
   ├─ unit/
   ├─ integration/
   └─ smoke/
```

- `unit/`: fast behavior-level checks for isolated logic.
- `integration/`: cross-module and persistence flow checks.
- `smoke/`: critical startup and route sanity checks.

## 2) Install Dev Dependencies

```bash
pip install -r requirements-dev.txt
```

At minimum this includes `pytest` and `pytest-asyncio`.

## 3) How To Run Tests

Unified entry (recommended):

```bash
python test_client.py
python test_client.py unit
python test_client.py integration
python test_client.py smoke
```

Direct pytest also works:

```bash
pytest
```

`test_client.py` prints:

- current scope
- target path
- pytest return code
- whether all tests passed

## 4) Fake AI / Fake Event Design Principles

Test infrastructure is centralized in:

- `test/conftest.py`
- `test/helpers/fake_ai.py`
- `test/helpers/fake_events.py`
- `test/helpers/sample_builders.py`
- `test/helpers/time_tools.py`

Principles:

- default to fake clients/events/time
- deterministic, reproducible test behavior
- temporary directories only
- no real service/network dependency in default tests

## 5) Why External APIs Are Disabled By Default

To keep tests stable, fast, and safe:

- no real AI API calls
- no real QQ runtime dependency
- no real OCR service dependency
- no writes to production `data/`

This avoids flaky tests and protects production data.

## 6) How To Add Tests For New Features

Rules:

1. For each complex new feature, add at least 1 matching test.
2. For each bug fix, add at least 1 regression test that reproduces the bug.
3. Keep tests small and focused; prefer unit tests first.
4. Use `test/helpers/` and shared fixtures instead of per-file ad-hoc setup.

## 7) Phased Rollout Plan

### Phase 1 (Required, completed baseline)

- test framework and directory layout
- shared `conftest` and helper fakes
- `/find` tests
- `commands` routing tests
- `aichat` context tests
- AI material incremental sync tests
- `handin` core tests
- smoke tests

### Phase 2 (Optional enhancements)

- deeper `logsvc` tests
- group notice / OCR tests
- more admin command coverage
- finer-grained failure-recovery tests

## 8) Acceptance Checklist

1. Root has `test_client.py`.
2. Test Python files are under `test/` (except `test_client.py`).
3. These commands are executable:
   - `python test_client.py`
   - `python test_client.py unit`
   - `python test_client.py integration`
   - `python test_client.py smoke`
4. High-value coverage exists for `/find`, command routing, AI context/sync, handin, smoke.
5. Tests do not depend on real AI/QQ/OCR services.
6. Production startup and production data directories are not impacted.

## 9) Codex Regression Workflow

When Codex makes medium/large changes:

1. Run `python test_client.py unit`.
2. Run `python test_client.py integration`.
3. Before release or startup refactors, run `python test_client.py smoke`.
4. For risky changes, run full suite: `python test_client.py`.
