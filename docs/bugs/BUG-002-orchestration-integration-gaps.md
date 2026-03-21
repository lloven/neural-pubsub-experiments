# BUG-002: Orchestration harness integration gaps

**Status:** Fixed (002a, 002b fixed; 002c already correct; 002d inherent limitation documented)
**Severity:** Medium (monitor unusable for remote, bash runner unreliable)
**Discovered:** 2026-03-21, during Phase B deployment on 5GTNF VM
**Fixed:** 2026-03-21, 9 TDD tests added (168 total tests pass)

## Summary

The orchestration harness (monitor, bash runner, smoke tests) had multiple integration bugs at the SSH/remote boundary. Unit tests covered local components well but missed cross-system interactions.

## Bug list

### BUG-002a: Monitor timezone/elapsed mismatch (remote) — FIXED

**Symptom:** Monitor shows "7h01m elapsed" for a run that started 3 minutes ago.

**Root cause:** `_update_progress()` in `_common.py` used `datetime.now().isoformat()` which writes naive (no timezone) timestamps. When the VM (EET) writes progress.json and the Mac (JST) reads it, `datetime.fromisoformat()` interprets the timestamp as local time, producing a multi-hour offset.

**Fix:** Changed `datetime.now().isoformat()` to `datetime.now(timezone.utc).isoformat()` in `_common.py`. Timestamps now include `+00:00` suffix, making them unambiguous.

**Tests added:**
- `test_progress_timestamps_are_utc` — timestamp has tzinfo and is UTC
- `test_progress_timestamps_roundtrip_accurately` — UTC timestamp is accurate
- `test_progress_file_contains_utc_timestamps` — persisted file has `+00:00`

### BUG-002b: Bash runner tmux indirection — FIXED

**Symptom:** `./run-experiments.sh --remote phase-b --resume` creates a tmux session that re-runs the bash script instead of calling Python directly.

**Root cause:** `maybe_tmux_wrap()` received `$0 ... phase-b ...` as the tmux command, causing bash re-entry with fragile venv/CWD assumptions.

**Fix:** Each phase now builds a `PY_CMD` variable (`python3 -m scripts.run_phase_b --resume`) and passes that directly to `maybe_tmux_wrap()`. No bash re-entry. The tmux session on the remote runs Python directly.

**Tests added:**
- `test_remote_tmux_calls_python_directly` — verifies no `$0` in tmux commands

### BUG-002c: Smoke test `--phases` multi-arg parsing — NOT A BUG

**Original symptom:** `--phases A B` only runs Phase A.

**Investigation:** The argparse definition already uses `nargs="*"`, which correctly parses `--phases A B` as `["A", "B"]`. The apparent issue was that only "stack", "A", and "figures" are implemented as smoke test phases; "B" simply has no handler (not a parsing bug).

**Tests added:**
- `test_smoke_phases_flag_accepts_multiple` — confirms parsing works
- `test_smoke_phases_default_includes_all` — confirms default set

### BUG-002d: Monitor CSV fallback shows wrong total — INHERENT LIMITATION

**Symptom:** Monitor shows "1/1 runs" when there are 20 runs planned.

**Root cause:** `_discover_progress_from_csvs()` can only count existing CSV files. It has no way to know the planned matrix size. This is only triggered when `.progress.json` is missing (e.g., if the bash runner was used instead of the Python orchestrator).

**Resolution:** The Python orchestrator already writes `.progress.json` with all planned runs. Since we've standardised on Python orchestration (BUG-002b fix ensures tmux calls Python directly), this is now the primary path. The CSV fallback remains as a safety net for legacy runs.

**Tests added:**
- `test_monitor_total_from_progress_json` — 20 entries load correctly
- `test_csv_fallback_does_not_know_total` — documents limitation
- `test_monitor_elapsed_uses_utc_timestamps` — UTC timestamps round-trip

## Test coverage (updated)

| Component | Unit tests | Integration tests | Gap |
|-----------|-----------|------------------|-----|
| Config resolution | ✅ 17 tests | ❌ None | Compose overlay merging not tested with Docker |
| Phase B matrix | ✅ 5 tests | ❌ None | Docker Compose execution not tested |
| Monitor (local) | ✅ 8 tests | ❌ None | - |
| Monitor (remote) | ❌ None | ❌ None | SSH-based reading untested |
| Bash dispatcher | ✅ 4 tests | ❌ None | Remote SSH path untested |
| BUG-001 drain | ✅ 5 tests | ❌ None | Real Docker overload not reproduced |
| BUG-002 integration | ✅ 9 tests | ❌ None | - |
| Workload generator | ✅ 5 tests | ❌ None | - |
| Broker/placement | ✅ 123 tests | ❌ None | Core logic well-tested |

**Total: 168 unit tests, 0 integration tests for remote path.**

## Files changed

| File | Change |
|------|--------|
| `scripts/_common.py` | 002a: `datetime.now(timezone.utc)` replaces `datetime.now()` |
| `run-experiments.sh` | 002b: `PY_CMD` passed to `maybe_tmux_wrap()` instead of `$0` |
| `tests/test_orchestration.py` | 9 new tests (3 for 002a, 1 for 002b, 2 for 002c, 3 for 002d) |
