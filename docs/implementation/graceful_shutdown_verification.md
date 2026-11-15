# Verification: graceful target shutdown

This note records how the shutdown fix was verified and what was not verified.

## Root cause

`Target._handle_termination` ran a full `drain_all()` inside the signal handler and
then exited `0`. When a `process_batch` was already in flight:

- the handler drained the same sinks a second time, so the same sink was submitted an
  empty batch again (a duplicate `process_batch` with an empty context);
- `clean_up()` ran while the original `process_batch` was still executing;
- the latest `STATE` was written even though no batch had committed yet;
- exit status was `0` regardless of the outcome.

The reproduction (two `BatchSink`s, one buffered record each, `STATE {"bookmark":1}`,
`max_parallelism=2`, both `process_batch` calls blocked before committing, then
`SIGTERM`) produced exactly this. Running the tests below against the unmodified
source reproduces the failure (11 of 15 scenarios fail).

## Fix summary

- The signal handler only records the request; it performs no sink work and no
  logging. If the target is blocked reading input, an internal
  `TerminationRequested` unwinds to the input boundary.
- A single, idempotent graceful shutdown awaits in-flight batches, never resubmits
  them, commits every remaining batch (including sinks retired by a `SCHEMA`
  change), commits every sink the target created, then cleans each sink up exactly
  once.
- `STATE` is written only after all commits succeed. A commit failure preserves its
  diagnostic, exits non-zero, and blocks any covering `STATE`. Cleanup is still
  attempted and can never hide the commit error; a cleanup-only failure exits
  non-zero after the state is written.
- Repeated signals during shutdown are ignored.

## Reproduce the verification

```bash
# from the repository root, with the project dependencies installed
uv run --python 3.12 python -m pytest \
    tests/core/test_target_shutdown.py -q -p no:randomly
```

The scenarios are:

| Test | Scenario | RECORD/BATCH |
|---|---|---|
| `test_two_blocked_sinks_sigterm_waits_for_commits_and_state` | Two blocked sinks, then SIGTERM | RECORD |
| `test_one_commit_fails_keeps_diagnostics_nonzero_and_no_covering_state` | One commit succeeds, one fails | RECORD |
| `test_old_and_current_schema_sinks_both_must_finish` | Old-schema sink + current sink coexist | RECORD |
| `test_repeated_signals_do_not_duplicate_commit_state_or_cleanup` | Repeated SIGTERM/SIGINT | RECORD |
| `test_sigterm_while_idle_waiting_for_input_exits_zero` | Signal while blocked on stdin | none |
| `test_record_input_final_commit_state_and_exit` | Normal RECORD stream, no signal | RECORD |
| `test_batch_input_final_commit_state_and_exit` | Normal JSONL/gzip BATCH stream | BATCH |
| `test_batch_input_sigterm_during_in_flight_commit` | Signal during a BATCH-triggered commit | BATCH |
| `test_sigterm_while_only_a_few_sinks_are_blocked_waits_for_all` | Some sinks blocked, one pending | RECORD |
| `test_no_new_input_is_accepted_after_shutdown_begins` | Records after a blocked drain are not read | RECORD |
| `test_default_parallelism_sigterm_waits_for_all_blocked_sinks` | Default parallelism (8) | RECORD |
| `test_max_parallelism_one_sigterm_waits_for_the_in_flight_commit` | `max_parallelism=1` | RECORD |
| `test_sigterm_during_cleanup_does_not_duplicate_cleanup_or_exit_early` | Signal during cleanup | RECORD |
| `test_cleanup_failure_alone_exits_nonzero_after_commit_and_state` | Cleanup-only failure | RECORD |
| `test_commit_and_cleanup_failure_keeps_commit_diagnostic` | Commit + cleanup failure | RECORD |

Each test starts a real subprocess, sends real OS signals with `os.kill`, blocks the
sink with a file-system barrier released by the test, and checks the event log
(enter/commit/fail/cleanup/state order), stdout and the exit code. Every wait is
bounded (no `sleep`-based race guessing).

## Regression suites

```bash
uv run --python 3.12 python -m pytest tests/core -q -p no:randomly
# 677 passed, 1 xfailed

uv run --python 3.12 python -m pytest \
    tests/core/test_target_base.py tests/core/test_batch_target.py \
    tests/core/test_target_shutdown.py tests/core/sinks \
    tests/core/test_target_class.py -q -p no:randomly
# 77 passed

uv run --python 3.12 python -m pytest tests/packages -q -p no:randomly -m packages
# 185 passed, 2 skipped, 3 xfailed, 1 failed
```

The single package failure, `test_sqlite_process_batch_parquet`, also fails on the
unmodified source (verified by stashing the change); it is unrelated to shutdown.

## Actual results

- New shutdown suite: 15 passed.
- Core suite: 677 passed, 1 xfailed.
- Target/sink/batch slice: 77 passed.
- Package suite: 185 passed, 2 skipped, 3 xfailed, 1 pre-existing failure.
- `mypy --python-version 3.12 singer_sdk`: same 3 pre-existing `sinks/core.py`
  errors as the unmodified source; the changed modules type-check cleanly.
- `ruff check` and `ruff format --check` on all changed files: clean.
- Docs build (`sphinx -b html docs`): succeeds.
- CLI exit codes verified directly: a successful sync exits `0` and prints the final
  `STATE`; a failing commit exits `1`, prints no `STATE`, and still runs cleanup.

## Not verified

- External database rollback after a partially-applied `process_batch`, and
  cross-process exactly-once delivery. These are explicitly outside the interface
  promise and remain the sink/connector's responsibility.
- Windows signal semantics (`SIGTERM`/`SIGINT` delivery). The tests are skipped on
  Windows; only the POSIX path is exercised.
- Python 3.10/3.11/3.13/3.14. Verification used Python 3.12; the code uses no
  version-specific constructs beyond the existing `from __future__ import
  annotations` style already used throughout the SDK.
- Third-party sinks that perform blocking work outside `process_batch` /
  `clean_up` (for example inside `process_record`); the SDK can only serialize the
  documented extension points.

## Known follow-up

`joblib` is no longer imported by the SDK (parallel draining now uses
`concurrent.futures.ThreadPoolExecutor` so the main thread stays responsive to
signals). The dependency is still declared in `pyproject.toml`; removing it is a
separate packaging change and was left untouched.
