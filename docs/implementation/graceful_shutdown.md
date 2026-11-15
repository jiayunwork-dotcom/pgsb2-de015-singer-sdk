# Graceful Target Shutdown

A target must never acknowledge progress it has not committed. When the process
receives `SIGTERM` or `SIGINT`, the Singer SDK target base class shuts down
gracefully instead of abandoning in-flight batches or confirming uncommitted data.

This page documents the ordering guarantees the SDK provides and what a Sink
implementation can rely on. These guarantees are about *this process's* commit and
progress-acknowledgement order. They do not promise external-database rollback or
cross-process exactly-once delivery.

## What happens on a signal

When a target receives `SIGTERM` or `SIGINT`:

1. **New input stops.** After the current line (or, for a `BATCH`, the current
   record within the expanded manifest) the target stops reading. No further
   `RECORD`, `BATCH`, `SCHEMA`, `STATE`, or `ACTIVATE_VERSION` messages are
   accepted.
2. **In-flight batches are awaited.** Any `process_batch` call that has already
   started is allowed to finish. An in-flight batch is never submitted a second
   time, and a sink is never cleaned up while one of its calls is still running.
3. **Every remaining batch is committed.** Buffered records — including those
   buffered by a sink retired after a `SCHEMA` change — are committed. Every sink
   the target created participates in the "all writes finished" decision, so
   success is global, never per-stream.
4. **Cleanup runs once per sink.** Every sink is cleaned up at most once, after its
   own calls have returned. Sinks retired by a schema change and current sinks are
   both cleaned up. A sink whose commit failed is still cleaned up.
5. **State is acknowledged only after all commits succeed.** The latest `STATE`
   payload is written to stdout only when every commit triggered by the shutdown
   succeeded. If any commit failed, no `STATE` that covers the failed write is
   emitted; progress that was already acknowledged before the failure is not
   withdrawn.
6. **The exit status reflects the outcome.** `0` when every commit and every
   cleanup succeeded. Non-zero when a commit failed, or when a cleanup failed even
   if all commits succeeded.

Repeated `SIGTERM`/`SIGINT` during shutdown are ignored. They cannot double-submit
a batch, acknowledge progress twice, or run cleanup again.

```{note}
A signal that arrives while the target is blocked reading standard input still
triggers the full shutdown. The signal handler records the request and the read is
unwound, so shutdown runs on the normal call stack rather than inside the handler.
The handler itself performs no sink work and no logging, which keeps it safe to run
at any point.
```

## Normal operation is unchanged

The shutdown machinery does not change how batches are triggered during a normal
sync. Batches still drain when:

- a sink reaches its batch size (`batch_size_rows`, or the sink's `max_size`),
- records exceed the maximum record age (default 5 minutes), and
- the input reaches end-of-file (`EOF`).

A normal end-of-file drain still commits every sink and then writes the final
`STATE`, exactly as before. That path and the signal path share the same drain,
commit, cleanup, and acknowledgement ordering.

## Commit failures

If a batch fails to commit:

- the original commit diagnostic is preserved on stderr and the process exits
  non-zero;
- every other sink is still attempted, so one failed stream does not hide progress
  or cleanup opportunities in another stream;
- cleanup is still attempted for every sink, and a cleanup error never replaces the
  commit error;
- no `STATE` is emitted that would cover the failed write. `STATE` messages
  acknowledged *before* the failure remain valid.

A cleanup-only failure (every commit succeeded) still emits the final `STATE`, then
exits non-zero.

## Sink compatibility

No new close call is required from Sink authors. `process_record`, `process_batch`,
and `clean_up` keep their signatures. Sinks remain responsible only for their own
`process_record` / `process_batch` / `clean_up` behavior.

## Scope

The SDK guarantees the ordering described above within a single target process. It
does not guarantee that an external database rolls back a partially applied write
after a `process_batch` failure, nor does it provide cross-process exactly-once
delivery. Those remain the Sink implementation's responsibility.
