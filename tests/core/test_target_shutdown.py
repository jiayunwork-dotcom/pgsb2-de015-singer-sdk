"""End-to-end shutdown tests using real processes and real OS signals.

These tests run ``shutdown_program.py`` in a subprocess, feed it Singer messages on
standard input, send real ``SIGTERM``/``SIGINT`` signals to the process, and inspect
an append-only event log plus the process's stdout and exit status.

Timing is controlled with file-system barriers: a controlled sink logs that it
entered ``process_batch`` (or ``clean_up``) and then blocks until the test creates a
release file. No test relies on ``sleep`` to guess when a signal should land, and
every wait is bounded.

Run with::

    uv run --python 3.12 python -m pytest tests/core/test_target_shutdown.py
"""

from __future__ import annotations

import json
import os
import signal
import subprocess  # noqa: S404
import sys
import time
import typing as t
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="POSIX signals are required",
)

_PROGRAM = Path(__file__).with_name("shutdown_program.py")
_BARRIER_TIMEOUT = 60.0
_TEST_TIMEOUT = 120.0

#: The SDK's default ``Target.max_parallelism``.
_MAX_DEFAULT_PARALLELISM = 8

SCHEMA_A = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
    },
}
SCHEMA_B = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "label": {"type": "string"},
    },
}

STATE = {"bookmark": 1}


class TargetProcess:
    """A running target subprocess plus its observable artifacts."""

    def __init__(self, tmp_path: Path) -> None:
        """Create the process wrapper.

        Args:
            tmp_path: A per-test temporary directory.
        """
        self.tmp_path = tmp_path
        self.barrier_dir = tmp_path / "barriers"
        self.barrier_dir.mkdir()
        self.event_log = tmp_path / "events.jsonl"
        self.stdout_path = tmp_path / "stdout.txt"
        self.stderr_path = tmp_path / "stderr.txt"
        self.proc: subprocess.Popen[bytes] | None = None
        self._stdout_file: t.IO[bytes] | None = None
        self._stderr_file: t.IO[bytes] | None = None

    def start(
        self,
        messages: list[dict],
        *,
        config: dict | None = None,
        max_parallelism: int | None = 2,
        wait_in_batch: bool = False,
        wait_in_cleanup: bool = False,
        fail_streams: set[str] | None = None,
        cleanup_fail_streams: set[str] | None = None,
    ) -> None:
        """Start the target and feed it the messages.

        Args:
            messages: Singer messages to write to standard input.
            config: Target config.
            max_parallelism: The target's ``max_parallelism``. ``None`` leaves the
                SDK default in place.
            wait_in_batch: Whether ``process_batch`` waits for a test release.
            wait_in_cleanup: Whether ``clean_up`` waits for a test release.
            fail_streams: Streams whose commit should fail.
            cleanup_fail_streams: Streams whose cleanup should fail.
        """
        env = os.environ.copy()
        env.update(
            {
                "SS_EVENT_LOG": str(self.event_log),
                "SS_BARRIER_DIR": str(self.barrier_dir),
                "SS_BARRIER_TIMEOUT": str(_BARRIER_TIMEOUT),
                "SS_MAX_PARALLELISM": (
                    "" if max_parallelism is None else str(max_parallelism)
                ),
                "SS_FAULTHANDLER": os.environ.get("SS_FAULTHANDLER", "0"),
                "SS_CONFIG": json.dumps(config or {}),
                "SS_WAIT_IN_BATCH": "1" if wait_in_batch else "0",
                "SS_WAIT_IN_CLEANUP": "1" if wait_in_cleanup else "0",
                "SS_FAIL_STREAMS": ",".join(sorted(fail_streams or set())),
                "SS_CLEANUP_FAIL_STREAMS": ",".join(
                    sorted(cleanup_fail_streams or set()),
                ),
            },
        )
        payload = "".join(json.dumps(message) + "\n" for message in messages).encode()

        self._stdout_file = self.stdout_path.open("wb")
        self._stderr_file = self.stderr_path.open("wb")
        self.proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-I", str(_PROGRAM)],
            stdin=subprocess.PIPE,
            stdout=self._stdout_file,
            stderr=self._stderr_file,
            env=env,
            cwd=tmp_working_directory(self.tmp_path),
        )
        assert self.proc.stdin is not None
        self.proc.stdin.write(payload)
        self.proc.stdin.flush()
        self.wait_for_event(name="ready")

    # Observation helpers

    def events(self) -> list[dict]:
        """Read every event logged so far.

        Returns:
            The event payloads in append order.
        """
        if not self.event_log.exists():
            return []
        return [
            json.loads(line)
            for line in self.event_log.read_text().splitlines()
            if line.strip()
        ]

    def event_names(self) -> list[str]:
        """Return the event names in order.

        Returns:
            The event names.
        """
        return [event["event"] for event in self.events()]

    def wait_for_event(
        self,
        *,
        name: str,
        sink: str | None = None,
        timeout: float = _TEST_TIMEOUT,
        count: int = 1,
    ) -> list[dict]:
        """Wait until an event has been logged at least ``count`` times.

        Args:
            name: The event name.
            sink: Optional sink label to match.
            timeout: Maximum seconds to wait.
            count: Minimum number of matches required.

        Returns:
            The matching events.

        Raises:
            AssertionError: If the event does not appear in time.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            matches = [
                event
                for event in self.events()
                if event["event"] == name
                and (sink is None or event.get("sink") == sink)
            ]
            if len(matches) >= count:
                return matches
            self._raise_if_exited_unexpectedly(name)
            time.sleep(0.01)
        details = f"events={self.event_names()}; stderr={self.stderr_text()!r}"
        msg = (
            f"event {name!r} for sink {sink!r} did not appear within "
            f"{timeout}s; {details}"
        )
        raise AssertionError(msg)

    def wait_for_line_read(self, line: str, *, timeout: float = _TEST_TIMEOUT) -> None:
        """Wait until a given input line is observed as read.

        Args:
            line: The literal line to wait for.
            timeout: Maximum seconds to wait.

        Raises:
            AssertionError: If the line is not read in time.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(
                event["event"] == "read" and event["line"] == line
                for event in self.events()
            ):
                return
            self._raise_if_exited_unexpectedly(f"read of {line!r}")
            time.sleep(0.01)
        msg = f"line {line!r} was not read within {timeout}s"
        raise AssertionError(msg)

    def _raise_if_exited_unexpectedly(self, waiting_for: str) -> None:
        if self.proc is not None and self.proc.poll() is not None:
            msg = (
                f"target exited while waiting for {waiting_for} "
                f"(returncode={self.proc.returncode}); "
                f"stderr={self.stderr_text()!r}"
            )
            raise AssertionError(msg)

    def release(self, kind: str, stream: str) -> None:
        """Release a named barrier.

        Args:
            kind: Barrier kind, e.g. ``batch`` or ``cleanup``.
            stream: The stream name the barrier belongs to.
        """
        (self.barrier_dir / f"release-{kind}-{stream}").touch()

    def signal(self, signum: int) -> None:
        """Send a signal to the target process.

        Args:
            signum: The signal number.
        """
        assert self.proc is not None
        os.kill(self.proc.pid, signum)

    def signal_if_alive(self, signum: int) -> None:
        """Send a signal only if the process has not exited yet.

        Args:
            signum: The signal number.
        """
        assert self.proc is not None
        if self.proc.poll() is None:
            os.kill(self.proc.pid, signum)

    def wait(self, *, timeout: float = _TEST_TIMEOUT) -> int:
        """Wait for the process to exit and return its exit status.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            The process exit status.

        Raises:
            AssertionError: If the process does not exit in time.
        """
        assert self.proc is not None
        try:
            returncode = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            msg = (
                f"target did not exit within {timeout}s; "
                f"events={self.event_names()}; stderr={self.stderr_text()!r}"
            )
            raise AssertionError(msg) from None
        finally:
            self._close_files()
        return returncode

    def close_stdin(self) -> None:
        """Close the target's standard input to signal EOF."""
        assert self.proc is not None
        assert self.proc.stdin is not None
        self.proc.stdin.close()

    def stdout_text(self) -> str:
        """Return captured standard output.

        Returns:
            The decoded standard output.
        """
        self._flush_files()
        return self.stdout_path.read_text()

    def stderr_text(self) -> str:
        """Return captured standard error.

        Returns:
            The decoded standard error.
        """
        self._flush_files()
        return self.stderr_path.read_text()

    def stdout_states(self) -> list[dict]:
        """Return every JSON object written to standard output.

        Returns:
            Parsed standard output JSON lines.
        """
        return [
            json.loads(line) for line in self.stdout_text().splitlines() if line.strip()
        ]

    def _flush_files(self) -> None:
        for handle in (self._stdout_file, self._stderr_file):
            if handle is not None and not handle.closed:
                handle.flush()

    def _close_files(self) -> None:
        for handle in (self._stdout_file, self._stderr_file):
            if handle is not None and not handle.closed:
                handle.close()


def tmp_working_directory(tmp_path: Path) -> str:
    """Return a scratch working directory for the target subprocess.

    Args:
        tmp_path: The per-test temporary directory.

    Returns:
        A path inside the temporary directory.
    """
    cwd = tmp_path / "cwd"
    cwd.mkdir(exist_ok=True)
    return str(cwd)


# Message builders


def schema_message(stream: str, schema: dict) -> dict:
    """Build a SCHEMA message.

    Args:
        stream: The stream name.
        schema: The stream schema.

    Returns:
        The Singer SCHEMA message.
    """
    return {
        "type": "SCHEMA",
        "stream": stream,
        "schema": schema,
        "key_properties": ["id"],
    }


def record_message(stream: str, record: dict) -> dict:
    """Build a RECORD message.

    Args:
        stream: The stream name.
        record: The record payload.

    Returns:
        The Singer RECORD message.
    """
    return {"type": "RECORD", "stream": stream, "record": record}


def state_message(state: dict) -> dict:
    """Build a STATE message.

    Args:
        state: The state value.

    Returns:
        The Singer STATE message.
    """
    return {"type": "STATE", "value": state}


def batch_message(
    stream: str,
    manifest: list[str],
    *,
    compression: str | None = None,
) -> dict:
    """Build a BATCH message.

    Args:
        stream: The stream name.
        manifest: The batch file URLs.
        compression: Optional compression format.

    Returns:
        The Singer BATCH message.
    """
    encoding: dict = {"format": "jsonl"}
    if compression is not None:
        encoding["compression"] = compression
    return {
        "type": "BATCH",
        "stream": stream,
        "encoding": encoding,
        "manifest": manifest,
    }


# Scenario helpers


def _assert_no_state_before_commit(events: list[dict]) -> None:
    """Assert every STATE event follows all commits of the records it covers.

    Args:
        events: The event log.
    """
    commit_count = sum(1 for event in events if event["event"] == "commit")
    state_positions = [
        index for index, event in enumerate(events) if event["event"] == "state"
    ]
    commit_positions = [
        index for index, event in enumerate(events) if event["event"] == "commit"
    ]
    if not state_positions:
        return
    assert commit_count >= 1
    assert max(commit_positions) < min(state_positions)


def _sink_cleanups(events: list[dict]) -> dict[str, int]:
    """Count ``clean_up`` entries per sink.

    Args:
        events: The event log.

    Returns:
        A mapping of sink label to cleanup-entry count.
    """
    counts: dict[str, int] = {}
    for event in events:
        if event["event"] == "cleanup_enter":
            counts[event["sink"]] = counts.get(event["sink"], 0) + 1
    return counts


def _sink_commits(events: list[dict]) -> dict[str, int]:
    """Count batch entries per sink.

    Args:
        events: The event log.

    Returns:
        A mapping of sink label to ``process_batch`` entry count.
    """
    counts: dict[str, int] = {}
    for event in events:
        if event["event"] == "batch_enter":
            counts[event["sink"]] = counts.get(event["sink"], 0) + 1
    return counts


# Tests


def test_two_blocked_sinks_sigterm_waits_for_commits_and_state(
    tmp_path: Path,
) -> None:
    """Two blocked, uncommitted batches: SIGTERM waits, then commits and states once."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            schema_message("beta", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            record_message("beta", {"id": 2, "name": "b2"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        wait_in_batch=True,
        wait_in_cleanup=True,
    )
    target.close_stdin()

    # Wait until both sinks are in process_batch and blocked before committing.
    target.wait_for_event(name="batch_enter", sink="alpha#0")
    target.wait_for_event(name="batch_enter", sink="beta#1")
    assert not any(event["event"] == "commit" for event in target.events())

    initial_state_events = [
        event for event in target.events() if event["event"] == "state"
    ]
    assert initial_state_events == []

    target.signal(signal.SIGTERM)
    target.wait_for_event(name="signal_received")

    # The signal handler runs on the main thread while both batches are blocked, so
    # once it has run the target must still be alive and must not have acknowledged
    # state or started cleanup: both would require the in-flight commits first.
    events_during = target.events()
    assert not any(event["event"] == "state" for event in events_during), (
        "STATE must not be written before the in-flight commits finish"
    )
    assert not any(event["event"] == "cleanup_enter" for event in events_during), (
        "a sink must not be cleaned up while its batch is still in flight"
    )
    assert target.proc is not None
    assert target.proc.poll() is None, (
        "the target must keep waiting for the in-flight batches"
    )

    # Release both batches. Each must commit exactly once, and only then may the
    # target acknowledge state and clean up.
    target.release("batch", "alpha")
    target.release("batch", "beta")
    target.wait_for_event(name="commit", sink="alpha#0")
    target.wait_for_event(name="commit", sink="beta#1")
    target.release("cleanup", "alpha")
    target.release("cleanup", "beta")

    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    _assert_no_state_before_commit(events)

    assert _sink_commits(events) == {"alpha#0": 1, "beta#1": 1}
    assert _sink_cleanups(events) == {"alpha#0": 1, "beta#1": 1}

    committed_records = [
        record
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    ]
    assert sorted(record["id"] for record in committed_records) == [1, 2]

    assert target.stdout_states() == [STATE]

    # The shutdown must not have started a second, empty batch or cleanup.
    assert not any(
        event["event"] == "batch_enter" and not event["records"] for event in events
    )
    assert not any(
        event["event"] == "cleanup_enter"
        and event.get("sink") not in {"alpha#0", "beta#1"}
        for event in events
    )


def test_one_commit_fails_keeps_diagnostics_nonzero_and_no_covering_state(
    tmp_path: Path,
) -> None:
    """A failing commit keeps its diagnostic, exits non-zero, and no state covers it."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            schema_message("beta", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            record_message("beta", {"id": 2, "name": "b2"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        wait_in_batch=True,
        fail_streams={"beta"},
    )
    target.close_stdin()

    target.wait_for_event(name="batch_enter", sink="alpha#0")
    target.wait_for_event(name="batch_enter", sink="beta#1")
    target.signal(signal.SIGTERM)
    target.wait_for_event(name="signal_received")

    target.release("batch", "alpha")
    target.release("batch", "beta")

    target.wait_for_event(name="commit", sink="alpha#0")
    target.wait_for_event(name="commit_fail", sink="beta#1")

    returncode = target.wait()
    assert returncode != 0
    assert "synthetic commit failure for 'beta'" in target.stderr_text()

    events = target.events()
    # No STATE is ever emitted because the state covers the failed beta write.
    assert not any(event["event"] == "state" for event in events)
    assert target.stdout_states() == []
    # Cleanup was still attempted for both sinks even though beta failed.
    assert _sink_cleanups(events) == {"alpha#0": 1, "beta#1": 1}
    # The successful alpha batch committed exactly once and was not retried.
    assert _sink_commits(events)["alpha#0"] == 1
    assert _sink_commits(events)["beta#1"] == 1


def test_old_and_current_schema_sinks_both_must_finish(tmp_path: Path) -> None:
    """A sink retired by a schema change and the current sink are both awaited."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("users", SCHEMA_A),
            record_message("users", {"id": 1, "name": "old"}),
            # A new schema retires the first sink and creates a second one.
            schema_message("users", SCHEMA_B),
            record_message("users", {"id": 2, "label": "new"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        wait_in_batch=True,
    )
    target.close_stdin()

    # Retired sinks drain first, then current sinks, so the old sink's batch is
    # in flight before the current sink's.
    target.wait_for_event(name="batch_enter", sink="users#0")
    target.signal(signal.SIGTERM)
    target.wait_for_event(name="signal_received")
    assert not any(event["event"] == "state" for event in target.events())
    assert not any(event["event"] == "cleanup_enter" for event in target.events()), (
        "no sink may be cleaned up while another sink's batch is in flight"
    )

    # Releasing the retired sink lets the current sink start its in-flight batch.
    target.release("batch", "users")
    target.wait_for_event(name="batch_enter", sink="users#1")
    target.release("batch", "users")

    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    state_events = [event for event in events if event["event"] == "state"]
    assert len(state_events) == 1
    assert target.stdout_states() == [STATE]

    committed_ids = sorted(
        record["id"]
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    )
    assert committed_ids == [1, 2]

    # Both the retired sink and the current sink were cleaned up exactly once.
    assert _sink_cleanups(events) == {"users#0": 1, "users#1": 1}
    # State was written only after every commit (including the retired sink's).
    last_commit = max(
        index for index, event in enumerate(events) if event["event"] == "commit"
    )
    state_index = next(
        index for index, event in enumerate(events) if event["event"] == "state"
    )
    assert state_index > last_commit


def test_repeated_signals_do_not_duplicate_commit_state_or_cleanup(
    tmp_path: Path,
) -> None:
    """Repeated SIGTERM/SIGINT during shutdown cause no duplicate effects."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        wait_in_batch=True,
        wait_in_cleanup=True,
    )
    target.close_stdin()

    target.wait_for_event(name="batch_enter", sink="alpha#0")
    target.signal(signal.SIGTERM)
    target.wait_for_event(name="signal_received")

    # A burst of further signals while the first is still being handled. They must
    # neither restart shutdown nor duplicate any work.
    target.signal(signal.SIGTERM)
    target.signal(signal.SIGINT)
    target.signal_if_alive(signal.SIGTERM)

    target.release("batch", "alpha")
    target.wait_for_event(name="commit", sink="alpha#0")
    target.signal(signal.SIGINT)
    target.release("cleanup", "alpha")

    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    assert _sink_commits(events) == {"alpha#0": 1}
    assert _sink_cleanups(events) == {"alpha#0": 1}
    assert len([event for event in events if event["event"] == "commit"]) == 1
    assert len([event for event in events if event["event"] == "state"]) == 1
    assert target.stdout_states() == [STATE]


def test_sigterm_while_idle_waiting_for_input_exits_zero(tmp_path: Path) -> None:
    """A signal while blocked reading input runs graceful shutdown and exits 0."""
    target = TargetProcess(tmp_path)
    target.start([], config={"batch_size_rows": 100}, max_parallelism=2)

    # Wait until the process has begun reading standard input. No line exists, so it
    # is blocked there when the signal arrives.
    target.wait_for_event(name="listening")
    target.signal(signal.SIGINT)
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    assert not any(event["event"] == "commit" for event in events)
    assert not any(event["event"] == "state" for event in events)
    assert target.stdout_states() == []


def _write_jsonl(path: Path, records: list[dict], *, compress: bool) -> str:
    """Write JSONL records and return the file URL.

    Args:
        path: Destination path.
        records: Records to write.
        compress: Whether to gzip the file.

    Returns:
        The file URL suitable for a BATCH manifest.
    """
    import gzip  # noqa: PLC0415

    payload = b"".join(json.dumps(record).encode() + b"\n" for record in records)
    if compress:
        path.write_bytes(gzip.compress(payload))
    else:
        path.write_bytes(payload)
    return path.as_uri()


def test_record_input_final_commit_state_and_exit(tmp_path: Path) -> None:
    """A normal RECORD stream with no signal commits once, states once, exits 0."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            record_message("alpha", {"id": 2, "name": "a2"}),
            state_message({"bookmark": 7}),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
    )
    target.close_stdin()
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    committed = [
        record
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    ]
    assert sorted(record["id"] for record in committed) == [1, 2]
    assert target.stdout_states() == [{"bookmark": 7}]
    assert _sink_cleanups(events) == {"alpha#0": 1}
    _assert_no_state_before_commit(events)


def test_batch_input_final_commit_state_and_exit(tmp_path: Path) -> None:
    """A JSONL/gzip BATCH stream commits the expanded records and states once."""
    records = [
        {"id": 1, "name": "one"},
        {"id": 2, "name": "two"},
        {"id": 3, "name": "three"},
    ]
    path = _write_jsonl(tmp_path / "batch.jsonl.gz", records, compress=True)

    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            batch_message("alpha", [path], compression="gzip"),
            state_message({"bookmark": 9}),
        ],
        config={"batch_size_rows": 2},
        max_parallelism=2,
    )
    target.close_stdin()
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    batches = [event["records"] for event in events if event["event"] == "commit"]
    assert [[record["id"] for record in batch] for batch in batches] == [[1, 2], [3]]
    assert target.stdout_states() == [{"bookmark": 9}]
    _assert_no_state_before_commit(events)


def test_batch_input_sigterm_during_in_flight_commit(tmp_path: Path) -> None:
    """SIGTERM during a BATCH-triggered commit waits for it and acknowledges after."""
    path = _write_jsonl(
        tmp_path / "batch.jsonl",
        [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}],
        compress=False,
    )
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            batch_message("alpha", [path]),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=1,
        wait_in_batch=True,
    )
    target.close_stdin()

    batch_enter = target.wait_for_event(name="batch_enter", sink="alpha#0")
    assert len(batch_enter[0]["records"]) == 2
    target.signal(signal.SIGTERM)
    target.wait_for_event(name="signal_received")
    assert not any(event["event"] == "state" for event in target.events())

    target.release("batch", "alpha")
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    assert _sink_commits(events) == {"alpha#0": 1}
    committed = [
        record
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    ]
    assert [record["id"] for record in committed] == [1, 2]
    assert target.stdout_states() == [STATE]
    _assert_no_state_before_commit(events)


def test_sigterm_while_only_a_few_sinks_are_blocked_waits_for_all(
    tmp_path: Path,
) -> None:
    """A signal while some sinks are blocked and others pending waits for all."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            schema_message("beta", SCHEMA_A),
            schema_message("gamma", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            record_message("beta", {"id": 2, "name": "b2"}),
            record_message("gamma", {"id": 3, "name": "c3"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        wait_in_batch=True,
    )
    target.close_stdin()

    target.wait_for_event(name="batch_enter", sink="alpha#0")
    target.wait_for_event(name="batch_enter", sink="beta#1")
    target.signal(signal.SIGTERM)
    target.wait_for_event(name="signal_received")
    assert not any(event["event"] == "state" for event in target.events())

    target.release("batch", "alpha")
    target.release("batch", "beta")
    target.wait_for_event(name="commit", sink="alpha#0")
    target.wait_for_event(name="commit", sink="beta#1")

    # The third sink was never in flight; shutdown may now commit it too.
    target.release("batch", "gamma")
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    committed_ids = sorted(
        record["id"]
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    )
    assert committed_ids == [1, 2, 3]
    assert target.stdout_states() == [STATE]
    _assert_no_state_before_commit(events)


def test_no_new_input_is_accepted_after_shutdown_begins(tmp_path: Path) -> None:
    """Records after the one whose drain is blocked are not read during shutdown."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            record_message("alpha", {"id": 2, "name": "a2"}),
            record_message("alpha", {"id": 3, "name": "a3"}),
            state_message(STATE),
        ],
        # batch_size_rows=1 makes the first record trigger a drain, which blocks.
        config={"batch_size_rows": 1},
        max_parallelism=1,
        wait_in_batch=True,
    )

    # The first record's drain is blocked, so the reader has not consumed record 2.
    target.wait_for_event(name="batch_enter", sink="alpha#0")
    target.signal(signal.SIGTERM)
    time.sleep(0.1)
    target.release("batch", "alpha")

    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    committed_ids = [
        record["id"]
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    ]
    # Only the record already staged before the signal was committed. Records 2 and
    # 3 were never read, so the shutdown did not accept new input. The trailing
    # STATE message was also never read, so there is nothing to acknowledge.
    assert committed_ids == [1]
    assert not any(
        event["event"] == "read" and '"id": 2' in event["line"] for event in events
    )
    assert target.stdout_states() == []


def test_cleanup_failure_alone_exits_nonzero_after_commit_and_state(
    tmp_path: Path,
) -> None:
    """Cleanup-only failure still commits and states, then exits non-zero."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        cleanup_fail_streams={"alpha"},
    )
    target.close_stdin()
    returncode = target.wait()
    assert returncode != 0
    assert "synthetic cleanup failure for 'alpha'" in target.stderr_text()

    events = target.events()
    assert _sink_commits(events) == {"alpha#0": 1}
    assert _sink_cleanups(events) == {"alpha#0": 1}
    assert target.stdout_states() == [STATE]


def test_commit_and_cleanup_failure_keeps_commit_diagnostic(tmp_path: Path) -> None:
    """A commit error is kept as the diagnostic even when cleanup also fails."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        fail_streams={"alpha"},
        cleanup_fail_streams={"alpha"},
    )
    target.close_stdin()
    returncode = target.wait()
    assert returncode != 0

    stderr = target.stderr_text()
    assert "synthetic commit failure for 'alpha'" in stderr
    # Cleanup was still attempted despite the commit failure.
    assert "synthetic cleanup failure for 'alpha'" in stderr
    assert target.stdout_states() == []


def test_default_parallelism_sigterm_waits_for_all_blocked_sinks(
    tmp_path: Path,
) -> None:
    """With default parallelism, all blocked batches finish before state and exit 0."""
    n_streams = _MAX_DEFAULT_PARALLELISM
    streams = [f"s{index}" for index in range(n_streams)]
    messages: list[dict] = []
    for index, stream in enumerate(streams):
        messages.extend(
            (
                schema_message(stream, SCHEMA_A),
                record_message(stream, {"id": index, "name": stream}),
            ),
        )
    messages.append(state_message(STATE))

    target = TargetProcess(tmp_path)
    target.start(
        messages,
        config={"batch_size_rows": 100},
        # No ``max_parallelism`` override: the SDK default (8) applies.
        max_parallelism=None,
        wait_in_batch=True,
    )
    target.close_stdin()

    for index in range(n_streams):
        target.wait_for_event(name="batch_enter", sink=f"{streams[index]}#{index}")

    target.signal(signal.SIGTERM)
    time.sleep(0.1)

    for stream in streams:
        target.release("batch", stream)
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    assert _sink_commits(events) == {
        f"{stream}#{index}": 1 for index, stream in enumerate(streams)
    }
    committed_ids = sorted(
        record["id"]
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    )
    assert committed_ids == list(range(n_streams))
    assert target.stdout_states() == [STATE]
    _assert_no_state_before_commit(events)


def test_max_parallelism_one_sigterm_waits_for_the_in_flight_commit(
    tmp_path: Path,
) -> None:
    """With max_parallelism=1, a signal waits for the one in-flight batch."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            schema_message("beta", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            record_message("beta", {"id": 2, "name": "b2"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=1,
        wait_in_batch=True,
    )
    target.close_stdin()

    target.wait_for_event(name="batch_enter", sink="alpha#0")
    # Only one drain can be in flight, so beta has not started yet.
    assert _sink_commits(target.events()).get("beta#1", 0) == 0

    target.signal(signal.SIGTERM)
    time.sleep(0.1)
    assert not any(event["event"] == "state" for event in target.events())

    target.release("batch", "alpha")
    target.wait_for_event(name="commit", sink="alpha#0")
    target.wait_for_event(name="batch_enter", sink="beta#1")
    target.release("batch", "beta")

    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    committed_ids = sorted(
        record["id"]
        for event in events
        if event["event"] == "commit"
        for record in event["records"]
    )
    assert committed_ids == [1, 2]
    assert _sink_commits(events) == {"alpha#0": 1, "beta#1": 1}
    assert target.stdout_states() == [STATE]
    _assert_no_state_before_commit(events)


def test_sigterm_during_cleanup_does_not_duplicate_cleanup_or_exit_early(
    tmp_path: Path,
) -> None:
    """A signal while cleanup is in progress neither repeats nor short-circuits it."""
    target = TargetProcess(tmp_path)
    target.start(
        [
            schema_message("alpha", SCHEMA_A),
            record_message("alpha", {"id": 1, "name": "a1"}),
            state_message(STATE),
        ],
        config={"batch_size_rows": 100},
        max_parallelism=2,
        wait_in_cleanup=True,
    )
    target.close_stdin()

    # The commit already succeeded; cleanup then blocks before the final STATE is
    # written (cleanup precedes the state acknowledgement in the drain).
    target.wait_for_event(name="commit", sink="alpha#0")
    target.wait_for_event(name="cleanup_enter", sink="alpha#0")
    assert not any(event["event"] == "state" for event in target.events())

    target.signal(signal.SIGTERM)
    time.sleep(0.1)
    assert target.proc is not None
    assert target.proc.poll() is None, "cleanup must be allowed to finish"

    target.release("cleanup", "alpha")
    returncode = target.wait()
    assert returncode == 0, target.stderr_text()

    events = target.events()
    assert _sink_cleanups(events) == {"alpha#0": 1}
    assert _sink_commits(events) == {"alpha#0": 1}
    assert target.stdout_states() == [STATE]
