"""Controlled target program used by the shutdown signal tests.

This module is executed in a real subprocess by ``test_target_shutdown.py``. The
target and its sinks record observable lifecycle events to an append-only JSONL
event log, block on file-system barriers under test control, and optionally inject
commit or cleanup failures.

All barriers have a finite timeout so a broken target can never hang the test run.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import typing as t
from pathlib import Path

from singer_sdk import typing as th
from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

# Reentrant: a signal handler runs on the main thread and may re-enter this logger
# while the interrupted frame still holds the lock.
_LOG_LOCK = threading.RLock()


def _event_log_path() -> Path:
    return Path(os.environ["SS_EVENT_LOG"])


def log_event(event: dict) -> None:
    """Append one event to the event log.

    Args:
        event: A JSON-serializable event payload.
    """
    payload = (json.dumps(event, sort_keys=True) + "\n").encode()
    with _LOG_LOCK:
        fd = os.open(
            _event_log_path(),
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o644,
        )
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)


def _env_flag(name: str) -> bool:
    return os.environ.get(name) == "1"


def _env_set(name: str) -> set[str]:
    return {item for item in os.environ.get(name, "").split(",") if item}


def _barrier_dir() -> Path:
    return Path(os.environ["SS_BARRIER_DIR"])


def _wait_for_release(kind: str, name: str) -> None:
    """Block until the test releases a named barrier.

    Args:
        kind: Barrier kind, e.g. ``batch`` or ``cleanup``.
        name: Barrier name, usually the stream name.

    Raises:
        TimeoutError: If the test does not release the barrier in time.
    """
    timeout = float(os.environ.get("SS_BARRIER_TIMEOUT", "30"))
    deadline = time.monotonic() + timeout
    release = _barrier_dir() / f"release-{kind}-{name}"
    while not release.exists():
        if time.monotonic() > deadline:
            msg = f"timed out waiting for {kind} barrier for stream {name!r}"
            raise TimeoutError(msg)
        time.sleep(0.01)


class ControlledSink(BatchSink):
    """A batch sink that logs lifecycle events and can fail on demand."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None,
    ) -> None:
        """Initialize the sink and assign it a stable sequence number.

        Args:
            target: The target instance.
            stream_name: The stream name.
            schema: The stream schema.
            key_properties: The stream key properties.
        """
        super().__init__(target, stream_name, schema, key_properties)
        self.target = t.cast("ControlledTarget", target)
        self.sequence = self.target.allocate_sink_sequence()
        self.label = f"{stream_name}#{self.sequence}"

    def process_batch(self, context: dict) -> None:
        """Log the batch, optionally wait, then commit or fail.

        Args:
            context: The batch context.

        Raises:
            RuntimeError: If this stream is configured to fail its commit.
        """
        records = context.get("records", [])
        log_event(
            {
                "event": "batch_enter",
                "sink": self.label,
                "stream": self.stream_name,
                "records": records,
                "context_keys": sorted(context),
            },
        )

        # The barrier models "entered process_batch but held before the real commit".
        # An empty context carries no data to commit and is never blocked, so a
        # spurious empty batch is observable as an event instead of a hang.
        if _env_flag("SS_WAIT_IN_BATCH") and records:
            _wait_for_release("batch", self.stream_name)

        if self.stream_name in _env_set("SS_FAIL_STREAMS"):
            log_event(
                {
                    "event": "commit_fail",
                    "sink": self.label,
                    "stream": self.stream_name,
                    "records": records,
                },
            )
            msg = f"synthetic commit failure for '{self.stream_name}'"
            raise RuntimeError(msg)

        log_event(
            {
                "event": "commit",
                "sink": self.label,
                "stream": self.stream_name,
                "records": records,
            },
        )

    def clean_up(self) -> None:
        """Log cleanup, optionally wait, then clean up or fail.

        Raises:
            RuntimeError: If this stream is configured to fail cleanup.
        """
        log_event(
            {"event": "cleanup_enter", "sink": self.label, "stream": self.stream_name},
        )

        if _env_flag("SS_WAIT_IN_CLEANUP"):
            _wait_for_release("cleanup", self.stream_name)

        if self.stream_name in _env_set("SS_CLEANUP_FAIL_STREAMS"):
            log_event(
                {
                    "event": "cleanup_fail",
                    "sink": self.label,
                    "stream": self.stream_name,
                },
            )
            msg = f"synthetic cleanup failure for '{self.stream_name}'"
            raise RuntimeError(msg)

        super().clean_up()
        log_event(
            {"event": "cleanup_done", "sink": self.label, "stream": self.stream_name},
        )


class _LoggingInput:
    """An input iterator that logs each line as it is read."""

    def __init__(self, lines: t.Iterator[str]) -> None:
        self._lines = lines

    def __iter__(self) -> _LoggingInput:
        return self

    def __next__(self) -> str:
        line = next(self._lines)
        log_event({"event": "read", "line": line.rstrip("\n")})
        return line

    def close(self) -> None:
        """Close the wrapped iterator if it is closeable."""
        close = getattr(self._lines, "close", None)
        if close is not None:
            close()


class ControlledTarget(Target):
    """A target that logs its sink lifecycle and state acknowledgements."""

    name = "target-shutdown-test"
    config_jsonschema = th.PropertiesList(
        th.Property("batch_size_rows", th.IntegerType),
    ).to_dict()
    default_sink_class = ControlledSink

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the target and its event bookkeeping.

        Args:
            *args: Positional arguments for ``Target``.
            **kwargs: Keyword arguments for ``Target``.
        """
        self._sink_sequence = 0
        super().__init__(*args, **kwargs)
        raw_parallelism = os.environ.get("SS_MAX_PARALLELISM", "")
        if raw_parallelism:
            self.max_parallelism = int(raw_parallelism)

    def allocate_sink_sequence(self) -> int:
        """Assign the next sink sequence number.

        Returns:
            A zero-based, monotonically increasing sink sequence number.
        """
        sequence = self._sink_sequence
        self._sink_sequence += 1
        return sequence

    def _write_state_message(self, state: dict) -> None:
        """Log the state acknowledgement before writing it to stdout.

        Args:
            state: The state payload.
        """
        log_event({"event": "state", "state": state})
        super()._write_state_message(state)

    def _handle_termination(self, signum: int, frame: t.Any) -> None:
        """Log that a termination signal was received, then run the SDK handler.

        Args:
            signum: The signal number.
            frame: The interrupted frame.
        """
        log_event({"event": "signal_received", "signum": signum})
        super()._handle_termination(signum, frame)

    def _prepare_input(self, file_input: t.IO[str] | None) -> t.IO[str]:
        """Log every input line that is actually read.

        Args:
            file_input: Readable stream of messages, or ``None``.

        Returns:
            A logging iterator over the interruptible input.
        """
        stream = super()._prepare_input(file_input)
        assert stream is not None
        wrapped = _LoggingInput(iter(stream))
        log_event({"event": "listening"})
        return t.cast("t.IO[str]", wrapped)


def main() -> None:
    """Run the controlled target against standard input."""
    if os.environ.get("SS_FAULTHANDLER") == "1":
        import faulthandler  # noqa: PLC0415
        import signal as signal_module  # noqa: PLC0415

        faulthandler.enable()
        faulthandler.register(signal_module.SIGUSR1, all_threads=True)

    config = json.loads(os.environ.get("SS_CONFIG", "{}"))
    target = ControlledTarget(config=config)
    log_event({"event": "ready"})
    target.listen(sys.stdin)


if __name__ == "__main__":
    main()
