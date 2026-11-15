"""Target abstract class."""

from __future__ import annotations

import abc
import copy
import json
import sys
import threading
import time
import typing as t
import warnings
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as wait_futures

import click

from singer_sdk.exceptions import (
    RecordsWithoutSchemaException,
    StreamMapsNotSupportedError,
)
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.helpers._termination import TerminationRequested
from singer_sdk.helpers.capabilities import (
    ACTIVATE_VERSION_CONFIG,
    ADD_RECORD_METADATA_CONFIG,
    BATCH_CONFIG,
    TARGET_BATCH_SIZE_ROWS_CONFIG,
    TARGET_LOAD_METHOD_CONFIG,
    TARGET_VALIDATE_RECORDS_CONFIG,
    PluginCapabilities,
    TargetCapabilities,
)
from singer_sdk.io_base import SingerReader
from singer_sdk.mapper import RemoveRecordTransform
from singer_sdk.plugin_base import BaseSingerReader, _ConfigInput
from singer_sdk.sinks.core import Sink, iter_batch_file_records

if t.TYPE_CHECKING:
    from pathlib import PurePath
    from types import FrameType

    from singer_sdk.helpers.capabilities import CapabilitiesEnum
    from singer_sdk.mapper import PluginMapper, StreamMap
    from singer_sdk.singerlib.encoding.base import GenericSingerReader
    from singer_sdk.sql import SQLTarget  # noqa: F401

_MAX_PARALLELISM = 8


class _InterruptibleInput:
    """An iterator over input lines that yields to a pending shutdown request.

    A termination signal can only interrupt Python code between bytecodes; it cannot
    interrupt a thread blocked in ``read(2)``. Before each blocking read the target
    marks the read as interruptible, so a signal handler can raise
    :class:`~singer_sdk.helpers._termination.TerminationRequested` and unwind to the
    input boundary. The mark is cleared as soon as the read returns.
    """

    def __init__(self, target: Target, lines: t.Iterator[str]) -> None:
        """Initialize the wrapper.

        Args:
            target: The target whose shutdown state is checked.
            lines: The underlying line iterator.
        """
        self._target = target
        self._lines = lines

    def __iter__(self) -> _InterruptibleInput:
        """Return the iterator.

        Returns:
            This wrapper.
        """
        return self

    def __next__(self) -> str:
        """Return the next line unless a termination was requested.

        Returns:
            The next input line.

        Raises:
            TerminationRequested: If a termination request is already pending.
        """
        target = self._target
        if target._shutdown_requested:  # noqa: SLF001
            raise TerminationRequested(target._shutdown_signum)  # noqa: SLF001

        target._interruptible = True  # noqa: SLF001
        try:
            # A signal that arrived between the check above and this point was
            # recorded by the handler but could not raise. Re-check now that the
            # read is marked interruptible so the request is never lost.
            if target._shutdown_requested:  # noqa: SLF001
                raise TerminationRequested(target._shutdown_signum)  # noqa: SLF001
            line = next(self._lines)
        finally:
            target._interruptible = False  # noqa: SLF001

        if target._shutdown_requested:  # noqa: SLF001
            raise TerminationRequested(target._shutdown_signum)  # noqa: SLF001
        return line

    def close(self) -> None:
        """Close the underlying stream, if it is closeable."""
        close = getattr(self._lines, "close", None)
        if close is not None:
            close()


class Target(BaseSingerReader, abc.ABC):
    """Abstract base class for targets.

    The `Target` class manages config information and is responsible for processing the
    incoming Singer data stream and orchestrating any needed target `Sink` objects. As
    messages are received from the tap, the `Target` class will automatically create
    any needed target `Sink` objects and send records along to the appropriate `Sink`
    object for that record.
    """

    _MAX_RECORD_AGE_IN_MINUTES: float = 5.0

    # Default class to use for creating new sink objects.
    # Required if `Target.get_sink_class()` is not defined.
    default_sink_class: type[Sink]

    message_reader_class: type[GenericSingerReader] = SingerReader
    """The message reader class to use for reading messages."""

    #: A list of plugin capabilities supported by this target.
    capabilities: t.ClassVar[list[CapabilitiesEnum]] = [
        PluginCapabilities.ABOUT,
        PluginCapabilities.STREAM_MAPS,
        PluginCapabilities.FLATTENING,
        PluginCapabilities.STRUCTURED_LOGGING,
        TargetCapabilities.VALIDATE_RECORDS,
    ]

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        setup_mapper: bool = True,
        message_reader: GenericSingerReader | None = None,
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
            setup_mapper: True to setup the mapper. Set to False if you want to
            message_reader: The message reader to use for reading messages.
        """
        # Termination bookkeeping must exist before the base class installs its
        # signal handlers: a signal may arrive while the constructor is still
        # running. The signal handler only ever records the request, so these
        # attributes are the only state it touches.
        self._shutdown_requested: bool = False
        self._shutdown_signum: int | None = None
        self._shutdown_completed: bool = False
        self._shutdown_lock = threading.RLock()
        self._interruptible: bool = False

        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
            message_reader=message_reader,
        )

        self._latest_state: dict[str, dict] | None = None
        self._drained_state: dict[str, dict] = {}
        self._sinks_active: dict[str, Sink] = {}
        self._sinks_to_clear: list[Sink] = []
        self._max_parallelism: int | None = _MAX_PARALLELISM

        # Registry of every sink this target ever created, in creation order. It is
        # used to decide whether a shutdown has waited for all in-flight writes and
        # to guarantee each sink is cleaned up at most once, including sinks left
        # behind by a schema change.
        self._sinks_registry: list[Sink] = []

        # Serializes drains of an individual sink so the same in-flight batch can
        # never be submitted twice, and records sinks whose batch permanently
        # failed so they are never retried.
        self._sink_drain_locks: dict[int, threading.Lock] = {}
        self._sink_drain_locks_guard = threading.Lock()
        self._failed_sinks: set[int] = set()
        self._cleaned_up_sinks: set[int] = set()

        # The first commit error, if any. While set, no progress can be acknowledged
        # because a state message would cover records that were never committed.
        self._commit_error: BaseException | None = None

        # Approximated for max record age enforcement
        self._last_full_drain_at: float = time.time()

        self._mapper: PluginMapper | None = None

        if setup_mapper:
            self.setup_mapper()

    @property
    def max_parallelism(self) -> int:
        """Get max parallel sinks.

        The default is 8 if not overridden.

        Returns:
            Max number of sinks that can be drained in parallel.
        """
        if self._max_parallelism is not None:
            return self._max_parallelism

        return _MAX_PARALLELISM

    @max_parallelism.setter
    def max_parallelism(self, new_value: int) -> None:
        """Override the default (max) parallelism.

        The default is 8 if not overridden.

        Args:
            new_value: The new max degree of parallelism for this target.
        """
        self._max_parallelism = new_value

    def get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: t.Sequence[str] | None = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWithoutSchemaException` if sink does
        not exist and schema is not sent.

        Args:
            stream_name: Name of the stream.
            record: Record being processed.
            schema: Stream schema.
            key_properties: Primary key of the stream.

        Returns:
            The sink used for this target.
        """
        _ = record  # Custom implementations may use record in sink selection.
        if schema is None:
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)

        if (
            existing_sink.original_schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                "Schema or key properties for '%s' stream have changed. "
                "Initializing a new '%s' sink...",
                stream_name,
                stream_name,
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sink(stream_name, schema, key_properties)

        return existing_sink

    def get_sink_class(self, stream_name: str) -> type[Sink]:
        """Get sink for a stream.

        Developers can override this method to return a custom Sink type depending
        on the value of `stream_name`. Optional when `default_sink_class` is set.

        Args:
            stream_name: Name of the stream.

        Returns:
            The sink class to be used with the stream.

        Raises:
            ValueError: If no :class:`singer_sdk.sinks.Sink` class is defined.
        """
        if self.default_sink_class:
            return self.default_sink_class

        msg = (
            f"No sink class defined for '{stream_name}' and no default sink class "
            "available."
        )
        raise ValueError(msg)

    def sink_exists(self, stream_name: str) -> bool:
        """Check sink for a stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream

        Returns:
            True if a sink has been initialized.
        """
        return stream_name in self._sinks_active

    @t.final
    def add_sink(
        self,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None = None,
    ) -> Sink:
        """Create a sink and register it.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink for the stream.
        """
        self.logger.debug("Initializing target sink '%s'...", self.name)
        sink_class = self.get_sink_class(stream_name=stream_name)
        sink = sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )

        try:
            sink.setup()
        except Exception:  # pragma: no cover
            self.logger.error("Error initializing target sink '%s'", self.name)  # noqa: TRY400
            raise

        self._register_sink(sink)
        self._sinks_active[stream_name] = sink
        return sink

    def _register_sink(self, sink: Sink) -> None:
        """Record a newly created sink in the target's sink registry.

        The registry keeps every sink alive for the lifetime of the target so that
        a shutdown can reason about all in-flight writes, including sinks that were
        already retired by a schema change.

        Args:
            sink: The new sink.
        """
        self._sinks_registry.append(sink)

    def _drain_lock_for(self, sink: Sink) -> threading.Lock:
        """Return the lock that serializes drains of a sink.

        Args:
            sink: The sink to lock.

        Returns:
            The sink's drain lock.
        """
        with self._sink_drain_locks_guard:
            return self._sink_drain_locks.setdefault(id(sink), threading.Lock())

    def _assert_sink_exists(self, stream_name: str) -> None:
        """Raise a RecordsWithoutSchemaException exception if stream doesn't exist.

        Args:
            stream_name: TODO

        Raises:
            RecordsWithoutSchemaException: If sink does not exist and schema
                is not sent.
        """
        if not self.sink_exists(stream_name):
            msg = (
                f"A record for stream '{stream_name}' was encountered before a "
                "corresponding schema. Check that the Tap correctly implements "
                "the Singer spec."
            )
            raise RecordsWithoutSchemaException(msg)

    # Message handling

    def _handle_max_record_age(self) -> None:
        """Check if _MAX_RECORD_AGE_IN_MINUTES reached, and if so trigger drain."""
        if self._max_record_age_in_minutes > self._MAX_RECORD_AGE_IN_MINUTES:
            self.logger.info(
                "One or more records have exceeded the max age of %d minutes. "
                "Draining all sinks.",
                self._MAX_RECORD_AGE_IN_MINUTES,
            )
            self.drain_all()

    def process_endofpipe(self) -> None:
        """Called after all input lines have been read."""
        if self._shutdown_requested:
            self._graceful_shutdown()
            return

        self.drain_all(is_endofpipe=True)

    # Input interruption hooks (see ``BaseSingerReader.listen``)

    def _prepare_input(self, file_input: t.IO[str] | None) -> t.IO[str] | None:
        """Wrap the input so a termination signal can interrupt a blocking read.

        Args:
            file_input: Readable stream of messages, or ``None`` for the default.

        Returns:
            A stream that checks for a pending termination request before every
            line and marks reads as interruptible.
        """
        stream = (
            file_input if file_input is not None else self.message_reader.default_input
        )
        return t.cast("t.IO[str]", _InterruptibleInput(self, iter(stream)))

    def _should_stop_before_reading(self) -> bool:
        """Whether a termination request is already pending.

        Returns:
            True if the target should not read any input.
        """
        return self._shutdown_requested

    def _should_stop_before_endofpipe(self) -> bool:
        """Whether a termination request arrived while input was being read.

        Returns:
            True if the normal end-of-pipe drain should be replaced by a
            graceful shutdown.
        """
        return self._shutdown_requested

    def _on_input_interrupted(self) -> None:
        """Run a graceful shutdown after a termination request interrupted reading."""
        self._graceful_shutdown()

    def _on_processing_error(self, error: BaseException) -> None:
        """Attempt sink cleanup after a commit failure aborts input processing.

        A failed commit must not prevent cleanup later in the stream, and a cleanup
        failure must never replace the original error. Cleanup is only attempted for
        commit failures; other input errors keep their previous behavior.

        Args:
            error: The error that is about to propagate.
        """
        _ = error
        if self._commit_error is None:
            return
        cleanup_errors = self._clean_up_sinks(list(self._sinks_registry))
        if cleanup_errors:
            self._log_cleanup_error(cleanup_errors[0])

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_name = message_dict["stream"]
        if stream_name not in self.mapper.stream_maps:
            self._assert_sink_exists(stream_name)

        for stream_map in self.mapper.stream_maps[stream_name]:
            raw_record = copy.copy(message_dict["record"])
            transformed_record = stream_map.transform(raw_record)
            if transformed_record is None:
                # Record was filtered out by the map transform
                continue

            self._assert_sink_exists(stream_map.stream_alias)
            sink = self.get_sink(stream_map.stream_alias, record=transformed_record)
            context = sink._get_context(transformed_record)  # noqa: SLF001
            self._write_record(
                sink,
                transformed_record,
                context,
                message_dict,
                stream_map.stream_alias,
            )

        self._handle_max_record_age()

    def _write_record(
        self,
        sink: Sink,
        record: dict,
        context: dict,
        message_dict: dict,
        stream_name: str,
    ) -> None:
        """Write a single (already mapped) record to its sink.

        Args:
            sink: The sink to write the record to.
            record: The mapped record.
            context: The record context.
            message_dict: The originating Singer message.
            stream_name: The name of the stream being written to.
        """
        if sink.include_sdc_metadata_properties:
            sink._add_sdc_metadata_to_record(  # noqa: SLF001
                record,
                message_dict,
                context,
            )
        else:
            sink._remove_sdc_metadata_from_record(record)  # noqa: SLF001

        sink._validate_and_parse(record)  # noqa: SLF001
        record = sink.preprocess_record(record, context)
        sink._singer_validate_message(record)  # noqa: SLF001

        sink.tally_record_read()
        sink.process_record(record, context)
        sink.record_counter_metric.increment()
        sink._after_process_record(context)  # noqa: SLF001

        if sink.is_full:
            self.logger.info(
                "Target sink for '%s' is full. Current size is '%s'. Draining...",
                stream_name,
                sink.current_size,
            )
            self.drain_one(sink)

    def _process_schema_message(self, message_dict: dict) -> None:
        """Process a SCHEMA messages.

        Args:
            message_dict: The newly received schema message.
        """
        self._assert_line_requires(message_dict, requires={"stream", "schema"})
        self._assert_line_requires(message_dict["schema"], requires={"properties"})

        stream_name = message_dict["stream"]
        schema = message_dict["schema"]
        key_properties = message_dict.get("key_properties")
        do_registration = False
        if stream_name not in self.mapper.stream_maps:
            do_registration = True
        elif self.mapper.stream_maps[stream_name][0].raw_schema != schema:
            self.logger.info(
                "Schema has changed for stream '%s'. "
                "Mapping definitions will be reset.",
                stream_name,
            )
            do_registration = True
        elif (
            self.mapper.stream_maps[stream_name][0].raw_key_properties != key_properties
        ):
            self.logger.info(
                "Key properties have changed for stream '%s'. "
                "Mapping definitions will be reset.",
                stream_name,
            )
            do_registration = True

        if not do_registration:
            self.logger.debug(
                "No changes detected in SCHEMA message for stream '%s'. Ignoring.",
                stream_name,
            )
            return

        self.mapper.register_raw_stream_schema(
            stream_name,
            schema,
            key_properties,
        )
        for stream_map in self.mapper.stream_maps[stream_name]:
            _ = self.get_sink(
                stream_map.stream_alias,
                schema=stream_map.transformed_schema,
                key_properties=stream_map.transformed_key_properties,
            )

    @property
    def _max_record_age_in_minutes(self) -> float:
        return (time.time() - self._last_full_drain_at) / 60

    def _reset_max_record_age(self) -> None:
        self._last_full_drain_at = time.time()

    def _process_state_message(self, message_dict: dict) -> None:
        """Process a state message. drain sinks if needed.

        If state is unchanged, no actions will be taken.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires={"value"})
        state = message_dict["value"]
        if self._latest_state == state:
            return
        self._latest_state = state

    def _process_activate_version_message(self, message_dict: dict) -> None:
        """Handle the optional ACTIVATE_VERSION message extension.

        Args:
            message_dict: TODO
        """
        stream_name = message_dict["stream"]

        for stream_map in self.mapper.stream_maps[stream_name]:
            sink = self.get_sink(stream_map.stream_alias)
            if not sink.process_activate_version_messages:
                self.logger.warning(
                    "`ACTIVATE_VERSION` messages are not enabled for '%s'. Ignoring.",
                    stream_map.stream_alias,
                )
                continue
            if not sink.include_sdc_metadata_properties:
                self.logger.warning(
                    "The `ACTIVATE_VERSION` feature uses the `_sdc_deleted_at` and "
                    "`_sdc_deleted_at` metadata properties so they will be added to "
                    "the schema for '%s' even though `add_record_metadata` is "
                    "disabled.",
                    sink.stream_name,
                )
            sink.activate_version(message_dict["version"])

    def _process_batch_message(self, message_dict: dict) -> None:
        """Handle the optional BATCH message extension.

        Semantically, a ``BATCH`` message is equivalent to the ``RECORD`` messages
        obtained by expanding each file listed in its manifest, in order and one line
        at a time, at the position of the ``BATCH`` message. The expanded records use
        the batch's ``stream``, and the ``time_extracted`` / ``version`` values of
        the batch message (if any).

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(
            message_dict,
            requires={"stream", "encoding", "manifest"},
        )

        stream_name = message_dict["stream"]
        encoding = BaseBatchFileEncoding.from_dict(message_dict["encoding"])
        manifest = message_dict["manifest"]

        if stream_name not in self.mapper.stream_maps:
            # Raises RecordsWithoutSchemaException, exactly as for a RECORD message.
            self._assert_sink_exists(stream_name)

        stream_maps = self.mapper.stream_maps[stream_name]

        # A stream that is entirely removed by its stream map produces no sink
        # activity at all. Its files are never read.
        active_maps = [
            stream_map
            for stream_map in stream_maps
            if not isinstance(stream_map, RemoveRecordTransform)
        ]
        if not active_maps:
            self._handle_max_record_age()
            return

        raw_file_maps, expanded_maps = self._classify_batch_maps(active_maps)

        # With no effective stream maps or flattening, custom `process_batch_files`
        # implementations receive the raw files, exactly as before.
        for stream_map in raw_file_maps:
            sink = self.get_sink(stream_map.stream_alias)
            sink.process_batch_files(encoding, manifest)

        if not expanded_maps:
            self._handle_max_record_age()
            return

        # Expand the batch into the equivalent RECORD sequence, in manifest order
        # and then line order within each file.
        batch_config = self.get_sink(expanded_maps[0].stream_alias).batch_config
        storage = batch_config.storage if batch_config else None

        for path in manifest:
            for line_number, record in iter_batch_file_records(
                encoding,
                path,
                storage,
            ):
                if self._shutdown_requested:
                    # No new records are accepted once shutdown has begun.
                    return
                self._write_expanded_records(
                    expanded_maps,
                    record,
                    message_dict,
                    path,
                    line_number,
                )
                self._handle_max_record_age()

    def _write_expanded_records(
        self,
        stream_maps: list[StreamMap],
        record: dict,
        message_dict: dict,
        path: str,
        line_number: int,
    ) -> None:
        """Write one batch record through each applicable stream map.

        Args:
            stream_maps: The stream maps that expand into sinks.
            record: The raw record read from the batch file.
            message_dict: The originating BATCH message.
            path: The batch file the record came from.
            line_number: The record's 1-based position in the batch file.
        """
        for stream_map in stream_maps:
            raw_record = copy.copy(record)
            transformed_record = stream_map.transform(raw_record)
            if transformed_record is None:
                continue

            self._assert_sink_exists(stream_map.stream_alias)
            sink = self.get_sink(
                stream_map.stream_alias,
                record=transformed_record,
            )
            context = sink._get_context(transformed_record)  # noqa: SLF001
            sink._batch_record_error_context = (path, line_number)  # noqa: SLF001
            try:
                self._write_record(
                    sink,
                    transformed_record,
                    context,
                    message_dict,
                    stream_map.stream_alias,
                )
            finally:
                sink._batch_record_error_context = None  # noqa: SLF001

    def _classify_batch_maps(
        self,
        active_maps: list[StreamMap],
    ) -> tuple[list[StreamMap], list[StreamMap]]:
        """Split batch stream maps into raw-file and expanded-record groups.

        Args:
            active_maps: Stream maps that were not fully removed.

        Returns:
            A ``(raw_file_maps, expanded_maps)`` pair.

        Raises:
            StreamMapsNotSupportedError: If a sink overrides ``process_batch_files``
                while stream maps or flattening are active.
        """
        any_map_changes_records = any(
            stream_map.changes_records for stream_map in active_maps
        )

        # Check custom batch handlers before reading any file.
        raw_file_maps: list[StreamMap] = []
        expanded_maps: list[StreamMap] = []
        for stream_map in active_maps:
            self._assert_sink_exists(stream_map.stream_alias)
            sink = self.get_sink(stream_map.stream_alias)
            if not self._sink_overrides_process_batch_files(sink):
                expanded_maps.append(stream_map)
                continue

            if any_map_changes_records:
                msg = (
                    f"The sink for stream '{stream_map.stream_alias}' overrides "
                    "`process_batch_files`, so it cannot apply the configured "
                    "stream maps or flattening to the records in a BATCH message. "
                    "Disable stream maps and flattening for this stream, or remove "
                    "the custom `process_batch_files` implementation."
                )
                raise StreamMapsNotSupportedError(msg)

            raw_file_maps.append(stream_map)

        return raw_file_maps, expanded_maps

    @staticmethod
    def _sink_overrides_process_batch_files(sink: Sink) -> bool:
        """Whether the sink provides its own batch file processing.

        Args:
            sink: The sink to check.

        Returns:
            True if ``process_batch_files`` is overridden by the sink class.
        """
        return type(sink).process_batch_files is not Sink.process_batch_files

    # Sink drain methods

    @t.final
    def drain_all(self, *, is_endofpipe: bool = False) -> None:
        """Drains all sinks, starting with those cleared due to changed schema.

        This method is internal to the SDK and should not need to be overridden.

        All sinks are attempted even if some fail. The latest state is emitted only
        after every commit succeeded.

        Args:
            is_endofpipe: This is called after the target instance has finished
                listening to the stdin. Every sink is cleaned up afterwards.

        """
        self._drain_pending(clean_up=is_endofpipe)
        self._reset_max_record_age()

    @t.final
    def drain_one(self, sink: Sink) -> None:
        """Drain a specific sink.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            sink: Sink to be drained.

        """
        self._drain_sink(sink)

    def _drain_sink(self, sink: Sink) -> None:
        """Drain one sink, serializing concurrent requests to drain the same sink.

        At most one drain per sink is in flight at a time. A sink whose batch failed
        is never drained again, so a failed in-flight batch can never be re-submitted.

        Args:
            sink: Sink to be drained.

        """
        if id(sink) in self._failed_sinks or sink.current_size == 0:
            return

        lock = self._drain_lock_for(sink)
        with lock:
            if id(sink) in self._failed_sinks or sink.current_size == 0:
                return

            draining_status = sink.start_drain()
            try:
                with sink.batch_processing_timer:
                    sink.process_batch(draining_status)
            except BaseException as ex:
                self._failed_sinks.add(id(sink))
                if self._commit_error is None:
                    self._commit_error = ex
                raise
            sink.mark_drained()

    def _drain_pending(
        self,
        *,
        clean_up: bool,
    ) -> None:
        """Drain every pending sink, then optionally clean every sink up.

        All sinks are attempted even if some fail, so a failure in one stream does
        not hide progress or cleanup opportunities in another. Cleanup errors never
        replace a commit error. State is written only if every commit so far has
        succeeded.

        Args:
            clean_up: Whether to clean up every sink after draining. Cleanup is also
                attempted after a commit failure when a termination was requested.

        """
        sinks_to_clear = list(self._sinks_to_clear)
        self._sinks_to_clear = []
        active_sinks = list(self._sinks_active.values())

        # Sinks retired by a schema change drain first, then current sinks. Every
        # sink ever created is still in the registry, so a retired sink that was
        # already drained is not drained again, but is still cleaned up exactly once.
        commit_errors = self._drain_sinks(sinks_to_clear, 1)
        commit_errors.extend(
            self._drain_sinks(active_sinks, self.max_parallelism),
        )

        if clean_up:
            # Safety net for targets that override ``get_sink`` and keep sinks
            # outside the two standard groups. Every sink call has returned at this
            # point, so this can neither resubmit an in-flight batch nor race it.
            drained = {id(sink) for sink in (*sinks_to_clear, *active_sinks)}
            remaining = [
                sink
                for sink in self._sinks_registry
                if id(sink) not in drained and sink.current_size > 0
            ]
            commit_errors.extend(self._drain_sinks(remaining, 1))

        cleanup_errors: list[BaseException] = []
        if clean_up or (commit_errors and self._shutdown_requested):
            cleanup_errors = self._clean_up_sinks(list(self._sinks_registry))

        # Both error kinds are logged so a cleanup error can never hide the commit
        # diagnostic, and a cleanup-only failure is still reported.
        if self._commit_error is not None:
            self._log_drain_error(self._commit_error)
        if cleanup_errors:
            self._log_cleanup_error(cleanup_errors[0])

        if self._commit_error is not None:
            raise self._commit_error

        # Every commit succeeded. The latest state can now be safely acknowledged.
        if self._latest_state:
            self._write_state_message(copy.deepcopy(self._latest_state))

        if cleanup_errors:
            raise cleanup_errors[0]

    def _drain_sinks(
        self,
        sinks: list[Sink],
        parallelism: int,
    ) -> list[BaseException]:
        """Drain a group of sinks, returning commit errors in sink order.

        Every sink in the group is attempted before errors are returned, and a batch
        that is already in flight is always awaited rather than submitted again.

        Args:
            sinks: The sinks to drain.
            parallelism: Maximum number of sinks to drain concurrently.

        Returns:
            The commit errors raised by this call, in sink order.
        """
        if not sinks:
            return []

        if parallelism <= 1 or len(sinks) == 1:
            return self._drain_serially(sinks)

        results: list[BaseException | None] = [None] * len(sinks)
        workers = min(parallelism, len(sinks))

        def _run(index: int, sink: Sink) -> None:
            try:
                self._drain_sink(sink)
            except BaseException as ex:  # noqa: BLE001
                results[index] = ex

        executor = ThreadPoolExecutor(max_workers=workers)
        try:
            futures = [
                executor.submit(_run, index, sink) for index, sink in enumerate(sinks)
            ]
            # Wait in short slices instead of one long blocking join. A Python
            # signal handler can only run on the main thread when it returns to the
            # interpreter, so a single blocking join would defer termination
            # handling until an unrelated batch finished.
            pending = set(futures)
            while pending:
                _done, pending = wait_futures(pending, timeout=0.05)
        finally:
            executor.shutdown(wait=True)

        return [error for error in results if error is not None]

    def _drain_serially(self, sinks: list[Sink]) -> list[BaseException]:
        """Drain sinks one at a time, collecting errors.

        Args:
            sinks: The sinks to drain.

        Returns:
            The commit errors raised by this call, in sink order.
        """
        errors: list[BaseException] = []
        for sink in sinks:
            error = self._drain_sink_collecting(sink)
            if error is not None:
                errors.append(error)
        return errors

    def _drain_sink_collecting(self, sink: Sink) -> BaseException | None:
        """Drain one sink, returning any error instead of raising it.

        Args:
            sink: The sink to drain.

        Returns:
            The commit error, if the batch failed.
        """
        try:
            self._drain_sink(sink)
        except BaseException as ex:  # noqa: BLE001
            return ex
        return None

    def _clean_up_sinks(self, sinks: list[Sink]) -> list[BaseException]:
        """Clean up each sink at most once, collecting errors.

        Cleanup runs only after every drain attempt has returned, so a sink is never
        cleaned while one of its batches is still in flight.

        Args:
            sinks: The sinks to clean up.

        Returns:
            The cleanup errors, in sink order.
        """
        errors: list[BaseException] = []
        for sink in sinks:
            error = self._clean_up_sink(sink)
            if error is not None:
                errors.append(error)
        return errors

    def _clean_up_sink(self, sink: Sink) -> BaseException | None:
        """Clean up one sink at most once.

        Args:
            sink: The sink to clean up.

        Returns:
            The cleanup error, if cleanup failed.
        """
        if id(sink) in self._cleaned_up_sinks:
            return None
        self._cleaned_up_sinks.add(id(sink))
        try:
            sink.clean_up()
        except BaseException as ex:  # noqa: BLE001
            return ex
        return None

    def _log_drain_error(self, error: BaseException) -> None:
        """Log a drain error without hiding the original exception.

        Args:
            error: The commit error.
        """
        self.logger.error("Error committing a batch: %s", error)

    def _log_cleanup_error(self, error: BaseException) -> None:
        """Log a cleanup error without hiding the original exception.

        Args:
            error: The cleanup error.
        """
        self.logger.error("Error cleaning up a sink: %s", error)

    def _write_state_message(self, state: dict) -> None:
        """Emit the stream's latest state.

        Args:
            state: TODO
        """
        state_json = json.dumps(state)
        self.logger.debug("Emitting completed target state %s", state_json)
        sys.stdout.write(f"{state_json}\n")
        sys.stdout.flush()

    # Graceful termination

    def _graceful_shutdown(self) -> None:
        """Complete a safe exit after a termination signal.

        Every sink whose batch is already in flight is awaited before any cleanup
        starts; no new input is read. Sinks are cleaned up once, even those retired
        by a schema change. The latest state is emitted only when every commit
        succeeded. The process exits with status 0 on success and a non-zero status
        if any commit or cleanup failed.

        Repeated signals during shutdown are ignored so they cannot double-submit,
        double-confirm, or double-clean.
        """
        with self._shutdown_lock:
            if self._shutdown_completed:
                return

            if self._shutdown_signum is not None:
                self.logger.info(
                    "Received termination signal %d, shutting down gracefully...",
                    self._shutdown_signum,
                )
            else:
                self.logger.info("Shutting down gracefully...")

            try:
                self._drain_pending(clean_up=True)
            except BaseException:
                self._shutdown_completed = True
                self.logger.exception("Graceful shutdown failed")
                sys.exit(1)

            self._shutdown_completed = True

    def _handle_termination(self, signum: int, frame: FrameType | None) -> None:
        """Handle termination signals without doing sink work in the handler.

        The handler only records the request. If the target is blocked reading
        standard input, an internal exception is raised to unwind to the input
        boundary, where the graceful shutdown runs on the normal call stack. If a
        batch is in flight or the target is in its end-of-pipe phase, the running
        call notices the request and completes shutdown itself.

        Args:
            signum: Signal number.
            frame: Frame object.

        Raises:
            TerminationRequested: To unwind a blocked read when the target is
                waiting for input.
        """
        _ = frame
        if self._shutdown_requested:
            # A repeated signal must not restart shutdown or duplicate work. The
            # handler performs no I/O: logging from a signal handler can deadlock if
            # the interrupted frame holds a logging lock.
            return

        self._shutdown_requested = True
        self._shutdown_signum = signum

        if self._interruptible:
            raise TerminationRequested(signum)

    @classmethod
    def invoke(  # type: ignore[override]
        cls,
        *,
        about: bool = False,
        about_format: str | None = None,
        config: _ConfigInput | None = None,
        file_input: t.IO[str] | None = None,
    ) -> None:  # ty:ignore[invalid-method-override]
        """Invoke the target.

        Args:
            about: Display package metadata and settings.
            about_format: Specify output style for `--about`.
            config: Configuration file location or 'ENV' to use environment
                variables. Accepts multiple inputs as a tuple.
            file_input: Optional file to read input from.
        """
        super().invoke(about=about, about_format=about_format)
        cls.print_version(print_fn=cls.logger.info)
        config = config or _ConfigInput()

        target = cls(
            config=config.config,
            validate_config=True,
            parse_env_config=config.parse_env,
        )
        target.listen(file_input)

    @classmethod
    def get_singer_command(cls) -> click.Command:
        """Execute standard CLI handler for taps.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
        command.help = "Execute the Singer target."
        command.params.extend(
            [
                click.Option(
                    ["--input", "file_input"],
                    help="A path to read messages from instead of from standard in.",
                    type=click.File("r"),
                ),
            ],
        )

        return command

    @classmethod
    def append_builtin_config(cls, config_jsonschema: dict) -> None:
        """Appends built-in config to `config_jsonschema` if not already set.

        To customize or disable this behavior, developers may either override this class
        method or override the `capabilities` property to disabled any unwanted
        built-in capabilities.

        For all except very advanced use cases, we recommend leaving these
        implementations "as-is", since this provides the most choice to users and is
        the most "future proof" in terms of taking advantage of built-in capabilities
        which may be added in the future.

        Args:
            config_jsonschema: [description]
        """

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        _merge_missing(ADD_RECORD_METADATA_CONFIG, config_jsonschema)
        _merge_missing(TARGET_LOAD_METHOD_CONFIG, config_jsonschema)
        _merge_missing(TARGET_BATCH_SIZE_ROWS_CONFIG, config_jsonschema)

        capabilities = cls.capabilities

        if PluginCapabilities.ACTIVATE_VERSION in capabilities:
            _merge_missing(ACTIVATE_VERSION_CONFIG, config_jsonschema)

        if PluginCapabilities.BATCH in capabilities:
            _merge_missing(BATCH_CONFIG, config_jsonschema)

        if TargetCapabilities.VALIDATE_RECORDS in capabilities:
            _merge_missing(TARGET_VALIDATE_RECORDS_CONFIG, config_jsonschema)

        super().append_builtin_config(config_jsonschema)


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for moved SQL classes.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    if name == "SQLTarget":
        warnings.warn(
            f"Importing {name} from singer_sdk.target_base is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLTarget  # noqa: PLC0415

        return SQLTarget

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
