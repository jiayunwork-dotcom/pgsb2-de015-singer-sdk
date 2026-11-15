"""Runtime controller for composite incremental replication cursors."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._composite_cursor import (
    compare_cursor_keys,
    cursor_keys,
    derive_cursor_fields,
    get_record_cursor_values,
    parse_start_datetime,
    scalar_key,
    state_contains_composite_keys,
    validate_composite_state,
    validate_start_value,
    write_composite_bookmark,
)
from singer_sdk.singerlib.catalog import REPLICATION_INCREMENTAL

if t.TYPE_CHECKING:
    from collections.abc import Mapping

    from singer_sdk import Stream
    from singer_sdk.helpers._composite_cursor import CursorField

FILTER_NONE = "none"
FILTER_SCALAR = "scalar"
FILTER_COMPOSITE = "composite"


@dataclass(frozen=True, slots=True)
class StartingCursor:
    """The fixed starting position for one stream partition."""

    filter_mode: str
    """One of ``none``, ``scalar`` or ``composite``."""

    composite_values: tuple[t.Any, ...] | None
    """Complete raw cursor values, when resuming from a composite bookmark."""

    composite_keys: tuple[t.Any, ...] | None
    """Normalized cursor keys for the composite bookmark."""

    scalar_value: t.Any
    """The scalar primary-key start value exposed by the legacy getter."""

    scalar_key: t.Any
    """Normalized scalar start value used for prefix filtering."""


@dataclass(frozen=True, slots=True)
class _SortState:
    keys: tuple[t.Any, ...]
    values: tuple[t.Any, ...]
    source_index: int


def context_key(context: Mapping[str, t.Any] | None) -> tuple[t.Any, ...]:
    """Build a hashable key for a partition context.

    Args:
        context: Stream partition context.

    Returns:
        A stable, hashable representation of the context.
    """
    if not context:
        return ()
    return tuple(sorted((str(key), repr(value)) for key, value in context.items()))


class CompositeCursorController:
    """Apply composite cursor validation, ordering and bookmarks for a stream."""

    def __init__(self, stream: Stream) -> None:
        """Initialize the controller.

        Args:
            stream: The stream that enables ``replication_key_tiebreakers``.
        """
        self.stream = stream
        self.fields: tuple[CursorField, ...] = derive_cursor_fields(stream)
        self._starting: dict[tuple[t.Any, ...], StartingCursor] = {}
        self._sort_states: dict[tuple[t.Any, ...], _SortState] = {}

    @property
    def stream_name(self) -> str:
        """Stream name used in messages."""
        return self.stream.name

    @property
    def tiebreaker_names(self) -> list[str]:
        """Tiebreaker field names, in declaration order."""
        return [field.name for field in self.fields[1:]]

    def reset(self) -> None:
        """Clear cached starting positions and ordering state.

        Called at the beginning of each :meth:`Stream.sync` call so getters
        are fixed for the duration of that sync only.
        """
        self._starting.clear()
        self._sort_states.clear()

    def validate_mode(self, *, batch_enabled: bool) -> None:
        """Validate that composite cursors can be used for this stream.

        Args:
            batch_enabled: Whether BATCH mode is enabled for this stream.

        Raises:
            ValueError: If any prerequisite is not met.
        """
        stream = self.stream
        name = self.stream_name

        if batch_enabled:
            msg = (
                f"Stream '{name}' enables replication_key_tiebreakers but the tap "
                "is running in BATCH mode. Composite cursors are only supported "
                "for per-record RECORD collection."
            )
            raise ValueError(msg)

        if stream.replication_method != REPLICATION_INCREMENTAL:
            msg = (
                f"Stream '{name}' enables replication_key_tiebreakers but is not "
                f"incremental (replication_method={stream.replication_method!r})."
            )
            raise ValueError(msg)

        if not stream.is_sorted:
            msg = (
                f"Stream '{name}' enables replication_key_tiebreakers but "
                "is_sorted is not True."
            )
            raise ValueError(msg)

        if not stream.check_sorted:
            msg = (
                f"Stream '{name}' enables replication_key_tiebreakers but "
                "check_sorted is not True."
            )
            raise ValueError(msg)

        if stream.state_partitioning_keys is not None:
            msg = (
                f"Stream '{name}' enables replication_key_tiebreakers but "
                "state_partitioning_keys is not None. Composite cursors require "
                "one bookmark per stream partition."
            )
            raise ValueError(msg)

    def validate_state(self) -> None:
        """Validate persisted composite state for this stream.

        Raises:
            ValueError: If any stream or partition bookmark is corrupt or does
                not match the current configuration or schema.
        """
        stream_state = self.stream.tap_state.get("bookmarks", {}).get(
            self.stream_name,
            {},
        )

        validate_composite_state(
            stream_state,
            stream_name=self.stream_name,
            replication_key=self.stream.replication_key,
            fields=self.fields,
            context=None,
        )

        partitions = stream_state.get("partitions", [])
        if not isinstance(partitions, list):  # pragma: no cover - defensive
            msg = (
                f"State partitions for stream '{self.stream_name}' must be a list; "
                "cannot validate composite replication cursor state."
            )
            raise ValueError(msg)  # noqa: TRY004

        for partition_state in partitions:
            if not isinstance(partition_state, dict):  # pragma: no cover - defensive
                msg = (
                    f"State partition entry for stream '{self.stream_name}' is not "
                    "a dictionary; cannot validate composite replication cursor "
                    "state."
                )
                raise ValueError(msg)  # noqa: TRY004
            validate_composite_state(
                partition_state,
                stream_name=self.stream_name,
                replication_key=self.stream.replication_key,
                fields=self.fields,
                context=partition_state.get("context"),
            )

    def get_starting(
        self,
        context: Mapping[str, t.Any] | None,
    ) -> StartingCursor:
        """Return the frozen starting cursor for a partition.

        The value is computed on first access at the beginning of the partition
        and never changes as bookmarks advance during the sync.

        Args:
            context: Stream partition context.

        Returns:
            The fixed starting cursor.

        Raises:
            ValueError: If state is corrupt or incompatible.
        """  # noqa: DOC502
        key = context_key(context)
        if key not in self._starting:
            self._starting[key] = self._compute_starting(context)
        return self._starting[key]

    def get_starting_values(
        self,
        context: Mapping[str, t.Any] | None,
    ) -> tuple[t.Any, ...] | None:
        """Return the complete starting cursor tuple, if any.

        Args:
            context: Stream partition context.

        Returns:
            Raw composite cursor values, or ``None`` when resuming from a
            scalar boundary or starting fresh.
        """
        starting = self.get_starting(context)
        if starting.filter_mode != FILTER_COMPOSITE:
            return None
        return starting.composite_values

    def _compute_starting(
        self,
        context: Mapping[str, t.Any] | None,
    ) -> StartingCursor:
        stream = self.stream
        state = stream.state_manager.get_context_state(context)
        composite_values = validate_composite_state(
            state,
            stream_name=self.stream_name,
            replication_key=stream.replication_key,
            fields=self.fields,
            context=context,
        )

        start_date = stream.config.get("start_date")
        primary_field = self.fields[0]

        if composite_values is not None:
            if primary_field.kind == "date-time" and start_date:
                saved_instant = parse_start_datetime(composite_values[0])
                start_instant = parse_start_datetime(start_date)
                if (
                    start_instant is not None
                    and saved_instant is not None
                    and start_instant > saved_instant
                ):
                    return StartingCursor(
                        filter_mode=FILTER_SCALAR,
                        composite_values=None,
                        composite_keys=None,
                        scalar_value=start_date,
                        scalar_key=start_instant,
                    )
            return StartingCursor(
                filter_mode=FILTER_COMPOSITE,
                composite_values=composite_values,
                composite_keys=cursor_keys(
                    composite_values,
                    self.fields,
                    stream_name=self.stream_name,
                    context=context,
                ),
                scalar_value=composite_values[0],
                scalar_key=None,
            )

        # Legacy scalar bookmark. Accept any non-null JSON scalar, including
        # falsey values such as 0 and the empty string. Legacy state may omit
        # the ``replication_key`` name entirely.
        replication_key = stream.replication_key
        legacy_value = state.get("replication_key_value")
        stored_replication_key = state.get("replication_key")
        has_legacy = (
            replication_key is not None
            and (
                stored_replication_key is None
                or stored_replication_key == replication_key
            )
            and legacy_value is not None
            and not state_contains_composite_keys(state)
        )

        if has_legacy:
            legacy_primary = validate_start_value(
                legacy_value,
                primary_field,
                stream_name=self.stream_name,
                context=context,
            )
            legacy_key = scalar_key(
                legacy_primary,
                primary_field,
                stream_name=self.stream_name,
                context=context,
            )

            if primary_field.kind == "date-time" and start_date:
                start_instant = parse_start_datetime(start_date)
                if start_instant is not None and start_instant > legacy_key:
                    return StartingCursor(
                        filter_mode=FILTER_SCALAR,
                        composite_values=None,
                        composite_keys=None,
                        scalar_value=start_date,
                        scalar_key=start_instant,
                    )

            return StartingCursor(
                filter_mode=FILTER_SCALAR,
                composite_values=None,
                composite_keys=None,
                scalar_value=legacy_primary,
                scalar_key=legacy_key,
            )

        return StartingCursor(
            filter_mode=FILTER_NONE,
            composite_values=None,
            composite_keys=None,
            # Preserve the legacy scalar starting point: when no bookmark exists,
            # ``start_date`` is the scalar start value. It only acts as a
            # record-level lower bound for date-time primary keys.
            scalar_value=start_date,
            scalar_key=None,
        )

    def extract(
        self,
        record: Mapping[str, t.Any],
        context: Mapping[str, t.Any] | None,
        source_index: int,
    ) -> tuple[t.Any, ...]:
        """Extract and validate the complete cursor from a record.

        Args:
            record: Record after ``post_process``.
            context: Partition context.
            source_index: One-based source record index in the partition.

        Returns:
            Raw cursor values, primary first.

        Raises:
            ValueError: If a field is missing or invalid.
        """  # noqa: DOC502
        return get_record_cursor_values(
            record,
            self.fields,
            stream_name=self.stream_name,
            context=context,
            source_index=source_index,
        )

    def normalize(
        self,
        values: tuple[t.Any, ...],
        context: Mapping[str, t.Any] | None,
        source_index: int,
    ) -> tuple[t.Any, ...]:
        """Normalize cursor values for comparison.

        Args:
            values: Raw cursor values, primary first.
            context: Partition context.
            source_index: One-based source record index in the partition.

        Returns:
            Comparable keys.

        Raises:
            ValueError: If a value is invalid.
        """  # noqa: DOC502
        return cursor_keys(
            values,
            self.fields,
            stream_name=self.stream_name,
            context=context,
            source_index=source_index,
        )

    def check_sorted(
        self,
        keys: tuple[t.Any, ...],
        values: tuple[t.Any, ...],
        context: Mapping[str, t.Any] | None,
        source_index: int,
    ) -> None:
        """Verify strictly increasing positions within a partition.

        Args:
            keys: Normalized cursor keys for the current record.
            values: Raw cursor values for the current record.
            context: Partition context.
            source_index: One-based source record index in the partition.

        Raises:
            InvalidStreamSortException: If the position is equal to or less
                than the previous record's position.
        """
        partition_key = context_key(context)
        previous = self._sort_states.get(partition_key)
        if previous is not None and compare_cursor_keys(keys, previous.keys) <= 0:
            location = f"stream '{self.stream_name}'"
            if context:
                location += f", partition {dict(context)!r}"
            msg = (
                f"Unsorted composite replication cursor in {location}: "
                f"source record #{source_index} at position {values!r} is not "
                f"strictly greater than source record #{previous.source_index} "
                f"at position {previous.values!r} (replication_key "
                f"'{self.fields[0].name}', tiebreakers "
                f"{self.tiebreaker_names!r})."
            )
            raise InvalidStreamSortException(msg)

        self._sort_states[partition_key] = _SortState(
            keys=keys,
            values=values,
            source_index=source_index,
        )

    def should_skip(
        self,
        keys: tuple[t.Any, ...],
        context: Mapping[str, t.Any] | None,
    ) -> bool:
        """Return whether a record is part of the already-processed prefix.

        Args:
            keys: Normalized cursor keys for the record.
            context: Partition context.

        Returns:
            True if the record must not be emitted.
        """
        starting = self.get_starting(context)
        if starting.filter_mode == FILTER_COMPOSITE:
            composite_keys = t.cast("tuple[t.Any, ...]", starting.composite_keys)
            return compare_cursor_keys(keys, composite_keys) <= 0
        if starting.filter_mode == FILTER_SCALAR:
            return bool(keys[0] < starting.scalar_key)
        return False

    def write_bookmark(
        self,
        state: dict[str, t.Any],
        values: tuple[t.Any, ...],
    ) -> None:
        """Persist a complete cursor atomically.

        Args:
            state: Writable stream or partition state.
            values: Raw cursor values from the successfully processed record.
        """
        write_composite_bookmark(state, fields=self.fields, values=values)


def validate_disabled_composite_state(stream: Stream) -> None:
    """Reject composite state when composite mode is disabled.

    Args:
        stream: A stream without ``replication_key_tiebreakers``.

    Raises:
        ValueError: If composite cursor keys are present in stream state.
    """
    stream_state = stream.tap_state.get("bookmarks", {}).get(stream.name, {})

    partitions = stream_state.get("partitions", [])
    states: list[tuple[Mapping[str, t.Any], Mapping[str, t.Any] | None]] = [
        (stream_state, None),
        *[
            (partition_state, partition_state.get("context"))
            for partition_state in (partitions if isinstance(partitions, list) else [])
            if isinstance(partition_state, dict)
        ],
    ]

    for state, context in states:
        if not state_contains_composite_keys(state):
            continue
        location = f"stream '{stream.name}'"
        if context:
            location += f", partition {dict(context)!r}"
        msg = (
            f"Composite replication cursor state exists for {location} but "
            "replication_key_tiebreakers is not enabled on this stream. Refusing "
            "to resume from an incompatible bookmark."
        )
        raise ValueError(msg)


def get_composite_controller(stream: Stream) -> CompositeCursorController | None:
    """Return a cached controller when composite mode is enabled.

    Args:
        stream: The stream to inspect.

    Returns:
        A controller, or ``None`` when tiebreakers are not configured.
    """
    tiebreakers = stream.replication_key_tiebreakers
    if tiebreakers is None:
        return None
    return CompositeCursorController(stream)


def composite_signature(stream: Stream) -> tuple[t.Any, ...]:
    """Return a signature that changes when cursor configuration changes.

    Args:
        stream: The stream to inspect.

    Returns:
        A hashable signature.
    """
    tiebreakers = stream.replication_key_tiebreakers
    if tiebreakers is None:
        names: tuple[str, ...] = ()
    elif isinstance(tiebreakers, (str, bytes)) or not isinstance(
        tiebreakers, t.Sequence
    ):
        # Invalid configuration is reported by validation, not the signature.
        names = (repr(tiebreakers),)
    else:
        names = tuple(tiebreakers)
    properties = stream.effective_schema.get("properties", {})
    return (
        stream.replication_key,
        names,
        properties.get(stream.replication_key) if stream.replication_key else None,
        tuple(properties.get(name) for name in names),
    )


__all__ = [
    "CompositeCursorController",
    "StartingCursor",
    "composite_signature",
    "context_key",
    "get_composite_controller",
    "validate_disabled_composite_state",
]
