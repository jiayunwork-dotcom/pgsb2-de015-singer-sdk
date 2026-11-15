"""Tests for composite incremental replication cursors."""

from __future__ import annotations

import datetime
import json
import tracemalloc
import typing as t

import pytest
from typing_extensions import override

from singer_sdk import Stream, Tap
from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.streams.core import REPLICATION_FULL_TABLE
from singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers import types


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------


class RecordingWriter:
    """Collect Singer messages as plain JSON-compatible dictionaries."""

    def __init__(self) -> None:
        """Initialize the writer."""
        self.messages: list[dict[str, t.Any]] = []

    def write_message(self, message: t.Any) -> None:
        """Record a message.

        Args:
            message: A Singer message object.
        """
        self.messages.append(json.loads(json.dumps(message.to_dict(), default=str)))

    @property
    def records(self) -> list[dict[str, t.Any]]:
        """Return RECORD payloads in emission order."""
        return [
            message["record"]
            for message in self.messages
            if str(message["type"]).endswith("RECORD")
        ]

    @property
    def states(self) -> list[dict[str, t.Any]]:
        """Return STATE payloads in emission order."""
        return [
            message["value"]
            for message in self.messages
            if str(message["type"]).endswith("STATE")
        ]

    def delivered_records(self) -> list[dict[str, t.Any]]:
        """Return every record emitted before each confirmed STATE.

        Used to model downstream acknowledgement: a target confirms records up
        to the STATE that follows them.

        Returns:
            Records in delivery order.
        """
        return self.records


def _int_schema(*names: str) -> dict:
    return PropertiesList(
        *(Property(name, IntegerType, required=True) for name in names),
    ).to_dict()


class OrderedSourceStream(Stream):
    """Sorted incremental stream backed by an in-memory ordered source."""

    name = "events"
    replication_key = "ts"
    replication_key_tiebreakers: t.Sequence[str] | None = ("seq", "id")
    is_sorted = True
    schema = PropertiesList(
        Property("ts", IntegerType, required=True),
        Property("seq", IntegerType, required=True),
        Property("id", StringType, required=True),
        Property("value", StringType),
    ).to_dict()

    source: t.ClassVar[list[dict[str, t.Any]]] = []
    received_contexts: t.ClassVar[list[t.Any]] = []
    post_process_behavior: t.ClassVar[t.Callable[[dict], dict | None] | None] = None

    def get_records(
        self,
        context: types.Context | None,
    ) -> Iterable[dict[str, t.Any]]:
        """Yield the configured source records.

        Args:
            context: Stream partition context.

        Yields:
            Record dictionaries.
        """
        self.received_contexts.append(context)
        yield from (dict(record) for record in self.source)

    @override
    def post_process(
        self,
        row: types.Record,
        context: types.Context | None = None,
    ) -> types.Record | None:
        if self.post_process_behavior is not None:
            return self.post_process_behavior(row)
        return row


class OrderedSourceTap(Tap):
    """Tap exposing the ordered source stream."""

    name = "composite-test-tap"
    config_jsonschema = PropertiesList(
        Property("start_date", DateTimeType),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Discover streams.

        Returns:
            A single test stream.
        """
        return [OrderedSourceStream(self)]


def make_tap(
    source: list[dict[str, t.Any]],
    *,
    state: dict | None = None,
    config: dict | None = None,
    writer: RecordingWriter | None = None,
) -> tuple[OrderedSourceTap, OrderedSourceStream, RecordingWriter]:
    """Build a tap, its stream and a recording writer.

    Args:
        source: Records returned by ``get_records``.
        state: Initial tap state.
        config: Tap config.
        writer: Optional pre-built writer.

    Returns:
        A tuple of ``(tap, stream, writer)``.
    """
    OrderedSourceStream.source = [dict(record) for record in source]
    OrderedSourceStream.received_contexts = []
    OrderedSourceStream.post_process_behavior = None
    tap = OrderedSourceTap(
        config=config or {},
        state=json.loads(json.dumps(state)) if state is not None else None,
        message_writer=writer or RecordingWriter(),
    )
    return tap, tap.streams[OrderedSourceStream.name], tap.message_writer


def run_sync(
    source: list[dict[str, t.Any]],
    *,
    state: dict | None = None,
    config: dict | None = None,
) -> tuple[RecordingWriter, OrderedSourceTap]:
    """Run a full sync and return the writer and tap.

    Args:
        source: Source records.
        state: Initial tap state.
        config: Tap config.

    Returns:
        The recording writer and completed tap.
    """
    tap, _, writer = make_tap(source, state=state, config=config)
    tap.sync_all()
    return writer, tap


def build_records(
    groups: int = 300,
    per_group: int = 400,
    *,
    string_id: bool = False,
) -> list[dict[str, t.Any]]:
    """Build ordered records with many duplicates per primary key.

    Args:
        groups: Number of distinct ``ts`` values.
        per_group: Records per ``ts`` value.
        string_id: Whether to use string tiebreakers.

    Returns:
        Ordered records.
    """
    records: list[dict[str, t.Any]] = [
        {
            "ts": group,
            "seq": seq,
            "id": f"{seq:06d}" if string_id else str(seq),
            "value": f"{group}:{seq}",
        }
        for group in range(groups)
        for seq in range(per_group)
    ]
    return records


BASE_RECORDS: list[dict[str, t.Any]] = [
    {"ts": 0, "seq": 0, "id": "", "value": "empty-string-id"},
    {"ts": 0, "seq": 0, "id": "a", "value": "same-primary"},
    {"ts": 1, "seq": 0, "id": "null", "value": "null-id"},
    {"ts": 1, "seq": 2**80, "id": "big", "value": "big-int"},
]


# ---------------------------------------------------------------------------
# Behaviour tests
# ---------------------------------------------------------------------------


def test_composite_cursor_getters_before_and_after_sync() -> None:
    """The composite getter returns a complete tuple and the scalar getter works."""
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 1,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [1, 5, "abc"],
            },
        },
    }
    _tap, stream, _ = make_tap([], state=state)

    assert stream.get_starting_replication_key_values(None) == (1, 5, "abc")
    assert stream.get_starting_replication_key_value(None) == 1
    assert stream.replication_key_tiebreakers == ("seq", "id")


def test_composite_disabled_getter_is_none() -> None:
    """Streams without tiebreakers keep legacy behaviour."""
    _tap, stream, _ = make_tap(BASE_RECORDS)
    stream.replication_key_tiebreakers = None

    assert stream.get_starting_replication_key_values(None) is None
    assert stream.get_starting_replication_key_value(None) is None


def test_composite_full_sync_emits_all_records_in_order() -> None:
    """A fresh composite sync emits all records and the final complete position."""
    records = [
        {"ts": 0, "seq": 0, "id": "", "value": "a"},
        {"ts": 0, "seq": 0, "id": "b", "value": "b"},
        {"ts": 0, "seq": 2**80, "id": "c", "value": "c"},
        {"ts": 1, "seq": 0, "id": "a", "value": "d"},
    ]
    writer, tap = run_sync(records)

    assert writer.records == records
    state = tap.state["bookmarks"]["events"]
    assert state["replication_key_value"] == 1
    assert state["replication_key_values"] == [1, 0, "a"]
    assert state["replication_key_tiebreakers"] == ["seq", "id"]


def test_composite_resume_skips_prefix() -> None:
    """Records at or before the saved composite position are not re-emitted."""
    records = [
        {"ts": 0, "seq": 0, "id": "a"},
        {"ts": 0, "seq": 1, "id": "b"},
        {"ts": 0, "seq": 1, "id": "c"},
        {"ts": 1, "seq": 0, "id": "a"},
    ]
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 0,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [0, 1, "b"],
            },
        },
    }
    writer, _ = run_sync(records, state=state)

    assert writer.records == [
        {"ts": 0, "seq": 1, "id": "c"},
        {"ts": 1, "seq": 0, "id": "a"},
    ]


def test_composite_legacy_scalar_migration_replays_equal_group() -> None:
    """A legacy scalar bookmark skips only strictly smaller primary values."""
    records = [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 1, "seq": 1, "id": "b"},
        {"ts": 2, "seq": 0, "id": "a"},
    ]
    state = {
        "bookmarks": {
            "events": {"replication_key": "ts", "replication_key_value": 1},
        },
    }
    tap, stream, writer = make_tap(records, state=state)

    assert stream.get_starting_replication_key_values(None) is None
    assert stream.get_starting_replication_key_value(None) == 1

    tap.sync_all()
    assert writer.records == records
    assert tap.state["bookmarks"]["events"]["replication_key_values"] == [2, 0, "a"]


def test_composite_legacy_scalar_migration_is_not_mutated_until_sync() -> None:
    """Input state objects are never modified by the tap."""
    original = {
        "bookmarks": {"events": {"replication_key": "ts", "replication_key_value": 1}},
    }
    snapshot = json.loads(json.dumps(original))
    tap, _, _ = make_tap([{"ts": 1, "seq": 0, "id": "a"}], state=original)
    assert original == snapshot

    tap.sync_all()
    assert original == snapshot


def test_composite_no_state_no_start_date() -> None:
    """Both getters return None without state or start_date."""
    _tap, stream, _ = make_tap([{"ts": 1, "seq": 0, "id": "a"}])
    assert stream.get_starting_replication_key_values(None) is None
    assert stream.get_starting_replication_key_value(None) is None


def test_composite_empty_source_keeps_progress() -> None:
    """An empty source does not clear or advance the bookmark."""
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 5,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [5, 9, "z"],
            },
        },
    }
    writer, tap = run_sync([], state=state)
    assert writer.records == []
    assert tap.state["bookmarks"]["events"]["replication_key_values"] == [5, 9, "z"]


def test_composite_source_only_returns_restored_prefix() -> None:
    """A source returning only the restored prefix keeps the saved bookmark."""
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 2,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [2, 1, "b"],
            },
        },
    }
    prefix = [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 2, "seq": 1, "id": "a"},
        {"ts": 2, "seq": 1, "id": "b"},
    ]
    writer, tap = run_sync(prefix, state=state)

    assert writer.records == []
    assert tap.state["bookmarks"]["events"]["replication_key_values"] == [2, 1, "b"]


def test_composite_duplicate_position_raises() -> None:
    """Equal complete positions raise InvalidStreamSortException."""
    records = [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 1, "seq": 0, "id": "a"},
    ]
    with pytest.raises(InvalidStreamSortException, match="source record #2"):
        run_sync(records)


def test_composite_regressed_position_raises() -> None:
    """Smaller complete positions raise InvalidStreamSortException."""
    records = [
        {"ts": 1, "seq": 1, "id": "b"},
        {"ts": 1, "seq": 0, "id": "z"},
    ]
    with pytest.raises(InvalidStreamSortException, match="#2"):
        run_sync(records)


def test_composite_equal_primary_with_different_tiebreakers_is_sorted() -> None:
    """Equal primary keys are allowed when tiebreakers strictly increase."""
    records = [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 1, "seq": 0, "id": "b"},
        {"ts": 1, "seq": 0, "id": "c"},
    ]
    writer, _ = run_sync(records)
    assert writer.records == records


def test_composite_sort_error_detected_before_children() -> None:
    """Sort violations are detected before child streams run."""
    parent_records = [
        {"ts": 1, "seq": 1, "id": "a"},
        {"ts": 1, "seq": 0, "id": "b"},
    ]
    child_contexts: list[dict] = []

    class Parent(OrderedSourceStream):
        name = "parent"

        def get_child_context(self, record, context):  # noqa: ARG002
            return {"id": record["id"], "ts": record["ts"]}

    class Child(Stream):
        name = "child"
        parent_stream_type = Parent
        replication_key = "ts"
        is_sorted = True
        schema = PropertiesList(
            Property("ts", IntegerType, required=True),
            Property("seq", IntegerType, required=True),
            Property("id", StringType, required=True),
        ).to_dict()

        def get_records(self, context):
            child_contexts.append(dict(context or {}))
            yield {"ts": 0, "seq": 0, "id": "x"}

    class TapWithChild(Tap):
        name = "parent-child-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                Parent and child streams.
            """
            return [Parent(self), Child(self)]

    Parent.source = parent_records
    tap = TapWithChild(config={}, state=None, message_writer=RecordingWriter())

    with pytest.raises(InvalidStreamSortException):
        tap.streams["parent"].sync()

    # The first valid record may sync its child; the invalid record must not.
    assert child_contexts == [{"id": "a", "ts": 1}]


def test_composite_invalid_values_raise_with_location() -> None:
    """Invalid cursor values report stream, partition, field and source index."""
    cases = [
        ({"ts": True, "seq": 0, "id": "a"}, "ts", "integer"),
        ({"ts": 1.0, "seq": 0, "id": "a"}, "ts", "integer"),
        ({"ts": 1, "seq": "0", "id": "a"}, "seq", "integer"),
        ({"ts": 1, "seq": 0, "id": 5}, "id", "string"),
        ({"ts": 1, "seq": None, "id": "a"}, "seq", "null"),
        ({"ts": 1, "id": "a"}, "seq", "missing"),
    ]
    for record, field, _label in cases:
        with pytest.raises(ValueError, match=f"'{field}'"):
            run_sync([{"ts": 0, "seq": -1, "id": "x"}, record])
        with pytest.raises(ValueError, match="source record #2"):
            run_sync([{"ts": 0, "seq": -1, "id": "x"}, record])
        assert field


def test_composite_invalid_config_raises() -> None:
    """Invalid tiebreaker configuration raises before source access."""
    tap, stream, _ = make_tap(BASE_RECORDS)
    stream.replication_key_tiebreakers = "seq"
    with pytest.raises(ValueError, match="must be a non-empty sequence"):
        tap.sync_all()

    stream.replication_key_tiebreakers = ()
    with pytest.raises(ValueError, match="non-empty"):
        tap.sync_all()

    stream.replication_key_tiebreakers = ("seq", "seq")
    with pytest.raises(ValueError, match="duplicate"):
        tap.sync_all()

    stream.replication_key_tiebreakers = ("ts",)
    with pytest.raises(ValueError, match="replication_key"):
        tap.sync_all()

    stream.replication_key_tiebreakers = ("missing",)
    with pytest.raises(ValueError, match="missing"):
        tap.sync_all()


def test_composite_requires_sorted_and_no_partitioning() -> None:
    """Unsupported modes raise ValueError before reading source records."""

    class UnsortedStream(OrderedSourceStream):
        name = "events"
        is_sorted = False

    class NoCheckStream(OrderedSourceStream):
        name = "events"
        check_sorted = False

    class PartitionedStream(OrderedSourceStream):
        name = "events"
        state_partitioning_keys: t.ClassVar[list[str]] = ["store"]

    for stream_type, match in [
        (UnsortedStream, "is_sorted"),
        (NoCheckStream, "check_sorted"),
        (PartitionedStream, "state_partitioning_keys"),
    ]:
        stream_type.source = [dict(record) for record in BASE_RECORDS]
        stream_type.received_contexts = []

        def make_tap_for_type(
            stream_type: type[Stream] = stream_type,
        ) -> Tap:
            class ModeTap(Tap):
                name = "mode-tap"

                def discover_streams(self) -> list[Stream]:
                    """Discover streams.

                    Returns:
                        A single stream of the parametrized type.
                    """
                    return [stream_type(self)]

            return ModeTap(config={}, state=None, message_writer=RecordingWriter())

        tap = make_tap_for_type()
        with pytest.raises(ValueError, match=match):
            tap.sync_all()
        assert stream_type.received_contexts == []


def test_composite_batch_mode_rejected() -> None:
    """BATCH mode is rejected before source access."""
    tap, _, _ = make_tap(
        BASE_RECORDS,
        config={
            "batch_config": {
                "encoding": {"format": "jsonl"},
                "storage": {"root": "/tmp/composite-batch"},  # noqa: S108
            }
        },
    )
    with pytest.raises(ValueError, match="BATCH"):
        tap.sync_all()
    assert OrderedSourceStream.received_contexts == []


def test_composite_corrupt_state_raises() -> None:
    """Corrupt or incomplete composite state raises without re-running."""
    base = {
        "replication_key": "ts",
        "replication_key_value": 1,
        "replication_key_type": "integer",
        "replication_key_tiebreakers": ["seq", "id"],
        "replication_key_tiebreaker_types": ["integer", "string"],
        "replication_key_values": [1, 2, "x"],
    }
    for mutation, match in [
        ({"replication_key_tiebreakers": ["id", "seq"]}, "tiebreakers"),
        ({"replication_key_tiebreaker_types": ["string", "integer"]}, "types"),
        ({"replication_key_type": "string"}, "primary type"),
        ({"replication_key_values": [1, 2]}, "cursor value"),
        ({"replication_key_value": 9}, "primary"),
        ({"replication_key": "other"}, "replication_key"),
    ]:
        state = {"bookmarks": {"events": {**base, **mutation}}}
        tap, _, _ = make_tap(BASE_RECORDS, state=state)
        with pytest.raises(ValueError, match=match):
            tap.sync_all()
        assert OrderedSourceStream.received_contexts == []

    for missing in base:
        partial = {key: value for key, value in base.items() if key != missing}
        tap, _, _ = make_tap(BASE_RECORDS, state={"bookmarks": {"events": partial}})
        if missing in {"replication_key", "replication_key_value"}:
            # Without these, the state is treated as a legacy scalar bookmark and
            # the remaining composite keys are rejected as corrupt.
            expected = "Composite replication cursor state"
        else:
            expected = "incomplete"
        with pytest.raises(ValueError, match=expected):
            tap.sync_all()

    # Invalid value types in state
    tap, _, _ = make_tap(
        BASE_RECORDS,
        state={"bookmarks": {"events": {**base, "replication_key_values": [1, 2, 3]}}},
    )
    with pytest.raises(ValueError, match="string"):
        tap.sync_all()


def test_composite_state_with_disabled_mode_raises() -> None:
    """Composite state with tiebreakers disabled raises."""
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 1,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [1, 2, "x"],
            },
        },
    }
    tap, stream, _ = make_tap(BASE_RECORDS, state=state)
    stream.replication_key_tiebreakers = None
    with pytest.raises(ValueError, match="not enabled"):
        tap.sync_all()


@pytest.mark.parametrize(
    "saved,saved_instant",
    [
        ("2021-01-01T00:00:00Z", "2021-01-01T00:00:00Z"),
        ("2021-01-01T01:00:00+01:00", "2021-01-01T00:00:00Z"),
    ],
)
def test_composite_datetime_start_date_handling(saved: str, saved_instant: str) -> None:
    """start_date earlier, equal, and later than a date-time bookmark."""

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": "2021-01-01T00:00:00Z", "seq": 0}
            yield {"ts": "2021-01-02T00:00:00Z", "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-tap"
        config_jsonschema = PropertiesList(
            Property("start_date", DateTimeType),
        ).to_dict()

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A single date-time stream.
            """
            return [DateTimeStream(self)]

    base_state = {
        "bookmarks": {
            "dt": {
                "replication_key": "ts",
                "replication_key_value": saved,
                "replication_key_type": "date-time",
                "replication_key_tiebreakers": ["seq"],
                "replication_key_tiebreaker_types": ["integer"],
                "replication_key_values": [saved, 0],
            },
        },
    }

    # Earlier start_date: composite cursor is retained.
    writer = RecordingWriter()
    tap = DateTimeTap(
        config={"start_date": "2020-01-01"},
        state=json.loads(json.dumps(base_state)),
        message_writer=writer,
    )
    stream = tap.streams["dt"]
    assert stream.get_starting_replication_key_values(None) == (saved, 0)
    tap.sync_all()
    # The record at the saved position is skipped; only the next one is emitted.
    assert writer.records == [{"ts": "2021-01-02T00:00:00Z", "seq": 0}]

    # Equal instant expressed with a different offset: composite retained.
    writer = RecordingWriter()
    tap = DateTimeTap(
        config={"start_date": saved_instant},
        state=json.loads(json.dumps(base_state)),
        message_writer=writer,
    )
    stream = tap.streams["dt"]
    assert stream.get_starting_replication_key_values(None) == (saved, 0)

    # Later start_date: scalar boundary is used and the getter is None.
    writer = RecordingWriter()
    tap = DateTimeTap(
        config={"start_date": "2021-01-01T06:00:00Z"},
        state=json.loads(json.dumps(base_state)),
        message_writer=writer,
    )
    stream = tap.streams["dt"]
    assert stream.get_starting_replication_key_values(None) is None
    assert stream.get_starting_replication_key_value(None) == "2021-01-01T06:00:00Z"
    tap.sync_all()
    # Records strictly before the start_date boundary are skipped.
    assert writer.records == [{"ts": "2021-01-02T00:00:00Z", "seq": 0}]


def test_composite_datetime_equal_instants_are_sorted_equal() -> None:
    """Different textual offsets for the same instant compare equal."""

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": "2021-01-01T00:00:00Z", "seq": 0}
            yield {"ts": "2021-01-01T01:00:00+01:00", "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A single date-time stream.
            """
            return [DateTimeStream(self)]

    tap = DateTimeTap(config={}, state=None, message_writer=RecordingWriter())
    with pytest.raises(InvalidStreamSortException, match="#2"):
        tap.sync_all()


def test_composite_datetime_invalid_formats() -> None:
    """Invalid date-time cursors raise with source location."""
    invalid = [
        "2021-01-01T00:00:00",  # no offset
        "2021-01-01 00:00:00Z",  # space separator
        "2021-01-01T00:00:60Z",  # leap second
        "2021-01-01T00:00:00.1234567Z",  # >6 fractional digits
        "2021-01-01T00:00:00+24:00",  # bad offset
        "not-a-date",
    ]

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        source = invalid

        def get_records(self, context):  # noqa: ARG002
            for value in self.source:
                yield {"ts": value, "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A single date-time stream.
            """
            return [DateTimeStream(self)]

    for _value in invalid:
        tap = DateTimeTap(config={}, state=None, message_writer=RecordingWriter())
        with pytest.raises(ValueError, match="invalid date-time"):
            tap.sync_all()


def test_composite_string_tiebreaker_comparison_is_unicode_order() -> None:
    """String tiebreakers compare by Unicode code point, without conversion."""
    records = [
        {"ts": 1, "seq": 0, "id": ""},
        {"ts": 1, "seq": 0, "id": "A"},
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 1, "seq": 0, "id": "é"},
    ]
    writer, _ = run_sync(records)
    assert writer.records == records


def test_composite_string_and_integer_values_preserved() -> None:
    """Cursor values are not rewritten by JSON round-trips."""
    records = [
        {"ts": 2**70, "seq": 0, "id": ""},
        {"ts": 2**70, "seq": 1, "id": "0"},
    ]
    _writer, tap = run_sync(records)
    state = tap.state["bookmarks"]["events"]
    assert state["replication_key_values"] == [2**70, 1, "0"]
    restored = json.loads(json.dumps(tap.state))
    assert restored == tap.state


def test_composite_state_roundtrip_new_instance() -> None:
    """A JSON round-trip state resumes correctly in a new tap instance."""
    records = build_records(groups=3, per_group=5)
    _writer, tap = run_sync(records)
    state = json.loads(json.dumps(tap.state))

    writer2, _ = run_sync(records, state=state)
    assert writer2.records == []


def test_composite_mapping_does_not_affect_cursor() -> None:
    """Mappings may drop cursor fields; positions come from pre-map records."""
    state = {
        "bookmarks": {"events": {"replication_key": "ts", "replication_key_value": 0}}
    }
    records = [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 2, "seq": 0, "id": "b"},
    ]
    OrderedSourceStream.source = records
    tap = OrderedSourceTap(
        config={"stream_maps": {"events": {"value": None}}},
        state=state,
        message_writer=RecordingWriter(),
    )
    tap.sync_all()
    assert tap.state["bookmarks"]["events"]["replication_key_values"] == [2, 0, "b"]


# ---------------------------------------------------------------------------
# Delivery / STATE boundaries
# ---------------------------------------------------------------------------


def test_composite_state_never_ahead_of_records() -> None:
    """Every STATE represents exactly the records delivered before it."""
    records = build_records(groups=2, per_group=30)
    writer, _ = run_sync(records)

    last_position = -1
    delivered = 0
    for message in writer.messages:
        message_type = str(message["type"])
        if message_type.endswith("RECORD"):
            delivered += 1
        elif message_type.endswith("STATE"):
            values = message["value"]["bookmarks"]["events"].get(
                "replication_key_values",
            )
            if not values:
                continue
            index = next(
                i
                for i, record in enumerate(records)
                if (record["ts"], record["seq"], record["id"])
                == (values[0], values[1], values[2])
            )
            assert index == delivered - 1
            assert index > last_position
            last_position = index


def _run_with_frequency(
    records: list[dict[str, t.Any]],
    frequency: int,
    *,
    state: dict | None = None,
) -> RecordingWriter:
    """Run a sync with a reduced STATE_MSG_FREQUENCY.

    Args:
        records: Source records.
        frequency: STATE message frequency.
        state: Initial tap state.

    Returns:
        The recording writer.
    """
    original = OrderedSourceStream.STATE_MSG_FREQUENCY
    OrderedSourceStream.STATE_MSG_FREQUENCY = frequency
    try:
        OrderedSourceStream.source = [dict(record) for record in records]
        OrderedSourceStream.received_contexts = []
        OrderedSourceStream.post_process_behavior = None
        tap = OrderedSourceTap(
            config={},
            state=json.loads(json.dumps(state)) if state is not None else None,
            message_writer=RecordingWriter(),
        )
        tap.sync_all()
    finally:
        OrderedSourceStream.STATE_MSG_FREQUENCY = original
    return tap.message_writer


def test_composite_restart_at_confirmed_states_matches_single_run() -> None:
    """Splitting at confirmed STATEs and resuming yields the same delivery."""
    records = build_records(groups=2, per_group=12)
    full_writer = _run_with_frequency(records, 5)
    full_records = full_writer.records

    messages = full_writer.messages
    confirmed_states: list[dict] = []
    records_at_state: list[int] = []
    count = 0
    for message in messages:
        if str(message["type"]).endswith("RECORD"):
            count += 1
        elif str(message["type"]).endswith("STATE"):
            state = message["value"]
            if state["bookmarks"].get("events", {}).get("replication_key_values"):
                confirmed_states.append(json.loads(json.dumps(state)))
                records_at_state.append(count)

    assert confirmed_states

    # Multiple distinct boundaries must exist, not just the final one.
    assert len(confirmed_states) > 1

    # Resume from each confirmed boundary and collect the remaining (not yet
    # confirmed) records.
    for state, confirmed_count in zip(confirmed_states, records_at_state, strict=True):
        writer = _run_with_frequency(records, 5, state=json.loads(json.dumps(state)))
        # Records still emitted after the boundary must be a suffix of the full run.
        expected_suffix = full_records[confirmed_count:]
        assert writer.records == expected_suffix


def test_composite_unconfirmed_tail_is_replayable() -> None:
    """Records after the last confirmed STATE are replayed on resume."""
    records = build_records(groups=1, per_group=10)
    full_writer = _run_with_frequency(records, 3)

    confirmed: list[tuple[dict, int]] = []
    delivered = 0
    for message in full_writer.messages:
        if str(message["type"]).endswith("RECORD"):
            delivered += 1
        elif str(message["type"]).endswith("STATE"):
            confirmed.append((message["value"], delivered))

    # Use a STATE emitted mid-stream, before the final record; records after it
    # are the unconfirmed tail that must be replayed.
    mid_state, delivered_at_state = next(
        (state, count)
        for state, count in reversed(confirmed)
        if 0 < count < len(records)
    )
    assert delivered_at_state < len(records)

    writer = _run_with_frequency(
        records,
        3,
        state=json.loads(json.dumps(mid_state)),
    )
    assert writer.records == records[delivered_at_state:]


def test_composite_injected_error_preserves_last_safe_state() -> None:
    """Writer errors do not advance a STATE past the failing record."""

    class FailingWriter(RecordingWriter):
        fail_at = 5

        def write_message(self, message: t.Any) -> None:
            if (
                str(message.to_dict()["type"]).endswith("RECORD")
                and len(self.records) == self.fail_at
            ):
                msg = "boom"
                raise RuntimeError(msg)
            super().write_message(message)

    records = build_records(groups=1, per_group=10)
    OrderedSourceStream.source = records
    writer = FailingWriter()
    tap = OrderedSourceTap(config={}, state=None, message_writer=writer)

    with pytest.raises(RuntimeError, match="boom"):
        tap.sync_all()

    assert len(writer.records) == writer.fail_at
    state_values = tap.state["bookmarks"]["events"].get("replication_key_values")
    # The bookmark must not be at or past the failed record.
    assert state_values is not None
    assert (
        next(
            i
            for i, record in enumerate(records)
            if (record["ts"], record["seq"], record["id"])
            == (state_values[0], state_values[1], state_values[2])
        )
        == writer.fail_at - 1
    )


def test_composite_mapper_filter_and_one_to_many() -> None:
    """Filtered and multiplied map outputs still advance the pre-map cursor."""
    records = [
        {"ts": 1, "seq": 0, "id": "a", "value": "keep"},
        {"ts": 2, "seq": 0, "id": "b", "value": "drop"},
        {"ts": 3, "seq": 0, "id": "c", "value": "keep"},
    ]
    OrderedSourceStream.source = records
    tap = OrderedSourceTap(
        config={
            "stream_maps": {
                "events": {
                    "__filter__": "value == 'keep'",
                },
                "events_copy": {
                    "__source__": "events",
                    "value": None,
                },
            },
        },
        state=None,
        message_writer=RecordingWriter(),
    )
    tap.sync_all()
    state = tap.state["bookmarks"]["events"]
    assert state["replication_key_values"] == [3, 0, "c"]


def test_composite_child_streams_do_not_run_for_skipped_prefix() -> None:
    """Skipped prefix records never trigger child streams."""
    child_contexts: list[dict] = []

    class ParentStream(OrderedSourceStream):
        name = "parent"

        def get_child_context(self, record, context):  # noqa: ARG002
            return {"id": record["id"], "ts": record["ts"]}

    class ChildStream(Stream):
        name = "child"
        parent_stream_type = ParentStream
        replication_key = "ts"
        is_sorted = True
        schema = PropertiesList(
            Property("ts", IntegerType, required=True),
            Property("seq", IntegerType, required=True),
            Property("id", StringType, required=True),
        ).to_dict()

        def get_records(self, context):
            child_contexts.append(dict(context or {}))
            yield {"ts": 3, "seq": 0, "id": "c"}

    class TapWithChild(Tap):
        name = "child-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                Parent and child streams.
            """
            return [ParentStream(self), ChildStream(self)]

    records = [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 2, "seq": 0, "id": "b"},
        {"ts": 3, "seq": 0, "id": "c"},
    ]
    ParentStream.source = records
    state = {
        "bookmarks": {
            "parent": {
                "replication_key": "ts",
                "replication_key_value": 2,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [2, 0, "b"],
            },
        },
    }
    writer = RecordingWriter()
    tap = TapWithChild(config={}, state=state, message_writer=writer)
    tap.sync_all()

    parent_records_out = [
        message["record"]
        for message in writer.messages
        if str(message["type"]).endswith("RECORD") and message["stream"] == "parent"
    ]
    assert [record["ts"] for record in parent_records_out] == [3]
    child_parent_contexts = [
        context for context in child_contexts if context.get("ts") in {1, 2, 3}
    ]
    assert len(child_parent_contexts) == 1
    assert child_parent_contexts[0]["ts"] == 3


def test_composite_delivery_counts_exclude_restored_prefix() -> None:
    """Restored prefix records do not count as delivered records."""
    records = build_records(groups=1, per_group=5)
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 0,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [0, 2, "2"],
            },
        },
    }
    writer, _ = run_sync(records, state=state)
    assert writer.records == records[3:]


def test_composite_state_size_is_constant() -> None:
    """State size does not grow with the number of records sharing a key."""
    records = build_records(groups=1, per_group=200)
    _, tap = run_sync(records)
    state = tap.state["bookmarks"]["events"]
    assert state["replication_key_values"] == [0, 199, "199"]
    assert len(json.dumps(tap.state)) < 500


def test_composite_streams_are_independent() -> None:
    """Two composite streams keep independent bookmarks and ordering."""

    class StreamA(Stream):
        name = "alpha"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = _int_schema("ts", "seq")

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": 10, "seq": 0}
            yield {"ts": 10, "seq": 1}

    class StreamB(Stream):
        name = "beta"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = _int_schema("ts", "seq")

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": 1, "seq": 0}

    class MultiTap(Tap):
        name = "multi-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                Two composite streams.
            """
            return [StreamA(self), StreamB(self)]

    tap = MultiTap(config={}, state=None, message_writer=RecordingWriter())
    tap.sync_all()

    assert tap.state["bookmarks"]["alpha"]["replication_key_values"] == [10, 1]
    assert tap.state["bookmarks"]["beta"]["replication_key_values"] == [1, 0]


def test_composite_partitions_are_independent() -> None:
    """Multiple contexts keep independent starting points and ordering checks."""

    class PartitionedStream(Stream):
        name = "partitioned"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = _int_schema("ts", "seq")

        def get_records(self, context):
            store = context["store"]
            base = 100 if store == 1 else 1
            yield {"ts": base, "seq": 0}
            yield {"ts": base, "seq": 1}

    class PartitionedTap(Tap):
        name = "partitioned-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A single partitioned stream.
            """
            return [PartitionedStream(self)]

    state = {
        "bookmarks": {
            "partitioned": {
                "partitions": [
                    {
                        "context": {"store": 1},
                        "replication_key": "ts",
                        "replication_key_value": 100,
                        "replication_key_type": "integer",
                        "replication_key_tiebreakers": ["seq"],
                        "replication_key_tiebreaker_types": ["integer"],
                        "replication_key_values": [100, 1],
                    },
                    {
                        "context": {"store": 2},
                        "replication_key": "ts",
                        "replication_key_value": 1,
                        "replication_key_type": "integer",
                        "replication_key_tiebreakers": ["seq"],
                        "replication_key_tiebreaker_types": ["integer"],
                        "replication_key_values": [1, 0],
                    },
                ],
            },
        },
    }
    writer = RecordingWriter()
    tap = PartitionedTap(config={}, state=state, message_writer=writer)
    stream = tap.streams["partitioned"]

    # Store 1 resumes at its saved position, store 2 replays only its own tail.
    stream.sync({"store": 1})
    stream.sync({"store": 2})

    partition_records = [
        message["record"]
        for message in writer.messages
        if str(message["type"]).endswith("RECORD")
    ]
    assert partition_records == [{"ts": 1, "seq": 1}]

    partitions = tap.state["bookmarks"]["partitioned"]["partitions"]
    assert {
        partition["context"]["store"]: partition["replication_key_values"]
        for partition in partitions
    } == {
        1: [100, 1],
        2: [1, 1],
    }

    # Neither partition may reuse the other's starting point or sort state.
    assert stream.get_starting_replication_key_values({"store": 1}) == (100, 1)
    assert stream.get_starting_replication_key_values({"store": 2}) == (1, 0)


# ---------------------------------------------------------------------------
# Scale / memory
# ---------------------------------------------------------------------------


def test_composite_hundred_thousand_records_same_primary_key() -> None:
    """The cursor stays O(1) for 100k records with the same primary key.

    The source is generated lazily and the writer discards records, so neither the
    test nor the SDK materializes the full table.
    """

    class CountingWriter:
        def __init__(self) -> None:
            self.record_count = 0
            self.messages: list[dict[str, t.Any]] = []

        def write_message(self, message: t.Any) -> None:
            payload_type = str(message.to_dict()["type"])
            if payload_type.endswith("RECORD"):
                self.record_count += 1
            elif payload_type.endswith("STATE"):
                self.messages.append(message.to_dict())

    class GeneratedStream(Stream):
        name = "generated"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq", "id")
        is_sorted = True
        schema = PropertiesList(
            Property("ts", IntegerType, required=True),
            Property("seq", IntegerType, required=True),
            Property("id", StringType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            for index in range(100_000):
                yield {"ts": 42, "seq": index, "id": f"{index:08d}"}

    class GeneratedTap(Tap):
        name = "generated-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A single generated stream.
            """
            return [GeneratedStream(self)]

    writer = CountingWriter()
    tap = GeneratedTap(config={}, state=None, message_writer=writer)

    tracemalloc.start()
    tap.sync_all()
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert writer.record_count == 100_000
    state = tap.state["bookmarks"]["generated"]
    assert state["replication_key_values"] == [42, 99_999, "00099999"]
    assert len(json.dumps(tap.state)) < 500
    # Cursor state and sort-check state are constant-size; peak memory must not
    # scale with all 100k records (records are generated one at a time).
    assert peak < 20_000_000


def test_composite_error_in_pipeline_keeps_last_safe_state() -> None:
    """Errors in post_process, maps, children and writer never over-advance state."""

    class Boom(Exception):
        pass

    records = [
        {"ts": 1, "seq": 0, "id": "a", "value": "ok"},
        {"ts": 2, "seq": 0, "id": "b", "value": "ok"},
        {"ts": 3, "seq": 0, "id": "c", "value": "ok"},
    ]

    def assert_safe_state(tap: OrderedSourceTap, failing_index: int) -> None:
        values = tap.state["bookmarks"]["events"].get("replication_key_values")
        assert values is not None
        assert values[0] < records[failing_index]["ts"]

    # post_process error
    OrderedSourceStream.source = records
    tap = OrderedSourceTap(config={}, state=None, message_writer=RecordingWriter())

    class PostProcessBehavior:
        def __call__(self, row):
            if row["ts"] == 3:
                raise Boom
            return row

    OrderedSourceStream.post_process_behavior = PostProcessBehavior()
    with pytest.raises(Boom):
        tap.sync_all()
    assert_safe_state(tap, 2)

    # writer error on the third record
    OrderedSourceStream.post_process_behavior = None

    class FailingWriter(RecordingWriter):
        def write_message(self, message: t.Any) -> None:
            payload = message.to_dict()
            if str(payload["type"]).endswith("RECORD") and payload["record"]["ts"] == 3:
                raise Boom
            super().write_message(message)

    tap = OrderedSourceTap(config={}, state=None, message_writer=FailingWriter())
    with pytest.raises(Boom):
        tap.sync_all()
    assert_safe_state(tap, 2)

    # mapper expression error
    OrderedSourceStream.source = records
    tap = OrderedSourceTap(
        config={"stream_maps": {"events": {"value": "missing_field.upper()"}}},
        state=None,
        message_writer=RecordingWriter(),
    )
    with pytest.raises(Exception):  # noqa: B017, PT011
        tap.sync_all()

    # child stream error: parent bookmark must not pass the record whose child failed.
    class ParentStream(OrderedSourceStream):
        name = "parent"

        def get_child_context(self, record, context):  # noqa: ARG002
            return {"ts": record["ts"], "seq": record["seq"], "id": record["id"]}

    class ChildStream(Stream):
        name = "child"
        parent_stream_type = ParentStream
        replication_key = "ts"
        is_sorted = True
        schema = PropertiesList(Property("ts", IntegerType, required=True)).to_dict()

        def get_records(self, context):
            if context is not None and context.get("ts") == 3:
                raise Boom
            yield {"ts": 1}

    class ParentChildTap(Tap):
        name = "parent-child-error-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                Parent and child streams.
            """
            return [ParentStream(self), ChildStream(self)]

    ParentStream.source = records
    tap = ParentChildTap(config={}, state=None, message_writer=RecordingWriter())
    with pytest.raises(Boom):
        tap.sync_all()
    parent_state = tap.state["bookmarks"]["parent"]
    assert parent_state["replication_key_values"][0] < 3


def test_composite_schema_validation_rules() -> None:
    """Schema-level validation accepts nullable scalars and rejects unions."""

    def build_stream(properties: dict) -> OrderedSourceStream:
        class SchemaStream(OrderedSourceStream):
            name = "schema"
            replication_key = "ts"
            replication_key_tiebreakers = ("seq",)
            schema: t.ClassVar[dict] = {"type": "object", "properties": properties}

        return SchemaStream

    base: dict[str, t.Any] = {
        "ts": {"type": ["integer", "null"]},
        "seq": {"anyOf": [{"type": "string"}, {"type": "null"}]},
    }

    class NullableTap(Tap):
        name = "nullable-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A nullable-composite stream.
            """
            return [build_stream(base)(self)]

    OrderedSourceStream.source = [{"ts": 1, "seq": "a"}]
    writer = RecordingWriter()
    tap = NullableTap(config={}, state=None, message_writer=writer)
    tap.sync_all()
    assert writer.records == [{"ts": 1, "seq": "a"}]

    # Actual null values are still rejected even if the schema is nullable.
    OrderedSourceStream.source = [{"ts": 1, "seq": None}]
    with pytest.raises(ValueError, match="null value"):
        NullableTap(config={}, state=None, message_writer=RecordingWriter()).sync_all()

    # Non-null union types and date-time tiebreakers are rejected.
    for properties, match in [
        (
            {
                "ts": {"type": "integer"},
                "seq": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
            },
            "union",
        ),
        (
            {
                "ts": {"type": "integer"},
                "seq": {"type": "string", "format": "date-time"},
            },
            "date-time",
        ),
        (
            {
                "ts": {"type": "integer"},
                "seq": {"type": "object"},
            },
            "unsupported type",
        ),
    ]:
        tap_cls = type(
            "SchemaTap",
            (Tap,),
            {
                "name": "schema-tap",
                "discover_streams": lambda self, props=properties: [
                    build_stream(props)(self)
                ],
            },
        )
        with pytest.raises(ValueError, match=match):
            tap_cls(config={}, state=None, message_writer=RecordingWriter()).sync_all()


def test_composite_starting_getter_is_frozen_during_sync() -> None:
    """Advancing bookmarks during the run does not change the getter."""
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 1,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "string"],
                "replication_key_values": [1, 0, "a"],
            },
        },
    }
    observed: list[tuple[t.Any, ...] | None] = []

    class PinnedStream(OrderedSourceStream):
        name = "events"

        def get_records(self, context):  # noqa: ARG002
            observed.append(self.get_starting_replication_key_values(None))
            yield {"ts": 1, "seq": 0, "id": "b"}
            # Simulate the bookmark advancing mid-run.
            observed.append(self.get_starting_replication_key_values(None))
            yield {"ts": 2, "seq": 0, "id": "a"}

    class PinnedTap(Tap):
        name = "pinned-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                The pinned stream.
            """
            return [PinnedStream(self)]

    writer = RecordingWriter()
    tap = PinnedTap(config={}, state=state, message_writer=writer)
    tap.sync_all()
    assert writer.records == [
        {"ts": 1, "seq": 0, "id": "b"},
        {"ts": 2, "seq": 0, "id": "a"},
    ]
    assert observed == [(1, 0, "a"), (1, 0, "a")]


def test_composite_legacy_falsey_scalar_bookmarks() -> None:
    """Legacy 0 and empty-string bookmarks are treated as real boundaries."""
    records = [
        {"ts": 0, "seq": 0, "id": "a"},
        {"ts": 0, "seq": 0, "id": "b"},
        {"ts": 1, "seq": 0, "id": "a"},
    ]
    state = {
        "bookmarks": {
            "events": {"replication_key": "ts", "replication_key_value": 0},
        },
    }
    _tap, stream, _ = make_tap([], state=state)
    assert stream.get_starting_replication_key_value(None) == 0
    assert stream.get_starting_replication_key_values(None) is None

    # Equal primary values are replayed; strictly smaller values are skipped.
    writer, _ = run_sync(records, state=state)
    assert writer.records == records

    class StringKeyStream(Stream):
        name = "strings"
        replication_key = "key"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("key", StringType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            yield {"key": "", "seq": 0}
            yield {"key": "", "seq": 1}
            yield {"key": "a", "seq": 0}

    class StringKeyTap(Tap):
        name = "string-key-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A string-key composite stream.
            """
            return [StringKeyStream(self)]

    writer = RecordingWriter()
    tap = StringKeyTap(
        config={},
        state={
            "bookmarks": {
                "strings": {"replication_key": "key", "replication_key_value": ""},
            },
        },
        message_writer=writer,
    )
    stream = tap.streams["strings"]
    assert not stream.get_starting_replication_key_value(None)
    assert stream.get_starting_replication_key_values(None) is None
    tap.sync_all()
    assert writer.records == [
        {"key": "", "seq": 0},
        {"key": "", "seq": 1},
        {"key": "a", "seq": 0},
    ]


def test_composite_legacy_state_without_replication_key_name() -> None:
    """Legacy state may contain only replication_key_value."""
    state = {"bookmarks": {"events": {"replication_key_value": 1}}}
    _tap, stream, _ = make_tap([], state=state)
    assert stream.get_starting_replication_key_value(None) == 1
    assert stream.get_starting_replication_key_values(None) is None

    writer, _ = run_sync(
        [
            {"ts": 1, "seq": 0, "id": "a"},
            {"ts": 2, "seq": 0, "id": "a"},
        ],
        state=state,
    )
    assert writer.records == [
        {"ts": 1, "seq": 0, "id": "a"},
        {"ts": 2, "seq": 0, "id": "a"},
    ]


def test_composite_get_starting_timestamp_returns_scalar_primary() -> None:
    """The legacy timestamp getter still returns the scalar primary start."""

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": "2021-01-02T00:00:00Z", "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A date-time stream.
            """
            return [DateTimeStream(self)]

    state = {
        "bookmarks": {
            "dt": {
                "replication_key": "ts",
                "replication_key_value": "2021-01-01T00:00:00Z",
                "replication_key_type": "date-time",
                "replication_key_tiebreakers": ["seq"],
                "replication_key_tiebreaker_types": ["integer"],
                "replication_key_values": ["2021-01-01T00:00:00Z", 3],
            },
        },
    }
    tap = DateTimeTap(config={}, state=state, message_writer=RecordingWriter())
    stream = tap.streams["dt"]
    assert stream.get_starting_replication_key_value(None) == "2021-01-01T00:00:00Z"
    assert stream.get_starting_replication_key_values(None) == (
        "2021-01-01T00:00:00Z",
        3,
    )
    assert stream.get_starting_timestamp(None) == datetime.datetime(
        2021,
        1,
        1,
        tzinfo=datetime.timezone.utc,
    )


def test_composite_mapping_removes_cursor_fields() -> None:
    """Mappings may delete cursor fields; positions come from pre-map records."""
    records = [
        {"ts": 1, "seq": 0, "id": "a", "value": "one"},
        {"ts": 2, "seq": 0, "id": "b", "value": "two"},
    ]
    OrderedSourceStream.source = records
    writer = RecordingWriter()
    tap = OrderedSourceTap(
        config={
            "stream_maps": {
                "events": {
                    "ts": None,
                    "seq": None,
                    "id": None,
                },
            },
        },
        state=None,
        message_writer=writer,
    )
    tap.sync_all()

    assert writer.records == [{"value": "one"}, {"value": "two"}]
    assert tap.state["bookmarks"]["events"]["replication_key_values"] == [2, 0, "b"]


def test_composite_post_process_none_is_skipped_but_does_not_break_order() -> None:
    """Rows dropped by post_process do not participate in ordering or delivery."""
    records = [
        {"ts": 1, "seq": 0, "id": "a", "value": "keep"},
        {"ts": 2, "seq": 0, "id": "b", "value": "drop"},
        {"ts": 3, "seq": 0, "id": "c", "value": "keep"},
    ]

    class DropBehavior:
        def __call__(self, row):
            return None if row["value"] == "drop" else row

    OrderedSourceStream.source = records
    OrderedSourceStream.post_process_behavior = DropBehavior()
    try:
        writer = RecordingWriter()
        tap = OrderedSourceTap(config={}, state=None, message_writer=writer)
        tap.sync_all()
    finally:
        OrderedSourceStream.post_process_behavior = None

    assert writer.records == [
        {"ts": 1, "seq": 0, "id": "a", "value": "keep"},
        {"ts": 3, "seq": 0, "id": "c", "value": "keep"},
    ]
    assert tap.state["bookmarks"]["events"]["replication_key_values"] == [3, 0, "c"]


def test_composite_source_is_consumed_once_per_partition() -> None:
    """Sources are iterated once; no buffering of the full result set."""
    calls: list[t.Any] = []

    class SinglePassStream(Stream):
        name = "single"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = _int_schema("ts", "seq")

        def get_records(self, context):  # noqa: ARG002
            calls.append(1)
            for index in range(50):
                yield {"ts": 1, "seq": index}

    class SinglePassTap(Tap):
        name = "single-pass-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A single-pass stream.
            """
            return [SinglePassStream(self)]

    writer = RecordingWriter()
    tap = SinglePassTap(config={}, state=None, message_writer=writer)
    tap.sync_all()
    assert calls == [1]
    assert len(writer.records) == 50


def test_composite_caller_objects_are_not_mutated() -> None:
    """Config, schema and state objects provided by the caller stay untouched."""
    caller_schema = PropertiesList(
        Property("ts", IntegerType, required=True),
        Property("seq", IntegerType, required=True),
        Property("id", StringType, required=True),
    ).to_dict()
    config = {"start_date": "2020-01-01"}
    state = {
        "bookmarks": {
            "events": {
                "partitions": [
                    {
                        "context": {"tenant": "a"},
                        "replication_key": "ts",
                        "replication_key_value": 1,
                        "replication_key_type": "integer",
                        "replication_key_tiebreakers": ["seq", "id"],
                        "replication_key_tiebreaker_types": ["integer", "string"],
                        "replication_key_values": [1, 0, "a"],
                    },
                ],
            },
        },
    }
    config_snapshot = json.loads(json.dumps(config))
    schema_snapshot = json.loads(json.dumps(caller_schema))
    state_snapshot = json.loads(json.dumps(state))

    class StatefulStream(Stream):
        name = "events"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq", "id")
        is_sorted = True
        schema: t.ClassVar[dict] = caller_schema

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": 2, "seq": 0, "id": "b"}

    class CallerTap(Tap):
        name = "caller-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A stream using the caller-provided schema.
            """
            return [StatefulStream(self)]

    tap = CallerTap(config=config, state=state, message_writer=RecordingWriter())
    tap.sync_all()

    assert config == config_snapshot
    assert caller_schema == schema_snapshot
    assert state == state_snapshot
    # The tap state itself was advanced on the partition context.
    partition = tap.state["bookmarks"]["events"]["partitions"][0]
    assert partition["context"] == {"tenant": "a"}
    assert partition["replication_key_values"] == [2, 0, "b"]


def test_composite_source_error_preserves_exception_and_progress() -> None:
    """Source errors propagate unchanged and never advance the bookmark."""
    boom = RuntimeError("source failed")

    class FlakyStream(Stream):
        name = "flaky"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = _int_schema("ts", "seq")

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": 1, "seq": 0}
            raise boom

    class FlakyTap(Tap):
        name = "flaky-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                The flaky stream.
            """
            return [FlakyStream(self)]

    writer = RecordingWriter()
    tap = FlakyTap(config={}, state=None, message_writer=writer)
    with pytest.raises(RuntimeError) as excinfo:
        tap.sync_all()
    assert excinfo.value is boom
    assert writer.records == [{"ts": 1, "seq": 0}]
    assert tap.state["bookmarks"]["flaky"]["replication_key_values"] == [1, 0]


def test_composite_config_validation_details() -> None:
    """Exercise less common configuration and schema validation branches."""

    def run_with(
        tiebreakers: t.Any,
        *,
        key: str | None = "ts",
    ) -> None:
        class ConfigStream(OrderedSourceStream):
            name = "events"
            replication_key = key
            replication_key_tiebreakers = tiebreakers

        class ConfigTap(Tap):
            name = "config-tap"

            def discover_streams(self) -> list[Stream]:
                """Discover streams.

                Returns:
                    A single stream with the requested configuration.
                """
                return [ConfigStream(self)]

        ConfigTap(config={}, state=None, message_writer=RecordingWriter()).sync_all()

    # Not a sequence at all.
    with pytest.raises(ValueError, match="non-empty sequence"):
        run_with(5)
    # Entries must be non-empty strings.
    with pytest.raises(ValueError, match="non-empty strings"):
        run_with(("",))
    # Missing replication key.
    with pytest.raises(ValueError, match="no replication_key"):
        run_with(("seq",), key=None)

    # Replication key missing from schema.
    class MissingKeyStream(OrderedSourceStream):
        name = "events"
        replication_key = "absent"
        replication_key_tiebreakers = ("seq",)

    class MissingKeyTap(Tap):
        name = "missing-key-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A stream whose replication key is missing from the schema.
            """
            return [MissingKeyStream(self)]

    with pytest.raises(ValueError, match=r"'absent'.*not present"):
        MissingKeyTap(
            config={}, state=None, message_writer=RecordingWriter()
        ).sync_all()


def test_composite_unsupported_schema_shapes() -> None:
    """Reject malformed union declarations in cursor schema fields."""

    def make_stream(
        ts_schema: dict,
        seq_schema: dict,
    ) -> type[OrderedSourceStream]:
        class ShapeStream(OrderedSourceStream):
            name = "shape"
            replication_key = "ts"
            replication_key_tiebreakers = ("seq",)
            schema: t.ClassVar[dict] = {
                "type": "object",
                "properties": {"ts": ts_schema, "seq": seq_schema},
            }

        return ShapeStream

    cases = [
        ({"oneOf": "not-a-list"}, {"type": "integer"}, "unsupported"),
        ({"type": "integer"}, {"allOf": [{"type": "string"}]}, "allOf"),
        ({"type": "integer"}, {"type": ["null", "string", "integer"]}, "union"),
        ({"anyOf": [{"type": "null"}]}, {"type": "integer"}, "union"),
    ]
    for ts_schema, seq_schema, match in cases:
        shape_stream_type = make_stream(ts_schema, seq_schema)

        def make_shape_tap(
            stream_type: type[Stream] = shape_stream_type,
        ) -> Tap:
            class ShapeTap(Tap):
                name = "shape-tap"

                def discover_streams(self) -> list[Stream]:
                    """Discover streams.

                    Returns:
                        A stream with the requested schema shape.
                    """
                    return [stream_type(self)]

            return ShapeTap(config={}, state=None, message_writer=RecordingWriter())

        with pytest.raises(ValueError, match=match):
            make_shape_tap().sync_all()


def test_composite_invalid_state_start_values() -> None:
    """Invalid saved values are rejected before reading the source."""
    base = {
        "replication_key": "ts",
        "replication_key_value": 1,
        "replication_key_type": "integer",
        "replication_key_tiebreakers": ["seq", "id"],
        "replication_key_tiebreaker_types": ["integer", "string"],
        "replication_key_values": [1, 0, "a"],
    }
    # Wrong number of values.
    tap, _, _ = make_tap(
        BASE_RECORDS,
        state={
            "bookmarks": {
                "events": {**base, "replication_key_values": [1, 0]},
            },
        },
    )
    with pytest.raises(ValueError, match="cursor value"):
        tap.sync_all()

    # Float in state is rejected for an integer cursor.
    tap, _, _ = make_tap(
        BASE_RECORDS,
        state={
            "bookmarks": {
                "events": {**base, "replication_key_values": [1, 1.5, "a"]},
            },
        },
    )
    with pytest.raises(ValueError, match="integer"):
        tap.sync_all()


def test_composite_date_time_invalid_start_values() -> None:
    """Invalid date-time state values and start_date values raise."""

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": "2021-01-02T00:00:00Z", "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-bad-tap"
        config_jsonschema = PropertiesList(
            Property("start_date", DateTimeType),
        ).to_dict()

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A date-time stream.
            """
            return [DateTimeStream(self)]

    bad_state = {
        "bookmarks": {
            "dt": {
                "replication_key": "ts",
                "replication_key_value": "2021-01-01T00:00:00Z",
                "replication_key_type": "date-time",
                "replication_key_tiebreakers": ["seq"],
                "replication_key_tiebreaker_types": ["integer"],
                "replication_key_values": ["2021-01-01T00:00:00Z", 0],
            },
        },
    }
    tap = DateTimeTap(config={}, state=bad_state, message_writer=RecordingWriter())
    stream = tap.streams["dt"]
    state = stream.state_manager.get_context_state(None)
    state["replication_key_values"] = ["2021-01-01 00:00:00", 0]
    state["replication_key_value"] = "2021-01-01 00:00:00"
    with pytest.raises(ValueError, match="invalid date-time"):
        stream.get_starting_replication_key_values(None)

    # Legacy scalar date-time bookmark value is validated too.
    tap = DateTimeTap(
        config={},
        state={
            "bookmarks": {
                "dt": {
                    "replication_key": "ts",
                    "replication_key_value": "not-a-date",
                },
            },
        },
        message_writer=RecordingWriter(),
    )
    stream = tap.streams["dt"]
    with pytest.raises(ValueError, match="not a valid date-time"):
        stream.get_starting_replication_key_values(None)


def test_composite_same_primary_key_replay_for_string_tiebreaker_order() -> None:
    """Tiebreakers are compared in declaration order, not field name order."""
    records = [
        {"ts": 1, "seq": 0, "id": "b"},
        {"ts": 1, "seq": 1, "id": "a"},
        {"ts": 1, "seq": 1, "id": "b"},
    ]
    writer, _ = run_sync(records)
    assert writer.records == records


def test_composite_datetime_edge_validation() -> None:
    """Invalid offsets and calendar date-times raise validated errors."""

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        values: t.ClassVar[list[str]] = []

        def get_records(self, context):  # noqa: ARG002
            for value in self.values:
                yield {"ts": value, "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-edge-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A date-time stream.
            """
            return [DateTimeStream(self)]

    for value in ["2021-01-01T00:00:00+24:00", "2021-02-30T00:00:00Z"]:
        DateTimeStream.values = [value]
        with pytest.raises(ValueError, match="invalid"):
            DateTimeTap(
                config={},
                state=None,
                message_writer=RecordingWriter(),
            ).sync_all()

    # Valid non-Z offsets parse correctly.
    DateTimeStream.values = [
        "2021-01-01T00:00:00+00:00",
        "2021-01-01T01:00:00.123456+01:00",
        "2021-01-01T04:00:00+03:00",
    ]
    writer = RecordingWriter()
    DateTimeTap(config={}, state=None, message_writer=writer).sync_all()
    assert len(writer.records) == 3


def test_composite_cursor_length_mismatch_in_record() -> None:
    """A record with a missing field is rejected as a missing field."""
    with pytest.raises(ValueError, match="missing"):
        run_sync([{"ts": 1}])


def test_composite_unexpected_tiebreaker_type_in_state() -> None:
    """Non-string tiebreaker type tokens in state are rejected."""
    state = {
        "bookmarks": {
            "events": {
                "replication_key": "ts",
                "replication_key_value": 1,
                "replication_key_type": "integer",
                "replication_key_tiebreakers": ["seq", "id"],
                "replication_key_tiebreaker_types": ["integer", "bogus"],
                "replication_key_values": [1, 0, "a"],
            },
        },
    }
    tap, _, _ = make_tap(BASE_RECORDS, state=state)
    with pytest.raises(ValueError, match="types"):
        tap.sync_all()


def test_composite_disable_after_enabling_then_state_is_scalar() -> None:
    """A normal scalar bookmark remains valid for composite streams."""
    state = {
        "bookmarks": {
            "events": {"replication_key": "ts", "replication_key_value": 2},
        },
    }
    tap, stream, _ = make_tap([], state=state)
    assert stream.get_starting_replication_key_values(None) is None
    assert stream.get_starting_replication_key_value(None) == 2
    tap.sync_all()


def test_composite_rejects_non_incremental_replication_method() -> None:
    """A stream forced to FULL_TABLE cannot use composite cursors."""

    class FullTableStream(OrderedSourceStream):
        name = "events"

    class FullTableTap(Tap):
        name = "full-table-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A stream forced to FULL_TABLE.
            """
            stream = FullTableStream(self)
            stream.forced_replication_method = REPLICATION_FULL_TABLE
            return [stream]

    FullTableStream.source = [dict(record) for record in BASE_RECORDS]
    FullTableStream.received_contexts = []
    with pytest.raises(ValueError, match="not incremental"):
        FullTableTap(config={}, state=None, message_writer=RecordingWriter()).sync_all()
    assert FullTableStream.received_contexts == []


def test_composite_partition_error_message_includes_context() -> None:
    """Per-partition validation errors name the partition context."""

    class PartitionedStream(Stream):
        name = "partitioned"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = _int_schema("ts", "seq")

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": 1, "seq": 0}

    class PartitionedTap(Tap):
        name = "partition-error-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A partitioned stream.
            """
            return [PartitionedStream(self)]

    state = {
        "bookmarks": {
            "partitioned": {
                "partitions": [
                    {
                        "context": {"store": 1},
                        "replication_key": "ts",
                        "replication_key_value": 1,
                        "replication_key_type": "integer",
                        "replication_key_tiebreakers": ["seq"],
                        "replication_key_tiebreaker_types": ["integer"],
                        "replication_key_values": [1, "not-an-int"],
                    },
                ],
            },
        },
    }
    with pytest.raises(ValueError, match=r"partition \{'store': 1\}"):
        PartitionedTap(
            config={},
            state=state,
            message_writer=RecordingWriter(),
        ).sync_all()


def test_composite_legacy_datetime_uses_later_start_date() -> None:
    """A later start_date replaces a legacy scalar date-time bookmark."""

    class DateTimeStream(Stream):
        name = "dt"
        replication_key = "ts"
        replication_key_tiebreakers = ("seq",)
        is_sorted = True
        schema = PropertiesList(
            Property("ts", DateTimeType, required=True),
            Property("seq", IntegerType, required=True),
        ).to_dict()

        def get_records(self, context):  # noqa: ARG002
            yield {"ts": "2021-01-01T09:00:00Z", "seq": 0}
            yield {"ts": "2021-01-02T00:00:00Z", "seq": 0}

    class DateTimeTap(Tap):
        name = "dt-legacy-tap"
        config_jsonschema = PropertiesList(
            Property("start_date", DateTimeType),
        ).to_dict()

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A date-time stream.
            """
            return [DateTimeStream(self)]

    state = {
        "bookmarks": {
            "dt": {
                "replication_key": "ts",
                "replication_key_value": "2021-01-01T00:00:00Z",
            },
        },
    }
    writer = RecordingWriter()
    tap = DateTimeTap(
        config={"start_date": "2021-01-01T09:00:00+08:00"},
        state=state,
        message_writer=writer,
    )
    stream = tap.streams["dt"]
    assert stream.get_starting_replication_key_values(None) is None
    assert (
        stream.get_starting_replication_key_value(None) == "2021-01-01T09:00:00+08:00"
    )
    tap.sync_all()
    # start_date (01:00Z) is later than the saved bookmark (00:00Z) but still
    # before the first source record, so both records are replayed.
    assert writer.records == [
        {"ts": "2021-01-01T09:00:00Z", "seq": 0},
        {"ts": "2021-01-02T00:00:00Z", "seq": 0},
    ]


def test_composite_custom_signpost_does_not_advance_bookmark() -> None:
    """A custom signpost never fabricates a composite position."""
    records = [{"ts": 1, "seq": 0, "id": "a"}]

    class SignpostStream(OrderedSourceStream):
        name = "events"

        def get_replication_key_signpost(self, context=None):  # noqa: ARG002
            return "999999"

    class SignpostTap(Tap):
        name = "signpost-tap"

        def discover_streams(self) -> list[Stream]:
            """Discover streams.

            Returns:
                A stream with a custom signpost.
            """
            return [SignpostStream(self)]

    SignpostStream.source = [dict(record) for record in records]
    writer = RecordingWriter()
    tap = SignpostTap(config={}, state=None, message_writer=writer)
    tap.sync_all()

    state = tap.state["bookmarks"]["events"]
    assert state["replication_key_values"] == [1, 0, "a"]
    assert state.get("replication_key_signpost") is None
    assert writer.records == records
