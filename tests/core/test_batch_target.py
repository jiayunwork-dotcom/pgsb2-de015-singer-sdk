"""Tests that a BATCH message is equivalent to its expanded RECORD messages.

The central rule under test is:

    For a target, a ``BATCH`` message is exactly equivalent to the ``RECORD``
    messages obtained by expanding each file in its manifest, in the listed order,
    and each file one line at a time, in the position the ``BATCH`` message
    occupied. Expanded records use the batch's ``stream`` and the batch message's
    ``time_extracted`` / ``version`` values, when present.

Equivalence is judged by what a sink can observe: the records passed to each
``process_batch`` call (contents, order and batching), the ``preprocess_record``
and ``process_record`` call sequences, validation errors, metrics and the target's
``STATE`` output. Metadata values generated at processing time are compared only
on presence, not value.
"""

from __future__ import annotations

import datetime
import gzip
import importlib.util
import json
import logging
import random
import typing as t
from io import StringIO

import pytest

from singer_sdk import metrics
from singer_sdk import typing as th
from singer_sdk.exceptions import (
    InvalidBatchFileError,
    InvalidRecord,
    RecordsWithoutSchemaException,
    StreamMapsNotSupportedError,
)
from singer_sdk.helpers.capabilities import PluginCapabilities
from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

if t.TYPE_CHECKING:
    from pathlib import Path

    from singer_sdk.helpers._batch import BaseBatchFileEncoding
    from singer_sdk.helpers.capabilities import CapabilitiesEnum

has_pyarrow = importlib.util.find_spec("pyarrow") is not None

# These metadata columns are generated from the current processing time, so their
# values are expected to differ between an all-RECORD run and a BATCH run.
_PROCESS_TIME_METADATA = frozenset(
    {
        "_sdc_received_at",
        "_sdc_batched_at",
        "_sdc_sequence",
        "_sdc_sync_started_at",
    },
)


def _normalize_record(record: dict) -> dict:
    """Normalize a record so processing-time metadata values are ignored."""
    return {
        key: "<present>" if key in _PROCESS_TIME_METADATA else value
        for key, value in record.items()
    }


class RecordingSink(BatchSink):
    """A batch sink that records everything a sink can observe."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None,
    ) -> None:
        """Initialize the sink and keep a typed reference to its target."""
        super().__init__(target, stream_name, schema, key_properties)
        self.target = t.cast(RecordingTarget, target)

    def get_sink_record_counter(self) -> metrics.Counter:
        """Return a counter that records each increment on the target."""
        counter = metrics.record_counter(self.stream_name)
        original_increment = counter.increment

        def increment(value: int = 1) -> None:
            self.target.counter_increments.append((self.stream_name, value))
            original_increment(value)

        counter.increment = increment  # type: ignore[method-assign]
        return counter

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Record the call and return the record unchanged."""
        self.target.events.append(
            ("preprocess_record", self.stream_name, _normalize_record(record)),
        )
        return super().preprocess_record(record, context)

    def process_record(self, record: dict, context: dict) -> None:
        """Record the call and stage the record for the next batch."""
        self.target.events.append(
            ("process_record", self.stream_name, _normalize_record(record)),
        )
        super().process_record(record, context)

    def process_batch(self, context: dict) -> None:
        """Record the exact records received by this batch."""
        self.target.batch_calls.append(
            (
                self.stream_name,
                [_normalize_record(record) for record in context["records"]],
            ),
        )


class CustomBatchFileSink(RecordingSink):
    """A sink that implements its own raw batch file processing."""

    def process_batch_files(
        self,
        encoding: BaseBatchFileEncoding,
        files: t.Sequence[str],
    ) -> None:
        """Record the raw files handed to the sink."""
        self.target.custom_batch_calls.append(
            (self.stream_name, encoding.format, list(files)),
        )


class RecordingTarget(Target):
    """A target that captures all observable sink activity."""

    name = "target-batch-equivalence"
    config_jsonschema = th.PropertiesList().to_dict()
    default_sink_class = RecordingSink
    capabilities: t.ClassVar[list[CapabilitiesEnum]] = [
        *Target.capabilities,
        PluginCapabilities.BATCH,
    ]

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the target and its trackers."""
        super().__init__(*args, **kwargs)
        self.max_parallelism = 1
        self.batch_calls: list[tuple[str, list[dict]]] = []
        self.custom_batch_calls: list[tuple[str, str, list[str]]] = []
        self.events: list[tuple] = []
        self.counter_increments: list[tuple[str, int]] = []
        self.state_messages_written: list[dict] = []

    def _write_state_message(self, state: dict) -> None:
        """Record emitted state and forward to the parent implementation."""
        self.state_messages_written.append(state)
        super()._write_state_message(state)


def _snapshot(target: RecordingTarget) -> dict:
    """Return all observable target activity in a comparable form."""
    return {
        "batches": target.batch_calls,
        "custom_batches": target.custom_batch_calls,
        "events": target.events,
        "counter_increments": target.counter_increments,
        "totals": {
            name: (
                sink._total_records_read,
                sink._total_records_written,
                sink._batch_records_read,
            )
            for name, sink in target._sinks_active.items()
        },
        "states": target.state_messages_written,
    }


SCHEMA_A = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "nested": {
            "type": "object",
            "properties": {
                "code": {"type": "string"},
                "inner": {
                    "type": "object",
                    "properties": {"flag": {"type": "boolean"}},
                },
            },
        },
    },
}

SCHEMA_B = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "label": {"type": "string"},
    },
}

STREAM_MAP_CONFIGS: dict[str, dict] = {
    "none": {},
    "filter_alias_remove_expression": {
        "stream_maps": {
            "users": {
                "__filter__": "id % 2 == 0",
                "__alias__": "even_users",
                "name": None,
                "email_hash": "md5(email)",
            },
        },
    },
    "source_projection_and_else": {
        "stream_maps": {
            "users": {
                "__else__": None,
                "id": "int(id)",
                "name": "'renamed'",
            },
            "users_copy": {
                "__source__": "users",
                "id": "int(id) * 2",
                "label": "'copy'",
            },
        },
    },
    "flattening": {
        "flattening_enabled": True,
        "flattening_max_depth": 2,
    },
    "maps_and_flattening": {
        "flattening_enabled": True,
        "flattening_max_depth": 2,
        "stream_maps": {
            "users": {
                "__filter__": "id % 2 == 0",
                "__alias__": "even_users",
                "name": None,
                "email_hash": "md5(email)",
            },
        },
    },
    "wildcard_source": {
        "stream_maps": {
            "user*": {
                "__source__": "users",
                "id": "int(id) * 3",
                "label": "'wild'",
            },
        },
    },
}


def _schema_message(stream: str, schema: dict, key_properties: list[str]) -> dict:
    return {
        "type": "SCHEMA",
        "stream": stream,
        "schema": schema,
        "key_properties": key_properties,
    }


def _record_message(
    stream: str,
    record: dict,
    *,
    time_extracted: str | None = None,
    version: int | None = None,
) -> dict:
    message: dict = {"type": "RECORD", "stream": stream, "record": record}
    if time_extracted is not None:
        message["time_extracted"] = time_extracted
    if version is not None:
        message["version"] = version
    return message


def _write_jsonl(
    path: Path,
    records: list[dict],
    *,
    compressed: bool,
) -> str:
    """Write JSONL records to a file, optionally gzipped, and return its URL."""
    payload = b"".join(
        json.dumps(record).encode() + b"\n" for record in records
    )
    if compressed:
        path.write_bytes(gzip.compress(payload))
    else:
        path.write_bytes(payload)
    return path.as_uri()


def _write_parquet(path: Path, records: list[dict]) -> str:
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    pq.write_table(pa.Table.from_pylist(records), path)
    return path.as_uri()


def _build_random_run(
    records: list[dict],
    *,
    stream: str,
    tmp_path: Path,
    rng: random.Random,
    encoding: str = "jsonl",
    gzip_allowed: bool = True,
) -> tuple[list[dict], list[dict]]:
    """Split records into alternating RECORD and BATCH messages.

    Returns a ``(batch_run, record_run)`` pair. Both message lists contain the same
    logical records in the same order; only the delivery mechanism differs.
    """
    batch_messages: list[dict] = []
    record_messages: list[dict] = []
    state_counter = 0

    batch_messages.append(_schema_message(stream, SCHEMA_A, ["id"]))
    record_messages.append(_schema_message(stream, SCHEMA_A, ["id"]))

    # Cut the record list into segments.
    cut_points = sorted(
        {0, len(records), *(rng.randint(0, len(records)) for _ in range(4))},
    )
    segments = [records[a:b] for a, b in zip(cut_points, cut_points[1:], strict=False)]
    segments = [segment for segment in segments if segment]

    for segment_index, segment in enumerate(segments):
        time_extracted = f"2024-01-0{1 + segment_index % 9}T00:00:00Z"
        version = 100 + segment_index

        if rng.random() < 0.4:
            # Send the segment as plain RECORD messages.
            for record in segment:
                batch_messages.append(
                    _record_message(
                        stream,
                        record,
                        time_extracted=time_extracted,
                        version=version,
                    ),
                )
            for record in segment:
                record_messages.append(
                    _record_message(
                        stream,
                        record,
                        time_extracted=time_extracted,
                        version=version,
                    ),
                )
        else:
            # Send the segment as a BATCH with one or more files.
            file_count = rng.randint(1, 3)
            file_cuts = sorted(
                {
                    0,
                    len(segment),
                    *(rng.randint(0, len(segment)) for _ in range(file_count - 1)),
                },
            )
            file_segments = [
                segment[a:b] for a, b in zip(file_cuts, file_cuts[1:], strict=False)
            ]
            file_segments = [fs for fs in file_segments if fs]
            if not file_segments:
                file_segments = [[]]

            manifest: list[str] = []
            compressed = encoding == "jsonl" and gzip_allowed and rng.random() < 0.5
            for file_index, file_segment in enumerate(file_segments):
                path = tmp_path / (
                    f"{stream}-seg{segment_index}-file{file_index}.jsonl"
                    + (".gz" if compressed else "")
                )
                if encoding == "jsonl":
                    manifest.append(
                        _write_jsonl(path, file_segment, compressed=compressed),
                    )
                else:
                    manifest.append(_write_parquet(path, file_segment))

                for record in file_segment:
                    record_messages.append(
                        _record_message(
                            stream,
                            record,
                            time_extracted=time_extracted,
                            version=version,
                        ),
                    )

            batch_message: dict = {
                "type": "BATCH",
                "stream": stream,
                "encoding": {"format": encoding},
                "manifest": manifest,
                "time_extracted": time_extracted,
                "version": version,
            }
            if encoding == "jsonl" and compressed:
                batch_message["encoding"]["compression"] = "gzip"

            batch_messages.append(batch_message)

        # Interleave STATE messages at a few segment boundaries.
        if segment_index % 2 == 0:
            state_counter += 1
            state = {
                "type": "STATE",
                "value": {"bookmark": {stream: state_counter}},
            }
            batch_messages.append(state)
            record_messages.append(dict(state))

    return batch_messages, record_messages


def _listen(target: RecordingTarget, messages: list[dict]) -> RecordingTarget:
    payload = "\n".join(json.dumps(message) for message in messages)
    target.listen(StringIO(payload))
    return target


def _run_pair(
    config: dict,
    *,
    records: list[dict],
    tmp_path: Path,
    seed: int,
    encoding: str = "jsonl",
    gzip_allowed: bool = True,
    stream: str = "users",
) -> tuple[dict, dict]:
    batch_messages, record_messages = _build_random_run(
        records,
        stream=stream,
        tmp_path=tmp_path,
        rng=random.Random(seed),
        encoding=encoding,
        gzip_allowed=gzip_allowed,
    )

    batch_target = _listen(RecordingTarget(config=dict(config)), batch_messages)
    record_target = _listen(RecordingTarget(config=dict(config)), record_messages)
    return _snapshot(batch_target), _snapshot(record_target)


def _sample_records(count: int = 14) -> list[dict]:
    return [
        {
            "id": index,
            "name": f"name-{index}",
            "email": f"user{index}@example.com",
            "nested": {
                "code": f"c{index}",
                "inner": {"flag": index % 2 == 0},
            },
        }
        for index in range(count)
    ]


@pytest.mark.parametrize(
    "config_name",
    [
        "none",
        "filter_alias_remove_expression",
        "source_projection_and_else",
        "flattening",
        "maps_and_flattening",
        "wildcard_source",
    ],
)
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_batch_equivalent_to_records_random_split(
    config_name: str,
    seed: int,
    tmp_path: Path,
) -> None:
    """BATCH and RECORD runs must be indistinguishable, including batch slicing."""
    config = {**STREAM_MAP_CONFIGS[config_name], "batch_size_rows": 3}

    batch_snapshot, record_snapshot = _run_pair(
        config,
        records=_sample_records(),
        tmp_path=tmp_path,
        seed=seed,
    )

    assert batch_snapshot == record_snapshot


def test_batch_equivalent_with_gzip_manifest_and_multiple_files(
    tmp_path: Path,
) -> None:
    """A gzip BATCH with several files is equivalent to the expanded RECORDs."""
    records = _sample_records()

    records_a, records_b = records[:5], records[5:]
    manifest = [
        _write_jsonl(tmp_path / "a.jsonl.gz", records_a, compressed=True),
        _write_jsonl(tmp_path / "b.jsonl.gz", records_b, compressed=True),
    ]

    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl", "compression": "gzip"},
        "manifest": manifest,
    }
    record_messages = [
        schema,
        *(_record_message("users", record) for record in records),
    ]

    config = {"batch_size_rows": 2}
    batch_snapshot = _snapshot(
        _listen(RecordingTarget(config=config), [schema, batch_message]),
    )
    record_snapshot = _snapshot(
        _listen(RecordingTarget(config=config), record_messages),
    )

    assert batch_snapshot == record_snapshot
    assert len(batch_snapshot["batches"]) == 7  # 14 records, batches of 2


@pytest.mark.skipif(not has_pyarrow, reason="requires pyarrow")
@pytest.mark.parametrize("seed", [4, 5])
def test_batch_equivalent_to_records_parquet(seed: int, tmp_path: Path) -> None:
    """Parquet batches follow the same equivalence rules when pyarrow is installed."""
    config = {
        **STREAM_MAP_CONFIGS["maps_and_flattening"],
        "batch_size_rows": 3,
    }

    batch_snapshot, record_snapshot = _run_pair(
        config,
        records=_sample_records(),
        tmp_path=tmp_path,
        seed=seed,
        encoding="parquet",
        gzip_allowed=False,
    )

    assert batch_snapshot == record_snapshot


def test_batch_size_rows_bounds_each_process_batch(tmp_path: Path) -> None:
    """batch_size_rows limits every process_batch call, including BATCH input."""
    records = _sample_records(9)
    path = _write_jsonl(tmp_path / "sized.jsonl", records, compressed=False)
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [path],
    }

    config = {"batch_size_rows": 4}
    target = _listen(RecordingTarget(config=config), [schema, batch_message])

    assert [len(records) for _, records in target.batch_calls] == [4, 4, 1]


def test_record_batch_alternation_preserves_order(tmp_path: Path) -> None:
    """RECORDs already received must be processed before a later BATCH."""
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_file = _write_jsonl(
        tmp_path / "later.jsonl",
        [{"id": 2, "name": "two", "email": "two@example.com"}],
        compressed=False,
    )

    messages = [
        schema,
        _record_message(
            "users",
            {"id": 1, "name": "one", "email": "one@example.com"},
        ),
        {
            "type": "BATCH",
            "stream": "users",
            "encoding": {"format": "jsonl"},
            "manifest": [batch_file],
        },
        _record_message(
            "users",
            {"id": 3, "name": "three", "email": "three@example.com"},
        ),
    ]

    # A small batch size keeps the first record pending when the BATCH arrives. The
    # first record is not overtaken: it shares the next batch with the BATCH's first
    # record, and stays ahead of it.
    target = _listen(RecordingTarget(config={"batch_size_rows": 2}), messages)

    assert target.batch_calls == [
        (
            "users",
            [
                {"id": 1, "name": "one", "email": "one@example.com"},
                {"id": 2, "name": "two", "email": "two@example.com"},
            ],
        ),
        ("users", [{"id": 3, "name": "three", "email": "three@example.com"}]),
    ]

    written_ids = [
        record["id"] for _, records in target.batch_calls for record in records
    ]
    assert written_ids == [1, 2, 3]


def test_multiple_streams_interleaved(tmp_path: Path) -> None:
    """Interleaving streams with RECORDs and BATCHes matches the RECORD-only run."""
    stream_a_file = _write_jsonl(
        tmp_path / "stream-a.jsonl",
        [
            {"id": 2, "name": "a2", "email": "a2@example.com"},
            {"id": 3, "name": "a3", "email": "a3@example.com"},
        ],
        compressed=True,
    )
    stream_b_file = _write_jsonl(
        tmp_path / "stream-b.jsonl",
        [{"id": 20, "label": "b20"}, {"id": 21, "label": "b21"}],
        compressed=False,
    )

    batch_messages = [
        _schema_message("users", SCHEMA_A, ["id"]),
        _schema_message("labels", SCHEMA_B, ["id"]),
        _record_message("users", {"id": 1, "name": "a1", "email": "a1@example.com"}),
        {
            "type": "BATCH",
            "stream": "labels",
            "encoding": {"format": "jsonl"},
            "manifest": [stream_b_file],
        },
        {
            "type": "BATCH",
            "stream": "users",
            "encoding": {"format": "jsonl", "compression": "gzip"},
            "manifest": [stream_a_file],
        },
        _record_message("labels", {"id": 22, "label": "b22"}),
    ]
    record_messages = [
        _schema_message("users", SCHEMA_A, ["id"]),
        _schema_message("labels", SCHEMA_B, ["id"]),
        _record_message("users", {"id": 1, "name": "a1", "email": "a1@example.com"}),
        _record_message("labels", {"id": 20, "label": "b20"}),
        _record_message("labels", {"id": 21, "label": "b21"}),
        _record_message("users", {"id": 2, "name": "a2", "email": "a2@example.com"}),
        _record_message("users", {"id": 3, "name": "a3", "email": "a3@example.com"}),
        _record_message("labels", {"id": 22, "label": "b22"}),
    ]

    config = {"batch_size_rows": 2}
    batch_snapshot = _snapshot(
        _listen(RecordingTarget(config=config), batch_messages),
    )
    record_snapshot = _snapshot(
        _listen(RecordingTarget(config=config), record_messages),
    )

    assert batch_snapshot == record_snapshot


def test_batch_before_schema_raises_like_record(tmp_path: Path) -> None:
    """A BATCH before SCHEMA raises the same exception as a RECORD before SCHEMA."""
    batch_file = _write_jsonl(
        tmp_path / "orphan.jsonl",
        [{"id": 1, "name": "one", "email": "one@example.com"}],
        compressed=False,
    )
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [batch_file],
    }
    record_message = _record_message(
        "users",
        {"id": 1, "name": "one", "email": "one@example.com"},
    )

    with pytest.raises(RecordsWithoutSchemaException) as batch_error:
        _listen(RecordingTarget(config={}), [batch_message])

    with pytest.raises(RecordsWithoutSchemaException) as record_error:
        _listen(RecordingTarget(config={}), [record_message])

    assert type(batch_error.value) is type(record_error.value)


def test_removed_stream_batch_produces_no_activity(tmp_path: Path) -> None:
    """A BATCH for a stream removed by a stream map is never read."""
    nonexistent = (tmp_path / "does-not-exist.jsonl").as_uri()
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [nonexistent],
    }
    schema = _schema_message("users", SCHEMA_A, ["id"])

    target = _listen(
        RecordingTarget(config={"stream_maps": {"users": None}}),
        [schema, batch_message],
    )

    assert target.batch_calls == []
    assert target.events == []
    assert target.counter_increments == []


def test_removed_stream_batch_ignores_invalid_content(tmp_path: Path) -> None:
    """A removed stream's BATCH does not fail on unreadable file content."""
    bad_file = tmp_path / "bad.jsonl"
    bad_file.write_text("this is not json\n")
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [bad_file.as_uri()],
    }
    schema = _schema_message("users", SCHEMA_A, ["id"])

    target = _listen(
        RecordingTarget(config={"stream_maps": {"users": None}}),
        [schema, batch_message],
    )

    assert target.batch_calls == []


def _single_record_batch(tmp_path: Path, record: dict) -> tuple[dict, dict]:
    schema = _schema_message("users", SCHEMA_A, ["id"])
    path = _write_jsonl(tmp_path / "single.jsonl", [record], compressed=False)
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [path],
    }
    return schema, batch_message


def test_batch_record_validation_failure_matches_record(tmp_path: Path) -> None:
    """A bad record in a BATCH raises the same exception type as a bad RECORD."""
    invalid_record = {"id": "not-an-integer", "name": "bad"}
    schema, batch_message = _single_record_batch(tmp_path, invalid_record)

    with pytest.raises(InvalidRecord) as batch_error:
        _listen(RecordingTarget(config={}), [schema, batch_message])

    with pytest.raises(InvalidRecord) as record_error:
        _listen(
            RecordingTarget(config={}),
            [schema, _record_message("users", invalid_record)],
        )

    assert type(batch_error.value) is type(record_error.value)

    # The batch error message carries the file path and 1-based line number.
    path = batch_message["manifest"][0]
    assert path in str(batch_error.value)
    assert "line 1" in str(batch_error.value)


def test_batch_record_validation_failure_continue_and_logs_location(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With fail_on_record_validation_exception disabled, the log includes location."""
    invalid_record = {"id": "not-an-integer", "name": "bad"}
    payload = [
        {"id": 1, "name": "ok", "email": "ok@example.com"},
        invalid_record,
        {"id": 2, "name": "ok2", "email": "ok2@example.com"},
    ]
    path = _write_jsonl(tmp_path / "mixed.jsonl", payload, compressed=False)
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [path],
    }

    class ContinueSink(RecordingSink):
        fail_on_record_validation_exception = False

    class ContinueTarget(RecordingTarget):
        default_sink_class = ContinueSink

    with caplog.at_level(logging.ERROR):
        target = _listen(ContinueTarget(config={}), [schema, batch_message])

    assert path in caplog.text
    assert "line 2" in caplog.text

    # Records after the invalid one were still processed.
    written_ids = [
        record["id"] for _, records in target.batch_calls for record in records
    ]
    assert written_ids == [1, "not-an-integer", 2]


def test_batch_bad_line_reports_path_and_line_number(tmp_path: Path) -> None:
    """A line that is not valid JSON reports its path and 1-based line number."""
    path = tmp_path / "bad-line.jsonl"
    path.write_bytes(
        b'{"id": 1, "name": "one", "email": "one@example.com"}\n'
        b'{"id": 2, "name": "two", "email": "two@example.com"}\n'
        b"not json\n",
    )
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [path.as_uri()],
    }

    with pytest.raises(InvalidBatchFileError) as error:
        _listen(RecordingTarget(config={}), [schema, batch_message])

    assert path.as_uri() in str(error.value)
    assert "line 3" in str(error.value)


def test_batch_non_object_line_reports_path_and_line_number(tmp_path: Path) -> None:
    """A JSON value that is not an object reports its path and line number."""
    path = tmp_path / "non-object.jsonl"
    path.write_bytes(b'{"id": 1}\n[2, 3]\n')
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [path.as_uri()],
    }

    with pytest.raises(InvalidBatchFileError) as error:
        _listen(RecordingTarget(config={}), [schema, batch_message])

    assert path.as_uri() in str(error.value)
    assert "line 2" in str(error.value)


def test_batch_missing_file_reports_path(tmp_path: Path) -> None:
    """A missing batch file raises an error that names the file."""
    missing = (tmp_path / "missing.jsonl").as_uri()
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [missing],
    }

    with pytest.raises(InvalidBatchFileError) as error:
        _listen(RecordingTarget(config={}), [schema, batch_message])

    assert missing in str(error.value)


def test_batch_unsupported_compression_reports_path(tmp_path: Path) -> None:
    """An unsupported compression raises an error that names the file."""
    path = _write_jsonl(tmp_path / "plain.jsonl", [], compressed=False)
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl", "compression": "bz2"},
        "manifest": [path],
    }

    with pytest.raises(InvalidBatchFileError) as error:
        _listen(RecordingTarget(config={}), [schema, batch_message])

    assert path in str(error.value)


def test_batch_unsupported_format_reports_path(tmp_path: Path) -> None:
    """An unsupported encoding format raises an error that names the file."""
    path = (tmp_path / "plain.csv").as_uri()
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "csv"},
        "manifest": [path],
    }

    with pytest.raises(NotImplementedError) as error:
        _listen(RecordingTarget(config={}), [schema, batch_message])

    assert path in str(error.value)


def test_batch_explicit_none_compression(tmp_path: Path) -> None:
    """An explicit ``compression: "none"`` is treated as uncompressed."""
    path = _write_jsonl(
        tmp_path / "none.jsonl",
        [{"id": 1, "name": "one", "email": "one@example.com"}],
        compressed=False,
    )
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl", "compression": "none"},
        "manifest": [path],
    }

    target = _listen(RecordingTarget(config={}), [schema, batch_message])

    written_ids = [
        record["id"] for _, records in target.batch_calls for record in records
    ]
    assert written_ids == [1]


def test_batch_add_record_metadata_matches_records(tmp_path: Path) -> None:
    """add_record_metadata produces the same columns for BATCH and RECORD input."""
    records = _sample_records(4)
    config = {"add_record_metadata": True, "batch_size_rows": 2}

    path = _write_jsonl(tmp_path / "meta.jsonl", records, compressed=True)
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl", "compression": "gzip"},
        "manifest": [path],
        "time_extracted": "2024-01-01T00:00:00Z",
        "version": 7,
    }
    record_messages = [
        schema,
        *(
            _record_message(
                "users",
                record,
                time_extracted="2024-01-01T00:00:00Z",
                version=7,
            )
            for record in records
        ),
    ]

    batch_snapshot = _snapshot(
        _listen(RecordingTarget(config=config), [schema, batch_message]),
    )
    record_snapshot = _snapshot(
        _listen(RecordingTarget(config=config), record_messages),
    )

    assert batch_snapshot == record_snapshot

    written = batch_snapshot["batches"][0][1][0]
    assert "_sdc_extracted_at" in written
    assert "_sdc_received_at" in written
    assert "_sdc_batched_at" in written
    assert "_sdc_sequence" in written
    # time_extracted is parsed into a datetime by the schema timestamp handling.
    assert written["_sdc_extracted_at"] == datetime.datetime(
        2024,
        1,
        1,
        tzinfo=datetime.timezone.utc,
    )
    assert written["_sdc_table_version"] == 7


def _run_custom_sink(config: dict, messages: list[dict]) -> RecordingTarget:
    class CustomTarget(RecordingTarget):
        default_sink_class = CustomBatchFileSink

    return _listen(CustomTarget(config=config), messages)


def test_custom_process_batch_files_without_maps_receives_raw_files(
    tmp_path: Path,
) -> None:
    """With no effective maps or flattening, custom handlers get the raw files."""
    path = _write_jsonl(
        tmp_path / "raw.jsonl",
        [{"id": 1, "name": "one", "email": "one@example.com"}],
        compressed=True,
    )
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl", "compression": "gzip"},
        "manifest": [path],
    }

    target = _run_custom_sink({}, [schema, batch_message])

    assert target.custom_batch_calls == [("users", "jsonl", [path])]
    assert target.batch_calls == []
    assert target.events == []


def test_custom_process_batch_files_with_maps_raises_before_reading(
    tmp_path: Path,
) -> None:
    """With effective maps, a custom file handler raises before reading any file."""
    missing = (tmp_path / "missing.jsonl").as_uri()
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [missing],
    }

    with pytest.raises(StreamMapsNotSupportedError) as error:
        _run_custom_sink(
            {"stream_maps": {"users": {"email_hash": "md5(email)"}}},
            [schema, batch_message],
        )

    assert "process_batch_files" in str(error.value)
    # The file was never read, so a missing file did not raise a file error.
    assert missing not in str(error.value)


def test_custom_process_batch_files_with_flattening_raises(
    tmp_path: Path,
) -> None:
    """Flattening alone also blocks a custom file handler."""
    missing = (tmp_path / "missing.jsonl").as_uri()
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [missing],
    }

    with pytest.raises(StreamMapsNotSupportedError):
        _run_custom_sink(
            {"flattening_enabled": True, "flattening_max_depth": 2},
            [schema, batch_message],
        )


def test_partial_effects_before_error_match_records(tmp_path: Path) -> None:
    """Records before a bad line have the same effects as in the RECORD run."""
    path = tmp_path / "partial.jsonl"
    path.write_bytes(
        b'{"id": 1, "name": "one", "email": "one@example.com"}\n'
        b"not json\n",
    )
    schema = _schema_message("users", SCHEMA_A, ["id"])
    batch_message = {
        "type": "BATCH",
        "stream": "users",
        "encoding": {"format": "jsonl"},
        "manifest": [path.as_uri()],
    }
    record_messages = [
        schema,
        _record_message("users", {"id": 1, "name": "one", "email": "one@example.com"}),
        _record_message("users", {"id": 2, "name": "two", "email": "two@example.com"}),
        {"type": "STATE", "value": {"bookmark": {"users": 2}}},
    ]

    batch_target = RecordingTarget(config={})
    with pytest.raises(InvalidBatchFileError):
        _listen(batch_target, [schema, batch_message])

    record_target = RecordingTarget(config={})
    # The equivalent RECORD run processes the first record, then the stream simply
    # ends (no bad-line equivalent exists in a RECORD stream), so its observable
    # pre-error activity must match.
    _listen(record_target, record_messages[:2])

    assert batch_target.events == record_target.events
    assert (
        batch_target._sinks_active["users"]._total_records_read
        == record_target._sinks_active["users"]._total_records_read
    )
