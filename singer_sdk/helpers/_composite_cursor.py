"""Helpers for composite incremental replication cursors.

A composite cursor is the complete sort position of a record: the scalar
``replication_key`` value followed by one or more stable tiebreaker values.
It lets a sorted incremental stream resume from an exact position even when
many records share the same replication key value.

This module is internal to the SDK. The public API is exposed by
:attr:`singer_sdk.Stream.replication_key_tiebreakers` and
:meth:`singer_sdk.Stream.get_starting_replication_key_values`.
"""

from __future__ import annotations

import datetime
import re
import typing as t
from dataclasses import dataclass

from singer_sdk.helpers._compat import datetime_fromisoformat

if t.TYPE_CHECKING:
    from collections.abc import Sequence

    from singer_sdk import Stream

# State keys used to persist a composite cursor alongside the legacy scalar
# ``replication_key`` / ``replication_key_value`` keys.
COMPOSITE_STATE_KEYS = frozenset(
    {
        "replication_key_type",
        "replication_key_tiebreakers",
        "replication_key_tiebreaker_types",
        "replication_key_values",
    },
)

#: The canonical, atomically-written composite cursor value list.
COMPOSITE_VALUES_KEY = "replication_key_values"

INTEGER = "integer"
STRING = "string"
DATE_TIME = "date-time"

# RFC3339 with a mandatory UTC offset: seconds 00-59, up to six fractional
# digits, ``T`` separator, and ``Z`` or ``+HH:MM`` / ``-HH:MM``.
_RFC3339_RE = re.compile(
    r"^"
    r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.[0-9]{1,6})?"
    r"(?P<offset>Z|[+-][0-9]{2}:[0-9]{2})"
    r"$",
)


@dataclass(frozen=True, slots=True)
class CursorField:
    """A single field in a composite replication cursor."""

    name: str
    """The top-level record field name."""

    kind: str
    """One of ``integer``, ``string`` or ``date-time``."""

    @property
    def type_token(self) -> str:
        """Return the stable state token for this field's type."""
        return self.kind


def _is_null_schema(option: t.Any) -> bool:  # noqa: ANN401
    """Return whether a schema option only permits ``null``.

    Args:
        option: A JSON Schema type definition.

    Returns:
        True when the option's ``type`` is exactly ``"null"``.
    """
    return isinstance(option, t.Mapping) and option.get("type") == "null"


def _schema_type_kind(  # noqa: C901
    *,
    stream_name: str,
    field_name: str,
    type_dict: t.Any,  # noqa: ANN401
    allow_datetime: bool,
) -> str:
    """Determine the cursor kind for a JSON Schema type definition.

    Args:
        stream_name: Stream name, used in error messages.
        field_name: Field name, used in error messages.
        type_dict: JSON Schema definition for the field.
        allow_datetime: Whether a ``date-time`` formatted string is allowed.

    Returns:
        The cursor kind: ``integer``, ``string`` or ``date-time``.

    Raises:
        ValueError: If the type is not a supported top-level scalar type.
    """
    if not isinstance(type_dict, t.Mapping):  # pragma: no cover - defensive
        msg = (
            f"Composite replication cursor field '{field_name}' in stream "
            f"'{stream_name}' has an invalid schema definition."
        )
        raise ValueError(msg)  # noqa: TRY004

    for union_keyword in ("anyOf", "oneOf"):
        if union_keyword in type_dict:
            options = type_dict[union_keyword]
            if not isinstance(options, list):
                msg = (
                    f"Composite replication cursor field '{field_name}' in stream "
                    f"'{stream_name}' uses an unsupported {union_keyword!r} union type."
                )
                raise ValueError(msg)
            # A nullable declaration such as
            # ``anyOf: [{"type": "integer"}, {"type": "null"}]`` is allowed as
            # long as exactly one non-null option remains.
            non_null_options = [
                option for option in options if not _is_null_schema(option)
            ]
            if len(non_null_options) != 1:
                msg = (
                    f"Composite replication cursor field '{field_name}' in stream "
                    f"'{stream_name}' uses an unsupported {union_keyword!r} union "
                    "type. Only a single non-null string or integer type is "
                    "supported."
                )
                raise ValueError(msg)
            return _schema_type_kind(
                stream_name=stream_name,
                field_name=field_name,
                type_dict=non_null_options[0],
                allow_datetime=allow_datetime,
            )

    if "allOf" in type_dict:
        msg = (
            f"Composite replication cursor field '{field_name}' in stream "
            f"'{stream_name}' uses an unsupported 'allOf' union type. "
            "Only a single non-null string or integer type is supported."
        )
        raise ValueError(msg)

    raw_type = type_dict.get("type")
    if isinstance(raw_type, list):
        non_null = [item for item in raw_type if item != "null"]
        if len(non_null) != 1:
            msg = (
                f"Composite replication cursor field '{field_name}' in stream "
                f"'{stream_name}' has an unsupported union type {raw_type!r}. "
                "Only a single non-null string or integer type is supported."
            )
            raise ValueError(msg)
        raw_type = non_null[0]

    if raw_type == INTEGER:
        return INTEGER
    if raw_type == STRING:
        if type_dict.get("format") != DATE_TIME:
            return STRING
        if not allow_datetime:
            msg = (
                f"Composite replication tiebreaker field '{field_name}' in "
                f"stream '{stream_name}' may not use the 'date-time' format."
            )
            raise ValueError(msg)
        return DATE_TIME

    msg = (
        f"Composite replication cursor field '{field_name}' in stream "
        f"'{stream_name}' has unsupported type {raw_type!r}. "
        "Only string and integer fields are supported."
    )
    raise ValueError(msg)


def _validate_tiebreaker_sequence(  # noqa: C901
    *,
    stream_name: str,
    replication_key: str | None,
    tiebreakers: t.Any,  # noqa: ANN401
    schema_properties: t.Mapping[str, t.Any],
) -> tuple[CursorField, ...]:
    """Validate and derive cursor fields from stream configuration.

    Args:
        stream_name: Stream name, used in error messages.
        replication_key: The stream's replication key.
        tiebreakers: The declared tiebreaker sequence.
        schema_properties: Top-level schema properties.

    Returns:
        The cursor fields, primary first.

    Raises:
        ValueError: If the configuration is invalid.
    """
    if isinstance(tiebreakers, (str, bytes)) or not isinstance(
        tiebreakers,
        t.Sequence,
    ):
        msg = (
            f"Stream '{stream_name}' replication_key_tiebreakers must be a "
            "non-empty sequence of field names, not a string or a scalar."
        )
        raise ValueError(msg)  # noqa: TRY004

    if not tiebreakers:
        msg = (
            f"Stream '{stream_name}' replication_key_tiebreakers must be non-empty "
            "when composite replication is enabled."
        )
        raise ValueError(msg)

    if not replication_key:
        msg = (
            f"Stream '{stream_name}' has replication_key_tiebreakers but no "
            "replication_key. Composite replication requires a replication key."
        )
        raise ValueError(msg)

    names: list[str] = []
    for item in tiebreakers:
        if not isinstance(item, str) or not item:
            msg = (
                f"Stream '{stream_name}' replication_key_tiebreakers entries must be "
                f"non-empty strings. Got {item!r}."
            )
            raise ValueError(msg)
        names.append(item)

    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        msg = (
            f"Stream '{stream_name}' replication_key_tiebreakers contains duplicate "
            f"fields: {duplicates!r}."
        )
        raise ValueError(msg)

    if replication_key in names:
        msg = (
            f"Stream '{stream_name}' replication_key_tiebreakers may not contain the "
            f"replication_key '{replication_key}'."
        )
        raise ValueError(msg)

    if replication_key not in schema_properties:
        msg = (
            f"Composite replication cursor field '{replication_key}' in stream "
            f"'{stream_name}' is not present in the effective schema."
        )
        raise ValueError(msg)

    fields = [
        CursorField(
            name=replication_key,
            kind=_schema_type_kind(
                stream_name=stream_name,
                field_name=replication_key,
                type_dict=schema_properties[replication_key],
                allow_datetime=True,
            ),
        ),
    ]

    for name in names:
        if name not in schema_properties:
            msg = (
                f"Composite replication cursor field '{name}' in stream "
                f"'{stream_name}' is not present in the effective schema."
            )
            raise ValueError(msg)
        fields.append(
            CursorField(
                name=name,
                kind=_schema_type_kind(
                    stream_name=stream_name,
                    field_name=name,
                    type_dict=schema_properties[name],
                    allow_datetime=False,
                ),
            ),
        )

    return tuple(fields)


def derive_cursor_fields(stream: Stream) -> tuple[CursorField, ...]:
    """Derive and validate the composite cursor fields for a stream.

    Args:
        stream: The stream defining ``replication_key`` and tiebreakers.

    Returns:
        The cursor fields in declaration order, primary first.

    Raises:
        ValueError: If the stream configuration or schema is invalid.
    """
    schema = stream.effective_schema
    properties = schema.get("properties", {})
    return _validate_tiebreaker_sequence(
        stream_name=stream.name,
        replication_key=stream.replication_key,
        tiebreakers=stream.replication_key_tiebreakers,
        schema_properties=properties,
    )


def _location(
    *,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None,
    source_index: int | None,
) -> str:
    """Build a human-readable location string for error messages.

    Args:
        stream_name: Stream name.
        context: Partition context, if any.
        source_index: One-based source record index, if any.

    Returns:
        A location string such as ``stream 'x', partition {...}, source record #1``.
    """
    location = f"stream '{stream_name}'"
    if context:
        location += f", partition {dict(context)!r}"
    if source_index is not None:
        location += f", source record #{source_index}"
    return location


def parse_rfc3339(
    value: t.Any,  # noqa: ANN401
    *,
    field_name: str,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None = None,
    source_index: int | None = None,
) -> datetime.datetime:
    """Parse a cursor date-time value using strict RFC3339 rules.

    Args:
        value: The string value from a record or state bookmark.
        field_name: Field name, used in error messages.
        stream_name: Stream name, used in error messages.
        context: Partition context, used in error messages.
        source_index: One-based source record index, used in error messages.

    Returns:
        A timezone-aware :class:`datetime.datetime`.

    Raises:
        ValueError: If the value is not a valid RFC3339 date-time.
    """
    location = _location(
        stream_name=stream_name,
        context=context,
        source_index=source_index,
    )
    match = _RFC3339_RE.match(value) if isinstance(value, str) else None
    if match is None:
        msg = (
            f"Composite replication cursor field '{field_name}' in {location} "
            f"has invalid date-time value {value!r}. Expected RFC3339 with an "
            "explicit UTC offset (seconds 00-59, at most 6 fractional digits, "
            "using 'T' and 'Z' or '+HH:MM'/'-HH:MM')."
        )
        raise ValueError(msg)

    offset = match.group("offset")
    if offset != "Z":
        hours, minutes = (int(part) for part in offset[1:].split(":"))
        max_offset_hours = 23
        max_offset_minutes = 59
        if hours > max_offset_hours or minutes > max_offset_minutes:
            msg = (
                f"Composite replication cursor field '{field_name}' in {location} "
                f"has invalid UTC offset in date-time value {value!r}."
            )
            raise ValueError(msg)

    try:
        result = datetime_fromisoformat(value)
    except ValueError as exc:
        msg = (
            f"Composite replication cursor field '{field_name}' in {location} "
            f"has invalid date-time value {value!r}."
        )
        raise ValueError(msg) from exc

    if result.tzinfo is None:  # pragma: no cover - regex requires an offset
        result = result.replace(tzinfo=datetime.timezone.utc)
    return result


def validate_cursor_value(
    value: t.Any,  # noqa: ANN401
    field: CursorField,
    *,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None,
    source_index: int | None,
) -> t.Any:  # noqa: ANN401
    """Validate a cursor value and return it untouched.

    Values are never converted: strings stay strings and integers stay
    integers. Booleans and floats are rejected for integer fields.

    Args:
        value: The value to validate.
        field: The cursor field definition.
        stream_name: Stream name, used in error messages.
        context: Partition context, used in error messages.
        source_index: One-based source record index, if applicable.

    Returns:
        The original value.

    Raises:
        ValueError: If the value does not match the field type.
    """
    location = _location(
        stream_name=stream_name,
        context=context,
        source_index=source_index,
    )

    if value is None:
        msg = (
            f"Composite replication cursor field '{field.name}' in {location} "
            "has a null value. Composite cursor values may not be null."
        )
        raise ValueError(msg)

    if field.kind == INTEGER:
        if isinstance(value, bool) or not isinstance(value, int):
            msg = (
                f"Composite replication cursor field '{field.name}' in {location} "
                f"has value {value!r} of type {type(value).__name__}; expected "
                "a non-null integer (booleans and floats are not accepted)."
            )
            raise ValueError(msg)
        return value

    if not isinstance(value, str):
        msg = (
            f"Composite replication cursor field '{field.name}' in {location} "
            f"has value {value!r} of type {type(value).__name__}; expected "
            "a non-null string."
        )
        raise ValueError(msg)  # noqa: TRY004

    if field.kind == DATE_TIME:
        parse_rfc3339(
            value,
            field_name=field.name,
            stream_name=stream_name,
            context=context,
            source_index=source_index,
        )
    return value


def validate_start_value(
    value: t.Any,  # noqa: ANN401
    field: CursorField,
    *,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None,
) -> t.Any:  # noqa: ANN401
    """Validate a scalar bookmark value used as a starting point.

    Args:
        value: Bookmark or ``start_date`` value.
        field: Primary cursor field definition.
        stream_name: Stream name, used in error messages.
        context: Partition context, used in error messages.

    Returns:
        The original value.

    Raises:
        ValueError: If the value does not match the primary field type.
    """
    kind_value = value
    if field.kind == DATE_TIME and isinstance(value, str):
        # ``start_date`` values are allowed to omit an offset (older SDK
        # behavior treats naive values as UTC), but must still parse.
        try:
            datetime_fromisoformat(value)
        except ValueError as exc:
            msg = (
                f"Composite replication cursor start value for stream "
                f"'{stream_name}'"
                + (f", partition {dict(context)!r}" if context else "")
                + f" is not a valid date-time: {value!r}."
            )
            raise ValueError(msg) from exc
        return kind_value

    return validate_cursor_value(
        value,
        field,
        stream_name=stream_name,
        context=context,
        source_index=None,
    )


def cursor_keys(
    values: Sequence[t.Any],
    fields: Sequence[CursorField],
    *,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None = None,
    source_index: int | None = None,
) -> tuple[t.Any, ...]:
    """Validate cursor values and build comparable keys.

    Args:
        values: Raw cursor values, primary first.
        fields: Cursor field definitions.
        stream_name: Stream name, used in error messages.
        context: Partition context, used in error messages.
        source_index: One-based source record index, if applicable.

    Returns:
        A tuple of comparable keys. Date-time fields are represented by
        timezone-aware datetimes so equal instants compare equal regardless of
        their textual offset.

    Raises:
        ValueError: If the number or type of values is invalid.
    """
    if len(values) != len(fields):
        msg = (
            f"Composite replication cursor for stream '{stream_name}' expected "
            f"{len(fields)} values for fields "
            f"{[field.name for field in fields]!r}, got {len(values)}."
        )
        raise ValueError(msg)

    keys: list[t.Any] = []
    for value, field in zip(values, fields, strict=True):
        validated = validate_cursor_value(
            value,
            field,
            stream_name=stream_name,
            context=context,
            source_index=source_index,
        )
        if field.kind == DATE_TIME:
            keys.append(
                parse_rfc3339(
                    validated,
                    field_name=field.name,
                    stream_name=stream_name,
                    context=context,
                    source_index=source_index,
                ),
            )
        else:
            keys.append(validated)
    return tuple(keys)


def compare_cursor_keys(
    left: tuple[t.Any, ...],
    right: tuple[t.Any, ...],
) -> int:
    """Compare two already-normalized cursor keys.

    Args:
        left: Left cursor key tuple.
        right: Right cursor key tuple.

    Returns:
        -1 if ``left < right``, 0 if equal, and 1 if ``left > right``.
    """
    if left < right:
        return -1
    if left > right:
        return 1
    return 0


def parse_start_datetime(value: t.Any) -> datetime.datetime | None:  # noqa: ANN401
    """Parse a scalar date-time start value, treating naive values as UTC.

    Args:
        value: A date-time string.

    Returns:
        An aware datetime, or ``None`` if the value cannot be parsed.
    """
    if not isinstance(value, str):
        return None
    try:
        result = datetime_fromisoformat(value)
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=datetime.timezone.utc)
    return result


def composite_state_tokens(
    fields: Sequence[CursorField],
) -> tuple[list[str], list[str]]:
    """Return the persisted field-name and field-type lists.

    Args:
        fields: Cursor field definitions, primary first.

    Returns:
        A tuple of ``(tiebreaker_names, tiebreaker_types)``.
    """
    return (
        [field.name for field in fields[1:]],
        [field.type_token for field in fields[1:]],
    )


def state_contains_composite_keys(state: t.Mapping[str, t.Any]) -> bool:
    """Return whether a state dict contains any composite cursor keys.

    Args:
        state: A stream or partition state dictionary.

    Returns:
        True if at least one composite cursor state key is present.
    """
    return any(key in state for key in COMPOSITE_STATE_KEYS)


def write_composite_bookmark(
    state: dict[str, t.Any],
    *,
    fields: Sequence[CursorField],
    values: Sequence[t.Any],
) -> None:
    """Persist a complete composite cursor as a single state write.

    All cursor fields are written together so that an interrupted run can
    never leave the primary value and tiebreaker values from different
    records combined in the bookmark.

    Args:
        state: Writable stream or partition state dictionary.
        fields: Cursor field definitions, primary first.
        values: Raw cursor values for the successful record, primary first.
    """
    names, types = composite_state_tokens(fields)
    state["replication_key"] = fields[0].name
    state["replication_key_value"] = values[0]
    state["replication_key_type"] = fields[0].type_token
    state["replication_key_tiebreakers"] = names
    state["replication_key_tiebreaker_types"] = types
    state[COMPOSITE_VALUES_KEY] = list(values)


def validate_composite_state(  # noqa: C901
    state: t.Mapping[str, t.Any],
    *,
    stream_name: str,
    replication_key: str | None,
    fields: Sequence[CursorField] | None,
    context: t.Mapping[str, t.Any] | None = None,
) -> tuple[t.Any, ...] | None:
    """Validate persisted composite cursor state against current configuration.

    Args:
        state: Stream or partition state dictionary.
        stream_name: Stream name, used in error messages.
        replication_key: The stream's current replication key.
        fields: Current cursor fields, or ``None`` if composite mode is disabled.
        context: Partition context, used in error messages.

    Returns:
        The raw cursor values ``(primary, *tiebreakers)`` if composite state is
        present, otherwise ``None``.

    Raises:
        ValueError: If the state is incomplete, corrupt, or inconsistent with
            the current configuration or schema.
    """
    if not state_contains_composite_keys(state):
        return None

    location = f"stream '{stream_name}'"
    if context:
        location += f", partition {dict(context)!r}"

    if fields is None:
        msg = (
            f"Composite replication cursor state exists for {location} but "
            "replication_key_tiebreakers is not enabled on this stream. Refusing "
            "to resume from an incompatible bookmark."
        )
        raise ValueError(msg)

    missing = sorted(key for key in COMPOSITE_STATE_KEYS if key not in state)
    if missing:
        msg = (
            f"Composite replication cursor state for {location} is incomplete; "
            f"missing keys: {missing!r}."
        )
        raise ValueError(msg)

    expected_names, expected_types = composite_state_tokens(fields)
    actual_names = state["replication_key_tiebreakers"]
    actual_types = state["replication_key_tiebreaker_types"]
    actual_values = state[COMPOSITE_VALUES_KEY]
    actual_primary_type = state["replication_key_type"]

    if not isinstance(actual_names, list) or actual_names != expected_names:
        msg = (
            f"Composite replication cursor state for {location} declares "
            f"tiebreakers {actual_names!r}, but the stream is configured with "
            f"{expected_names!r}."
        )
        raise ValueError(msg)

    if not isinstance(actual_types, list) or actual_types != expected_types:
        msg = (
            f"Composite replication cursor state for {location} declares "
            f"tiebreaker types {actual_types!r}, but the current schema requires "
            f"{expected_types!r}."
        )
        raise ValueError(msg)

    if actual_primary_type != fields[0].type_token:
        msg = (
            f"Composite replication cursor state for {location} declares primary "
            f"type {actual_primary_type!r}, but the current schema requires "
            f"{fields[0].type_token!r}."
        )
        raise ValueError(msg)

    if not isinstance(actual_values, list) or len(actual_values) != len(fields):
        msg = (
            f"Composite replication cursor state for {location} must contain "
            f"{len(fields)} cursor value(s), got {actual_values!r}."
        )
        raise ValueError(msg)

    if state.get("replication_key") != replication_key:
        msg = (
            f"Composite replication cursor state for {location} references "
            f"replication_key {state.get('replication_key')!r}, but the stream "
            f"uses {replication_key!r}."
        )
        raise ValueError(msg)

    if state.get("replication_key_value") != actual_values[0]:
        msg = (
            f"Composite replication cursor state for {location} has a primary "
            "'replication_key_value' that does not match the stored cursor values."
        )
        raise ValueError(msg)

    raw_values = tuple(actual_values)

    for field, value in zip(fields, raw_values, strict=True):
        validate_cursor_value(
            value,
            field,
            stream_name=stream_name,
            context=context,
            source_index=None,
        )

    return raw_values


def get_record_cursor_values(
    record: t.Mapping[str, t.Any],
    fields: Sequence[CursorField],
    *,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None,
    source_index: int,
) -> tuple[t.Any, ...]:
    """Extract and validate all composite cursor values from a record.

    Args:
        record: The record, after ``post_process``.
        fields: Cursor field definitions, primary first.
        stream_name: Stream name, used in error messages.
        context: Partition context, used in error messages.
        source_index: One-based source record index.

    Returns:
        Raw cursor values, primary first.

    Raises:
        ValueError: If a cursor field is missing or has an invalid value.
    """
    values: list[t.Any] = []
    for field in fields:
        if field.name not in record:
            msg = (
                f"Composite replication cursor field '{field.name}' is missing from "
                f"stream '{stream_name}'"
                + (f", partition {dict(context)!r}" if context else "")
                + f", source record #{source_index}."
            )
            raise ValueError(msg)
        values.append(
            validate_cursor_value(
                record[field.name],
                field,
                stream_name=stream_name,
                context=context,
                source_index=source_index,
            ),
        )
    return tuple(values)


def scalar_key(
    value: t.Any,  # noqa: ANN401
    field: CursorField,
    *,
    stream_name: str,
    context: t.Mapping[str, t.Any] | None,
) -> t.Any:  # noqa: ANN401
    """Normalize a scalar bookmark value for comparison.

    Args:
        value: The scalar value from state or ``start_date``.
        field: Primary cursor field definition.
        stream_name: Stream name, used in error messages.
        context: Partition context, used in error messages.

    Returns:
        A comparable key: an aware datetime for ``date-time``, otherwise the
        original string or integer.
    """
    if field.kind == DATE_TIME:
        parsed = parse_start_datetime(value)
        if parsed is None:
            msg = (
                f"Composite replication cursor start value for stream "
                f"'{stream_name}'"
                + (f", partition {dict(context)!r}" if context else "")
                + f" is not a valid date-time: {value!r}."
            )
            raise ValueError(msg)
        return parsed

    return validate_cursor_value(
        value,
        field,
        stream_name=stream_name,
        context=context,
        source_index=None,
    )
