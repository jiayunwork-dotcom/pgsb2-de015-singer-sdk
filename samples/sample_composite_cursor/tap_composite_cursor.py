"""Sample tap illustrating composite replication cursors.

The source here is an in-memory ordered list standing in for a database table.
Real taps would translate the complete starting cursor into a SQL/API query,
for example::

    WHERE (updated_at, shard_id, event_id) > (?, ?, ?)

or, for sources without row-value comparison::

    WHERE updated_at > ?
       OR (updated_at = ? AND shard_id > ?)
       OR (updated_at = ? AND shard_id = ? AND event_id > ?)
"""

from __future__ import annotations

import typing as t

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers import types


class EventsStream(Stream):
    """An incrementally replicated, fully ordered event stream."""

    name = "events"

    replication_key = "updated_at"
    # Track the complete sort position, not just the timestamp. Many events can
    # share an ``updated_at`` value, so ``shard_id`` and ``event_id`` make the
    # position unique and resumable.
    replication_key_tiebreakers = ("shard_id", "event_id")
    is_sorted = True
    check_sorted = True

    primary_keys: t.ClassVar[list[str]] = ["event_id"]
    schema = th.PropertiesList(
        th.Property("updated_at", th.DateTimeType, required=True),
        th.Property("shard_id", th.IntegerType, required=True),
        th.Property("event_id", th.StringType, required=True),
        th.Property("payload", th.StringType),
    ).to_dict()

    # Emulated source data, already sorted by the complete cursor position.
    SOURCE_ROWS: t.ClassVar[list[dict[str, t.Any]]] = [
        {
            "updated_at": "2024-01-01T00:00:00Z",
            "shard_id": 0,
            "event_id": "evt_0",
            "payload": "a",
        },
        {
            "updated_at": "2024-01-01T00:00:00Z",
            "shard_id": 0,
            "event_id": "evt_1",
            "payload": "b",
        },
        {
            "updated_at": "2024-01-01T00:00:00Z",
            "shard_id": 1,
            "event_id": "evt_2",
            "payload": "c",
        },
        {
            "updated_at": "2024-01-01T00:00:01Z",
            "shard_id": 0,
            "event_id": "evt_3",
            "payload": "d",
        },
    ]

    def get_records(
        self,
        context: types.Context | None,
    ) -> Iterable[dict[str, t.Any]]:
        """Yield source rows after the complete starting position.

        The SDK also filters the restored prefix, so returning every row is safe.
        Building the query from the complete starting cursor is an optimization
        that lets the source skip the already-processed prefix itself.

        Args:
            context: Stream partition context.

        Yields:
            Source rows sorted by ``(updated_at, shard_id, event_id)``.
        """
        position = self.get_starting_replication_key_values(context)

        for row in self.SOURCE_ROWS:
            if position is None:
                yield row
                continue

            # Compare the complete tuple ``(primary, *tiebreakers)``. Date-time
            # values must be strings with an explicit UTC offset for a plain
            # tuple comparison to be correct.
            current = (row["updated_at"], row["shard_id"], row["event_id"])
            if current > tuple(position):
                yield row


class TapCompositeCursor(Tap):
    """Sample tap using composite replication cursors."""

    name = "tap-composite-cursor"

    def discover_streams(self) -> list[Stream]:
        """Discover streams.

        Returns:
            The sample events stream.
        """
        return [EventsStream(self)]


if __name__ == "__main__":
    TapCompositeCursor.cli()
