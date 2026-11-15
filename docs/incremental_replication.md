# Incremental Replication

With incremental replication, a Singer tap emits only data that were created or updated since the previous import rather than the full table.

To support incremental replication, the tap must first define how its replication state will be tracked, e.g. the id of the newest record or the maximal update timestamp in the previous import.

You'll either have to manage your own [state file](https://hub.meltano.com/singer/spec#state-files-1), or use Meltano. The Singer SDK makes the tap state available through the [context object](./context_object.md) on subsequent runs. Using the state, the tap should then skip returning rows where the replication key comes _strictly before_ than previous maximal replication key value stored in the state.

## Example Code: Timestamp-Based Incremental Replication

```py
class CommentsStream(RESTStream):
    replication_key = "date_gmt"
    is_sorted = True

    schema = th.PropertiesList(
        th.Property(
            "date_gmt",
            th.DateTimeType(nullable=True),
            description="Date",
        ),
    ).to_dict()

    def get_http_request(self, *, page: PageContext) -> HTTPRequest:
        request = super().get_http_request(page=page)

        if starting_date := self.get_starting_timestamp(page.stream_context):
            request.params["after"] = starting_date.isoformat()

        request.params["page"] = page.next_page_token

        self.logger.info("QUERY PARAMS: %s", request.params)
        return request
```

1. First we inform the SDK of the `replication_key`, which automatically triggers incremental import mode.

1. Second, optionally, set `is_sorted` to true if the records are monotonically increasing (i.e. newer records always come later). With this setting, the sync will be resumable if it's interrupted at any point and the state file will reflect this. Otherwise, the tap has to run to completion so the state can safely reflect the largest replication value seen.

1. Last, we have to adapt the query to the remote system, in this example by adding a query parameter with the ISO timestamp.

   The [`get_starting_timestamp`](singer_sdk.Stream.get_starting_timestamp) method and the related [`get_starting_replication_key_value`](singer_sdk.Stream.get_starting_replication_key_value) method, are provided by the SDK and return the last replication key value seen in the previous run. If the tap is run for the first time and the value for the `start_date` setting is null, the method will return `None`.

```{note}
- The SDK will throw an error if records come out of order when `is_sorted` is true.
- Unlike a `primary_key`, a `replication_key` does not have to be unique
- In incremental replication, it is OK and usually recommended to resend rows where the replication key is equal to previous highest key. Targets are expected to update rows that are re-synced.
```

## Composite replication cursors

A single `replication_key` cannot describe a resumable position when many records
share the same value (for example, thousands of rows with the same `updated_at`
timestamp). In that case the bookmark only says which timestamp was processed, not
which of those rows was the last one delivered.

Set `replication_key_tiebreakers` on the stream to track the **complete position**
instead: the `replication_key` value followed by one or more stable tiebreaker
fields, compared in declaration order. Records must be sorted by that complete
position.

```py
class EventsStream(RESTStream):
    name = "events"
    replication_key = "updated_at"
    # Secondary sort fields, in comparison order.
    replication_key_tiebreakers = ("shard_id", "event_id")
    is_sorted = True

    def get_records(self, context):
        # ``position`` is the complete starting cursor, frozen at the start of
        # the sync: (updated_at, shard_id, event_id) or None on a first run.
        position = self.get_starting_replication_key_values(context)
        ...
```

Requirements and rules:

- `replication_key_tiebreakers` must be a non-empty sequence of unique field names.
  It may not be a string, may not contain the `replication_key`, and each field must
  exist in the effective schema as a top-level `string` or `integer` field
  (`["string", "null"]` / nullable declarations are accepted, but the actual cursor
  values may not be `null`).
- Composite cursors are only supported for incremental streams with
  `is_sorted = True`, `check_sorted = True` and `state_partitioning_keys = None`,
  and never in `BATCH` mode. If any requirement is not met, the stream raises
  `ValueError` before reading any source data instead of silently falling back.
- A `date-time` formatted `replication_key` is compared as an instant. Strings must
  be RFC3339 with an explicit UTC offset (`T` separator, seconds `00`-`59`, at most
  6 fractional digits, `Z` or `+HH:MM`/`-HH:MM`). Different textual offsets for the
  same instant are equal. Other `string` tiebreakers are compared by Unicode code
  point; `date-time` is not accepted for tiebreakers.
- Integer cursor values accept integers only. `bool` and `float` are rejected, and
  values are never converted (values such as `0`, `""` and large integers survive
  JSON round-trips unchanged).

`get_starting_replication_key_values(context)` returns the complete starting cursor
tuple, or `None` when the stream is not in composite mode, when no composite
bookmark exists, or when a later `start_date` replaces it. The original
`get_starting_replication_key_value(context)` and `get_starting_timestamp(context)`
keep returning the scalar primary-key starting point. The starting position is fixed
for the duration of the sync and does not advance as records are processed; sources
can use it to build an efficient `(key, tiebreaker...) > (?, ?...)` query. The SDK
still filters out the already-processed prefix after `post_process()` and before
stream maps, so a source may also return the full sorted prefix.

`get_records` remains responsible for the actual query. Returning a complete sorted
prefix is always safe; narrowing the query with the complete starting cursor is an
optimization.

### Strict ordering

Within a partition, complete positions left by `post_process()` (including restored
prefix records that will be filtered out) must strictly increase. The primary key may
repeat, but a complete position that is equal to or smaller than the previous one
raises `InvalidStreamSortException`, naming both positions and source record numbers.
This check runs before the record is written or any child stream is triggered.
Source record numbers start at 1 in each partition; restored prefix records count.

### Migrating from a scalar bookmark

When composite mode is enabled for the first time on a stream that has only a scalar
`replication_key_value` bookmark, the SDK migrates safely:

- Records whose primary value is **strictly less** than the saved value are skipped.
- Records whose primary value **equals** the saved value are replayed (the SDK does
  not guess the tiebreaker values).
- `get_starting_replication_key_values()` returns `None` in this case; the scalar
  getter still returns the saved value.

For date-time replication keys, `start_date` is still compared to the saved bookmark
and the later boundary wins. If `start_date` is strictly later, it becomes the new
scalar lower bound and the equal-value group is replayed. Different offsets for the
same instant are treated as equal.

### Persisted state

The composite bookmark is stored next to the legacy scalar keys:

```json
{
  "bookmarks": {
    "events": {
      "replication_key": "updated_at",
      "replication_key_value": "2024-01-01T00:00:00Z",
      "replication_key_type": "date-time",
      "replication_key_tiebreakers": ["shard_id", "event_id"],
      "replication_key_tiebreaker_types": ["integer", "string"],
      "replication_key_values": ["2024-01-01T00:00:00Z", 7, "evt_42"]
    }
  }
}
```

The values are written together in a single step, so an interrupted run cannot
combine a primary value with tiebreaker values from a different record. State is
plain JSON and can be passed to a new `Tap` instance with `json.dumps`/`json.loads`.
Corrupt, incomplete, or configuration/schema-incompatible composite state — and
composite state present while the feature is disabled — raises `ValueError` before
source data is read. Legacy scalar-only state remains readable.

On resume, records at or before the saved position are not emitted as RECORD
messages, do not trigger child streams, and are not counted as delivered records.
Empty results, or sources that only return the restored prefix, leave the existing
progress unchanged.

### Delivery and acknowledgement boundaries

The bookmark only advances after the record has been successfully written and its
child streams have run. If a record fails (writer, mapping, or child stream error),
a subsequent STATE never moves past the failing record; already-emitted valid
prefixes are not retracted and the original exception propagates. Stream maps run
after the cursor is captured, so mappings may drop cursor fields, filter rows, or
emit multiple records while positions still come from the pre-mapping record.
Successfully processed rows that maps filter out still advance the bookmark.
The existing RECORD/STATE ordering and `STATE_MSG_FREQUENCY` semantics are unchanged.

Composite streams do not generate signposts; the committed position is always a
record that was actually processed. Only STATE messages confirmed downstream should
be used as input to the next run. This still does not provide exactly-once delivery
to arbitrary external systems: output not confirmed before a crash may be replayed.
See [At Least Once](./implementation/at_least_once.md).

## Manually testing incremental import during development

To test the tap in standalone mode, manually create a state file and run the tap:

```shell
$ echo '{"bookmarks": {"documents": {"replication_key": "date_gmt", "replication_key_value": "2023-01-15T12:00:00.120000"}}}' > state_test.json

$ tap-my-example --config tap_config_test.json --state state_test.json
```

## Additional References

- [Tap SDK State](./implementation/state.md)
- [Context Object](./context_object.md)
- [Example tap with get_starting_replication_key_value](https://github.com/flexponsive/tap-eu-ted/blob/main/tap_eu_ted/client.py)
