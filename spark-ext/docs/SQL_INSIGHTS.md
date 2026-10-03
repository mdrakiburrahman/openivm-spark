# Request-scoped SQL insights (SQL API v1)

The insight broker exposes OpenIVM behavior through SQL only. It retains
bounded request captures in driver memory; it writes no insight files, does not
use Spark UI state, and `SHOW OPENIVM INSIGHTS` is a driver-only
`LeafRunnableCommand` that submits no Spark job.

Wrap one outer dbt/model SQL action with a unique request identity:

```sql
OPENIVM INSIGHTS BEGIN REQUEST 'request-42'
  RUN 'dbt-run-20261003'
  NODE 'model.analytics.sales_summary'
  MATERIALIZATION 'materialized_view'
  TARGET 'analytics.sales_summary';

CREATE MATERIALIZED VIEW sales_summary AS
SELECT region, SUM(amount) AS total
FROM sales
GROUP BY region;

OPENIVM INSIGHTS END REQUEST 'request-42' STATUS SUCCEEDED;

SHOW OPENIVM INSIGHTS FOR REQUEST 'request-42' AFTER 0 LIMIT 1000;

OPENIVM INSIGHTS RELEASE REQUEST 'request-42';
```

The `MATERIALIZATION ... TARGET ...` pair is optional, so the original short
`BEGIN ... NODE ...` form remains valid. The broker derives stable
`execution_mode` (`streaming`, `materialized_view`, or `regular_spark`) from
the declared materialization and emits the model context in request-start and
request-end details.

Use `STATUS FAILED` when the wrapped SQL action throws. It may carry bounded
identity-only failure context; both fields are optional as a pair and are
invalid with `STATUS SUCCEEDED`:

```sql
OPENIVM INSIGHTS END REQUEST 'request-42' STATUS FAILED
  ERROR_CLASS 'org.apache.spark.SparkException'
  ERROR_CODE 'SPARK_JOB_CANCELLED';
```

The failure fields accept only short identifier characters, not exception
messages or SQL. `BEGIN` reserves the capture before model SQL and sets the
Spark local properties `openivm.request_id`, `openivm.run_id`, and
`openivm.node_id`; `END` restores their prior values. This relies on BEGIN, the
wrapped SQL, and END executing on the same driver thread, as they do for a
synchronous sequence of `SparkSession.sql(...).collect()` calls. Servers that
dispatch consecutive SQL statements to different driver threads must
preserve/propagate these local properties themselves.

An active materialized-view request can record the dbt query-hash decision
before its existing DROP/CREATE behavior through the typed annotation command:

```sql
OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW QUERY HASH
  TARGET 'analytics.sales_summary'
  OLD '0123456789abcdef0123456789abcdef'
  NEW 'fedcba9876543210fedcba9876543210'
  POLICY 'rebuild'
  DECISION 'rebuild'
  REASON 'query_hash_changed';
```

This fixed shape accepts a 32-character hexadecimal new hash, a hexadecimal
old hash (or the typed `missing` / `invalid` sentinel), and only the
`rebuild/rebuild` or `fail/rejected` policy/decision pair. Relation and reason
fields are bounded safe identities. It is not an arbitrary logging surface and
has no SQL or message field.

`SHOW` always returns a `status` row first, followed by events whose
`sequence > AFTER`, up to `LIMIT`. `next_sequence` is the last returned event
sequence, or the supplied cursor when no event was returned. `has_more`
indicates another page. An unknown or released request returns one
`capture_status=missing` status row instead of throwing. Capture status is one
of `running`, `complete`, `failed`, `degraded`, or `missing`; `degraded` means
insight emission or a configured bound failed, never that OpenIVM changed the
model outcome.

The ordered output schema is:

| Column              | Type      | Nullable | Meaning                                                                           |
| ------------------- | --------- | -------- | --------------------------------------------------------------------------------- |
| `record_type`       | STRING    | no       | `status` or `event`                                                               |
| `request_id`        | STRING    | no       | Request capture identity                                                          |
| `run_id`            | STRING    | yes      | dbt/run identity supplied to BEGIN                                                |
| `dbt_node_id`       | STRING    | yes      | dbt node identity supplied to BEGIN                                               |
| `capture_status`    | STRING    | no       | Broker capture state                                                              |
| `next_sequence`     | BIGINT    | no       | Cursor for the next SHOW page                                                     |
| `has_more`          | BOOLEAN   | no       | Whether later retained events exist                                               |
| `sequence`          | BIGINT    | yes      | Monotonic per-request event sequence                                              |
| `event_timestamp`   | TIMESTAMP | yes      | Driver event timestamp                                                            |
| `operation_id`      | STRING    | yes      | Refresh/profile or drop/preflight operation ID                                    |
| `materialized_view` | STRING    | yes      | Materialized-view identity                                                        |
| `operation`         | STRING    | yes      | `request`, `preflight`, `create`, `refresh`, `drop`, `streaming`, `regular_spark` |
| `stage`             | STRING    | yes      | Stable lifecycle stage                                                            |
| `event_type`        | STRING    | yes      | Stable semantic event type                                                        |
| `level`             | STRING    | yes      | `info`, `warn`, or `error`                                                        |
| `code`              | STRING    | yes      | Stable action/reason/failure code                                                 |
| `message`           | STRING    | yes      | Bounded human-readable message                                                    |
| `status`            | STRING    | yes      | Request/operation/action outcome                                                  |
| `duration_ms`       | BIGINT    | yes      | Measured duration                                                                 |
| `parent_sequence`   | BIGINT    | yes      | Parent operation-start sequence                                                   |
| `details_json`      | STRING    | yes      | Bounded Jackson-encoded structured details                                        |
| `terminal`          | BOOLEAN   | yes      | Whether the record closes its scope                                               |

Events cover request start/end, CREATE/REFRESH/preflight/DROP operations,
refresh classification and route decisions, source versions and pending
deltas, profile steps, query-log action categories, no-op and failure outcomes,
and DROP cascade/cleanup domains. Query-log action events carry category,
statement kind/order/attempt, duration, `refresh_id`, `request_id`, and whether
query logging is enabled. They never carry SQL. Raw SQL remains exclusively in
the existing `QueryLogExport` / `SHOW OPENIVM QUERY LOG` surfaces.

`details_json` carries stable `branch_code`, `execution_mode`,
`materialization`, and `target_relation` fields where applicable. Public
branch codes are `S1`–`S8` for streaming, `M1`–`M8` for materialized views,
and `R1`–`R3` for regular Spark. Their exact labels are exposed by
`OpenIvmInsightsContract.BranchCode`; human wording remains consumer-owned.
The MV mapping distinguishes incremental create/refresh/no-op (`M1`–`M3`),
compiler-native full refresh (`M4`), an incremental classification demoted to
full refresh with its exact persisted reason (`M5`), verified signed-delta
recompute (`M6`), and query-change rebuild/reject decisions (`M7`/`M8`).
For a request not owned by a streaming or materialized-view operation, END
emits `R1` on success or `R3` on failure. A successful ordinary Spark write
intercepted because its source has dependent OpenIVM views also emits `R2`; it
remains `operation=regular_spark` and is never presented as an MV refresh.
`R2` details contain only the source/target identities, sorted
dependent-MV identities/count, staging operation categories, signed old/new
row semantics, and SHA-256 staging-path identities. They contain neither raw
SQL nor raw storage paths.

The broker installs one application-scoped Spark listener and correlates jobs
only through `openivm.request_id` in job-start properties. The optional
`metrics` object aggregates the wall-clock job span; unique job, stage, and
logical-task counts; input/output records and bytes; shuffle read/write records
and bytes; memory/disk spill bytes; and recognized file read/write
accumulators. Successful stage/task attempts supersede retries, accumulator
IDs are deduplicated, and stage accumulator scans are bounded. A metric key is
omitted when Spark did not expose evidence for it; an absent key must not be
interpreted as zero. Listener synchronization at END drains already-posted
events and never launches a Spark job.

`EXPLAIN CREATE MATERIALIZED VIEW` keeps every existing JSON field and also
adds `graph_version`, `operation`, `materialized_view`,
`compile_refresh_type_name`, `direct_dependencies`, and
`cascade_capability` for DAG rendering.

Retention settings use the `spark.openivm.insights.` prefix and are read from
SparkContext configuration when the application's first request begins:

| Suffix                |  Default | Bound                                        |
| --------------------- | -------: | -------------------------------------------- |
| `maxCaptures`         |     1024 | Unreleased request captures                  |
| `maxEventsPerCapture` |     4096 | Events retained per request                  |
| `maxBytesPerCapture`  |  8388608 | UTF-8 event bytes retained per request       |
| `maxTotalEvents`      |    65536 | Events across unreleased requests            |
| `maxTotalBytes`       | 67108864 | UTF-8 event bytes across unreleased requests |
| `maxDetailsBytes`     |    32768 | `details_json` bytes per event               |
| `maxMessageBytes`     |     2048 | Human message bytes per event                |
| `maxFieldBytes`       |     4096 | Identity/stage/code/status bytes per field   |

There is no eviction of unreleased captures. Duplicate IDs, invalid
configuration, or exhausted capture slots fail `BEGIN` explicitly before model
SQL. Event/byte overflow and emitter defects drop later insight events, mark
the capture `degraded` with a stable `INSIGHTS_*` code, and never fail the
wrapped model SQL. `RELEASE` requires a terminal capture; releasing a missing
request is idempotent.

The public constants and row/page types live in
`org.openivm.spark.insights.OpenIvmInsightsContract`; the broker entry point is
`OpenIvmInsightsBroker`.
