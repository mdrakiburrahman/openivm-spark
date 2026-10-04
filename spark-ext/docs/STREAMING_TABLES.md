# Standalone streaming tables

With the feature gate enabled, the extension adds a declarative SQL surface for
native Structured Streaming queries in the caller's existing `SparkSession`:

```sql
CREATE STREAMING TABLE IF NOT EXISTS monitoring.cleaned_events
USING DELTA
LOCATION '/tables/cleaned_events'
PARTITIONED BY (event_date)
OPTIONS (
  'displayName' = 'model.analytics.cleaned_events',
  'outputMode' = 'append',
  'trigger' = 'processingTime',
  'triggerInterval' = '10 seconds'
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS
SELECT e.id, e.event_time, e.event_date, e.payload
FROM STREAM monitoring.raw_events
WITH (
  'skipChangeCommits' = 'true',
  'maxFilesPerTrigger' = '1000'
) AS e;
```

`STREAM table` and `STREAM(table)` mark only that relation occurrence as
streaming. An ordinary occurrence of the same table remains static. Each
streaming source can carry its own case-insensitive `WITH (...)` reader options;
duplicate keys and simultaneous `startingVersion` / `startingTimestamp` are
rejected before Spark receives the native options.

`displayName` controls the friendly Structured Streaming query label shown in
the Spark UI and micro-batch job descriptions. OpenIVM sanitizes the label and
appends a stable target-identity suffix so independently active tables cannot
collide. When omitted, the target table name is used. Changing only
`displayName` restarts the native writer on its existing checkpoint without
rebuilding the target or replaying committed input.

## Destination layouts

Hive-style partitioning and Delta liquid clustering are separate, mutually
exclusive destination layouts. Layout columns reference the `SELECT` output
names, including aliases:

```sql
-- Hive-partitioned destination
CREATE STREAMING TABLE monitoring.events_by_day
PARTITIONED BY (event_date)
OPTIONS ('trigger' = 'availableNow')
AS
SELECT id, source_date AS event_date, region, payload
FROM STREAM monitoring.raw_events;

-- Liquid-clustered destination
CREATE STREAMING TABLE monitoring.events_clustered
CLUSTER BY (region, event_date)
OPTIONS ('trigger' = 'availableNow')
AS
SELECT id, region_code AS region, source_date AS event_date, payload
FROM STREAM monitoring.raw_events;
```

`PARTITIONED BY` produces native Delta partition columns and directories.
`CLUSTER BY` records native Delta clustering-domain and protocol metadata
without Hive partition columns. Streaming appends preserve the declaration but
do not automatically recluster existing data. Run native `OPTIMIZE` explicitly
when physical clustering maintenance is required; Delta 3.2 supports declaring
a single clustering key, but its Hilbert `OPTIMIZE` path requires multiple keys.

`WATERMARK <named-expression> DELAY OF INTERVAL ...` is optional. When present,
it appears before the relation alias and accepts a named input column or an
explicitly aliased derived timestamp expression:

```sql
FROM STREAM raw_events
WATERMARK timestamp_seconds(epoch_seconds) AS event_time
  DELAY OF INTERVAL 5 MINUTES AS events
```

The complete `SELECT` is parsed and checked by Spark 3.5. CTEs, nested
subqueries, stream-static joins, stream-stream joins, functions, and unsupported
streaming operations retain native Spark semantics. Streaming source providers
and reader options retain native Spark behavior where supported. The managed
target sink remains Delta-only: omitting `USING` selects Delta, and an explicit
non-Delta target provider is rejected.

Lifecycle commands use the same caller session:

```sql
SHOW STREAMING TABLES;
SHOW STREAMING TABLES IN monitoring;
ALTER STREAMING TABLE monitoring.cleaned_events STOP;
DROP STREAMING TABLE IF EXISTS monitoring.cleaned_events;
```

`CREATE` starts asynchronously and the query remains visible through
`spark.streams`. `STOP` retains the target and checkpoint for a matching
declaration to resume. `DROP STREAMING TABLE` stops the owned query and removes
its owned registration and target data after moving its checkpoint to the
configured archive.

Checkpoint archival requires a durable Hadoop-filesystem root outside managed
table storage:

```sql
SET spark.openivm.streaming.checkpointArchive.uri =
  'abfss://<workspace>@<onelake-endpoint>/<lakehouse>/Files/_openivm-archive';
```

Each archived checkpoint is written under
`<archive-root>/<catalog>/<namespace>/<table>/_openivm-checkpoint-<utc-epoch-ms>`.
Its `_openivm-archive-event-v1.json` records the lifecycle action, operation ID,
root target, old target identity, and—when a semantic change caused a rebuild—a
redacted field-level definition diff. OpenIVM fails closed before deleting a
checkpoint when the archive setting is absent or the move cannot complete.

For a SQL client or dbt integration, successful `CREATE` statement completion
means that the declaration was accepted, not that the query finished. Capture
the returned `table_name`, `query_id`, and `run_id`, then poll `SHOW STREAMING
TABLES IN <namespace>` on the same owning `SparkContext`/driver until the row
with that identity is inactive, has status `stopped`, and has an empty
`last_failure`. Treat failed, missing, disconnected, and timeout states as
errors.

Release downstream table references and run post-hooks only after that terminal
check. Repeating an identical `CREATE STREAMING TABLE` resumes its checkpoint;
there is no separate `REFRESH ST` syntax. Once rows are persisted in Delta,
ordinary Spark or SQL clients that share the metastore and storage can query
them.

`AvailableNow` executions terminate naturally after consuming all data currently
available:

```sql
CREATE STREAMING TABLE monitoring.snapshot
OPTIONS ('trigger' = 'availableNow')
AS SELECT id, value FROM STREAM monitoring.raw_events;
```

Reissuing the identical declaration after completion resumes the same checkpoint
and persistent query ID with a new run ID. With no new source commit, the new run
does no source-row or target-data work. New inserts are consumed exactly once on
the next run; already committed input is not replayed.

The checkpoint is bound to the persisted semantic definition. A changed query,
source identity, source semantic option, watermark, output mode, partitioning,
or target property fails without stopping or mutating the existing table by
default. Explicit `OPTIONS ('onQueryChange' = 'rebuild')` opts into destructive
replacement of the extension-owned target. The previous checkpoint remains in
the archive for diagnosis and audit.

Before rebuilding or dropping a managed target, OpenIVM resolves one dependency
graph spanning both streaming tables and materialized views, then drops every
transitively downstream managed object in reverse topological order (leaves
first). This applies to `onQueryChange=rebuild`, `DROP STREAMING TABLE`, and
`DROP MATERIALIZED VIEW`, including mixed chains such as streaming table →
materialized view → streaming table. Downstream native queries are stopped
before their targets and checkpoints are removed. Fan-out and diamond
dependencies are deduplicated. Missing, corrupt, or generation-mismatched
dependency metadata aborts the operation before the requested upstream target
is mutated.

The cascade does not recreate descendants. After an upstream streaming rebuild,
an orchestrator must resubmit all dropped streaming-table and materialized-view
declarations in topological order so new checkpoints and definitions bind to
the replacement source generation. An interrupted streaming rebuild resumes
from its durable reset journal when the exact replacement declaration is
retried.

State-store selection remains ordinary Spark configuration; the extension does
not clone the session or mutate `SparkConf` / `SQLConf`:

```sql
SET spark.sql.streaming.stateStore.providerClass =
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider;
```

The implementation uses native append/complete output modes and native
processing-time or `AvailableNow` triggers. It does not provide continuous
processing, an update-mode MERGE sink, arbitrary Scala state callbacks, or a
pipeline scheduler.
