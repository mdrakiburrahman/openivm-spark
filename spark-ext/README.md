# openivm-spark

Spark 3.5 / Delta Lake 3.2 SQL extension delivering **OpenIVM incremental view maintenance** without Delta CDF.

> Status: under active development

## Layout

```text
spark-ext/
├── build.sbt
├── project/{Dependencies.scala, Settings.scala, plugins.sbt, build.properties}
├── .sbtopts, .scalafmt.conf
├── ivm-executor/      # executor-side classes (DeltaStagingExec, MergeWriterExec)
├── ivm-common/        # library: catalogs, metadata, assemblers, FeatureGate
├── ivm-compiler/      # OpenIvmCompiler (DuckDB CLI) + LptsSparkDialect
├── ivm-extension/     # SparkSessionExtensions entry + ANTLR grammar + commands + rules
├── ivm-it/            # integration tests + 100-spec parity suite vs openivm
└── dev/
    ├── dev            # single entry-point CLI wrapper (build, test, verify, shell, …)
    ├── docker/        # multi-stage Dockerfile + docker-compose.yml
    └── pins.env       # pinned SHAs of openivm / lpts / ivm-bench forks + spark / delta refs
```

## Supported RefreshTypes

The bridge classifies materialized-view queries (via openivm's `PRAGMA
compile_refresh`) and dispatches to one of several Spark-side rewrite paths:

| RefreshType            | Code | Strategy                                         | Reference spec         |
| ---------------------- | ---- | ------------------------------------------------ | ---------------------- |
| `AGGREGATE_GROUP`      | 0    | Keyed MERGE (additive monoid)                    | `AggregateGroupSpec`   |
| `SIMPLE_AGGREGATE`     | 1    | Scalar MERGE / UPDATE                            | `SimpleAggregateSpec`  |
| `SIMPLE_PROJECTION`    | 2    | Rowid-keyed signed MERGE                         | `SimpleProjectionSpec` |
| `FULL_REFRESH`         | 3    | `INSERT OVERWRITE`                               | `FullRefreshSpec`      |
| `AGGREGATE_HAVING`     | 4    | MERGE on data table + view wrapper               | `AggregateHavingSpec`  |
| `WINDOW_PARTITION`     | 5    | Partition-scoped DELETE+INSERT                   | `WindowPartitionSpec`  |
| `GROUP_RECOMPUTE`      | 6    | Affected-keys DELETE+INSERT                      | `GroupRecomputeSpec`   |
| `TOP_K`                | 7    | Explicit demote to `FULL_REFRESH`                | `TopKSpec`             |
| `DISTINCT_INCREMENTAL` | 8    | COUNT(\*)-monoid MERGE                           | `DistinctSpec`         |
| `SEMI_ANTI_RECOMPUTE`  | 9    | Currently fallbacks to FULL_REFRESH (documented) | `SemiAntiSpec`         |

MIN/MAX grouped aggregates use the `AGGREGATE_GROUP` affected-groups path
(`AggregateMinMaxSpec`). N-way INNER/LEFT/RIGHT/FULL OUTER joins ride on the
AGGREGATE_GROUP or SIMPLE_PROJECTION path (`JoinsSpec`). MV-over-MV chains are
supported at depth ≤ 2 (`ChainedSpec`); depth > 2 is out of scope.

## Dev loop (host needs only Docker)

A single entry-point lives at `spark-ext/dev/dev`. Run it with one of the
following subcommands:

```bash
./spark-ext/dev/dev.sh verify                                                     # pins-sync + lint + build + assembly + full test
./spark-ext/dev/dev.sh pins-sync                                                  # clone .temp/{openivm,lpts,ivm-bench} + shallow .temp/{spark,delta} refs, align branches, validate HEAD + ivm-bench Dockerfile ARGs against pins.env
./spark-ext/dev/dev.sh pins-fix                                                   # commit + push uncommitted changes (refusing main/master), then rewrite pins.env + ivm-bench Dockerfile so the next pins-sync reports green
./spark-ext/dev/dev.sh build                                                      # sbt compile
./spark-ext/dev/dev.sh assembly                                                   # sbt ivmExtension/assembly (fat jar)
./spark-ext/dev/dev.sh publish                                                    # publish the versioned fat jar to the ADO Maven feed
./spark-ext/dev/dev.sh test                                                       # sbt test (every suite)
./spark-ext/dev/dev.sh test 'testOnly org.openivm.spark.it.ExtensionLoadingSpec'
./spark-ext/dev/dev.sh fmt                                                        # scalafmtAll (auto-format)
./spark-ext/dev/dev.sh shell                                                      # interactive bash inside the dev image
./spark-ext/dev/dev.sh openivm-test                                               # upstream openivm sqllogictests
./spark-ext/dev/dev.sh dev-build [build|test|all]                                 # iterate on .temp/openivm + .temp/lpts
./spark-ext/dev/dev.sh image-build                                                # docker compose build (force rebuild)
./spark-ext/dev/dev.sh help                                                       # this help text
```

`publish` reads `MAVEN_URL` and `MAVEN_PAT` from the gitignored root `.env`,
computes one immutable version as
`<epoch>.<working-tree-content-hash-int>.0`, and uses native sbt publishing to
upload the assembly classifier at:

```text
org.openivm:ivmextension_2.12:jar:assembly:<version>
ivmextension_2.12-<version>-assembly.jar
```

The content hash covers every tracked file plus every untracked, non-ignored
file, so publishing completed but not-yet-committed feature work cannot reuse
the identity of an older source tree. Ignored credentials, build outputs,
`.temp/`, and `.research/` remain excluded.

The feed contract is intentionally assembly-only: the thin main jar is not
published. The generated POM retains only Spark/Delta/SLF4J dependencies marked
`provided`; internal OpenIVM modules and other compile dependencies are already
inside the fat jar. Consumers must request the `assembly` classifier rather
than the unclassified artifact. Local `ivmExtension/assembly` output retains the
legacy `ivmExtension-<version>-assembly.jar` filename used by existing image
builds; Maven artifact metadata publishes the same bytes under the lowercase,
Scala-suffixed filename shown above.
Copy `.env.example` to `.env` and populate the private-feed values before use.

`verify` is the canonical one-liner — it first runs `pins-sync` (cloning any
missing `.temp/{openivm,lpts,ivm-bench}` checkouts, fetching origin, and
aligning each to its pinned branch, plus shallow-cloning the read-only
`.temp/{spark,delta}` upstream references at their pinned release tags), then
lints, compiles, assembles the fat jar, and runs every unit + integration +
parity suite in a single sbt JVM. Wall-clock on the reference 32-core / 124 GiB
host is ~40 minutes end-to-end.

`pins-sync` exits non-zero only when a pinned repo or branch is missing on
GitHub (or `.temp/` is corrupt). Drift between the local HEAD and the pinned
COMMIT — or between the `ivm-bench` Dockerfile's `ARG OPENIVM_/LPTS_*`
defaults and `pins.env` — is reported as a `⚠ WARNING` but does not block
`verify`. Bumping any pinned SHA therefore requires editing **both**
`spark-ext/dev/pins.env` **and** the matching `ARG` in
`.temp/ivm-bench/src/containers/spark-openivm-build/Dockerfile`.

`pins-fix` automates that bump end-to-end. Given any combination of
uncommitted changes across `openivm-spark` and `.temp/{openivm,lpts,
ivm-bench}`, it commits each working tree, pushes to `origin/<branch>`
(refusing `main` / `master` / detached HEAD in any of the four repos, and
aborting on rebase conflicts), then deterministically rewrites
`spark-ext/dev/pins.env` and the `ivm-bench` Dockerfile `ARG` defaults so the
next `pins-sync` reports `✓` green. The ordering is designed around the
chicken-and-egg where bumping `IVM_BENCH_COMMIT` advances `openivm-spark`
origin past whatever was just baked into Dockerfile `OPENIVM_SPARK_COMMIT`:
the final pin lags by exactly one commit whose only diff is `pins.env`,
which `pins-sync` accepts as a "benign lag". The command is idempotent —
running it on an already-aligned tree is a no-op.

### Environment variables

| Variable                  | Default | Scope    | Effect                                                                                                                                                                |
| ------------------------- | ------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PRE_CLEAN`               | `0`     | `verify` | When `1`, force-removes every running Docker container on the host before sbt starts. Named cache volumes (`sbt-cache`, `ivy-cache`, `coursier-cache`) are preserved. |
| `openivm.test.forks` (-D) | `32`    | sbt JVM  | Cap on parallel forked test JVMs. Pass via `./spark-ext/dev/dev.sh verify -Dopenivm.test.forks=8` on smaller hosts.                                                   |

The container image is named
`openivm-spark/spark-ext:${OPENIVM_COMMIT}-${LPTS_COMMIT}-${DUCKDB_REF}` so
dependency or ABI changes produce a fresh image without replacing workspace
caches. `DUCKDB_REF` and `DUCKDB_COMMIT` in `pins.env` explicitly select
DuckDB v1.5.2 for both the CLI and native extension; do not infer the deployed ABI
from OpenIVM's upstream submodule or CI version. JDBC stays on 1.5.2.1.
`NATIVE_BUILD_JOBS` bounds native build parallelism (default 8); lower it on
shared hosts.

## Activation in spark-shell / spark-submit

```bash
spark-shell \
    --jars spark-ext/ivm-extension/target/scala-2.12/ivmExtension-0.1.0-SNAPSHOT-assembly.jar \
    --conf spark.sql.extensions=org.openivm.spark.OpenIvmSparkExtensions \
    --conf spark.openivm.enabled=true \
    --conf spark.driver.extraJavaOptions="$(cat .sbtopts | grep -oE '^-J.*' | sed 's/^-J//' | xargs)"
```

The feature gate (`spark.openivm.enabled`) defaults to false, so the jar is
opt-in even when on the classpath.

### Completed execution telemetry

Set `spark.openivm.telemetry.uri` to a campaign-scoped Hadoop filesystem URI
to publish one completed JSON object per CREATE/REFRESH request. The request
must also supply nonsecret `openivm.campaign_id`, `openivm.request_id`,
`openivm.correlation_id`, `openivm.node_id`, and `openivm.phase` local
properties; the campaign, correlation, and phase values may instead use the
`spark.openivm.telemetry.campaignId`,
`spark.openivm.telemetry.correlationId`, and
`spark.openivm.telemetry.phase` configuration keys.

Version 1 objects are atomically renamed from `_temporary/v1/*.partial` to
`completed/v1/<sha256-execution-identity>.json`. Consumers ingest only the
completed path. Re-publishing identical content is idempotent; different
content for the same identity fails. The public constants live in
`OpenIvmTelemetryContract`, and the packaged schema is
`openivm-telemetry-span-v1.schema.json`. Export failures fail the SQL operation;
when the URI is unset, the existing log-only span behavior is unchanged.
Same-request retries of `CREATE MATERIALIZED VIEW IF NOT EXISTS` and
already-applied `ADVANCE SOURCE VERSIONS` reuse only a complete, identity-matched
accepted object; a new request identity publishes a complete explicit
short-circuit outcome. The schema's
`x-openivm-w6-required-success-fields` annotation is the ingestion-required
field set for every accepted outcome. `create_already_exists` is valid only
when `operation=create`; the schema's operation/outcome map is authoritative.
Reusable successes require a nonempty `source_versions` array that is unique
and ascending by lower-cased canonical relation key. The internal duration may
differ from the timestamp interval by at most 5 ms.

### Request-scoped SQL log export (JVM API v1)

Enable `spark.openivm.queryLog.enabled=true` in **SparkContext startup
configuration**. `spark.openivm.profile.refresh` can remain false. This exports
the existing OpenIVM CREATE/REFRESH logger, not universal Spark SQL interception
or every DataFrame operation.

The public Scala object `org.openivm.spark.common.QueryLogExport` exposes:

```scala
apiVersion(): Int // 1
begin(spark: SparkSession, requestId: String): Unit
end(spark: SparkSession, requestId: String, sqlSucceeded: Boolean): Unit
snapshotJson(spark: SparkSession, requestId: String, maxRows: Int, maxBytes: Int): String
release(spark: SparkSession, requestId: String): Unit
```

Reserve a unique request immediately before the **outer** SQL action. The SQL
worker must have the matching `openivm.request_id` Spark local property:

```python
api = spark._jvm.org.openivm.spark.common.QueryLogExport
request_id = "example-create-unique-request"
previous = spark.sparkContext.getLocalProperty("openivm.request_id")
spark.sparkContext.setLocalProperty("openivm.request_id", request_id)
try:
    api.begin(spark._jsparkSession, request_id)
    succeeded = False
    try:
        spark.sql(ddl).collect()
        succeeded = True
    finally:
        api.end(spark._jsparkSession, request_id, succeeded)
finally:
    spark.sparkContext.setLocalProperty("openivm.request_id", previous)
```

Return the request descriptor from the model worker immediately; do not poll or
export there. An independent exporter can call
`snapshotJson(spark._jsparkSession, request_id, 100000, 67108864)` from another
Py4J thread/session in the **same application**. `begin`, `end`, and readers do
not use thread IDs. Only native logger startup reads the worker's local property.
There is no `SHOW OPENIVM QUERY LOG`, whole-catalog scan, global flush barrier,
synchronous queue-overflow persistence, or Spark write job in this export path.
Indexed reads use the existing registry-owned RocksDB handle and can experience
ordinary storage-lock contention, but never wait for unrelated captures to finish.

The UTF-8 JSON envelope has:

| Field | Meaning |
| --- | --- |
| `schema`, `version` | `openivm.query-log-export`, `1` |
| `application_id`, `request_id` | Exact capture identity |
| `status` | `running`, `pending_flush`, `complete`, `failed`, or `missing` |
| `capture_complete` | Outer action ended, native lifecycles finished, and all admitted rows persisted successfully |
| `sql_succeeded` | Caller-supplied Boolean; `null` before `end` |
| `record_count`, `pending_flushes` | Admitted row count and this request's unacknowledged flush count |
| `invocations` | Ordered objects with native `refresh_id`, `view_name`, `mode`, `outcome`, `record_count`, `completed` |
| `records` | Complete rows in invocation/collector append order; empty while capture is incomplete |
| `failure` | `null` or `{ "code": "...", "message": "..." }` |
| `truncated` | Always `false`; partial successful traces are never returned |

Each record preserves `refresh_id`, `view_name`, `profile_timestamp` (UTC ISO
timestamp), `stmt_order`, `attempt_idx`, `mode`, `category`, `stmt_kind`,
`duration_ms`, and **full** `sql_text`. Retries and rows sharing the legacy
timestamp/order/attempt key are retained separately. `representation_kind` is
conservative: `original_query`, `explain_plan`, `submitted_sql`, or `diagnostic`.
Only known SQL submission call sites are `submitted_sql` (including failed
attempts; this label does **not** assert statement success). Source-delta
reconstructions, synthetic cascade/cleanup representations, and unknown
categories are `diagnostic`, regardless of their SQL-looking text.

Accept only `status=complete`, `capture_complete=true`, `sql_succeeded=true`,
and `failure=null`. A successful **zero-row** invocation additionally requires
a completed native refresh no-op outcome: `no_pending_deltas`, `noop_fast_exit`,
`source_versions_already_applied`, or `runtime_empty_delta_skip`. Outcomes retain
their native lowercase spelling. An ended request without a native logger
lifecycle fails with `NO_LIFECYCLE`; scan emptiness is never no-op evidence.
A captured SQL failure has `status=failed` / `SQL_FAILED` and may still have
`capture_complete=true` with its entire diagnostic trace. `failed` can appear
while that request still has pending flushes: when collecting failed-SQL
diagnostics, wait for `pending_flushes=0` and every invocation's `completed=true`
before evaluating `capture_complete` or releasing it. Logging/async failures are
exposed by this API and do not replace the original SQL error or alter MV state.

Stage a complete JSON snapshot to a run-owned driver file and transfer it using
the client's file API when stdout is too small. The client can render a readable
SQL artifact from the raw records, preserving category/order/attempt metadata
and distinguishing diagnostics from submitted SQL. Call `release` **only after
both local artifacts are durably stored**. It removes this request's export
index/metadata, not the cumulative legacy SHOW log. Unfinished/busy releases
fail explicitly; missing/already-released requests are an idempotent no-op.

All retention settings below use the `spark.openivm.queryLog.export.` prefix
and are read from SparkContext configuration at the application's first `begin`:

| Suffix | Default | Bound |
| --- | ---: | --- |
| `maxCaptures` | 1024 | Unreleased request slots per application |
| `maxInvocations` | 128 | Native lifecycles per request |
| `maxRecords` | 100000 | Admitted rows per request |
| `maxBytes` | 67108864 | UTF-8 row payload plus accounting overhead per request |
| `maxTotalRecords` | 1000000 | Admitted rows across unreleased requests |
| `maxTotalBytes` | 536870912 | Admitted row bytes across unreleased requests |

There is no eviction of unexported captures. Invalid IDs/configuration, duplicate
reservation, or exhausted slots throw from `begin` before SQL starts. Row/byte
limits, missing records, disabled logging, and asynchronous failures fail capture
explicitly. Reader `maxRows` and `maxBytes` must be positive; `maxBytes` bounds
the **entire serialized UTF-8 envelope**. A too-small reader bound returns
`SNAPSHOT_LIMIT_EXCEEDED` without partial records and can be retried with larger
bounds. If even its failure envelope cannot fit, the call throws instead.

Completion metadata is application-lifetime, not a crash-recovery protocol.
An unknown/released request or a different/restarted application returns
`missing` / `CAPTURE_MISSING`. Export before stopping the application; missing
capture or transfer failure must prevent downstream acceptance.

### JVM module-opens block (JDK 17)

The extension and its tests require Spark's standard JDK-17 `--add-opens` /
`--add-exports` flags:

```text
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
```

These live in `spark-ext/.sbtopts` for sbt-launched JVMs and must be replicated
in `spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions` when
running spark-shell / spark-submit.

## Standalone streaming tables

With the feature gate enabled, the extension adds a declarative SQL surface for
native Structured Streaming queries in the caller's existing `SparkSession`:

```sql
CREATE STREAMING TABLE IF NOT EXISTS monitoring.cleaned_events
USING DELTA
LOCATION '/tables/cleaned_events'
PARTITIONED BY (event_date)
OPTIONS (
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

### Destination layouts

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
declaration to resume. `DROP STREAMING TABLE` is destructive: it stops the
owned query and removes its owned registration, target data, and checkpoint.

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
replacement of the extension-owned target and checkpoint.

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

## IVM DDL

```sql
-- Create a materialized view
CREATE MATERIALIZED VIEW sales_summary AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
  FROM sales GROUP BY region;

-- Refresh after DML on base tables
REFRESH MATERIALIZED VIEW sales_summary;

-- Atomically advance every immutable VERSION AS OF source pin
ALTER MATERIALIZED VIEW historical_sales
  ADVANCE SOURCE VERSIONS (sales = 43, regions = 17);

-- Drop
DROP MATERIALIZED VIEW IF EXISTS sales_summary;
```

`ADVANCE SOURCE VERSIONS` requires an exact map covering all and only pinned
sources. It never resolves "latest": each supplied Delta version is validated,
the old/new snapshot delta is applied through the existing incremental program,
and the query pins, pin telemetry, source watermarks, and MV version are
published together only after the data apply succeeds. Repeating the same map is
a no-op.

DML on a tracked base table (INSERT / DELETE / UPDATE / MERGE) is intercepted
by `IvmDmlInterceptorRule`, which tees the change set to a per-base-table Delta
staging path keyed by `(base_table, op_type, txn_ts)`. `REFRESH MATERIALIZED VIEW`
recompiles the view via openivm + lpts (target dialect `spark`), rewrites the
emitted DuckDB-style SQL into Spark-executable form (`SparkRefreshRewriter` +
`LptsSparkDialect`), and applies it to the MV's Delta table.

## License

See [LICENSE](../LICENSE).
