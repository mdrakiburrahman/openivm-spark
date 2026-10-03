# Request-scoped SQL log export (JVM API v1)

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

| Field                             | Meaning                                                                                               |
| --------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `schema`, `version`               | `openivm.query-log-export`, `1`                                                                       |
| `application_id`, `request_id`    | Exact capture identity                                                                                |
| `status`                          | `running`, `pending_flush`, `complete`, `failed`, or `missing`                                        |
| `capture_complete`                | Outer action ended, native lifecycles finished, and all admitted rows persisted successfully          |
| `sql_succeeded`                   | Caller-supplied Boolean; `null` before `end`                                                          |
| `record_count`, `pending_flushes` | Admitted row count and this request's unacknowledged flush count                                      |
| `invocations`                     | Ordered objects with native `refresh_id`, `view_name`, `mode`, `outcome`, `record_count`, `completed` |
| `records`                         | Complete rows in invocation/collector append order; empty while capture is incomplete                 |
| `failure`                         | `null` or `{ "code": "...", "message": "..." }`                                                       |
| `truncated`                       | Always `false`; partial successful traces are never returned                                          |

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

| Suffix            |   Default | Bound                                                  |
| ----------------- | --------: | ------------------------------------------------------ |
| `maxCaptures`     |      1024 | Unreleased request slots per application               |
| `maxInvocations`  |       128 | Native lifecycles per request                          |
| `maxRecords`      |    100000 | Admitted rows per request                              |
| `maxBytes`        |  67108864 | UTF-8 row payload plus accounting overhead per request |
| `maxTotalRecords` |   1000000 | Admitted rows across unreleased requests               |
| `maxTotalBytes`   | 536870912 | Admitted row bytes across unreleased requests          |

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
