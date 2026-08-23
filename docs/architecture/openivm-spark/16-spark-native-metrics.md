# 16 — Spark-native OpenIVM metrics

OpenIVM exports runtime performance metrics through Spark's native Dropwizard
registry. The implementation is a `SparkPlugin`; it registers an `openivm`
`MetricSet` on the driver and executor, then hot paths update counters, gauges,
histograms, and timers directly. No OpenTelemetry SDK is loaded into Spark.

Enable it with:

```properties
spark.plugins=org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin
spark.openivm.metrics.enabled=true
spark.metrics.conf=/path/to/metrics.properties
```

A Prometheus scrape uses Spark's built-in servlet sink, for example:

```properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
```

In the benchmark container, `spark.metrics.namespace` is normally
`local_spark_openivm`, so Spark exports driver plugin metrics with this prefix:

```
metrics_local_spark_openivm_driver_plugin_org_openivm_spark_plugin_metrics_OpenIvmMetricsPlugin_openivm_
```

Spark's `PrometheusServlet` replaces every non-alphanumeric character with `_`
and appends a Dropwizard suffix. For example, the logical timer
`openivm.rocksdb.scope.mv.operation.get.lock.wait` is exported as families such
as:

```
metrics_local_spark_openivm_driver_plugin_org_openivm_spark_plugin_metrics_OpenIvmMetricsPlugin_openivm_rocksdb_scope_mv_operation_get_lock_wait_Count{type="timers"}
metrics_local_spark_openivm_driver_plugin_org_openivm_spark_plugin_metrics_OpenIvmMetricsPlugin_openivm_rocksdb_scope_mv_operation_get_lock_wait_95thPercentile{type="timers"}
```

Gauges use `Number` and `Value` suffixes, counters use `Count`, histograms use
`Count/Max/Mean/...`, and timers use histogram suffixes plus rate suffixes.
There are no Prometheus labels beyond Spark's `{type="..."}` label; low-cardinality
dimensions are encoded into the family name.

## Metric catalogue

All names below are under Spark's normal source prefix and the `openivm.` group.
Dropwizard timers report count/rates plus duration snapshots in Spark's metrics
unit conventions; duration inputs are nanoseconds.

| Metric | Type | Unit / scope | Answers |
| --- | --- | --- | --- |
| `openivm.rocksdb.scope.<scope>.operation.<op>.count` | counter | operations; scope `mv/table/index/source/refresh_profile/refresh_sql_log/other` | How many RocksDB calls ran by state scope? |
| `openivm.rocksdb.scope.<scope>.operation.<op>.failed` | counter | failed operations | Are RocksDB calls failing? |
| `openivm.rocksdb.scope.<scope>.operation.<op>.total` | timer | ns | Total operation latency. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.lock.wait` | timer | ns | Time waiting for the in-JVM exclusive write lock. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.lock.hold` | timer | ns | Time holding the in-JVM exclusive write lock. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.external_lock.wait` | timer | ns | Time waiting for the multi-process POSIX file lock. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.native.open` | timer | ns | Native RocksDB open latency. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.native.open.count` | counter | opens | Whether multi-process mode is reopening per operation. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.native.close` | timer | ns | Native RocksDB close latency. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.native.close.count` | counter | closes | Whether multi-process mode is closing per operation. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.body` | timer | ns | Operation body time after locks/open. |
| `openivm.rocksdb.scope.<scope>.operation.<op>.multi_process.count` | counter | operations | Which calls used multi-process mode. |
| `openivm.rocksdb.scope.<scope>.physical_write.count` | counter | RocksDB writes | How many bounded physical `WriteBatch` writes backed one logical commit? |
| `openivm.rocksdb.scope.<scope>.physical_write.failed` | counter | failed writes | Did a native batch write fail? |
| `openivm.rocksdb.scope.<scope>.physical_write.latency` | timer | ns | Native batch-write latency, excluding manual flush wait. |
| `openivm.rocksdb.scope.<scope>.physical_write.bytes` | histogram | bytes | Approximate bytes per bounded physical write. |
| `openivm.rocksdb.scope.<scope>.physical_write.keys` | histogram | keys | Keys per bounded physical write. |
| `openivm.rocksdb.scope.<scope>.flush.count` | counter | manual flushes | How many coalesced durability flushes ran? |
| `openivm.rocksdb.scope.<scope>.flush.failed` | counter | failed flushes | Did a manual flush fail? |
| `openivm.rocksdb.scope.<scope>.flush.latency` | timer | ns | Actual native manual-flush wait, excluding lock acquisition and writes. |
| `openivm.rocksdb.scope.<scope>.flush.column_families` | histogram | column families | How many touched column families were flushed together? |
| `openivm.rocksdb.scope.<scope>.commit_batch.latency` | timer | ns | End-to-end logical commit latency. |
| `openivm.rocksdb.scope.<scope>.commit_batch.failed` | counter | failed commits | Did the logical commit fail before publishing a version? |
| `openivm.rocksdb.scope.<scope>.commit_batch.bytes` | histogram | bytes | Approximate key/value bytes per batch. |
| `openivm.rocksdb.scope.<scope>.commit_batch.keys` | histogram | keys | Keys mutated per batch. |
| `openivm.rocksdb.scope.<scope>.commit_batch.sst_files` | histogram | files | Live SST count after commit. |
| `openivm.rocksdb.scope.<scope>.commit_batch.version_bump` | counter | versions | Manifest/version bumps. |
| `openivm.rocksdb.column_family.<cf>.read.count` | counter | reads; cf `meta/properties/consumed/cdf_watermarks/staging/dependent_mvs` | Which column families are read most? |
| `openivm.rocksdb.column_family.<cf>.read.bytes` | counter | bytes | Approximate bytes read by CF. |
| `openivm.rocksdb.column_family.<cf>.write.count` | counter | writes | Which column families are written most? |
| `openivm.rocksdb.column_family.<cf>.write.bytes` | counter | bytes | Approximate bytes written by CF. |
| `openivm.rocksdb.registry.open_handles` | gauge | handles | Number of cached native DB handles. |
| `openivm.rocksdb.registry.get_or_open.hit` | counter | calls | Registry cache hits. |
| `openivm.rocksdb.registry.get_or_open.miss` | counter | calls | Registry cache misses / new opens. |
| `openivm.rocksdb.maintenance.run.count` | counter | runs | Maintenance daemon executions. |
| `openivm.rocksdb.maintenance.run` | timer | ns | Maintenance wall time. |
| `openivm.rocksdb.maintenance.lock_hold` | timer | ns | Maintenance exclusive-lock hold time. |
| `openivm.rocksdb.maintenance.compaction.count` | counter | compactions | Maintenance compactions. |
| `openivm.refresh.queued` | gauge | refreshes | Refreshes waiting on the per-MV mutex. |
| `openivm.refresh.inflight` | gauge | refreshes | Refreshes currently executing. |
| `openivm.refresh.lock.wait` | timer | ns | Refresh mutex wait time. |
| `openivm.refresh.phase.<phase>` | timer | ns | Per-refresh phase latency bridged from `RefreshProfile`. |
| `openivm.refresh.sql_stmt.<kind>` | timer | ns | Per SQL statement latency by kind (`merge`, `ctas`, `delete`, etc.). |
| `openivm.refresh.sql_stmt.<kind>.bytes` | histogram | SQL bytes | Statement size by kind. |
| `openivm.refresh.sql_stmt.<kind>.retry` | counter | retries | Delta OCC retries by statement kind. |
| `openivm.refresh.sql_stmt.<kind>.rows_read` | counter | rows | Rows read, when Spark/Delta exposes row-read SQL metrics for the executed plan. |
| `openivm.refresh.sql_stmt.<kind>.rows_written` | counter | rows | Rows written/updated/deleted/output, when exposed by Spark/Delta SQL metrics. |
| `openivm.refresh.sql_stmt.<kind>.plan.<metric>` | histogram | Spark SQL metric value | Raw executed-plan SQL metrics by Spark metric name. |
| `openivm.refresh.sql_stmt.<kind>.bytes_observed` | counter | bytes | Bytes observed in executed-plan SQL metrics. |
| `openivm.create.inflight` | gauge | creates | CREATE MATERIALIZED VIEW commands executing. |
| `openivm.create.phase.<phase>` | timer | ns | Per-CREATE phase latency, including analyze, compile, CTAS, metadata publish. |
| `openivm.compiler.cache.lock.wait` | timer | ns | Contention on the compiler singleton cache. |
| `openivm.compiler.compile.count` | counter | compiles | DuckDB compile requests. |
| `openivm.compiler.compile` | timer | ns | End-to-end compile latency. |
| `openivm.compiler.duckdb_subprocess` | timer | ns | DuckDB CLI subprocess latency. |
| `openivm.compiler.inflight` | gauge | compiles | Concurrent compiles. |
| `openivm.compiler.classification_cache.hit` | counter | hits | Refresh classification-cache hits. |
| `openivm.compiler.classification_cache.miss` | counter | misses | Refresh classification-cache misses. |
| `openivm.catalog.mv_catalog.<op>` | timer | ns | Delta MV catalog commit latency by op. |
| `openivm.catalog.mv_catalog.retry` | counter | retries | Delta MV catalog conflict retries. |
| `openivm.catalog.cdf_watermark.<op>` | timer | ns | Delta CDF watermark commit latency by op. |
| `openivm.catalog.cdf_watermark.retry` | counter | retries | Delta CDF watermark conflict retries. |
| `openivm.ctas.queue_depth` | gauge | tasks | CTAS dispatcher queued tasks. |
| `openivm.ctas.admission_width` | gauge | tasks | Current CTAS admission width. |
| `openivm.ctas.active_threads` | gauge | threads | CTAS worker threads in flight. |
| `openivm.ctas.task.queue_wait` | timer | ns | Per-task queue wait. |
| `openivm.ctas.task.execution` | timer | ns | Per-task execution time. |
| `openivm.ctas.task.retry` | counter | pressure failures | Capacity-pressure retry/backoff signals. |

The Dropwizard registry has no label model, so low-cardinality labels are encoded
in dotted names. High-cardinality values such as individual MV names and file
paths are deliberately excluded.
