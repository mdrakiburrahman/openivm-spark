# Cloud RocksDB state roadmap

OpenIVM-Spark currently treats RocksDB as both a local embedded database and a
durable catalog shared by Spark driver processes. Those are incompatible roles:
RocksDB supports concurrent threads sharing one writable handle, but it does not
support multiple writer processes opening one database. Object stores also do
not provide the filesystem locking and atomic file operations expected by a live
RocksDB directory.

Production telemetry from a ten-driver CTAS reproduction attributed 40.1 aggregate
seconds to the RocksDB wrapper: 10.3 seconds waiting for the cross-process lock,
27.7 seconds opening and closing native databases, and 0.94 seconds executing
database operations. The implementation is split into independently testable and
deployable phases.

## Target architecture

Delta is the authoritative catalog shared by Spark drivers. RocksDB becomes an
optional local cache or a single-writer per-shard store. Durable RocksDB state is
published as immutable, versioned checkpoints rather than by copying a live
database tree.

```text
Spark drivers ───────────────> Delta catalog (authoritative)
      |                         MV metadata, dependencies, watermarks,
      |                         refresh transactions
      |
      +── local/per-shard ───> RocksDB (rebuildable)
                                versioned checkpoints in object storage
```

## Phase 1: operation-scoped sessions (implemented)

One logical catalog operation acquires a database lock and native handle once.
Nested point reads, prefix scans, and write batches reuse that scope. Metadata
fields are fetched with a batched multi-get. A session must remain short and
must never contain Spark execution or acquire another database session.

Success criteria:

- one native open/close pair for an MV metadata load in multi-process mode;
- no behavioral change in single-process mode;
- existing multi-process correctness tests remain green;
- telemetry distinguishes the outer session from nested operations.

## Phase 2: remove the global RocksDB index (implemented)

The MV and base-table database paths are deterministic, so `mv_index` and
`table_index` are removed. State is partitioned by MV and base table. CDF
watermarks move into the owning MV shard. The source-to-MV reverse index is
temporarily partitioned into one shard per source and later moves to the durable
catalog in phase 3. Fresh deployments never create the global RocksDB index;
legacy CDF rows migrate lazily into MV shards. Unrelated CTAS operations then
share no RocksDB lock, while views over the same source serialize only their
short dependency-index update.

## Phase 3: Delta-backed catalog authority (implemented)

Introduce a catalog-backend interface with local RocksDB compatibility and a
Delta implementation for cloud deployments. Delta owns MV definitions,
dependencies, watermarks, and refresh transaction state. Refreshes use an
idempotent `PREPARED -> DATA_COMMITTED -> COMMITTED` protocol so recovery can
reconcile a driver failure between the data and catalog commits.

Select it with:

```properties
spark.openivm.catalog.backend=delta
spark.openivm.catalog.path=abfss://container@account/path/openivm-catalog
```

The path contains separate Delta tables for MV metadata, CDF watermarks, and
refresh transactions. The default remains `rocksdb` for local compatibility.
With the Delta backend enabled, a real refresh creates `PREPARED` before data
execution, records the observed MV Delta version as `DATA_COMMITTED`, advances
metadata and watermarks, and publishes `COMMITTED` last. Incomplete rows remain
queryable through `RefreshTransactionCatalog.incompleteForView` for recovery.

## Phase 5: versioned checkpoint publication (planned)

Any RocksDB state retained after phase 3 is checkpointed through a stable
snapshot, uploaded beneath a unique version, verified, and made visible by
publishing a small manifest last. The published version and writer fencing epoch
are recorded in the Delta catalog. Incomplete uploads remain unreachable and are
garbage-collected later. Live RocksDB files and mutable manifests are never
overwritten in place in object storage.

Phase 5 improves durability and object-store compatibility. It does not remove
Spark query analysis or Delta CTAS work from the CREATE critical path.

## CTAS wall-time findings

The same clean workload creates ten projection MVs from a one-million-row Delta
source through ten independent Livy driver processes. Phase telemetry separates
RocksDB time from Spark analysis and the initial Delta write.

| Configuration | Batch wall | Median CREATE | Median analysis | Median facts | Median initial load |
|---|---:|---:|---:|---:|---:|
| Original RocksDB design | 33.31 s | 28.76 s | Not recorded | 6.80 s | 7.95 s |
| Current branch, cold, 32 threads per driver | 30.32 s | 26.85 s | 8.79 s | 0.35 s | 12.78 s |
| Current branch, warm, 32 threads per driver | 26.34 s | 19.19 s | 0.04 s | 0.21 s | 16.06 s |

Source warming is diagnostic. It takes 11--13 seconds and is excluded from the
reported batch wall. It shows that cold Spark and Hive-metastore resolution, not
RocksDB, accounts for the remaining analysis delay.

The CREATE path now reuses its analyzed plan and relation schemas. It also skips
a Delta file-statistics Spark job whose output does not affect OpenIVM compilation.
Refresh still collects current table and delta statistics for its cost model.

RocksDB is no longer the dominant CTAS bottleneck. The remaining timed work is
Spark process startup and catalog resolution for cold sessions, followed by Delta
writes to independent destinations.

### CTAS lower bound and concurrency

A clean concurrency sweep used full `local[32]` execution and a fresh RocksDB
catalog for every point. The source has 16 partitions because
`spark.default.parallelism=16`.

| Concurrent CTAS jobs | Batch wall | Median CREATE | Median initial load |
|---:|---:|---:|---:|
| 1 | 6.02 s | 5.17 s | 4.21 s |
| 2 | 8.06 s | 6.56 s | 5.53 s |
| 4 | 11.06 s | 9.43 s | 8.26 s |
| 8 | 18.16 s | 14.25 s | 12.84 s |
| 10 | 22.22 s | 17.46 s | 15.43 s |

The single-CTAS floor is about 5.2 seconds inside OpenIVM. The harness adds up
to one second of Livy polling delay. The 4.2-second initial load includes Spark
job setup, Delta commit work, and a 16-task file write. It is not all indivisible
CPU work, so multiplying it by five overestimates the ten-CTAS lower bound.

The execution topology is more important than a per-process thread cap:

| Topology | Jobs | Batch wall | Result |
|---|---:|---:|---|
| Independent Spark contexts | 10 | 22.22 s | Jobs overlap, but ten schedulers and JVMs compete for one host |
| One Livy SQL session | 2 | 8.03 s | Livy queues the SQL statements before Spark execution |
| One Spark context, driver thread pool | 2 | 7.07 s | Both CTAS jobs execute concurrently |
| One Spark context, driver thread pool | 10 | 10.08 s | All ten jobs overlap inside Spark |
| Same, cached single-process RocksDB | 10 | 9.09 s | Removes cross-process locking and repeated native close/open |

All ten views in the fastest run contained exactly 1,000,000 rows. Their CREATE
critical paths completed in at most 8.19 seconds. The remaining difference is
Livy submission and polling overhead.

Use one warm, long-lived Spark application with an in-driver CTAS dispatcher.
Each request must execute from its own driver thread. Spark can then schedule the
jobs concurrently through one thread-safe `SparkContext`. Configure FAIR
scheduling when concurrent requests need latency fairness; FIFO remains
work-conserving but favors earlier jobs. A single Livy SQL interpreter does not
provide this concurrency because it serializes statements before they reach
Spark.

This topology also restores the intended single-process RocksDB model. RocksDB
handles remain local and cached while Spark tasks execute in parallel. Phase 3
remains the durable multi-driver catalog when deployments require more than one
Spark application.

The dispatcher can run CTAS jobs concurrently only when their output locations
are independent. Operations targeting the same MV still require explicit
serialization or transactional conflict handling.

### Adaptive admission policy

Do not configure one CTAS thread or one fixed concurrency value per machine.
Queue every request and let one Spark application admit work through a bounded
executor. The bound changes at runtime.

Spark already performs hardware-aware task scheduling. An admitted CTAS can
expose every runnable partition without reserving a fixed core count for that
query. FAIR scheduler pools divide available task slots across active CTAS jobs.
The dispatcher controls query-level pressure that Spark's task scheduler does
not bound, including driver memory, concurrent planning, output commits, and
storage bandwidth.

Use this feedback loop:

1. When no workload history exists, optimistically submit the complete finite
   batch of independent CTAS jobs. Spark FAIR scheduling, rather than a
   one-query warm-up, allocates executor task slots between them.
2. Run admission feedback in observation-only mode during this cold batch.
   Track active and pending tasks, JVM heap and GC pressure, spill bytes, input
   and output throughput, Delta commit latency, and explicit failures.
3. Apply multiplicative backoff immediately if hard pressure appears. Otherwise,
   do not delay the first batch merely to find a latency-optimal concurrency.
4. For a continuous request stream, increase the admission window while
   completed throughput improves and normalized service-time inflation remains
   low, and reduce it when throughput stops improving, spill or GC rises, or
   Delta commit conflicts occur.
5. Persist the learned window by Spark application, query-shape class, and
   storage endpoint. Re-probe after executor capacity, workload shape, or
   storage behavior changes.

The control signals use ratios against the workload's own recent baseline, not
machine-specific core counts or bandwidth constants. Executor changes and cloud
autoscaling therefore cause the controller to converge on a new limit.

A ten-job width sweep demonstrates why the limit cannot be fixed:

| Maximum active CTAS | Batch wall | Median CTAS critical path |
|---:|---:|---:|
| 1 | 20.10 s | 1.48 s |
| 2 | 14.09 s | 1.76 s |
| 4 | 11.11 s | 2.18 s |
| 8 | 11.09 s | 7.51 s |
| 10 | 10.07 s | 8.41 s |

Four active jobs are near the throughput knee on the measured host. Ten active
jobs minimize this finite batch's makespan but increase individual CTAS latency.
The controller must optimize an explicit objective such as throughput, latency,
or a weighted combination.

Starting at width one would make a cold ten-view benchmark take 20.10 seconds,
roughly twice the 10.07-second all-at-once result. No feedback controller can
infer an unseen machine's optimum before receiving samples. The cold-start
choice must therefore express the objective: optimize finite-batch makespan by
starting wide, or protect interactive tail latency by starting conservatively.
OpenIVM's CTAS batch path uses the former. Benchmark reports must label cold and
learned runs separately so persisted controller state cannot silently improve a
reported result.

This does not require a Spark query cost model. A reusable implementation can
take the concurrency-window algorithm from Netflix `concurrency-limits` and add
a small OpenIVM adapter for FAIR-pool assignment, observation-only sampling,
persisted limits, and pressure signals. Latency-only samples should be normalized
by query-shape history because CTAS jobs are heterogeneous. Before such history
exists, loss-based AIMD driven by spill, GC, commit conflicts, and failures is a
safer guardrail than treating a naturally long CTAS as congestion.

Concurrency does not reduce or increase the logical output required by CTAS.
Each independent materialized view still writes its own Delta table. In the
width sweep, every configuration produced 170 Parquet files and approximately
164 MB across the source and ten views. Higher concurrency changes when those
bytes are written, not how many bytes or files are produced.

Spark does not fuse independent CTAS statements or share their scans by default.
Ten views over the same source perform ten logical reads and ten required output
writes. The operating-system and storage caches can reduce physical source reads.
Explicit source caching or multi-query optimization can remove repeated reads,
but it is a separate optimization and must account for cache capacity and query
snapshot consistency.

## Verification

All phases and telemetry are maintained on one issue branch. The multi-driver
ivm-bench topology is the acceptance workload. Reported metrics include operation
count, JVM and external lock wait, native open/close time, database body time,
per-phase CREATE time, CTAS wall time, and failures under forced driver termination.
