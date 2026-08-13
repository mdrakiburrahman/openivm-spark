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
| Current branch, warm, 3 threads per driver | 24.21 s | 17.17 s | 0.05 s | 0.23 s | 14.56 s |

Source warming is diagnostic. It takes 11--13 seconds and is excluded from the
reported batch wall. It shows that cold Spark and Hive-metastore resolution, not
RocksDB, accounts for the remaining analysis delay.

The CREATE path now reuses its analyzed plan and relation schemas. It also skips
a Delta file-statistics Spark job whose output does not affect OpenIVM compilation.
Refresh still collects current table and delta statistics for its cost model.

RocksDB is no longer the dominant CTAS bottleneck. The remaining timed work is
Spark process startup and catalog resolution for cold sessions, followed by ten
concurrent Delta writes to independent destinations. The benchmark exposes
`SPARK_LOCAL_THREADS` so each local-mode driver can receive a bounded CPU share.
The default preserves `local[*]` behavior.

Further CTAS work must target the execution topology:

- reuse warm, long-lived Spark driver sessions;
- allocate local worker threads across concurrent drivers;
- dispatch concurrent CTAS jobs through fewer Spark contexts where the service
  architecture permits it;
- tune Delta output partitioning only after profiling file counts and task time.

## Verification

All phases and telemetry are maintained on one issue branch. The multi-driver
ivm-bench topology is the acceptance workload. Reported metrics include operation
count, JVM and external lock wait, native open/close time, database body time,
per-phase CREATE time, CTAS wall time, and failures under forced driver termination.
