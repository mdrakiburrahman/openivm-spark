# Cloud RocksDB state roadmap

OpenIVM-Spark currently treats RocksDB as both a local embedded database and a
durable catalog shared by Spark driver processes. Those are incompatible roles:
RocksDB supports concurrent threads sharing one writable handle, but it does not
support multiple writer processes opening one database. Object stores also do
not provide the filesystem locking and atomic file operations expected by a live
RocksDB directory.

Production telemetry from a ten-driver CTAS reproduction attributed 40.1 seconds
to the RocksDB wrapper: 10.3 seconds waiting for the cross-process lock, 27.7
seconds opening and closing native databases, and 0.94 seconds executing database
operations. The implementation is therefore split into independently testable
and deployable phases.

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

## Phase 1: operation-scoped sessions

One logical catalog operation acquires a database lock and native handle once.
Nested point reads, prefix scans, and write batches reuse that scope. Metadata
fields are fetched with a batched multi-get. A session must remain short and
must never contain Spark execution or acquire another database session.

Success criteria:

- one native open/close pair for an MV metadata load in multi-process mode;
- no behavioral change in single-process mode;
- existing multi-process correctness tests remain green;
- telemetry distinguishes the outer session from nested operations.

## Phase 2: remove the global RocksDB index

The MV and base-table database paths are deterministic, so `mv_index` and
`table_index` are removed. State is partitioned by MV and base table. CDF
watermarks move into the owning MV shard; the source-to-MV reverse index moves
to the durable catalog. Unrelated CTAS operations then share no RocksDB lock.

## Phase 3: Delta-backed catalog authority

Introduce a catalog-backend interface with local RocksDB compatibility and a
Delta implementation for cloud deployments. Delta owns MV definitions,
dependencies, watermarks, and refresh transaction state. Refreshes use an
idempotent `PREPARED -> DATA_COMMITTED -> COMMITTED` protocol so recovery can
reconcile a driver failure between the data and catalog commits.

## Phase 5: versioned checkpoint publication

Any RocksDB state retained after phase 3 is checkpointed through a stable
snapshot, uploaded beneath a unique version, verified, and made visible by
publishing a small manifest last. The published version and writer fencing epoch
are recorded in the Delta catalog. Incomplete uploads remain unreachable and are
garbage-collected later. Live RocksDB files and mutable manifests are never
overwritten in place in object storage.

## Verification

Each phase is maintained as a stacked branch and pushed separately. The
multi-driver ivm-bench topology is the acceptance workload. Reported metrics
include operation count, JVM and external lock wait, native open/close time,
database body time, CTAS wall time, and failures under forced driver termination.
