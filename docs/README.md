# 📚 openivm-spark Architecture Documentation

> This document was written by AI but verified and proof-read by a human - `@mdrakiburrahman`

A practical, demo-driven walkthrough of how OpenIVM delivers Incremental View
Maintenance on Spark 3.5 + Delta Lake 3.2 — and the two upstream DuckDB
extensions it leans on (`openivm` and `lpts`).

The docs are organised as **three architectural layers**:

```
                                  ┌────────────────────────────────┐
   User SQL  ─► Spark Catalyst ──►│  openivm-spark   (Scala/Spark) │ ← Layer A: 15 chapters
                                  └────────────┬───────────────────┘
                                               │  DuckDB CLI subprocess
                                  ┌────────────▼───────────────────┐
                                  │  openivm   (DuckDB C++ ext.)   │ ← Layer B: 13 chapters
                                  └────────────┬───────────────────┘
                                               │  function call
                                  ┌────────────▼───────────────────┐
                                  │  lpts      (DuckDB C++ ext.)   │ ← Layer C:  9 chapters
                                  └────────────────────────────────┘
                                       (LogicalPlan → SQL emitter)
```

---

## How to read this

- **Brand new?** Start with [openivm-spark/0.OVERVIEW.md](architecture/openivm-spark/0.OVERVIEW.md), then [openivm/0.OVERVIEW.md](architecture/openivm/0.OVERVIEW.md), then [lpts/0.OVERVIEW.md](architecture/lpts/0.OVERVIEW.md). Each has its own "reading order" section.
- **Want a live walk-through?** Jump straight to the flagship demo: [openivm-spark/13 — End-to-end trace: `AggregateSumSpec`](architecture/openivm-spark/13-end-to-end-trace-aggregatesumspec.md).
- **Trying to understand state-on-disk?** [openivm-spark/7 — State Storage: RocksDB & Delta](architecture/openivm-spark/7-state-storage-rocksdb-and-delta.md).
- **Debugging a FULL_REFRESH?** [openivm-spark/11 — FULL_REFRESH demotion debugging](architecture/openivm-spark/11-full-refresh-demotion-debugging.md) and [openivm/11 — FULL_REFRESH debugging, DuckDB side](architecture/openivm/11-full-refresh-debugging-duckdb-side.md).
- **Want to close a parity gap?** [openivm-spark/12 — Parity gap forensics](architecture/openivm-spark/12-parity-gap-forensics.md).
- **Curious about the math?** [openivm/1 — Z-sets, DBSP, and Möbius](architecture/openivm/1-math-zsets-dbsp-and-the-paper.md).
- **Cost model & profiling?** [openivm/8 — Cost model and adaptive refresh](architecture/openivm/8-cost-model-and-adaptive-refresh.md) and [openivm/9 — Profiling OpenIVM refreshes](architecture/openivm/9-profiling-openivm-refresh-profile.md).

---

## Layer A — `openivm-spark` (Spark 3.5 / Delta 3.2 integration)

The Scala/sbt SessionExtension that wires OpenIVM into Spark.

| #   | Chapter                                                                                                                   | What it answers                                                                 |
| --- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| 0   | [Overview: openivm-spark from 10,000 feet](architecture/openivm-spark/0.OVERVIEW.md)                                      | 3-injection bootstrap, RefreshType table, module chain, master sequence diagram |
| 1   | [SessionExtension wiring and FeatureGate](architecture/openivm-spark/1-session-extension-and-feature-gate.md)             | The docstring-vs-reality discrepancy; per-component gate-check table            |
| 2   | [IVM DDL parser and ANTLR4 grammar](architecture/openivm-spark/2-ivm-ddl-parser-grammar.md)                               | `IvmSqlBase.g4`, re-parse trick, intersection-of-dialects constraint            |
| 3   | [DML interception and staging tee](architecture/openivm-spark/3-dml-interception-and-staging.md)                          | How V2 + Delta-lowered DML are tee-ed to staging without Delta CDF              |
| 4   | [DuckDB CLI compile bridge](architecture/openivm-spark/4-duckdb-cli-compile-bridge.md)                                    | The full PRAGMA contract, JSON-lines protocol, Spark function shims             |
| 5   | [`LptsSparkDialect`: the SQL post-processor](architecture/openivm-spark/5-lpts-spark-dialect-postprocessor.md)            | The ~13 rewrites + `memory.main.*` ThreadLocal expansion                        |
| 6   | [SparkRefreshRewriter and per-StatementKind assemblers](architecture/openivm-spark/6-refresh-rewriter-and-assemblers.md)  | StatementKind dispatch, surviving-statement-count table, `hasRealDelta`         |
| 7   | [⭐ **State Storage: RocksDB and Delta**](architecture/openivm-spark/7-state-storage-rocksdb-and-delta.md)                | `_openivm/` + `_ivm/` layouts, CF schemas, `safePathSegment`, live state        |
| 8   | [Materialized-view lifecycle: CREATE, REFRESH, DROP](architecture/openivm-spark/8-mv-lifecycle-create-refresh-drop.md)    | Per-command code-flow + 3 sequence diagrams + try/catch demotion                |
| 9   | [MV-over-MV cascade, watermarks, and fingerprints](architecture/openivm-spark/9-mv-over-mv-cascade-and-fingerprints.md)   | Depth-2 constraint, `MV_VIEW_DELTA` opType, fingerprint replay                  |
| 10  | [Concurrency: mutex, retry, multi-process locking](architecture/openivm-spark/10-concurrency-mutex-retry-multiprocess.md) | `RefreshMutex`, `RetryPolicy`, `multiProcess` POSIX FileChannel lock            |
| 11  | [FULL_REFRESH demotion debugging](architecture/openivm-spark/11-full-refresh-demotion-debugging.md)                       | The 8 demotion reasons + step-by-step debug recipe                              |
| 12  | [Parity gap forensics](architecture/openivm-spark/12-parity-gap-forensics.md)                                             | Contributor's guide to closing Spark↔DuckDB parity gaps                         |
| 13  | [⭐ **End-to-end trace: `AggregateSumSpec`**](architecture/openivm-spark/13-end-to-end-trace-aggregatesumspec.md)         | The flagship demo — real, executed walkthrough of every `it(...)`               |
| 14  | [TPC-DI deep dive: live warehouse state](architecture/openivm-spark/14-tpcdi-deep-dive-live-state.md)                     | Bronze → silver → gold cascade, decoded base64 dirs, real row data              |
| 15  | [Cloud RocksDB state roadmap](architecture/openivm-spark/15-cloud-rocksdb-state-roadmap.md)                              | Sessions, sharding, Delta catalog authority, versioned checkpoint publication   |
| 16  | [Spark-native metrics](architecture/openivm-spark/16-spark-native-metrics.md)                                            | Dropwizard/SparkPlugin metrics for RocksDB, refresh, compiler, Delta retries    |

---

## Layer B — `openivm` (DuckDB C++ extension)

The IVM engine: classifier, optimizer rules, and refresh-SQL emitter.

| #   | Chapter                                                                                                                        | What it answers                                                                   |
| --- | ------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| 0   | [OpenIVM DuckDB extension: 10,000-foot overview](architecture/openivm/0.OVERVIEW.md)                                           | PRAGMA contract, module map, system catalog list                                  |
| 1   | [⭐ Math: Z-sets, DBSP, and Möbius](architecture/openivm/1-math-zsets-dbsp-and-the-paper.md)                                   | Z-sets, DBSP derivative/integral, count monoids, Möbius derivation                |
| 2   | [OpenIVM parser and plan rewrite](architecture/openivm/2-parser-and-plan-rewrite.md)                                           | `openivm_compile_with_facts` entry, operator tagging, WINDOW special case         |
| 3   | [IncrementalChecker and RefreshType ordinals](architecture/openivm/3-incremental-checker-and-refresh-types.md)                 | The 10 RefreshType enum + classification decision tree                            |
| 4   | [OpenIVM system catalogs and DuckDB delta tables](architecture/openivm/4-system-catalogs-and-delta-tables.md)                  | `openivm_views`, `openivm_refresh_profile`, etc. — and why they're empty on Spark |
| 5   | [Optimizer rewrite rules per operator](architecture/openivm/5-optimizer-rewrite-rules-per-operator.md)                         | Tour of `src/rules/` — one rule file per operator                                 |
| 6   | [Möbius-signed multi-way joins](architecture/openivm/6-mobius-and-multiway-joins.md)                                           | `(-1)^(k-1) * ∏ ΔR_i ⋈ ∏ R_j`, `2^n − 1` UNION ALL emission                       |
| 7   | [Compile paths per `RefreshType`](architecture/openivm/7-compile-paths-per-refresh-type.md)                                    | Tour of `src/upsert/` — emission per RefreshType + surviving-stmt table           |
| 8   | [Cost model and adaptive refresh](architecture/openivm/8-cost-model-and-adaptive-refresh.md)                                   | What openivm provides + what the user must build externally                       |
| 9   | [Profiling OpenIVM refreshes](architecture/openivm/9-profiling-openivm-refresh-profile.md)                                     | `openivm_refresh_profile` + Spark-side log events + Python parser                 |
| 10  | [DuckLake, daemon, schema evolution, view matching](architecture/openivm/10-ducklake-daemon-schema-evolution-view-matching.md) | PRAGMA delta, "full envelope" vs the narrow slice openivm-spark uses              |
| 11  | [FULL_REFRESH debugging — DuckDB side](architecture/openivm/11-full-refresh-debugging-duckdb-side.md)                          | Why openivm itself emits FULL_REFRESH + standalone DuckDB recipe                  |
| 12  | [TPC-DI live-state inspection: DuckDB side vs Spark side](architecture/openivm/12-tpcdi-live-state-inspection.md)              | What's in `bronze.db`/`silver.db`/`gold.db`/`tpcdi.db` (Hive vs openivm)          |

---

## Layer C — `lpts` (LogicalPlan → SQL emitter, DuckDB C++ extension)

The serialization layer that openivm uses to emit incremental refresh SQL.

| #   | Chapter                                                                                         | What it answers                                                             |
| --- | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| 0   | [Overview: LPTS from 10,000 feet](architecture/lpts/0.OVERVIEW.md)                              | The 3-phase pipeline, dialect targeting, `lpts_check`                       |
| 1   | [LPTS 3-phase pipeline: orchestration view](architecture/lpts/1-three-phase-pipeline.md)        | AstBuilder → AstFlattener → CteList::ToQuery, end-to-end example            |
| 2   | [The LPTS `AstBuilder` phase](architecture/lpts/2-ast-builder.md)                               | LogicalOperator → AST visitor; expression conversion; correlated subqueries |
| 3   | [AstFlattener: lowering an AST into flat SQL](architecture/lpts/3-ast-flattener.md)             | The simplification passes; no-information-loss invariant                    |
| 4   | [`CteList::ToQuery`: final emit from CTE list to SQL](architecture/lpts/4-cte-list-to-query.md) | CTE dependency sort, dialect quoting, function-name lookup                  |
| 5   | [LPTS operator coverage map](architecture/lpts/5-operator-coverage-map.md)                      | Every DuckDB LogicalOperatorType + expression type — what's supported       |
| 6   | [LPTS dialect support](architecture/lpts/6-dialect-support.md)                                  | `DUCKDB`, `SPARK`, etc.; function-name tables; adding a new dialect         |
| 7   | [Roundtrip correctness: `lpts_check`](architecture/lpts/7-roundtrip-correctness-lpts-check.md)  | The plan-equality roundtrip test harness + how to use it during dev         |
| 8   | [Integration with OpenIVM and Spark](architecture/lpts/8-integration-with-openivm-and-spark.md) | The 3-layer handoff, dialect contract, ABI version pinning                  |

---

## Reproducibility

Every probe in these docs uses **isolated Python venvs** with `requirements.txt`
(no host-pip installs). The canonical recipe is:

```bash
mkdir -p /tmp/probe && cd /tmp/probe
cat <<'EOF' > requirements.txt
deltalake==1.6.0
duckdb==1.5.3
pandas==3.0.3
EOF
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python <your-probe-script>.py <warehouse_path>
```

`rocksdict` **cannot** open openivm-spark's RocksDB instances (comparator
mismatch — `leveldb.BytewiseComparator`). For RocksDB introspection, use the
project's own `OpenIvmRocksDB` class via `sbt console` inside the
docker-compose build container; see chapter 7 of `openivm-spark` for the
recipe.

The TPC-DI live warehouse used as reference throughout is at:

```
.temp/ivm-bench/mount/results/3/spark-openivm/
```
