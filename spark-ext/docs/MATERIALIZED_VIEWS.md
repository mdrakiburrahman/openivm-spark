# Materialized views

The bridge classifies materialized-view queries (via openivm's `PRAGMA
compile_refresh`) and dispatches to one of several Spark-side rewrite paths:

## Supported RefreshTypes

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

## Opt-in running-window refresh

Set `spark.openivm.refresh.windowRunningIncremental.enabled=true` before starting
the Spark or Fabric session to test the cumulative-window suffix path. It remains
disabled by default. Supported append batches extend cumulative results from
the pre-refresh state; backdated partitions use the recompute path. The compiler
materializes suffix results once, and Spark eagerly caches them so the MV write
and downstream delta append reuse the same rows. This is separate from the
DuckLake-only compact-diff and publication optimizations.

Fast/fallback key filters reuse cached bounds without separate cache actions.
One probe over those bounds skips branches with no matching partitions. An
append-only refresh still creates the empty fallback delta table needed by the
downstream append; state, suffix positions and results remain materialized.

`WindowRunningIncrementalSpec` and `WindowRunningIncrementalCdfSpec` exercise
this setting with exact bag comparisons, including mixed fallback/append batches
and downstream materialized views. Performance must be measured with the setting
explicitly enabled; updating the compiler pin alone does not enable it.

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
