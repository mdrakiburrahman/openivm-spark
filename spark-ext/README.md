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
├── ivm-compiler/      # OpenIvmCompiler (DuckDB JDBC) + LptsSparkDialect
├── ivm-extension/     # SparkSessionExtensions entry + ANTLR grammar + commands + rules
├── ivm-it/            # integration tests + 100-spec parity suite vs openivm
└── dev/
    ├── dev            # single entry-point CLI wrapper (build, test, verify, shell, …)
    ├── docker/        # multi-stage Dockerfile + docker-compose.yml
    └── pins.env       # pinned SHAs of openivm / lpts forks
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
./spark-ext/dev/dev.sh verify                  # lint + build + assembly + full test
./spark-ext/dev/dev.sh build                   # sbt compile
./spark-ext/dev/dev.sh assembly                # sbt ivmExtension/assembly (fat jar)
./spark-ext/dev/dev.sh test                    # sbt test (every suite)
./spark-ext/dev/dev.sh test 'testOnly org.openivm.spark.it.ExtensionLoadingSpec'
./spark-ext/dev/dev.sh fmt                     # scalafmtAll (auto-format)
./spark-ext/dev/dev.sh shell                   # interactive bash inside the dev image
./spark-ext/dev/dev.sh openivm-test            # upstream openivm sqllogictests
./spark-ext/dev/dev.sh dev-build [build|test|all]   # iterate on .temp/openivm + .temp/lpts
./spark-ext/dev/dev.sh image-build             # docker compose build (force rebuild)
./spark-ext/dev/dev.sh help                    # this help text
```

`verify` is the canonical one-liner — it lints, compiles, assembles the fat
jar, and runs every unit + integration + parity suite in a single sbt JVM.
Wall-clock on the reference 32-core / 124 GiB host is ~40 minutes end-to-end.

### Environment variables

| Variable                  | Default | Scope    | Effect                                                                                                                                                                |
| ------------------------- | ------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PRE_CLEAN`               | `0`     | `verify` | When `1`, force-removes every running Docker container on the host before sbt starts. Named cache volumes (`sbt-cache`, `ivy-cache`, `coursier-cache`) are preserved. |
| `openivm.test.forks` (-D) | `32`    | sbt JVM  | Cap on parallel forked test JVMs. Pass via `./spark-ext/dev/dev.sh verify -- -Dopenivm.test.forks=8` on smaller hosts.                                                |

The container image is named `openivm-spark/spark-ext:${OPENIVM_COMMIT}-${LPTS_COMMIT}` so that bumping
SHAs in `pins.env` produces a fresh image, leaving any in-progress workspace
caches intact.

## Activation in spark-shell / spark-submit

```bash
spark-shell \
    --jars target/scala-2.12/ivm-extension-0.1.0-SNAPSHOT-assembly.jar \
    --conf spark.sql.extensions=org.openivm.spark.OpenIvmSparkExtensions \
    --conf spark.openivm.enabled=true \
    --conf spark.driver.extraJavaOptions="$(cat .sbtopts | grep -oE '^-J.*' | sed 's/^-J//' | xargs)"
```

The feature gate (`spark.openivm.enabled`) defaults to false, so the jar is
opt-in even when on the classpath.

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

## IVM DDL

```sql
-- Create a materialized view
CREATE MATERIALIZED VIEW sales_summary AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
  FROM sales GROUP BY region;

-- Refresh after DML on base tables
REFRESH MATERIALIZED VIEW sales_summary;

-- Drop
DROP MATERIALIZED VIEW IF EXISTS sales_summary;
```

DML on a tracked base table (INSERT / DELETE / UPDATE / MERGE) is intercepted
by `IvmDmlInterceptorRule`, which tees the change set to a per-base-table Delta
staging path keyed by `(base_table, op_type, txn_ts)`. `REFRESH MATERIALIZED VIEW`
recompiles the view via openivm + lpts (target dialect `spark`), rewrites the
emitted DuckDB-style SQL into Spark-executable form (`SparkRefreshRewriter` +
`LptsSparkDialect`), and applies it to the MV's Delta table.

## License

See [LICENSE](../LICENSE).
