# openivm-spark — Copilot instructions

Spark 3.5 / Delta Lake 3.2 SQL extension that delivers OpenIVM incremental
materialized-view maintenance **without Delta CDF**. The Scala/sbt project
lives entirely under `spark-ext/`. The repository root only holds `contrib/`
(host bootstrap scripts), `CONTRIBUTING.md`, and the `LICENSE`. `.temp/` is
gitignored scratch space (research, upstream forks) — never reference it from
committed code.

## Build, test, lint

All work happens inside the dev container — the host only needs Docker. Drive
it through `spark-ext/dev/dev.sh`:

```bash
./spark-ext/dev/dev.sh fmt                              # scalafmtAll + scalafmtSbt
./spark-ext/dev/dev.sh build                            # sbt compile
./spark-ext/dev/dev.sh assembly                         # sbt ivmExtension/assembly (fat jar)
./spark-ext/dev/dev.sh test                             # sbt test (every suite)
./spark-ext/dev/dev.sh test 'testOnly org.openivm.spark.parity.AggregateSumSpec'
./spark-ext/dev/dev.sh verify                           # lint + compile + Test/compile + assembly + test
./spark-ext/dev/dev.sh verify -- -Dopenivm.test.forks=8 # cap forked-test concurrency
PRE_CLEAN=1 ./spark-ext/dev/dev.sh verify               # nuke every running container first, then verify
```

For ad-hoc sbt invocations or running a single spec from inside the container,
use the docker-compose `build` service directly so output stays unbuffered:

```bash
cd spark-ext/dev && docker compose --env-file pins.env -f docker/docker-compose.yml \
    run --rm -T build sbt 'ivmIt/testOnly org.openivm.spark.parity.AggregateSumSpec'
```

Notes:

- Scala 2.12.17 / JDK 17 / Spark 3.5.1 / Delta 3.2.0 — all pinned in
  `spark-ext/dev/pins.env` and `project/Dependencies.scala`. Bumping any of
  these requires bumping the matching SHA in `pins.env`, which is what cuts a
  fresh `openivm-spark/spark-ext:${OPENIVM_COMMIT}-${LPTS_COMMIT}` image.
- The compiler is `-Xfatal-warnings -Ywarn-unused:imports`: any unused import
  fails the whole compile, so strip imports before pushing.
- `scalafmt` config is at `spark-ext/.scalafmt.conf` (max column 120,
  `align.preset = more`). `verify` runs `scalafmtCheckAll` + `scalafmtSbtCheck`.
- JDK-17 `--add-opens` flags live in `spark-ext/.sbtopts` and
  `Settings.jvmModuleOpts`; copy them verbatim when launching Spark outside sbt.

## Architecture

Read top-to-bottom; each module depends only on the one above it
(`build.sbt`):

`ivm-executor` → `ivm-common` → `ivm-compiler` → `ivm-extension` → `ivm-it`

- **`ivm-extension/OpenIvmSparkExtensions`** is the entry class wired via
  `spark.sql.extensions`. It injects three things in order: an `IvmParser`
  (ANTLR4 grammar `IvmSqlBase.g4`), an `IvmDmlInterceptorRule` (resolution
  rule), and an `IvmStrategy` (planner). All behaviour is gated by
  `spark.openivm.enabled` (`FeatureGate.EnabledKey`, default `false`).
- **Parser**: `IvmSqlBase.g4` only declares `CREATE / REFRESH / DROP
MATERIALIZED VIEW`. Everything else (including the MV body, captured as raw
  text `.+?`) is re-parsed via Spark's own `ParserInterface`. No `REFRESH
EVERY`, no `ALTER MATERIALIZED VIEW`, no double-quoted identifiers.
- **MV-body constraint**: the body must parse in **both** DuckDB (for openivm
  classification) and Spark — use only the intersection of both dialects.
- **DML interception**: `IvmDmlInterceptorRule` tees every DML on a tracked
  base table to a per-`(base_table, op_type, txn_ts)` Delta staging path
  managed by `StagingCatalog`. Delta's `DeltaAnalysis` runs _before_ this rule,
  so it matches both the V2 forms (`AppendData`, `OverwriteByExpression`) and
  the Delta-lowered forms (`ReplaceData`, `WriteDelta`).
- **Compile bridge** (`ivm-compiler/OpenIvmCompiler`): runs `PRAGMA
compile_refresh` through the **DuckDB CLI** (`/opt/openivm/duckdb`) with the
  bundled `openivm.duckdb_extension`. The DuckDB JDBC dep (`duckdb_jdbc`
  v1.5.2.1) is only used for ABI matching — the compile path is CLI-driven.
- **Refresh rewriter** (`ivm-common/SparkRefreshRewriter`): takes the
  multi-statement DuckDB-dialect program emitted by openivm and rewrites it to
  Spark-executable SQL (MERGE / UPDATE / INSERT OVERWRITE / etc.) keyed on the
  `RefreshTypeCode` (0–9). The `RefreshType` ↔ strategy mapping is documented
  in `spark-ext/README.md` and the spec name pattern below.
- **Assembled jar shading** (`Settings.assemblySettings`): `org.duckdb.**`
  and `org.antlr.v4.runtime.**` are shaded under `org.openivm.shaded.*` to
  avoid clashing with Spark's own ANTLR / DuckDB on the classpath.
- **MV-over-MV chains** are supported only at depth ≤ 2; the upstream refresh
  writes synthetic staging via `postRefreshCleanup` so the downstream MV picks
  it up. Deeper chains are out of scope.

## Test conventions (ivm-it)

`spark-ext/ivm-it` holds 100+ parity specs against the upstream openivm
DuckDB test suite. They are the highest-signal regression net and have strict
conventions:

- **Per-spec forked JVM, parallel up to 32** (`Settings.parallelForkSettings`
  - `Tags.ForkedTestGroup` cap in `build.sbt`). Each ScalaTest spec runs in
    its own subprocess, capped at `-Dopenivm.test.forks=N` (default 32, dropped
    to 8/16 on smaller hosts).
- **Keep each spec ≲ 10 `it(...)` cases** — wall-clock scales with file count
  more than test count because of Spark startup cost per JVM. Split files
  thematically (e.g. `SimpleProjectionDmlSpec` vs `SimpleProjectionFilterSpec`).
- **Unique per-spec table/MV name prefixes** are mandatory (e.g. `aggsum_`,
  `aggrpsum_`, `users_p1`, `mv_sp1`). Two specs sharing a table name will
  collide on the Delta warehouse path because parallel forks share the on-disk
  layout under `target/test-warehouse-*`.
- **Per-suite UUID warehouse dir**: each spec instantiates
  `new File(s"target/test-warehouse-<slug>-${UUID.randomUUID().toString.take(8)}")`
  in `beforeAll`, builds a `local[1]` SparkSession with both Delta and openivm
  extensions, then calls `MvCatalog.ensureTables(spark)` and
  `StagingCatalog.ensureTables(spark)`. Mirror this template when adding a
  new spec.
- **Bidirectional `EXCEPT ALL` is the only correctness oracle.** Every spec
  has a local `assertMvCorrect(mvName, expectedSql)` helper that selects only
  the user-visible columns (dropping hidden `openivm_*` bookkeeping) and
  asserts both `mv.exceptAll(expected).count == 0` and the reverse. Never
  replace this with COUNT(\*) / row-spot checks when refactoring.
- **When porting a test blocked by an engine gap, mark it
  `ignore("...") /* TODO: ... */`.** Never silently demote a real refresh
  path to `FULL_REFRESH` or weaken an assertion to make a failing test pass.
- **Spec ↔ RefreshType naming**: parity specs are typically named after the
  refresh path they exercise — `AggregateGroup*Spec`, `SimpleAggregate*Spec`,
  `SimpleProjection*Spec`, `FullRefresh*Spec`, `Window*Spec`, `Distinct*Spec`,
  `SemiAnti*Spec`, `TopKSpec`, `Joins*Spec`, `Chained*Spec`, etc. See the
  RefreshType table in `spark-ext/README.md`.

## Known sharp edges

- `MvMetadata.sourceTables` stores `db.table` (V2 namespace) but a
  db-less `CREATE MATERIALIZED VIEW` produces a db-less `TableIdentifier`;
  match by trailing short-name segment, not by equality
  (see `MaterializedViewCommands.postRefreshCleanup`).
- Spark normalises `spark.sql.warehouse.dir` to a `file:` URI; tests
  comparing it to `MvMetadata.location` must strip the scheme or use
  `new File(new URI(loc))`.
- `DROP MATERIALIZED VIEW` does NOT prune orphaned `StagingCatalog` rows;
  tests that DROP+CREATE the same view must mark stale staging consumed
  before the next REFRESH (see `SchemaEvolutionSpec.clearStaging`).
- Spark 3.5's `Union.rewriteConstraints` throws `NoSuchElementException` on
  `EXCEPT ALL` over a scalar correlated subquery — fall back to a
  collect-based multiset compare (see `LateralSpec.assertMvCorrectCollect`).
- `SIMPLE_PROJECTION` MVs over a source with byte-identical duplicate rows
  are not fully supported (the value-equality MERGE deletes all copies).

## Principles

- We do NOT tolerate verbose logging in test code. Tests should ONLY emit the test status from the test framework,
  there should be nothing else in the console stdout.

  Verbose logs are written to `.logs/test-<YYYYMMDD-HHMMSS>/` at the repo root on every
  `./spark-ext/dev/dev.sh test` and `./spark-ext/dev/dev.sh verify` run.

  Each forked test JVM writes its own `fork-<HHmmss-SSS>.log`.

  `.logs/` is git-ignored.

- Always try to parallelize tasks using subagents and isolated docker containers and delegating
  tasks to agents per container so we can get things done faster.

## Activation outside tests

```bash
spark-shell \
  --jars spark-ext/ivm-extension/target/scala-2.12/ivm-extension-0.1.0-SNAPSHOT-assembly.jar \
  --conf spark.sql.extensions=org.openivm.spark.OpenIvmSparkExtensions \
  --conf spark.openivm.enabled=true \
  --conf spark.driver.extraJavaOptions="$(grep -oE '^-J.*' spark-ext/.sbtopts | sed 's/^-J//' | xargs)"
```

The feature gate defaults to `false`, so the jar is opt-in even when on the
classpath.

## Robust CI in ivm-bench

To ensure PR CI is robust and not flaky, follow this process:

1. Push up code into the PR branch
2. Monitor the PR for CI failures, for example: https://github.com/mdrakiburrahman/ivm-bench/pull/29
3. Go download logs
4. Triage failure
5. Fix (even if it's not due to openivm-spark, fix it in ivm-bench). If in openivm-spark, triage, fix code, push.
6. Run green
7. Once green CI, push up **FIVE SENTINEL COMMITS** one after another into the repo to fire CI
8. Must run green 5 times back to back. If any failure, repeat from Step 3.