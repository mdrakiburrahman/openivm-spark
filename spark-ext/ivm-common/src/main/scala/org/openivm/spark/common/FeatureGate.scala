package org.openivm.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Master feature gate for the openivm-spark extension.
 *
 * All extension behaviour (the new `CREATE / REFRESH / DROP MATERIALIZED VIEW`
 * statements, DML interception, MV catalog, refresh assemblers) is conditional
 * on this flag. Default: `false` — the jar is opt-in even when installed.
 *
 * Activated by setting `spark.openivm.enabled=true` in the SparkConf before
 * the session is created, or via `--conf spark.openivm.enabled=true` on
 * spark-shell / spark-submit.
 */
object FeatureGate {

  val EnabledKey: String = "spark.openivm.enabled"

  /** Delta table performance knobs for MV backing data tables.
    *
    * `DeltaEnableDeletionVectorsKey` enables Delta deletion vectors so MERGE
    * `WHEN MATCHED DELETE` can mark removed rows in a soft-delete bitmap instead
    * of rewriting whole Parquet files. `DeltaOptimizeWriteKey` enables optimized
    * writes to reduce small-file fragmentation during refresh appends and MERGEs.
    * `DeltaAutoCompactKey` enables Delta's post-write auto-compaction.
    *
    * `DeletionVectors` and `AutoCompact` default ON. `OptimizeWrite` defaults
    * OFF because the AQE shuffle it introduces before every Delta write is a
    * net-negative for the small per-refresh INSERTs an incremental MV emits
    * (avg 1.4 s per stmt on TPC-DI SF3, with batch sizes of tens to thousands
    * of rows). The shuffle helps when batch sizes are large enough to benefit
    * from file-coalescing — flip it ON via `spark.openivm.delta.optimizeWrite=true`
    * for workloads with large per-refresh row counts.
    *
    * The test build (`spark-ext/project/Settings.scala`) overrides selected
    * knobs OFF where the extra per-fork IO is not needed.
    */
  val DeltaEnableDeletionVectorsKey: String = "spark.openivm.delta.enableDeletionVectors"
  val DeltaOptimizeWriteKey: String         = "spark.openivm.delta.optimizeWrite"
  val DeltaAutoCompactKey: String           = "spark.openivm.delta.autoCompact"

  /** Fuse `view_delta_ctas` + `insert_into` for SIMPLE_PROJECTION MVs.
    *
    * The per-refresh scratch Delta table that openivm emits as stmt[0] is
    * consumed by stmt[1] (the INSERT INTO mv_data) and, optionally, stmt[2]
    * (the value-equality delete MERGE).
    *
    * Writing the scratch to a Delta path then re-reading it costs ~2.8 s on
    * average for the TPC-DI gold layer; fusing — running stmt[0]'s SELECT
    * as a cached global-temp view and rewriting subsequent path refs to read
    * from the cache — saves most of that wall-clock per refresh. If an
    * intercept-mode downstream MV exists, the same cached view is recorded as
    * the cascade input and pruned after downstream consumption.
    *
    * Default ON. Flip OFF via `spark.openivm.fuseScratch.enabled=false` to
    * fall back to the on-disk scratch path (e.g. for diagnostics).
    */
  val FuseScratchEnabledKey: String             = "spark.openivm.fuseScratch.enabled"
  val FuseScratchCascadeCacheEnabledKey: String = "spark.openivm.fuseScratch.cascadeCache.enabled"

  /** Capture per-step refresh + create profile rows into the RocksDB
    * `refresh_profile` column family ([[RefreshProfileCatalog]]).
    *
    * Default OFF — production paths pay zero overhead. The ivm-bench
    * spark-openivm container flips this ON via `spark-defaults.conf.tmpl`
    * parameterized off `OPENIVM_PROFILE_REFRESH=1` so benchmark runs always
    * collect telemetry while normal users get the silent-fast-path default.
    *
    * Read via `SHOW OPENIVM REFRESH PROFILE` (mirrors DuckDB-OpenIVM's
    * `SELECT * FROM openivm_refresh_profile`).
    */
  val ProfileRefreshKey: String = "spark.openivm.profile.refresh"

  /** Cache openivm compile/classification results per MV schema + facts tier.
    * Default OFF while W7.1 is validated; when enabled, REFRESH still rewrites
    * the cached shape-stable SQL every time so per-refresh snapshot temp views
    * are recreated on each execution.
    */
  val CompileClassificationCacheEnabledKey: String = "spark.openivm.compile.classificationCache.enabled"
  val RefreshProgramCacheEnabledKey: String        = "spark.openivm.refresh.programCache.enabled"
  val RefreshStatementFusionEnabledKey: String     = "spark.openivm.refresh.statementFusion.enabled"
  val RefreshSiblingParallelEnabledKey: String     = "spark.openivm.refresh.siblingParallel.enabled"

  /** Capture every SQL statement actually executed by a CREATE / REFRESH
    * MATERIALIZED VIEW lifecycle into the RocksDB `refresh_sql_log` column
    * family ([[RefreshSqlLogCatalog]]).
    *
    * Default OFF — production paths pay only a microsecond-per-statement
    * inline cost when the gate is OFF (a NoOp recorder is returned).
    *
    * The ivm-bench spark-openivm container flips this ON via
    * `spark-defaults.conf.tmpl` parameterised off `OPENIVM_QUERY_LOG=1` so
    * benchmark runs always collect the full per-MV SQL trace while normal
    * users get the silent-fast-path default.
    *
    * Read via `SHOW OPENIVM QUERY LOG`. The catalog row's `refresh_id`
    * matches the corresponding `RefreshProfileCatalog` row so the two are
    * joinable.
    */
  val QueryLogEnabledKey: String = "spark.openivm.queryLog.enabled"

  /** Capture a Spark `EXPLAIN FORMATTED` physical plan per executed refresh
    * statement, recorded alongside the SQL in the query log. Default OFF so it
    * never adds planning/formatting overhead to a benchmark run — enable it
    * (together with the query log) only for a diagnostic refresh when you need
    * to see broadcast-vs-sort-merge join choices and scan sizes.
    */
  val ExplainCaptureKey: String = "spark.openivm.explain.capture"

  /** Change-propagation mode for tracking what changed on base tables since
    * the last refresh.
    *
    *  - `intercept` (default) — install the Catalyst resolution rule
    *    [[org.openivm.spark.analyzer.IvmDmlInterceptorRule]] so every
    *    INSERT / DELETE / UPDATE / MERGE / OVERWRITE on a tracked base table
    *    tees its change rows to a per-DML staging Delta directory and a
    *    RocksDB row in [[StagingCatalog]].
    *  - `cdf` — do not intercept; read Delta Change Data Feed
    *    (`readChangeFeed = true`) on every tracked source at refresh time.
    *    Requires `delta.enableChangeDataFeed = true` on every base table the
    *    user creates an MV over (enforced at CREATE time with a clear
    *    error). Backing Delta tables of MVs are auto-created with the same
    *    table property so MV-over-MV cascade continues to work end-to-end.
    *
    * Mutually exclusive at session scope.  Default `intercept` preserves
    * backwards-compatibility for every existing user / test.
    */
  val ChangeFeedModeKey: String = "spark.openivm.changeFeed.mode"

  /** Let Adaptive Query Execution broadcast the small side of the SCD2
    * view-delta joins at RUNTIME, even while plan-time broadcast stays disabled.
    *
    * The refresh statements that wrap the full MV body — the view-delta CTAS
    * (refresh stmt[0]) and the recompute INSERT MERGEs — disable PLAN-TIME
    * broadcast (`spark.sql.autoBroadcastJoinThreshold=-1`) because Catalyst
    * under-estimates the SCD2 multiplicity: it would auto-broadcast a relation
    * it thinks is tiny but that explodes past Spark's hard 8 GiB build cap at
    * execution. That blanket disable, however, is inherited by AQE's adaptive
    * threshold, so every equi+range SCD2 dimension join falls back to a
    * sort-merge join that needlessly shuffles the full dimension against the
    * (always-small) incremental delta — the dominant cost of the heavy
    * `gold.fact_*` MVs.
    *
    * When this gate is ON (default) the disable scope instead raises
    * `spark.sql.adaptive.autoBroadcastJoinThreshold` to a runtime-safe budget
    * (see [[AdaptiveBroadcastThresholdKey]]). AQE then converts the small-side
    * sort-merge joins to broadcast-hash joins using the **actual** materialized
    * shuffle size — which can never broadcast the genuinely-large exploding
    * intermediate (it measures it large and keeps the sort-merge join). The
    * join result is identical, so this is a pure execution speed-up guarded by
    * the parity suite. Flip OFF via
    * `spark.openivm.refresh.adaptiveBroadcast.enabled=false` to fall back to the
    * fully-disabled, sort-merge-only behaviour. Requires AQE
    * (`spark.sql.adaptive.enabled=true`, the Spark 3.5 default).
    */
  val AdaptiveBroadcastEnabledKey: String = "spark.openivm.refresh.adaptiveBroadcast.enabled"

  /** Explicit byte budget for the AQE runtime broadcast described at
    * [[AdaptiveBroadcastEnabledKey]]. `-1` (default) means "inherit the
    * session's configured `spark.sql.autoBroadcastJoinThreshold` when positive,
    * else fall back to 100 MiB" — keeping the operator's stated broadcast
    * appetite but routing it through the runtime-safe AQE path.
    */
  val AdaptiveBroadcastThresholdKey: String = "spark.openivm.refresh.adaptiveBroadcast.thresholdBytes"

  /** Fallback AQE broadcast budget (100 MiB) — 80x under Spark's 8 GiB cap. */
  val AdaptiveBroadcastDefaultBytes: Long = 104857600L

  /** Add explicit Spark SQL `BROADCAST` hints for refresh join sides whose
    * backing Delta table size is known to be small. Default OFF: the refresh
    * executor still disables plan-time auto-broadcast as the safe baseline, and
    * this gate only opts proven-small relations back into broadcast by hint.
    */
  val SelectiveBroadcastEnabledKey: String = "spark.openivm.refresh.selectiveBroadcast.enabled"

  /** Inject exact semi-join prefilters into JOIN_DELTA `full_source` CTEs.
    *
    * OpenIVM's range-join delta programs can materialise a full base-table CTE
    * and only later join it to a tiny changed-source delta. When this gate is
    * enabled, the Spark rewriter wraps that `full_source` CTE with a
    * result-invariant `WHERE <fk> IN (SELECT <key> FROM Δsrc)` prefilter derived
    * from the existing join predicate. Default ON; flip OFF via
    * `spark.openivm.refresh.semiJoinPrune.enabled=false`.
    */
  val SemiJoinPruneEnabledKey: String = "spark.openivm.refresh.semiJoinPrune.enabled"

  /** Enable skew-aware delta fanout join hints. Default OFF: when opted in, the
    * refresh planner inspects per-refresh delta stats against source column
    * stats and broadcasts only the narrow/small source-delta side of
    * delta⋈base joins. This is a safe prototype of histogram-bin overlap
    * planning: Spark/Delta currently expose min/max/null/NDV here, not actual
    * histogram bins, so wider hot-bin salting is intentionally not emitted.
    */
  val SkewFanoutEnabledKey: String                = "spark.openivm.refresh.skewFanout.enabled"
  val SkewFanoutNarrowDeltaRowsKey: String        = "spark.openivm.refresh.skewFanout.narrowDeltaRows"
  val SkewFanoutNarrowOverlapRatioKey: String     = "spark.openivm.refresh.skewFanout.narrowOverlapRatio"
  val SkewFanoutDefaultNarrowDeltaRows: Long      = 10000L
  val SkewFanoutDefaultNarrowOverlapRatio: Double = 0.05d

  /** Drop FK-redundant inclusion-exclusion JOIN_DELTA terms in the Spark
    * rewriter, after Delta batch-shape classification has proven the referenced
    * tables are insert-only/unchanged. This composes with
    * [[SemiJoinPruneEnabledKey]] because it operates on the same openivm-emitted
    * SQL shape instead of changing the compiler output. Default OFF until the
    * SF10 benchmark validates the combined win.
    */
  val FkTermPruneEnabledKey: String = "spark.openivm.refresh.fkTermPrune.enabled"

  /** Declare trusted FK metadata into openivm's per-connection constraints
    * cache before compiling refresh SQL. Default OFF until the matching
    * openivm constraints-cache build is pinned.
    */
  val DeclareRelyFkEnabledKey: String = "spark.openivm.refresh.declareRelyFk.enabled"

  /** Simplify refresh joins whose right-side join key is known unique from
    * [[WorkloadFacts.uniqueKeys]]. INNER joins with unused right columns are
    * demoted to EXISTS probes; LEFT joins with unused right columns are dropped.
    * Default OFF while shape coverage grows.
    */
  val UniqueJoinSimplifyEnabledKey: String = "spark.openivm.refresh.uniqueJoinSimplify.enabled"

  /** Enable runtime-filter (bloom / semi-join) pushdown for the SCD2 view-delta
    * joins. Every IVM view-delta is a union of delta-rule terms, and the
    * `FULL_SOURCE ⋈ Δdimension` term scans the entire source table against the
    * handful of changed dimension rows — the dominant SF10 cost (e.g.
    * `gold.fact_market_history` scanning all of `daily_market`). A runtime
    * filter built from the tiny Δ side prunes that scan to the affected keys.
    * Result-invariant (an exact/superset filter never drops matching rows).
    * Spark's runtime filters are OFF by default; this turns them on (and lowers
    * the application-side scan-size threshold so they fire on SF10-scale source
    * tables) only for the wrapped refresh statements. Flip OFF via
    * `spark.openivm.refresh.runtimeFilter.enabled=false`.
    */
  val RuntimeFilterEnabledKey: String = "spark.openivm.refresh.runtimeFilter.enabled"

  /** Enable Spark-side SCD-2 range-join acceleration for refresh statements.
    * Default ON: the refresh SQL rewriter adds result-invariant overlap
    * predicates for `ts BETWEEN effective_timestamp AND end_timestamp` joins
    * whose probe side is a source delta, and broadcasts the SCD alias by hint.
    */
  val Scd2RangeAccelEnabledKey: String = "spark.openivm.refresh.scd2RangeAccel.enabled"

  /** Enable openivm-side SCD-2 range-join acceleration during refresh SQL
    * compilation. Default OFF: when opted in, openivm narrows SCD-2 dimension
    * scans with delta-fact bounds filters in the emitted refresh program.
    */
  val Scd2RangeJoinAccelEnabledKey: String = "spark.openivm.refresh.scd2RangeJoinAccel.enabled"

  /** Enable bounded recompute for top-K ROW_NUMBER/RANK WINDOW_PARTITION MVs.
    *
    * Default OFF: when opted in, eligible top-K ranking refreshes replace the
    * openivm whole-partition recompute INSERT with a Spark-side equivalent that
    * ranks only the affected partition's bounded candidate set. This is a
    * result-invariant execution rewrite; ineligible shapes keep the existing
    * incremental WINDOW_PARTITION program rather than demoting to FULL_REFRESH.
    */
  val BoundedRankEnabledKey: String = "spark.openivm.refresh.boundedRank.enabled"

  /** Enable append-only strict-suffix INSERT rewrite for WINDOW_PARTITION MVs.
    * Default ON to preserve the existing terminal-MV fast path; in cascade
    * cases, refresh still materializes the MV view-delta before inserting.
    */
  val WindowSuffixSkipEnabledKey: String = "spark.openivm.refresh.windowSuffixSkip.enabled"

  /** Collapse WINDOW_PARTITION's OR-expanded affected-partition DELETE into one
    * Delta MERGE over the UNION of affected keys. Default OFF: the legacy
    * one-MERGE-per-source/partition-clause plan remains byte-identical unless
    * explicitly opted in.
    */
  val WindowPartitionSingleDeleteMergeEnabledKey: String =
    "spark.openivm.refresh.windowPartitionSingleDeleteMerge.enabled"

  /** Liquid-cluster eligible WINDOW_PARTITION MV data tables by their resolved
    * window PARTITION BY columns at CREATE time. Default OFF so the storage
    * layout change is opt-in and reversible.
    */
  val WindowClusterPruneEnabledKey: String = "spark.openivm.refresh.windowClusterPrune.enabled"

  /** Replace WINDOW_PARTITION delete+insert recompute pairs with one Delta
    * `INSERT ... REPLACE WHERE` when the affected partition literal list is
    * small enough to collect safely. Default ON; flip OFF to restore the legacy
    * MERGE-delete plus INSERT execution path byte-for-byte.
    */
  val WindowSinglePassReplaceEnabledKey: String = "spark.openivm.refresh.windowSinglePassReplace.enabled"

  /** Cache WINDOW_PARTITION's post-refresh snapshot when the single-pass
    * `REPLACE WHERE` path will consume it twice (cascade view-delta plus MV
    * data write). Default OFF: flag-off execution remains byte-identical.
    */
  val WindowSnapshotCacheEnabledKey: String = "spark.openivm.refresh.windowSnapshotCache.enabled"

  /** Emit the running-window suffix-extend refresh (P5.2) for cumulative
    * `SUM/MIN/MAX/COUNT/AVG OVER (PARTITION BY k ORDER BY d)` WINDOW MVs on a
    * proven insert-only batch: affected partitions whose new rows sort strictly
    * after the existing partition max are extended from the persisted running
    * value instead of recomputed from base; backdated partitions fall back to
    * the full window recompute. Default OFF (opt-in, append-only-shape only).
    */
  val WindowRunningIncrementalEnabledKey: String = "spark.openivm.refresh.windowRunningIncremental.enabled"

  /** Maintain an opt-in per-partition backing-state Delta side table for
    * LAST_VALUE/LAST IGNORE NULLS forward-fill WINDOW_PARTITION MVs. Default
    * OFF: legacy CREATE and REFRESH SQL/execution remain byte-identical unless
    * explicitly enabled.
    */
  val WindowLastValueBackingStateEnabledKey: String =
    "spark.openivm.refresh.windowLastValueBackingState.enabled"

  /** Fast-exit a CDF refresh when every source-table Delta commit since the
    * last consumed version is metadata-only / dataChange=false NOOP. Default
    * OFF: the legacy empty-batch guard remains unchanged, and this opt-in skips
    * the heavier per-MV refresh phases only when [[DeltaCommitClassifier]]
    * proves that the advanced Delta versions contain no row changes.
    */
  val NoopFastExitEnabledKey: String = "spark.openivm.refresh.noopFastExit.enabled"

  /** Compile-time cost model observability. Default OFF: when opted in, REFRESH
    * records the deterministic WorkloadFacts-derived cost hint in MV metadata.
    */
  val CostModelEnabledKey: String = "spark.openivm.refresh.costModel.enabled"

  /** Runtime delta-size aware refresh skip. Default OFF: when opted in, REFRESH
    * probes the already-registered `openivm_delta_<source>` temp views and
    * consumes the batch without executing the recompute program only when every
    * materialized source delta is empty.
    */
  val RuntimeEmptyDeltaSkipEnabledKey: String = "spark.openivm.refresh.runtimeEmptyDeltaSkip.enabled"

  /** Unified compile/runtime refresh intelligence observability. Default OFF:
    * when opted in, REFRESH records a single decision surface that combines the
    * WorkloadFacts cost estimate with the runtime materialized delta size.
    */
  val UnifiedRefreshIntelligenceEnabledKey: String = "spark.openivm.refresh.unifiedIntelligence.enabled"

  /** Guard incremental refresh reuse with a normalized logical-plan fingerprint.
    * Default OFF: legacy metadata rows and refresh routing remain byte-identical
    * unless explicitly enabled.
    */
  val FingerprintGuardEnabledKey: String = "spark.openivm.refresh.fingerprintGuard.enabled"

  def enabled(conf: SparkConf): Boolean =
    conf.getBoolean(EnabledKey, defaultValue = false)

  def enabled(spark: SparkSession): Boolean =
    enabled(spark.sparkContext.getConf)

  private def boolConf(conf: SparkConf, key: String, default: Boolean): Boolean =
    conf.getBoolean(key, default)

  def deletionVectorsEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, DeltaEnableDeletionVectorsKey, default = true)

  def optimizeWriteEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, DeltaOptimizeWriteKey, default = false)

  def autoCompactEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, DeltaAutoCompactKey, default = true)

  def fuseScratchEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, FuseScratchEnabledKey, default = false)

  def fuseScratchCascadeCacheEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, FuseScratchCascadeCacheEnabledKey, default = false)

  def profileRefreshEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, ProfileRefreshKey, default = false)

  def profileRefreshEnabled(conf: SparkConf): Boolean =
    boolConf(conf, ProfileRefreshKey, default = false)

  def compileClassificationCacheEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, CompileClassificationCacheEnabledKey, default = false)

  def compileClassificationCacheEnabled(conf: SparkConf): Boolean =
    boolConf(conf, CompileClassificationCacheEnabledKey, default = false) ||
      boolConf(conf, RefreshProgramCacheEnabledKey, default = false)

  def refreshProgramCacheEnabled(conf: SparkConf): Boolean =
    boolConf(conf, RefreshProgramCacheEnabledKey, default = false)

  def refreshProgramCacheEnabled(spark: SparkSession): Boolean =
    refreshProgramCacheEnabled(spark.sparkContext.getConf)

  def refreshStatementFusionEnabled(conf: SparkConf): Boolean =
    boolConf(conf, RefreshStatementFusionEnabledKey, default = false)

  def refreshStatementFusionEnabled(spark: SparkSession): Boolean =
    refreshStatementFusionEnabled(spark.sparkContext.getConf)

  def refreshSiblingParallelEnabled(conf: SparkConf): Boolean =
    boolConf(conf, RefreshSiblingParallelEnabledKey, default = false)

  def refreshSiblingParallelEnabled(spark: SparkSession): Boolean =
    refreshSiblingParallelEnabled(spark.sparkContext.getConf)

  def fingerprintGuardEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, FingerprintGuardEnabledKey, default = false)

  def queryLogEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, QueryLogEnabledKey, default = false)

  def queryLogEnabled(conf: SparkConf): Boolean =
    boolConf(conf, QueryLogEnabledKey, default = false)

  def explainCaptureEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, ExplainCaptureKey, default = false)

  def adaptiveBroadcastEnabled(conf: SparkConf): Boolean =
    boolConf(conf, AdaptiveBroadcastEnabledKey, default = true)

  def adaptiveBroadcastEnabled(spark: SparkSession): Boolean =
    adaptiveBroadcastEnabled(spark.sparkContext.getConf)

  def selectiveBroadcastEnabled(conf: SparkConf): Boolean =
    boolConf(conf, SelectiveBroadcastEnabledKey, default = false)

  def selectiveBroadcastEnabled(spark: SparkSession): Boolean =
    selectiveBroadcastEnabled(spark.sparkContext.getConf)

  def semiJoinPruneEnabled(conf: SparkConf): Boolean =
    boolConf(conf, SemiJoinPruneEnabledKey, default = true)

  def semiJoinPruneEnabled(spark: SparkSession): Boolean =
    semiJoinPruneEnabled(spark.sparkContext.getConf)

  def skewFanoutEnabled(conf: SparkConf): Boolean =
    boolConf(conf, SkewFanoutEnabledKey, default = false)

  def skewFanoutEnabled(spark: SparkSession): Boolean =
    skewFanoutEnabled(spark.sparkContext.getConf)

  def skewFanoutNarrowDeltaRows(conf: SparkConf): Long =
    scala.util
      .Try(conf.getLong(SkewFanoutNarrowDeltaRowsKey, SkewFanoutDefaultNarrowDeltaRows))
      .getOrElse(
        SkewFanoutDefaultNarrowDeltaRows
      )

  def skewFanoutNarrowOverlapRatio(conf: SparkConf): Double =
    scala.util
      .Try(conf.getDouble(SkewFanoutNarrowOverlapRatioKey, SkewFanoutDefaultNarrowOverlapRatio))
      .getOrElse(
        SkewFanoutDefaultNarrowOverlapRatio
      )

  def fkTermPruneEnabled(conf: SparkConf): Boolean =
    boolConf(conf, FkTermPruneEnabledKey, default = false)

  def fkTermPruneEnabled(spark: SparkSession): Boolean =
    fkTermPruneEnabled(spark.sparkContext.getConf)

  def declareRelyFkEnabled(conf: SparkConf): Boolean =
    boolConf(conf, DeclareRelyFkEnabledKey, default = false)

  def declareRelyFkEnabled(spark: SparkSession): Boolean =
    declareRelyFkEnabled(spark.sparkContext.getConf)

  def uniqueJoinSimplifyEnabled(conf: SparkConf): Boolean =
    boolConf(conf, UniqueJoinSimplifyEnabledKey, default = false)

  def uniqueJoinSimplifyEnabled(spark: SparkSession): Boolean =
    uniqueJoinSimplifyEnabled(spark.sparkContext.getConf)

  /** Resolve the AQE runtime-broadcast byte budget. An explicit
    * [[AdaptiveBroadcastThresholdKey]] > 0 wins; otherwise inherit the supplied
    * session static broadcast threshold when positive; otherwise fall back to
    * [[AdaptiveBroadcastDefaultBytes]].
    */
  def adaptiveBroadcastThresholdBytes(conf: SparkConf, sessionStaticBytes: Option[Long]): Long = {
    val explicit = scala.util.Try(conf.getLong(AdaptiveBroadcastThresholdKey, -1L)).getOrElse(-1L)
    if (explicit > 0) explicit
    else sessionStaticBytes.filter(_ > 0).getOrElse(AdaptiveBroadcastDefaultBytes)
  }

  def runtimeFilterEnabled(conf: SparkConf): Boolean =
    boolConf(conf, RuntimeFilterEnabledKey, default = true)

  def runtimeFilterEnabled(spark: SparkSession): Boolean =
    runtimeFilterEnabled(spark.sparkContext.getConf)

  def scd2RangeAccelEnabled(conf: SparkConf): Boolean =
    boolConf(conf, Scd2RangeAccelEnabledKey, default = true)

  def scd2RangeAccelEnabled(spark: SparkSession): Boolean =
    scd2RangeAccelEnabled(spark.sparkContext.getConf)

  def scd2RangeJoinAccelEnabled(conf: SparkConf): Boolean =
    boolConf(conf, Scd2RangeJoinAccelEnabledKey, default = false)

  def scd2RangeJoinAccelEnabled(spark: SparkSession): Boolean =
    scd2RangeJoinAccelEnabled(spark.sparkContext.getConf)

  def boundedRankEnabled(conf: SparkConf): Boolean =
    boolConf(conf, BoundedRankEnabledKey, default = false)

  def boundedRankEnabled(spark: SparkSession): Boolean =
    boundedRankEnabled(spark.sparkContext.getConf)

  def windowSuffixSkipEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowSuffixSkipEnabledKey, default = true)

  def windowSuffixSkipEnabled(spark: SparkSession): Boolean =
    windowSuffixSkipEnabled(spark.sparkContext.getConf)

  def windowPartitionSingleDeleteMergeEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowPartitionSingleDeleteMergeEnabledKey, default = false)

  def windowPartitionSingleDeleteMergeEnabled(spark: SparkSession): Boolean =
    windowPartitionSingleDeleteMergeEnabled(spark.sparkContext.getConf)

  def windowClusterPruneEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowClusterPruneEnabledKey, default = false)

  def windowClusterPruneEnabled(spark: SparkSession): Boolean =
    windowClusterPruneEnabled(spark.sparkContext.getConf)

  def windowSinglePassReplaceEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowSinglePassReplaceEnabledKey, default = true)

  def windowSinglePassReplaceEnabled(spark: SparkSession): Boolean =
    windowSinglePassReplaceEnabled(spark.sparkContext.getConf)

  def windowSnapshotCacheEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowSnapshotCacheEnabledKey, default = false)

  def windowSnapshotCacheEnabled(spark: SparkSession): Boolean =
    windowSnapshotCacheEnabled(spark.sparkContext.getConf)

  def windowLastValueBackingStateEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowLastValueBackingStateEnabledKey, default = false)

  def windowLastValueBackingStateEnabled(spark: SparkSession): Boolean =
    windowLastValueBackingStateEnabled(spark.sparkContext.getConf)

  def windowRunningIncrementalEnabled(conf: SparkConf): Boolean =
    boolConf(conf, WindowRunningIncrementalEnabledKey, default = false)

  def windowRunningIncrementalEnabled(spark: SparkSession): Boolean =
    windowRunningIncrementalEnabled(spark.sparkContext.getConf)

  def noopFastExitEnabled(conf: SparkConf): Boolean =
    boolConf(conf, NoopFastExitEnabledKey, default = false)

  def noopFastExitEnabled(spark: SparkSession): Boolean =
    noopFastExitEnabled(spark.sparkContext.getConf)

  def costModelEnabled(conf: SparkConf): Boolean =
    boolConf(conf, CostModelEnabledKey, default = false)

  def costModelEnabled(spark: SparkSession): Boolean =
    costModelEnabled(spark.sparkContext.getConf)

  def runtimeEmptyDeltaSkipEnabled(conf: SparkConf): Boolean =
    boolConf(conf, RuntimeEmptyDeltaSkipEnabledKey, default = false)

  def runtimeEmptyDeltaSkipEnabled(spark: SparkSession): Boolean =
    runtimeEmptyDeltaSkipEnabled(spark.sparkContext.getConf)

  def unifiedRefreshIntelligenceEnabled(conf: SparkConf): Boolean =
    boolConf(conf, UnifiedRefreshIntelligenceEnabledKey, default = false)

  def unifiedRefreshIntelligenceEnabled(spark: SparkSession): Boolean =
    unifiedRefreshIntelligenceEnabled(spark.sparkContext.getConf)

  /** Spark conf overrides that switch on runtime-filter pushdown for the wrapped
    * refresh statements. Empty when [[RuntimeFilterEnabledKey]] is off. The
    * application-side threshold is lowered from Spark's 10 GiB default so the
    * filter fires on SF10-scale source scans; the creation-side default
    * (10 MiB) still restricts it to joins whose build side is the tiny delta.
    */
  def runtimeFilterConfOverrides(conf: SparkConf): Map[String, String] =
    if (!runtimeFilterEnabled(conf)) Map.empty
    else
      Map(
        "spark.sql.optimizer.runtime.bloomFilter.enabled"                          -> "true",
        "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "1MB",
        "spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled"              -> "true"
      )

  def runtimeFilterConfOverrides(spark: SparkSession): Map[String, String] =
    runtimeFilterConfOverrides(spark.sparkContext.getConf)

  def changeFeedMode(spark: SparkSession): ChangeFeedMode =
    ChangeFeedMode.fromSession(spark)

  def changeFeedMode(conf: SparkConf): ChangeFeedMode =
    ChangeFeedMode.fromConf(conf)

  /** Build the TBLPROPERTIES list for an MV data table. Empty Seq means none enabled. */
  def buildMvDataTblProperties(spark: SparkSession): Seq[String] = {
    val props = scala.collection.mutable.ArrayBuffer.empty[String]
    if (deletionVectorsEnabled(spark)) props += "'delta.enableDeletionVectors' = 'true'"
    if (optimizeWriteEnabled(spark)) props += "'delta.autoOptimize.optimizeWrite' = 'true'"
    if (autoCompactEnabled(spark)) props += "'delta.autoOptimize.autoCompact' = 'true'"
    if (ChangePropagationFactory.forSession(spark).requiresMvCdf)
      props += "'delta.enableChangeDataFeed' = 'true'"
    props.toSeq
  }
}
