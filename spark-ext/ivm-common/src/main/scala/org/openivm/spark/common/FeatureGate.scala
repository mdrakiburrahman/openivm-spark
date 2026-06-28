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

  /** Fuse `view_delta_ctas` + `insert_into` for leaf SIMPLE_PROJECTION MVs.
    *
    * When a SIMPLE_PROJECTION MV has NO current downstream MV consumer (its
    * short name does not appear in any other MV's `sourceTables` per
    * `MvCatalog.list`), the per-refresh scratch Delta table that openivm
    * emits as stmt[0] is consumed exactly once by stmt[1] (the INSERT INTO
    * mv_data) and, optionally, stmt[2] (the value-equality delete MERGE).
    *
    * Writing the scratch to a Delta path then re-reading it costs ~2.8 s on
    * average for the TPC-DI gold layer; fusing — running stmt[0]'s SELECT
    * as a cached DataFrame + temp view and rewriting subsequent path refs
    * to read from the cache — saves most of that wall-clock per refresh.
    *
    * Default ON. Flip OFF via `spark.openivm.fuseScratch.enabled=false` to
    * fall back to the on-disk scratch path (e.g. for diagnostics).
    */
  val FuseScratchEnabledKey: String = "spark.openivm.fuseScratch.enabled"

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
    boolConf(spark.sparkContext.getConf, FuseScratchEnabledKey, default = true)

  def profileRefreshEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, ProfileRefreshKey, default = false)

  def profileRefreshEnabled(conf: SparkConf): Boolean =
    boolConf(conf, ProfileRefreshKey, default = false)

  def queryLogEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, QueryLogEnabledKey, default = false)

  def queryLogEnabled(conf: SparkConf): Boolean =
    boolConf(conf, QueryLogEnabledKey, default = false)

  def explainCaptureEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, ExplainCaptureKey, default = false)

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
