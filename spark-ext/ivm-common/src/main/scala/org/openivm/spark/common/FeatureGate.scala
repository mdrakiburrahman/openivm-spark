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

  /** Build the TBLPROPERTIES list for an MV data table. Empty Seq means none enabled. */
  def buildMvDataTblProperties(spark: SparkSession): Seq[String] = {
    val props = scala.collection.mutable.ArrayBuffer.empty[String]
    if (deletionVectorsEnabled(spark)) props += "'delta.enableDeletionVectors' = 'true'"
    if (optimizeWriteEnabled(spark)) props += "'delta.autoOptimize.optimizeWrite' = 'true'"
    if (autoCompactEnabled(spark)) props += "'delta.autoOptimize.autoCompact' = 'true'"
    props.toSeq
  }
}
