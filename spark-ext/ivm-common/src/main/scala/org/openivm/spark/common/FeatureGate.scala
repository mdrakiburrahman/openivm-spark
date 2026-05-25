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
    * All three default ON for production MV data tables. The test build
    * (`spark-ext/project/Settings.scala`) overrides selected knobs OFF where the
    * extra per-fork IO is not needed.
    */
  val DeltaEnableDeletionVectorsKey: String = "spark.openivm.delta.enableDeletionVectors"
  val DeltaOptimizeWriteKey: String         = "spark.openivm.delta.optimizeWrite"
  val DeltaAutoCompactKey: String           = "spark.openivm.delta.autoCompact"

  def enabled(conf: SparkConf): Boolean =
    conf.getBoolean(EnabledKey, defaultValue = false)

  def enabled(spark: SparkSession): Boolean =
    enabled(spark.sparkContext.getConf)

  private def boolConf(conf: SparkConf, key: String, default: Boolean): Boolean =
    conf.getBoolean(key, default)

  def deletionVectorsEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, DeltaEnableDeletionVectorsKey, default = true)

  def optimizeWriteEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, DeltaOptimizeWriteKey, default = true)

  def autoCompactEnabled(spark: SparkSession): Boolean =
    boolConf(spark.sparkContext.getConf, DeltaAutoCompactKey, default = true)

  /** Build the TBLPROPERTIES list for an MV data table. Empty Seq means none enabled. */
  def buildMvDataTblProperties(spark: SparkSession): Seq[String] = {
    val props = scala.collection.mutable.ArrayBuffer.empty[String]
    if (deletionVectorsEnabled(spark)) props += "'delta.enableDeletionVectors' = 'true'"
    if (optimizeWriteEnabled(spark)) props += "'delta.autoOptimize.optimizeWrite' = 'true'"
    if (autoCompactEnabled(spark)) props += "'delta.autoOptimize.autoCompact' = 'true'"
    props.toSeq
  }
}
