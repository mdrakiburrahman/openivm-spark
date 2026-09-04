package org.openivm.spark.common

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

import scala.util.control.NonFatal

/**
 * Latest committed Delta version of a table or path, resolved from the
 * transaction-log snapshot on the driver.
 *
 * `DeltaTable.forPath(...).history(1).collect()` answers the same question,
 * but it materializes a Delta relation and submits a Spark job to read the
 * commit history. That job has to be scheduled: while many CREATE/REFRESH
 * commands run concurrently on one driver, every task slot is held by the
 * concurrent Delta data writes, so the version read — the only Spark work
 * left in the catalog-publication phase — queues behind them. The version is
 * already known to the driver from the log segment, so the job is redundant
 * metadata work.
 *
 * `DeltaLog.update()` lists the log directory and returns the snapshot whose
 * `version` is the latest commit; it never runs state reconstruction and
 * never submits a job.
 */
object DeltaTableVersion {

  /** Timer metric name; routed to the execution span as a dedicated field. */
  val LookupMetric: String = "catalog.delta_version.lookup"

  /** `DeltaLog.update().version` for a table with no commits. */
  val NoCommits: Long = -1L

  /**
   * Latest committed version of `tableNameOrPath`, or [[NoCommits]] when the
   * target has no Delta transaction log.
   */
  def latest(spark: SparkSession, tableNameOrPath: String): Long =
    OpenIvmMetrics.time(LookupMetric) {
      deltaLogFor(spark, tableNameOrPath).update().version
    }

  /**
   * Latest committed version, or `None` when the target cannot be read as a
   * Delta table (unknown identifier, no transaction log, unreadable path).
   */
  def latestOption(spark: SparkSession, tableNameOrPath: String): Option[Long] =
    try {
      val version = latest(spark, tableNameOrPath)
      if (version < 0L) None else Some(version)
    } catch {
      case NonFatal(_) => None
    }

  /**
   * Latest committed version of a table that must already be committed.
   * Mirrors the failure mode of the Delta history read it replaces: a target
   * without a transaction log is an error, not a `-1` version.
   */
  def requireLatest(spark: SparkSession, tableNameOrPath: String): Long = {
    val version = latest(spark, tableNameOrPath)
    if (version < 0L)
      throw new IllegalStateException(
        s"$tableNameOrPath has no committed Delta version; expected a committed Delta table"
      )
    version
  }

  private[common] def deltaLogFor(spark: SparkSession, tableNameOrPath: String): DeltaLog =
    if (looksLikePath(tableNameOrPath)) DeltaLog.forTable(spark, new Path(tableNameOrPath))
    else DeltaLog.forTable(spark, CatalystSqlParser.parseTableIdentifier(tableNameOrPath))

  private def looksLikePath(tableNameOrPath: String): Boolean =
    tableNameOrPath.startsWith("/") || tableNameOrPath.startsWith("file:") || tableNameOrPath.contains("/")
}
