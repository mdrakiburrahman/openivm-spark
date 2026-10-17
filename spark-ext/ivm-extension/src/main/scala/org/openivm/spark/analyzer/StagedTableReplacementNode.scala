package org.openivm.spark.analyzer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.openivm.spark.common.{DeltaTableVersion, SourceVersionDelta, StagingCatalog, StagingDelta}
import org.openivm.spark.insights.OpenIvmInsightsBroker

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Runnable command wrapping CREATE TABLE AS SELECT, REPLACE TABLE and CREATE OR REPLACE TABLE
 * on a base table that feeds materialized views.
 *
 * Spark writes the rows of these commands into an uncommitted `StagedTable`: it cannot be read,
 * Spark 3.5 presents the write as an append, and it commits only after the nested write returns.
 * The command is therefore intercepted whole and run with [[IvmDmlInterceptorRule.bypass]] set,
 * and its change is derived from the committed snapshots before and after it.
 *
 * When both snapshots exist with the same comparable schema, dependents receive the exact
 * multiset change: `old EXCEPT ALL new` staged as DELETE and `new EXCEPT ALL old` as INSERT. They
 * refresh incrementally, and a rebuild that reproduces the same rows publishes nothing. Otherwise
 * the committed replacement is staged as OVERWRITE, preceded by the prior rows as DELETE when
 * they are readable, which dependents apply as a full refresh. Change records are published only
 * after the replacement commits.
 *
 * @param command                    the original CTAS / RTAS / REPLACE TABLE logical plan
 * @param baseTable                  qualified name of the replaced base table
 * @param dependentMaterializedViews identities of MVs whose change feed receives the replacement
 */
case class StagedTableReplacementNode(
    command: LogicalPlan,
    baseTable: String,
    dependentMaterializedViews: Seq[String]
) extends LeafRunnableCommand {

  override lazy val output: Seq[Attribute] = command.output

  override def run(spark: SparkSession): Seq[Row] =
    StagedDmlNode.withBaseTableMutationLock(baseTable) {
      val previousBypass = IvmDmlInterceptorRule.bypass.get()
      IvmDmlInterceptorRule.bypass.set(true)
      try {
        val before = DeltaTableVersion.latestOption(spark, baseTable)
        spark.sessionState.executePlan(command).executedPlan.executeCollect()
        val after  = DeltaTableVersion.latestOption(spark, baseTable)
        val deltas = stageReplacement(spark, before, after)
        deltas.foreach(StagingCatalog.record(spark, _))
        if (OpenIvmInsightsBroker.hasActiveRegularSparkCapture(spark))
          StagedDmlNode.emitChangeCapture(spark, baseTable, deltas, dependentMaterializedViews)
      } finally {
        IvmDmlInterceptorRule.bypass.set(previousBypass)
      }
      Seq.empty
    }

  private def stageReplacement(
      spark: SparkSession,
      before: Option[Long],
      after: Option[Long]
  ): Seq[StagingDelta] =
    (before, after) match {
      case (Some(oldVersion), Some(newVersion)) if newVersion == oldVersion =>
        Seq.empty
      case (Some(oldVersion), Some(newVersion)) if comparable(spark, oldVersion, newVersion) =>
        try exactChange(spark, oldVersion, newVersion)
        catch {
          case NonFatal(exactFailure) =>
            logWarning(
              s"[openivm] exact change set for replacement of $baseTable " +
                s"$oldVersion->$newVersion failed; publishing a full replacement instead",
              exactFailure
            )
            try fullReplacement(spark, before, after)
            catch {
              case NonFatal(fallbackFailure) =>
                fallbackFailure.addSuppressed(exactFailure)
                throw fallbackFailure
            }
        }
      case _ =>
        fullReplacement(spark, before, after)
    }

  /** Whether the endpoints admit an exact row-level change set. Schemas are compared by DDL, the
    * same identity dependents fingerprint, so writer-internal column metadata cannot force a
    * full refresh while a change dependents would reject never passes as incremental.
    */
  private def comparable(spark: SparkSession, oldVersion: Long, newVersion: Long): Boolean =
    try {
      val schema = snapshot(spark, oldVersion).schema
      val same   = schema.toDDL == snapshot(spark, newVersion).schema.toDDL
      if (!same)
        logInfo(s"[openivm] replacement of $baseTable changed its schema; publishing a full replacement")
      same && SourceVersionDelta.supportsBagDiff(schema)
    } catch {
      case NonFatal(error) =>
        logWarning(s"[openivm] cannot compare $baseTable versions $oldVersion and $newVersion", error)
        false
    }

  private def exactChange(spark: SparkSession, oldVersion: Long, newVersion: Long): Seq[StagingDelta] = {
    val oldRows = snapshot(spark, oldVersion)
    val newRows = snapshot(spark, newVersion)
    stageAll(
      spark,
      Seq(
        (oldRows.exceptAll(newRows), StagingDelta.OpTypes.Delete, false),
        (newRows.exceptAll(oldRows), StagingDelta.OpTypes.Insert, false)
      )
    )
  }

  private def fullReplacement(spark: SparkSession, before: Option[Long], after: Option[Long]): Seq[StagingDelta] = {
    // Dependents recompute from the live table; the prior rows are informational only.
    val previous = before.toSeq.flatMap { version =>
      try stageAll(spark, Seq((snapshot(spark, version), StagingDelta.OpTypes.Delete, false)))
      catch {
        case NonFatal(error) =>
          logWarning(s"[openivm] prior rows of $baseTable at version $version are unreadable", error)
          Seq.empty
      }
    }
    val current = after.fold(spark.table(baseTable))(snapshot(spark, _))
    try previous ++ stageAll(spark, Seq((current, StagingDelta.OpTypes.Overwrite, true)))
    catch {
      case failure: Throwable =>
        previous.foreach(delta => StagedDmlNode.cleanupStagingPath(spark, delta.stagingPath, failure))
        throw failure
    }
  }

  /** Rows of the committed version, read through SQL time travel so the snapshot's own schema
    * applies even after the replacement changed the table's current schema.
    */
  private def snapshot(spark: SparkSession, version: Long): DataFrame = {
    val table = baseTable.split("\\.").map(part => s"`${part.replace("`", "``")}`").mkString(".")
    spark.sql(s"SELECT * FROM $table VERSION AS OF $version")
  }

  /** Stage each `(rows, opType, keepEmpty)` in order; on failure, no staged output survives. */
  private def stageAll(spark: SparkSession, ops: Seq[(DataFrame, String, Boolean)]): Seq[StagingDelta] = {
    val staged = mutable.ArrayBuffer.empty[StagingDelta]
    try {
      ops.foreach { case (rows, opType, keepEmpty) =>
        val path  = IvmDmlInterceptorRule.stagingPath(spark, baseTable, opType)
        val delta = StagedDmlNode.writeStaging(spark, rows, baseTable, path, opType)
        if (keepEmpty || !spark.read.format("delta").load(path).isEmpty) staged += delta
        else StagedDmlNode.deleteStagingPath(spark, path)
      }
      staged.toVector
    } catch {
      case failure: Throwable =>
        staged.foreach(delta => StagedDmlNode.cleanupStagingPath(spark, delta.stagingPath, failure))
        throw failure
    }
  }
}
