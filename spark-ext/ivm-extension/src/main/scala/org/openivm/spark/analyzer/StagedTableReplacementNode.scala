package org.openivm.spark.analyzer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.openivm.spark.common.{DeltaTableVersion, StagingDelta}
import org.openivm.spark.insights.OpenIvmInsightsBroker

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
        // Diff from the recorded position, so commits by other writers since are included.
        val before = SourceChangeLedger.reconcileLocked(spark, baseTable).map(_.version)
        spark.sessionState.executePlan(command).executedPlan.executeCollect()
        val after  = DeltaTableVersion.latestOption(spark, baseTable)
        val deltas = stageReplacement(spark, before, after)
        SourceChangeLedger.publishReplacement(spark, baseTable, deltas)
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

  private def comparable(spark: SparkSession, oldVersion: Long, newVersion: Long): Boolean =
    SourceChangeLedger.comparable(spark, baseTable, oldVersion, newVersion)

  private def exactChange(spark: SparkSession, oldVersion: Long, newVersion: Long): Seq[StagingDelta] =
    SourceChangeLedger.exactChange(spark, baseTable, oldVersion, newVersion)

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

  private def snapshot(spark: SparkSession, version: Long): DataFrame =
    SourceChangeLedger.snapshot(spark, baseTable, version)

  private def stageAll(spark: SparkSession, ops: Seq[(DataFrame, String, Boolean)]): Seq[StagingDelta] =
    SourceChangeLedger.stage(spark, baseTable, ops)
}
