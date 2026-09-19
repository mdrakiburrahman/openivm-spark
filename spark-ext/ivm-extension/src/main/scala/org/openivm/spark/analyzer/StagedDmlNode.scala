package org.openivm.spark.analyzer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{StructField, StructType}
import org.openivm.spark.common.{StagingCatalog, StagingDelta}

import org.apache.hadoop.fs.Path
import java.sql.Timestamp
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/**
 * Runnable command wrapping DELETE / UPDATE / MERGE / INSERT / OVERWRITE operations.
 *
 * Execution contract:
 *   1. Set [[IvmDmlInterceptorRule.bypass]] to prevent re-interception.
 *   2. For each op in [[stagingOps]] (pre-DML): execute the plan and write rows
 *      to [[stagingPath]] as a Delta table (append). If a staging write fails,
 *      the exception propagates and [[dml]] is never executed.
 *   3. Execute [[dml]] via `executePlan`.  Delta re-lowers the plan if it is
 *      still in pre-lowered form; if it is already `ReplaceData` / `WriteDelta`
 *      it passes through analysis unchanged and is physically executed by Delta.
 *      Publish the pre-DML staging records only after this commit succeeds, so
 *      an OCC retry cannot consume a phantom delta from the failed attempt.
 *   4. For each op in [[postStagingOps]] (post-DML): same as step 2 but executed
 *      after [[dml]] completes so the plan reads the updated table state, then
 *      publish its staging record.
 *   5. Clear [[IvmDmlInterceptorRule.bypass]] in the `finally` block.
 *
 * @param dml             the original DML logical plan (pre- or post-Delta-lowered)
 * @param stagingOps      pre-DML sequence of (plan, stagingPath, opType) tuples
 * @param baseTable       qualified name of the base table being modified
 * @param postStagingOps  post-DML sequence of (plan, stagingPath, opType) tuples
 */
case class StagedDmlNode(
    dml: LogicalPlan,
    stagingOps: Seq[(LogicalPlan, String, String)],
    baseTable: String,
    postStagingOps: Seq[(LogicalPlan, String, String)] = Seq.empty
) extends LeafRunnableCommand {

  override lazy val output: Seq[Attribute] = dml.output

  override def run(spark: SparkSession): Seq[Row] =
    StagedDmlNode.withBaseTableMutationLock(baseTable) {
      val previousBypass = IvmDmlInterceptorRule.bypass.get()
      IvmDmlInterceptorRule.bypass.set(true)
      try {
        val preDeltas = stagingOps.map { case (preReadPlan, stagPath, opTyp) =>
          writeStagingDelta(spark, preReadPlan, stagPath, opTyp)
        }
        try {
          // Execute the original DML. With bypass=true our rule is a no-op so
          // Delta can lower / plan / execute without re-interception.
          spark.sessionState.executePlan(dml).executedPlan.executeCollect()
        } catch {
          case failure: Throwable =>
            preDeltas.foreach(delta => cleanupStagingPath(spark, delta.stagingPath, failure))
            throw failure
        }

        preDeltas.foreach(StagingCatalog.record(spark, _))
        for ((postReadPlan, stagPath, opTyp) <- postStagingOps) {
          val delta = writeStagingDelta(spark, postReadPlan, stagPath, opTyp)
          StagingCatalog.record(spark, delta)
        }
      } finally {
        IvmDmlInterceptorRule.bypass.set(previousBypass)
      }
      Seq.empty
    }

  private def writeStagingDelta(
      spark: SparkSession,
      plan: LogicalPlan,
      stagPath: String,
      opTyp: String
  ): StagingDelta = {
    val schema = StructType(
      plan.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))
    )
    val qe = spark.sessionState.executePlan(plan)
    val externalRdd = qe.toRdd.mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(ir => converter(ir).asInstanceOf[Row])
    }
    try {
      spark
        .createDataFrame(externalRdd, schema)
        .write
        .format("delta")
        .mode("append")
        .save(stagPath)
    } catch {
      case failure: Throwable =>
        cleanupStagingPath(spark, stagPath, failure)
        throw failure
    }
    StagingCatalog.ensureTables(spark)
    StagingDelta(
      baseTable = baseTable,
      opType = opTyp,
      stagingPath = stagPath,
      txnTs = new Timestamp(System.currentTimeMillis()),
      consumedBy = Seq.empty
    )
  }

  private def cleanupStagingPath(spark: SparkSession, stagPath: String, failure: Throwable): Unit =
    try {
      val path = new Path(stagPath)
      val fs   = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(path) && !fs.delete(path, true)) {
        throw new IllegalStateException(s"Failed to clean uncommitted OpenIVM staging path: $stagPath")
      }
    } catch {
      case cleanupFailure: Throwable => failure.addSuppressed(cleanupFailure)
    }
}

private[analyzer] object StagedDmlNode {
  private val mutationLocks = new ConcurrentHashMap[String, AnyRef]()

  def withBaseTableMutationLock[A](baseTable: String)(body: => A): A = {
    val key       = baseTable.trim.toLowerCase(Locale.ROOT)
    val candidate = new Object
    val existing  = mutationLocks.putIfAbsent(key, candidate)
    val lock      = if (existing == null) candidate else existing
    lock.synchronized(body)
  }
}
