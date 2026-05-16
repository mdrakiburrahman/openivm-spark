package org.openivm.spark.analyzer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{StructField, StructType}
import org.openivm.spark.common.{StagingCatalog, StagingDelta}

import java.sql.Timestamp

/**
 * Runnable command wrapping DELETE / UPDATE / MERGE / INSERT / OVERWRITE operations.
 *
 * Execution contract:
 *   1. Set [[IvmDmlInterceptorRule.bypass]] to prevent re-interception.
 *   2. For each op in [[stagingOps]] (pre-DML): execute the plan, write rows to
 *      [[stagingPath]] as a Delta table (append), record [[StagingDelta]] in the
 *      catalog.  If a staging write fails the exception propagates and [[dml]] is
 *      never executed (fail-safe: no phantom base-table change without a staging record).
 *   3. Execute [[dml]] via `executePlan`.  Delta re-lowers the plan if it is
 *      still in pre-lowered form; if it is already `ReplaceData` / `WriteDelta`
 *      it passes through analysis unchanged and is physically executed by Delta.
 *   4. For each op in [[postStagingOps]] (post-DML): same as step 2 but executed
 *      after [[dml]] completes so the plan reads the updated table state.
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

  override def run(spark: SparkSession): Seq[Row] = {
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      for ((preReadPlan, stagPath, opTyp) <- stagingOps) {
        writeStagingDelta(spark, preReadPlan, stagPath, opTyp)
      }
      // Execute the original DML.  With bypass=true our rule is a no-op so
      // Delta can lower / plan / execute without re-interception.
      spark.sessionState.executePlan(dml).executedPlan.executeCollect()
      for ((postReadPlan, stagPath, opTyp) <- postStagingOps) {
        writeStagingDelta(spark, postReadPlan, stagPath, opTyp)
      }
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
    }
    Seq.empty
  }

  private def writeStagingDelta(
      spark: SparkSession,
      plan: LogicalPlan,
      stagPath: String,
      opTyp: String
  ): Unit = {
    val schema = StructType(
      plan.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))
    )
    val qe = spark.sessionState.executePlan(plan)
    val externalRdd = qe.toRdd.mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(ir => converter(ir).asInstanceOf[Row])
    }
    spark
      .createDataFrame(externalRdd, schema)
      .write
      .format("delta")
      .mode("append")
      .save(stagPath)
    StagingCatalog.ensureTables(spark)
    StagingCatalog.record(
      spark,
      StagingDelta(
        baseTable  = baseTable,
        opType     = opTyp,
        stagingPath = stagPath,
        txnTs      = new Timestamp(System.currentTimeMillis()),
        consumedBy = Seq.empty
      )
    )
  }
}
