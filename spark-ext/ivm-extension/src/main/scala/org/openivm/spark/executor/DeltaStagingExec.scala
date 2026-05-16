package org.openivm.spark.executor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{StructField, StructType}
import org.openivm.spark.common.{StagingCatalog, StagingDelta}

import java.sql.Timestamp

/**
 * Physical tee node for INSERT / OVERWRITE paths.
 *
 * Placement in the physical plan:
 * {{{
 *   DeltaWriteExec
 *     └─ DeltaStagingExec          ← inserted by IvmStrategy
 *          └─ <data query plan>
 * }}}
 *
 * Execution contract:
 *   1. Execute [[child]], caching the resulting [[RDD]] so it is materialised
 *      only once.
 *   2. Convert [[org.apache.spark.sql.catalyst.InternalRow]]s to external
 *      [[Row]]s and write them to [[stagingPath]] as a Delta table (append
 *      mode).  This is a synchronous inner Spark job; if it throws the
 *      exception propagates up and the parent (Delta) write is aborted.
 *   3. Record a [[StagingDelta]] entry in the staging catalog.
 *   4. Return the cached [[RDD]] so the parent write consumes the same data.
 */
case class DeltaStagingExec(
    child: SparkPlan,
    stagingPath: String,
    opType: String,
    baseTable: String
) extends UnaryExecNode {

  override protected def withNewChildInternal(newChild: SparkPlan): DeltaStagingExec =
    copy(child = newChild)

  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = child.output

  override protected def doExecute(): RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    val spark = SparkSession
      .getActiveSession
      .getOrElse(throw new IllegalStateException("No active SparkSession for DeltaStagingExec"))
    val schema = StructType(child.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

    val inputRdd = child.execute()
    inputRdd.cache()

    val externalRdd = inputRdd.mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(ir => converter(ir).asInstanceOf[Row])
    }

    spark
      .createDataFrame(externalRdd, schema)
      .write
      .format("delta")
      .mode("append")
      .save(stagingPath)

    StagingCatalog.ensureTables(spark)
    StagingCatalog.record(
      spark,
      StagingDelta(
        baseTable  = baseTable,
        opType     = opType,
        stagingPath = stagingPath,
        txnTs      = new Timestamp(System.currentTimeMillis()),
        consumedBy = Seq.empty
      )
    )

    inputRdd
  }
}
