package org.openivm.spark.analyzer

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

/**
 * Logical plan marker inserted around the data-bearing child of a V2 write
 * command (AppendData / OverwriteByExpression) to signal that a staging Delta
 * table must be written at [[stagingPath]] before the data reaches the parent
 * DML operator.
 *
 * Physical counterpart: [[org.openivm.spark.executor.DeltaStagingExec]].
 *
 * @param child       the original data query (same as the original write's query child)
 * @param stagingPath absolute path for the staging Delta table
 * @param opType      "INSERT" or "OVERWRITE"
 * @param baseTable   qualified table name of the base Delta table being written
 */
case class WithDeltaStaging(
    child: LogicalPlan,
    stagingPath: String,
    opType: String,
    baseTable: String
) extends UnaryNode {
  override def output: Seq[Attribute]                                               = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): WithDeltaStaging =
    copy(child = newChild)
}
