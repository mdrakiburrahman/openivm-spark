package org.openivm.spark.commands

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/**
 * Placeholder LogicalPlan nodes for the three materialized-view DDL statements.
 *
 * The `run` body is intentionally unimplemented here: execution wiring lands in P4.cmds.
 * Only the plan-shape (types + constructor fields) is defined in the P4.parser surface.
 */

case class CreateMaterializedViewCommand(
    name: TableIdentifier,
    query: LogicalPlan,
    properties: Map[String, String],
    ifNotExists: Boolean,
    provider: Option[String]
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      "openivm-spark: CreateMaterializedViewCommand execution lands in P4.cmds"
    )
}

case class RefreshMaterializedViewCommand(
    name: TableIdentifier
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      "openivm-spark: RefreshMaterializedViewCommand execution lands in P4.cmds"
    )
}

case class DropMaterializedViewCommand(
    name: TableIdentifier,
    ifExists: Boolean
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      "openivm-spark: DropMaterializedViewCommand execution lands in P4.cmds"
    )
}
