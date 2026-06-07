package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.openivm.spark.common.{RefreshProfileCatalog, RefreshProfileRow}

case class ShowRefreshProfileCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("refresh_id", StringType, nullable = false)(),
    AttributeReference("view_name", StringType, nullable = false)(),
    AttributeReference("profile_timestamp", TimestampType, nullable = false)(),
    AttributeReference("step_order", IntegerType, nullable = false)(),
    AttributeReference("step_name", StringType, nullable = false)(),
    AttributeReference("duration_ms", LongType, nullable = false)(),
    AttributeReference("detail", StringType, nullable = false)()
  )

  override def run(spark: SparkSession): Seq[Row] =
    RefreshProfileCatalog.scanAll(spark).map { row: RefreshProfileRow =>
      Row(
        row.refreshId,
        row.viewName,
        row.profileTimestamp,
        row.stepOrder,
        row.stepName,
        row.durationMs,
        Option(row.detail).getOrElse("")
      )
    }
}
