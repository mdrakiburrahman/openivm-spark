package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.openivm.spark.streaming.{StreamingTableManager, StreamingTableSpec, StreamingTableStatus}

/** Command nodes for the standalone native Structured Streaming table surface. */
case class CreateStreamingTableCommand(spec: StreamingTableSpec) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = StreamingTableCommands.lifecycleOutput

  override def run(spark: SparkSession): Seq[Row] =
    Seq(StreamingTableCommands.lifecycleRow(StreamingTableManager.create(spark, spec)))
}

case class StopStreamingTableCommand(name: Seq[String]) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = StreamingTableCommands.lifecycleOutput

  override def run(spark: SparkSession): Seq[Row] =
    Seq(StreamingTableCommands.lifecycleRow(StreamingTableManager.stop(spark, name)))
}

case class DropStreamingTableCommand(name: Seq[String], ifExists: Boolean) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = StreamingTableCommands.lifecycleOutput

  override def run(spark: SparkSession): Seq[Row] =
    Seq(StreamingTableCommands.lifecycleRow(StreamingTableManager.drop(spark, name, ifExists)))
}

case class ShowStreamingTablesCommand(namespace: Option[Seq[String]] = None) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = StreamingTableCommands.showOutput

  override def run(spark: SparkSession): Seq[Row] =
    StreamingTableManager.show(spark, namespace).map(StreamingTableCommands.showRow)
}

object StreamingTableCommands {

  val lifecycleOutput: Seq[Attribute] = Seq(
    AttributeReference("table_name", StringType, nullable = false)(),
    AttributeReference("query_id", StringType, nullable = true)(),
    AttributeReference("run_id", StringType, nullable = true)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("checkpoint_location", StringType, nullable = true)(),
    AttributeReference("definition_hash", StringType, nullable = true)()
  )

  val showOutput: Seq[Attribute] = Seq(
    AttributeReference("table_name", StringType, nullable = false)(),
    AttributeReference("query_id", StringType, nullable = true)(),
    AttributeReference("run_id", StringType, nullable = true)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("is_active", BooleanType, nullable = false)(),
    AttributeReference("checkpoint_location", StringType, nullable = true)(),
    AttributeReference("definition_hash", StringType, nullable = true)(),
    AttributeReference("last_progress", StringType, nullable = true)(),
    AttributeReference("last_failure", StringType, nullable = true)()
  )

  private[commands] def lifecycleRow(status: StreamingTableStatus): Row =
    Row(
      status.tableName,
      status.queryId.orNull,
      status.runId.orNull,
      status.status,
      status.checkpointLocation.orNull,
      status.definitionHash.orNull
    )

  private[commands] def showRow(status: StreamingTableStatus): Row =
    Row(
      status.tableName,
      status.queryId.orNull,
      status.runId.orNull,
      status.status,
      status.isActive,
      status.checkpointLocation.orNull,
      status.definitionHash.orNull,
      status.lastProgress.orNull,
      status.lastFailure.orNull
    )
}
