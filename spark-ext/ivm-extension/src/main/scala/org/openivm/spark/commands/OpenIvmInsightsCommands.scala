package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, TimestampType}
import org.openivm.spark.insights.{OpenIvmInsightsBroker, OpenIvmInsightsContract}

case class BeginOpenIvmInsightsRequestCommand(
    requestId: String,
    runId: String,
    dbtNodeId: String,
    materialization: Option[String] = None,
    targetRelation: Option[String] = None
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    OpenIvmInsightsBroker.begin(
      spark,
      requestId,
      runId,
      dbtNodeId,
      materialization,
      targetRelation
    )
    Seq.empty
  }
}

case class EndOpenIvmInsightsRequestCommand(
    requestId: String,
    succeeded: Boolean,
    errorClass: Option[String] = None,
    errorCode: Option[String] = None
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val failureIdentity = (errorClass, errorCode) match {
      case (Some(safeClass), Some(safeCode)) =>
        Some(OpenIvmInsightsContract.FailureIdentity(safeClass, safeCode))
      case (None, None) => None
      case _ =>
        throw new IllegalArgumentException(
          s"${OpenIvmInsightsContract.FailureCode.ConfigInvalid}: " +
            "ERROR_CLASS and ERROR_CODE must be supplied together"
        )
    }
    OpenIvmInsightsBroker.end(spark, requestId, succeeded, failureIdentity)
    Seq.empty
  }
}

case class AnnotateOpenIvmMvQueryHashCommand(
    targetRelation: String,
    oldQueryHash: String,
    newQueryHash: String,
    policy: String,
    decision: String,
    reason: String
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    OpenIvmInsightsBroker.annotateMvQueryHashDecision(
      spark,
      OpenIvmInsightsContract.MvQueryHashDecision(
        targetRelation,
        oldQueryHash,
        newQueryHash,
        policy,
        decision,
        reason
      )
    )
    Seq.empty
  }
}

case class ReleaseOpenIvmInsightsRequestCommand(requestId: String) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    OpenIvmInsightsBroker.release(spark, requestId)
    Seq.empty
  }
}

case class ShowOpenIvmInsightsCommand(
    requestId: String,
    afterSequence: Long,
    maxEvents: Int
) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("record_type", StringType, nullable = false)(),
    AttributeReference("request_id", StringType, nullable = false)(),
    AttributeReference("run_id", StringType, nullable = true)(),
    AttributeReference("dbt_node_id", StringType, nullable = true)(),
    AttributeReference("capture_status", StringType, nullable = false)(),
    AttributeReference("next_sequence", LongType, nullable = false)(),
    AttributeReference("has_more", BooleanType, nullable = false)(),
    AttributeReference("sequence", LongType, nullable = true)(),
    AttributeReference("event_timestamp", TimestampType, nullable = true)(),
    AttributeReference("operation_id", StringType, nullable = true)(),
    AttributeReference("materialized_view", StringType, nullable = true)(),
    AttributeReference("operation", StringType, nullable = true)(),
    AttributeReference("stage", StringType, nullable = true)(),
    AttributeReference("event_type", StringType, nullable = true)(),
    AttributeReference("level", StringType, nullable = true)(),
    AttributeReference("code", StringType, nullable = true)(),
    AttributeReference("message", StringType, nullable = true)(),
    AttributeReference("status", StringType, nullable = true)(),
    AttributeReference("duration_ms", LongType, nullable = true)(),
    AttributeReference("parent_sequence", LongType, nullable = true)(),
    AttributeReference("details_json", StringType, nullable = true)(),
    AttributeReference("terminal", BooleanType, nullable = true)()
  )

  override def run(spark: SparkSession): Seq[Row] = {
    val page   = OpenIvmInsightsBroker.page(spark, requestId, afterSequence, maxEvents)
    val status = page.status
    val statusRow = Row(
      OpenIvmInsightsContract.RecordType.Status,
      status.requestId,
      status.runId.orNull,
      status.dbtNodeId.orNull,
      status.captureStatus,
      page.nextSequence,
      page.hasMore,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      status.code.orNull,
      status.message.orNull,
      status.requestStatus.orNull,
      null,
      null,
      null,
      Boolean.box(status.terminal)
    )
    statusRow +: page.events.map { event =>
      Row(
        OpenIvmInsightsContract.RecordType.Event,
        status.requestId,
        status.runId.orNull,
        status.dbtNodeId.orNull,
        status.captureStatus,
        page.nextSequence,
        page.hasMore,
        Long.box(event.sequence),
        event.eventTimestamp,
        event.operationId.orNull,
        event.materializedView.orNull,
        event.operation,
        event.stage,
        event.eventType,
        event.level,
        event.code,
        event.message,
        event.status.orNull,
        event.durationMs.map(Long.box).orNull,
        event.parentSequence.map(Long.box).orNull,
        event.detailsJson.orNull,
        event.terminal.map(Boolean.box).orNull
      )
    }
  }
}
