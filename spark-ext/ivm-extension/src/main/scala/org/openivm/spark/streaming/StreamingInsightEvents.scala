package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.insights.{OpenIvmInsightJson, OpenIvmInsightsBroker, OpenIvmInsightsContract}

import scala.util.control.NonFatal

private[spark] object StreamingInsightEvents {

  private val Mapper          = new ObjectMapper()
  private val ShortHashLength = 12

  def operationStarted(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      command: String
  ): Unit =
    emit(
      spark,
      operationId,
      targetRelation,
      OpenIvmInsightsContract.Operation.Streaming,
      stage = "operation",
      eventType = OpenIvmInsightsContract.EventType.OperationStarted,
      level = OpenIvmInsightsContract.Level.Info,
      code = OpenIvmInsightsContract.Code.OperationStarted,
      message = s"OpenIVM streaming $command operation started.",
      status = "running",
      terminal = false,
      details = baseDetails(
        spark,
        operationId,
        targetRelation,
        targetKind = "streaming",
        command = Some(command)
      )
    )

  def operationFinished(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      command: String,
      outcome: String,
      durationMs: Long,
      failed: Boolean,
      branchCode: Option[String],
      runtime: Option[StreamingTableStatus],
      error: Option[Throwable] = None
  ): Unit =
    emit(
      spark,
      operationId,
      targetRelation,
      OpenIvmInsightsContract.Operation.Streaming,
      stage = "operation",
      eventType =
        if (failed) OpenIvmInsightsContract.EventType.OperationFailed
        else OpenIvmInsightsContract.EventType.OperationCompleted,
      level = if (failed) OpenIvmInsightsContract.Level.Error else OpenIvmInsightsContract.Level.Info,
      code =
        if (failed) OpenIvmInsightsContract.Code.OperationFailed
        else OpenIvmInsightsContract.Code.OperationCompleted,
      message =
        if (failed) s"OpenIVM streaming $command operation failed."
        else s"OpenIVM streaming $command operation completed.",
      status = outcome,
      durationMs = Some(durationMs),
      terminal = true,
      details = baseDetails(
        spark,
        operationId,
        targetRelation,
        targetKind = "streaming",
        command = Some(command),
        branchCode = branchCode
      ) ++
        runtime.toSeq.flatMap(runtimeDetails) ++
        error.toSeq.flatMap(value => Seq("error_class" -> value.getClass.getName))
    )

  def branch(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      branchCode: String,
      policy: String,
      details: Seq[(String, Any)] = Seq.empty,
      level: String = OpenIvmInsightsContract.Level.Info
  ): Unit =
    emit(
      spark,
      operationId,
      targetRelation,
      OpenIvmInsightsContract.Operation.Streaming,
      stage = "decision",
      eventType = OpenIvmInsightsContract.EventType.DecisionCompleted,
      level = level,
      code = branchCode,
      message = s"Selected streaming lifecycle branch ${OpenIvmInsightsContract.BranchCode.Labels(branchCode)}.",
      status = OpenIvmInsightsContract.BranchCode.Labels(branchCode),
      terminal = false,
      details = baseDetails(
        spark,
        operationId,
        targetRelation,
        targetKind = "streaming",
        branchCode = Some(branchCode)
      ) ++ Seq("policy" -> policy) ++ details
    )

  def semanticBranch(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      branchCode: String,
      decision: StreamingRebuildDecision,
      policy: String,
      details: Seq[(String, Any)] = Seq.empty,
      level: String = OpenIvmInsightsContract.Level.Info
  ): Unit =
    branch(
      spark,
      operationId,
      targetRelation,
      branchCode,
      policy,
      semanticDecisionDetails(decision) ++ details,
      level
    )

  def cascadePlan(
      spark: SparkSession,
      operationId: String,
      rootRelation: String,
      rootIdentity: String,
      descendants: Seq[StreamingTableCascadeTarget],
      completedIdentities: Set[String],
      reason: String,
      operation: String = OpenIvmInsightsContract.Operation.Streaming
  ): Unit =
    emit(
      spark,
      operationId,
      rootRelation,
      operation,
      stage = "cascade_plan",
      eventType = OpenIvmInsightsContract.EventType.DecisionCompleted,
      level = OpenIvmInsightsContract.Level.Info,
      code = OpenIvmInsightsContract.Code.StreamingCascadePlan,
      message = "Resolved the managed streaming descendant cascade plan.",
      status = "planned",
      terminal = false,
      details = baseDetails(
        spark,
        operationId,
        rootRelation,
        targetKind = "streaming",
        executionMode = executionMode(operation)
      ) ++ Seq(
        "root_identity"                            -> rootIdentity,
        OpenIvmInsightsContract.DetailField.Reason -> reason,
        "descendant_count"                         -> descendants.size,
        "descendants" -> descendants.zipWithIndex.map { case (target, index) =>
          Map(
            "order"                                    -> index,
            "target_kind"                              -> target.kind,
            "target_relation"                          -> target.name.mkString("."),
            "target_identity"                          -> target.identity,
            "caused_by"                                -> target.causedBy.getOrElse(rootIdentity),
            OpenIvmInsightsContract.DetailField.Reason -> reason,
            "recovery_state" ->
              (if (completedIdentities.contains(target.identity)) "completed" else "remaining")
          )
        }
      )
    )

  def recoveryState(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      intent: StreamingTableResetIntent,
      phase: String
  ): Unit = {
    val completed = intent.completedDescendantIdentities.toSet
    val remaining = intent.descendants.map(_.identity).filterNot(completed)
    action(
      spark,
      operationId,
      targetRelation,
      targetKind = "streaming",
      targetIdentity = intent.targetIdentity,
      causedBy = None,
      stage = "recovery",
      code = OpenIvmInsightsContract.Code.StreamingRecoveryStateObserved,
      message = "Observed streaming rebuild recovery state.",
      status = phase,
      details = Seq(
        "journal_operation_id"            -> intent.operationId,
        "completed_descendant_identities" -> intent.completedDescendantIdentities,
        "remaining_descendant_identities" -> remaining,
        "upstream_dropped"                -> intent.upstreamDropped
      )
    )
  }

  def action(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      targetKind: String,
      targetIdentity: String,
      causedBy: Option[String],
      stage: String,
      code: String,
      message: String,
      status: String = "completed",
      level: String = OpenIvmInsightsContract.Level.Info,
      runtime: Option[StreamingTableStatus] = None,
      operation: String = OpenIvmInsightsContract.Operation.Streaming,
      details: Seq[(String, Any)] = Seq.empty
  ): Unit =
    emit(
      spark,
      operationId,
      targetRelation,
      operation,
      stage,
      eventType =
        if (level == OpenIvmInsightsContract.Level.Error)
          OpenIvmInsightsContract.EventType.ActionFailed
        else OpenIvmInsightsContract.EventType.ActionCompleted,
      level = level,
      code = code,
      message = message,
      status = status,
      terminal = false,
      details = baseDetails(
        spark,
        operationId,
        targetRelation,
        targetKind = targetKind,
        executionMode = executionMode(operation)
      ) ++ Seq(
        "target_kind"     -> targetKind,
        "target_identity" -> targetIdentity,
        "caused_by"       -> causedBy
      ) ++ runtime.toSeq.flatMap(runtimeDetails) ++ details
    )

  def fingerprintDetails(prefix: String, fingerprint: String): Seq[(String, Any)] =
    Seq(
      s"${prefix}_fingerprint"       -> fingerprint,
      s"${prefix}_fingerprint_short" -> shortHash(fingerprint)
    )

  private def semanticDecisionDetails(decision: StreamingRebuildDecision): Seq[(String, Any)] =
    fingerprintDetails("previous", decision.previousFingerprint) ++
      fingerprintDetails("requested", decision.requestedFingerprint) ++
      Seq(
        "decision_code"          -> decision.code,
        "decision_summary"       -> decision.summary,
        "normalizedQueryChanged" -> decision.normalizedQueryChanged,
        "changed_categories"     -> decision.changes.map(_.category).distinct.sorted,
        "changes" -> decision.changes.map { change =>
          Map(
            "path"      -> change.path,
            "category"  -> change.category,
            "previous"  -> change.previous,
            "requested" -> change.requested
          )
        }
      )

  private def runtimeDetails(status: StreamingTableStatus): Seq[(String, Any)] =
    Seq(
      "checkpoint_location"          -> status.checkpointLocation,
      "query_id"                     -> status.queryId,
      "run_id"                       -> status.runId,
      "active"                       -> status.isActive,
      "runtime_status"               -> status.status,
      "definition_fingerprint"       -> status.definitionHash,
      "definition_fingerprint_short" -> status.definitionHash.map(shortHash),
      "last_progress"                -> progressSummary(status.lastProgress)
    )

  private[streaming] def progressSummary(progressJson: Option[String]): Option[Map[String, Any]] =
    progressJson.flatMap { raw =>
      try {
        val root = Mapper.readTree(raw)
        val values = Seq(
          longField(root, "batchId").map("batch_id" -> _),
          longField(root, "numInputRows").map("input_rows" -> _),
          nestedLongField(root, "sink", "numOutputRows").map("output_rows" -> _),
          doubleField(root, "inputRowsPerSecond").map("input_rows_per_second" -> _),
          doubleField(root, "processedRowsPerSecond").map("processed_rows_per_second" -> _),
          nestedLongField(root, "durationMs", "triggerExecution").map("batch_duration_ms" -> _),
          textField(root, "timestamp").map(value => "timestamp" -> value.take(128))
        ).flatten
        if (values.isEmpty) None else Some(values.toMap)
      } catch {
        case NonFatal(_) => None
      }
    }

  private def shortHash(value: String): String =
    Option(value).getOrElse("").take(ShortHashLength)

  private def longField(node: JsonNode, field: String): Option[Long] =
    Option(node.get(field)).filter(_.isNumber).map(_.asLong())

  private def doubleField(node: JsonNode, field: String): Option[Double] =
    Option(node.get(field)).filter(_.isNumber).map(_.asDouble())

  private def nestedLongField(node: JsonNode, parent: String, field: String): Option[Long] =
    Option(node.get(parent)).flatMap(value => longField(value, field))

  private def textField(node: JsonNode, field: String): Option[String] =
    Option(node.get(field)).filter(_.isTextual).map(_.asText())

  private def baseDetails(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      targetKind: String,
      command: Option[String] = None,
      branchCode: Option[String] = None,
      executionMode: String = OpenIvmInsightsContract.ExecutionMode.Streaming
  ): Seq[(String, Any)] =
    Seq(
      "operation_id"          -> operationId,
      "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
      "execution_mode"        -> executionMode,
      "materialization"       -> materialization(targetKind),
      "target_relation"       -> targetRelation,
      "command"               -> command,
      "branch_code"           -> branchCode,
      "branch_label"          -> branchCode.flatMap(OpenIvmInsightsContract.BranchCode.Labels.get),
      "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
    )

  private def materialization(targetKind: String): String =
    if (targetKind == "materialized") OpenIvmInsightsContract.Materialization.MaterializedView
    else OpenIvmInsightsContract.Materialization.StreamingTable

  private def executionMode(operation: String): String =
    if (operation == OpenIvmInsightsContract.Operation.Streaming)
      OpenIvmInsightsContract.ExecutionMode.Streaming
    else OpenIvmInsightsContract.ExecutionMode.MaterializedView

  private def emit(
      spark: SparkSession,
      operationId: String,
      targetRelation: String,
      operation: String,
      stage: String,
      eventType: String,
      level: String,
      code: String,
      message: String,
      status: String,
      terminal: Boolean,
      details: Seq[(String, Any)],
      durationMs: Option[Long] = None
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = stage,
        eventType = eventType,
        level = level,
        code = code,
        message = message,
        operationId = Some(operationId),
        materializedView = Some(targetRelation),
        status = Some(status),
        durationMs = durationMs,
        detailsJson = Some(OpenIvmInsightJson.obj(details: _*)),
        terminal = Some(terminal)
      )
    }
    ()
  }
}
