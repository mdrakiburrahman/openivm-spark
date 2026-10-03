package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, RefreshDecision}
import org.openivm.spark.insights.{OpenIvmInsightJson, OpenIvmInsightsBroker, OpenIvmInsightsContract}
import org.openivm.spark.telemetry.OpenIvmTelemetryContract

private[spark] object OpenIvmInsightEvents {

  def operationStarted(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = "operation",
        eventType = OpenIvmInsightsContract.EventType.OperationStarted,
        level = OpenIvmInsightsContract.Level.Info,
        code = OpenIvmInsightsContract.Code.OperationStarted,
        message = s"OpenIVM $operation operation started.",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some("running"),
        detailsJson = Some(linkDetails(spark, operationId)),
        terminal = Some(false)
      )
    }
    ()
  }

  def operationFinished(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      outcome: String,
      durationMs: Long,
      failed: Boolean,
      warning: Boolean = false
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = "operation",
        eventType =
          if (failed) OpenIvmInsightsContract.EventType.OperationFailed
          else OpenIvmInsightsContract.EventType.OperationCompleted,
        level =
          if (failed) OpenIvmInsightsContract.Level.Error
          else if (warning) OpenIvmInsightsContract.Level.Warn
          else OpenIvmInsightsContract.Level.Info,
        code =
          if (failed) OpenIvmInsightsContract.Code.OperationFailed
          else if (warning) OpenIvmInsightsContract.Code.OperationCompletedWarning
          else OpenIvmInsightsContract.Code.OperationCompleted,
        message =
          if (failed) s"OpenIVM $operation operation failed."
          else if (warning) s"OpenIVM $operation operation completed with a warning."
          else s"OpenIVM $operation operation completed.",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(outcome),
        durationMs = Some(durationMs),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "operation_id"          -> operationId,
            "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
            "outcome"               -> outcome,
            "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
          )
        ),
        terminal = Some(true)
      )
    }
    ()
  }

  def classification(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      compileRefreshType: String,
      effectiveRefreshType: String,
      reason: String,
      emitsCascadeViewDelta: Boolean,
      upstreamSnapshotTrigger: Option[String] = None
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = "classification",
        eventType = OpenIvmInsightsContract.EventType.DecisionCompleted,
        level =
          if (effectiveRefreshType == "FULL_REFRESH" && compileRefreshType != "FULL_REFRESH")
            OpenIvmInsightsContract.Level.Warn
          else OpenIvmInsightsContract.Level.Info,
        code = OpenIvmInsightsContract.Code.ClassificationCompleted,
        message = "OpenIVM refresh classification completed.",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(effectiveRefreshType),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "operation_id"              -> operationId,
            "request_id"                -> OpenIvmInsightsBroker.currentRequestId(spark),
            "compile_refresh_type"      -> compileRefreshType,
            "effective_refresh_type"    -> effectiveRefreshType,
            "reason"                    -> reason,
            "emits_cascade_view_delta"  -> emitsCascadeViewDelta,
            "upstream_snapshot_trigger" -> upstreamSnapshotTrigger,
            "query_logging_enabled"     -> FeatureGate.queryLogEnabled(spark)
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  def sourceVersions(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      stage: String,
      values: => Seq[OpenIvmTelemetryContract.SourceVersion]
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      val observed = values
      val versions = observed.sortBy(_.relation.toLowerCase(java.util.Locale.ROOT)).map { value =>
        Map(
          "relation"      -> value.relation,
          "start_version" -> value.startVersion,
          "end_version"   -> value.endVersion
        )
      }
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = stage,
        eventType = OpenIvmInsightsContract.EventType.ObservationCompleted,
        level = OpenIvmInsightsContract.Level.Info,
        code = OpenIvmInsightsContract.Code.SourceVersionsObserved,
        message = s"Observed source versions for ${observed.size} source(s).",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some("observed"),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "operation_id"          -> operationId,
            "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
            "source_versions"       -> versions,
            "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  def pendingDeltas(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      pendingCount: Int,
      batchesBySource: Map[String, Int]
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = "pending_deltas",
        eventType = OpenIvmInsightsContract.EventType.ObservationCompleted,
        level = OpenIvmInsightsContract.Level.Info,
        code = OpenIvmInsightsContract.Code.PendingDeltasObserved,
        message = s"Observed $pendingCount pending source delta batch(es).",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(if (pendingCount == 0) "empty" else "pending"),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "operation_id"          -> operationId,
            "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
            "pending_delta_count"   -> pendingCount,
            "batches_by_source"     -> batchesBySource,
            "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  def refreshDecision(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      decision: RefreshDecision
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      val cost = decision.costEstimate
      val runtime = decision.runtimeDeltaSize.map { size =>
        Map(
          "total_rows" -> size.totalRows,
          "sources"    -> size.rowsBySource
        )
      }
      OpenIvmInsightsContract.EventDraft(
        operation = OpenIvmInsightsContract.Operation.Refresh,
        stage = "route",
        eventType = OpenIvmInsightsContract.EventType.DecisionCompleted,
        level = OpenIvmInsightsContract.Level.Info,
        code = OpenIvmInsightsContract.Code.RefreshRouteSelected,
        message = s"OpenIVM selected refresh route ${decision.route.name}.",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(decision.route.name),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "operation_id" -> operationId,
            "request_id"   -> OpenIvmInsightsBroker.currentRequestId(spark),
            "route"        -> decision.route.name,
            "reasons"      -> decision.reasons,
            "cost_estimate" -> Map(
              "base_rows"                  -> cost.baseRows,
              "delta_rows"                 -> cost.deltaRows,
              "delta_to_base_ratio"        -> cost.deltaToBaseRatio,
              "full_recompute_recommended" -> cost.fullRecomputeRecommended,
              "reasons"                    -> cost.reasons
            ),
            "runtime_delta_size"    -> runtime,
            "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  def decision(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      stage: String,
      code: String,
      message: String,
      status: String,
      details: Seq[(String, Any)] = Seq.empty
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = stage,
        eventType = OpenIvmInsightsContract.EventType.DecisionCompleted,
        level = OpenIvmInsightsContract.Level.Info,
        code = code,
        message = message,
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(status),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            (Seq(
              "operation_id"          -> operationId,
              "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
              "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
            ) ++ details): _*
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  def action(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      stage: String,
      code: String,
      message: String,
      status: String = "completed",
      level: String = OpenIvmInsightsContract.Level.Info,
      details: Seq[(String, Any)] = Seq.empty
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = stage,
        eventType =
          if (level == OpenIvmInsightsContract.Level.Error)
            OpenIvmInsightsContract.EventType.ActionFailed
          else OpenIvmInsightsContract.EventType.ActionCompleted,
        level = level,
        code = code,
        message = message,
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(status),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            (Seq(
              "operation_id"          -> operationId,
              "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
              "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
            ) ++ details): _*
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  private def linkDetails(spark: SparkSession, operationId: String): String =
    OpenIvmInsightJson.obj(
      "operation_id"          -> operationId,
      "request_id"            -> OpenIvmInsightsBroker.currentRequestId(spark),
      "query_logging_enabled" -> FeatureGate.queryLogEnabled(spark)
    )
}
