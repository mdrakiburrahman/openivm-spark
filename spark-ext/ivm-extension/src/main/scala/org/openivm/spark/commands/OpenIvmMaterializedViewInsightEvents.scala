package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.RefreshTypeCode
import org.openivm.spark.insights.{OpenIvmInsightJson, OpenIvmInsightsBroker, OpenIvmInsightsContract}
import org.openivm.spark.telemetry.OpenIvmTelemetryContract

private[spark] object OpenIvmMaterializedViewInsightEvents {

  final case class Classification(
      compileRefreshType: String,
      effectiveRefreshType: String,
      reason: String
  )

  final case class Branch(code: String, reason: String)

  def createCompleted(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      classification: Classification
  ): Unit = {
    val branch = OpenIvmInsightsBroker
      .currentMvQueryHashDecision(spark)
      .filter(_.targetRelation.equalsIgnoreCase(materializedView))
      .filter(annotation =>
        annotation.policy == OpenIvmInsightsContract.MvQueryHashDecision.RebuildPolicy &&
          annotation.decision == OpenIvmInsightsContract.MvQueryHashDecision.RebuildDecision
      )
      .map(annotation => Branch(OpenIvmInsightsContract.BranchCode.M7, annotation.reason))
      .getOrElse(branchForClassification(OpenIvmInsightsContract.Operation.Create, classification))
    emit(
      spark,
      operationId,
      materializedView,
      OpenIvmInsightsContract.Operation.Create,
      outcome = "create_executed",
      branch,
      classification
    )
  }

  def refreshCompleted(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      outcome: String,
      classification: Classification
  ): Unit =
    branchForRefreshOutcome(outcome, classification).foreach { branch =>
      emit(
        spark,
        operationId,
        materializedView,
        OpenIvmInsightsContract.Operation.Refresh,
        outcome,
        branch,
        classification
      )
    }

  private[commands] def branchForClassification(
      operation: String,
      classification: Classification
  ): Branch = {
    val compileType   = normalized(classification.compileRefreshType)
    val effectiveType = normalized(classification.effectiveRefreshType)
    val code =
      if (effectiveType == RefreshTypeCode.SignedDeltaRecomputeName)
        OpenIvmInsightsContract.BranchCode.M6
      else if (isFullRefresh(effectiveType)) {
        if (isFullRefresh(compileType)) OpenIvmInsightsContract.BranchCode.M4
        else OpenIvmInsightsContract.BranchCode.M5
      } else if (operation == OpenIvmInsightsContract.Operation.Create)
        OpenIvmInsightsContract.BranchCode.M1
      else
        OpenIvmInsightsContract.BranchCode.M2
    Branch(code, classification.reason)
  }

  private[commands] def branchForRefreshOutcome(
      outcome: String,
      classification: Classification
  ): Option[Branch] =
    if (!OpenIvmTelemetryContract.RefreshSuccessOutcomes.contains(outcome)) None
    else if (outcome == "no_pending_deltas" || outcome == "source_versions_already_applied")
      Some(Branch(OpenIvmInsightsContract.BranchCode.M3, outcome))
    else
      Some(branchForClassification(OpenIvmInsightsContract.Operation.Refresh, classification))

  def dropReason(spark: SparkSession, materializedView: String): String =
    OpenIvmInsightsBroker
      .currentMvQueryHashDecision(spark)
      .filter(_.targetRelation.equalsIgnoreCase(materializedView))
      .map(_.reason)
      .getOrElse("explicit_drop")

  private def emit(
      spark: SparkSession,
      operationId: String,
      materializedView: String,
      operation: String,
      outcome: String,
      branch: Branch,
      classification: Classification
  ): Unit = {
    OpenIvmInsightsBroker.emit(spark) {
      OpenIvmInsightsContract.EventDraft(
        operation = operation,
        stage = "decision",
        eventType = OpenIvmInsightsContract.EventType.DecisionCompleted,
        level =
          if (branch.code == OpenIvmInsightsContract.BranchCode.M5)
            OpenIvmInsightsContract.Level.Warn
          else OpenIvmInsightsContract.Level.Info,
        code = branch.code,
        message = s"Selected materialized-view branch ${OpenIvmInsightsContract.BranchCode.Labels(branch.code)}.",
        operationId = Some(operationId),
        materializedView = Some(materializedView),
        status = Some(OpenIvmInsightsContract.BranchCode.Labels(branch.code)),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "operation_id" -> operationId,
            "request_id"   -> OpenIvmInsightsBroker.currentRequestId(spark),
            OpenIvmInsightsContract.DetailField.BranchCode ->
              branch.code,
            "branch_label" -> OpenIvmInsightsContract.BranchCode.Labels(branch.code),
            OpenIvmInsightsContract.DetailField.ExecutionMode ->
              OpenIvmInsightsContract.ExecutionMode.MaterializedView,
            OpenIvmInsightsContract.DetailField.Materialization ->
              OpenIvmInsightsContract.Materialization.MaterializedView,
            OpenIvmInsightsContract.DetailField.TargetRelation ->
              materializedView,
            OpenIvmInsightsContract.DetailField.Reason -> branch.reason,
            "compile_refresh_type"                     -> classification.compileRefreshType,
            "effective_refresh_type"                   -> classification.effectiveRefreshType,
            "outcome"                                  -> outcome
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  private def normalized(value: String): String =
    Option(value).map(_.trim.toUpperCase(java.util.Locale.ROOT)).getOrElse("")

  private def isFullRefresh(value: String): Boolean =
    value == "FULL" || value == RefreshTypeCode.FullRefreshName
}
