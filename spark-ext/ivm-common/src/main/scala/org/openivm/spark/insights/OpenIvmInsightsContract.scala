package org.openivm.spark.insights

import java.sql.Timestamp

/** Public, versioned contract for request-scoped OpenIVM SQL insights. */
object OpenIvmInsightsContract {

  val SchemaId: String   = "openivm.insights"
  val SchemaVersion: Int = 1

  val ConfigPrefix: String      = "spark.openivm.insights."
  val RequestIdProperty: String = "openivm.request_id"
  val RunIdProperty: String     = "openivm.run_id"
  val DbtNodeIdProperty: String = "openivm.node_id"

  val MaxCapturesKey: String         = ConfigPrefix + "maxCaptures"
  val MaxEventsPerCaptureKey: String = ConfigPrefix + "maxEventsPerCapture"
  val MaxBytesPerCaptureKey: String  = ConfigPrefix + "maxBytesPerCapture"
  val MaxTotalEventsKey: String      = ConfigPrefix + "maxTotalEvents"
  val MaxTotalBytesKey: String       = ConfigPrefix + "maxTotalBytes"
  val MaxDetailsBytesKey: String     = ConfigPrefix + "maxDetailsBytes"
  val MaxMessageBytesKey: String     = ConfigPrefix + "maxMessageBytes"
  val MaxFieldBytesKey: String       = ConfigPrefix + "maxFieldBytes"

  val DefaultMaxCaptures: Long         = 1024L
  val DefaultMaxEventsPerCapture: Long = 4096L
  val DefaultMaxBytesPerCapture: Long  = 8L * 1024 * 1024
  val DefaultMaxTotalEvents: Long      = 65536L
  val DefaultMaxTotalBytes: Long       = 64L * 1024 * 1024
  val DefaultMaxDetailsBytes: Long     = 32L * 1024
  val DefaultMaxMessageBytes: Long     = 2048L
  val DefaultMaxFieldBytes: Long       = 4096L
  val MaxSafeIdentityBytes: Long       = 256L

  object DetailField {
    val BranchCode: String                     = "branch_code"
    val ExecutionMode: String                  = "execution_mode"
    val Materialization: String                = "materialization"
    val TargetRelation: String                 = "target_relation"
    val ErrorClass: String                     = "error_class"
    val ErrorCode: String                      = "error_code"
    val Metrics: String                        = "metrics"
    val SourceRelation: String                 = "source_relation"
    val OperationTypes: String                 = "operation_types"
    val DependentMaterializedViews: String     = "dependent_materialized_views"
    val DependentMaterializedViewCount: String = "dependent_materialized_view_count"
    val SignedChanges: String                  = "signed_changes"
    val StagingArtifacts: String               = "staging_artifacts"
    val OldQueryHash: String                   = "old_query_hash"
    val NewQueryHash: String                   = "new_query_hash"
    val Policy: String                         = "policy"
    val Decision: String                       = "decision"
    val Reason: String                         = "reason"
  }

  object MetricField {
    val WallDurationMs: String      = "wall_duration_ms"
    val JobCount: String            = "job_count"
    val StageCount: String          = "stage_count"
    val TaskCount: String           = "task_count"
    val InputRecords: String        = "input_records"
    val InputBytes: String          = "input_bytes"
    val OutputRecords: String       = "output_records"
    val OutputBytes: String         = "output_bytes"
    val ShuffleReadRecords: String  = "shuffle_read_records"
    val ShuffleReadBytes: String    = "shuffle_read_bytes"
    val ShuffleWriteRecords: String = "shuffle_write_records"
    val ShuffleWriteBytes: String   = "shuffle_write_bytes"
    val MemorySpillBytes: String    = "memory_spill_bytes"
    val DiskSpillBytes: String      = "disk_spill_bytes"
    val FilesRead: String           = "files_read"
    val FilesWritten: String        = "files_written"

    val Values: Set[String] = Set(
      WallDurationMs,
      JobCount,
      StageCount,
      TaskCount,
      InputRecords,
      InputBytes,
      OutputRecords,
      OutputBytes,
      ShuffleReadRecords,
      ShuffleReadBytes,
      ShuffleWriteRecords,
      ShuffleWriteBytes,
      MemorySpillBytes,
      DiskSpillBytes,
      FilesRead,
      FilesWritten
    )
  }

  object Materialization {
    val StreamingTable: String   = "streaming_table"
    val MaterializedView: String = "materialized_view"
  }

  object ExecutionMode {
    val Streaming: String        = "streaming"
    val MaterializedView: String = "materialized_view"
    val RegularSpark: String     = "regular_spark"

    val Values: Set[String] = Set(Streaming, MaterializedView, RegularSpark)

    def forMaterialization(materialization: String): String = {
      val normalized =
        Option(materialization).map(_.toLowerCase(java.util.Locale.ROOT)).getOrElse("")
      if (normalized == Materialization.StreamingTable || normalized == Streaming) Streaming
      else if (normalized == Materialization.MaterializedView) MaterializedView
      else RegularSpark
    }
  }

  object BranchCode {
    val S1: String = "S1"
    val S2: String = "S2"
    val S3: String = "S3"
    val S4: String = "S4"
    val S5: String = "S5"
    val S6: String = "S6"
    val S7: String = "S7"
    val S8: String = "S8"

    val M1: String = "M1"
    val M2: String = "M2"
    val M3: String = "M3"
    val M4: String = "M4"
    val M5: String = "M5"
    val M6: String = "M6"
    val M7: String = "M7"
    val M8: String = "M8"

    val R1: String = "R1"
    val R2: String = "R2"
    val R3: String = "R3"

    val Values: Set[String] =
      Set(S1, S2, S3, S4, S5, S6, S7, S8, M1, M2, M3, M4, M5, M6, M7, M8, R1, R2, R3)

    val Labels: Map[String, String] = Map(
      S1 -> "NEW_STREAM",
      S2 -> "SAME_DEFINITION_ACTIVE",
      S3 -> "SAME_DEFINITION_RESUME",
      S4 -> "OPERATIONAL_TUNING_RESTART",
      S5 -> "SEMANTIC_CHANGE_REBUILD_CASCADE",
      S6 -> "SEMANTIC_CHANGE_REJECTED",
      S7 -> "REBUILD_RECOVERY",
      S8 -> "EXPLICIT_DROP_CASCADE",
      M1 -> "NEW_MV_INCREMENTAL",
      M2 -> "INCREMENTAL_REFRESH",
      M3 -> "NO_PENDING_DELTAS",
      M4 -> "NATIVE_FULL_REFRESH",
      M5 -> "DEMOTED_TO_FULL_REFRESH",
      M6 -> "SIGNED_DELTA_RECOMPUTE",
      M7 -> "QUERY_CHANGE_REBUILD_CASCADE",
      M8 -> "QUERY_CHANGE_REJECTED",
      R1 -> "REGULAR_SPARK",
      R2 -> "REGULAR_SPARK_WITH_OPENIVM_CHANGE_CAPTURE",
      R3 -> "REGULAR_SPARK_FAILED"
    )
  }

  object RecordType {
    val Status: String = "status"
    val Event: String  = "event"
  }

  object CaptureStatus {
    val Running: String  = "running"
    val Complete: String = "complete"
    val Failed: String   = "failed"
    val Degraded: String = "degraded"
    val Missing: String  = "missing"
  }

  object RequestStatus {
    val Succeeded: String = "succeeded"
    val Failed: String    = "failed"
  }

  object Operation {
    val Request: String      = "request"
    val Preflight: String    = "preflight"
    val Create: String       = "create"
    val Refresh: String      = "refresh"
    val Drop: String         = "drop"
    val Streaming: String    = "streaming"
    val RegularSpark: String = "regular_spark"

    val Values: Set[String] =
      Set(Request, Preflight, Create, Refresh, Drop, Streaming, RegularSpark)
  }

  object EventType {
    val RequestStarted: String         = "request.started"
    val RequestCompleted: String       = "request.completed"
    val RequestFailed: String          = "request.failed"
    val OperationStarted: String       = "operation.started"
    val OperationCompleted: String     = "operation.completed"
    val OperationFailed: String        = "operation.failed"
    val LifecycleStepCompleted: String = "lifecycle.step.completed"
    val LifecycleStepFailed: String    = "lifecycle.step.failed"
    val DecisionCompleted: String      = "decision.completed"
    val ObservationCompleted: String   = "observation.completed"
    val ActionCompleted: String        = "action.completed"
    val ActionFailed: String           = "action.failed"

    val Values: Set[String] = Set(
      RequestStarted,
      RequestCompleted,
      RequestFailed,
      OperationStarted,
      OperationCompleted,
      OperationFailed,
      LifecycleStepCompleted,
      LifecycleStepFailed,
      DecisionCompleted,
      ObservationCompleted,
      ActionCompleted,
      ActionFailed
    )
  }

  object Level {
    val Info: String  = "info"
    val Warn: String  = "warn"
    val Error: String = "error"

    val Values: Set[String] = Set(Info, Warn, Error)
  }

  object Code {
    val RequestStarted: String                = "request_started"
    val RequestSucceeded: String              = "request_succeeded"
    val RequestFailed: String                 = "request_failed"
    val CaptureMissing: String                = "capture_missing"
    val OperationStarted: String              = "operation_started"
    val OperationCompleted: String            = "operation_completed"
    val OperationFailed: String               = "operation_failed"
    val OperationCompletedWarning: String     = "operation_completed_with_warning"
    val LifecycleStepCompleted: String        = "lifecycle_step_completed"
    val LifecycleStepFailed: String           = "lifecycle_step_failed"
    val ClassificationCompleted: String       = "classification_completed"
    val SourceVersionsObserved: String        = "source_versions_observed"
    val PendingDeltasObserved: String         = "pending_deltas_observed"
    val RefreshRouteSelected: String          = "refresh_route_selected"
    val QueryBodyObserved: String             = "query_body_observed"
    val QueryLogActionObserved: String        = "query_log_action_observed"
    val DropCascadePlan: String               = "drop_cascade_plan"
    val DropDependentStarted: String          = "drop_dependent_started"
    val DropDependentCompleted: String        = "drop_dependent_completed"
    val DropDependentArchiveCompleted: String = "drop_dependent_archive_completed"
    val PublicViewRemoved: String             = "public_view_removed"
    val BackingTableRemoved: String           = "backing_table_removed"
    val MaterializedTableRemoved: String      = "materialized_table_removed"
    val StoragePathRemoved: String            = "storage_path_removed"
    val ChangePropagationRowsRemoved: String  = "change_propagation_rows_removed"
    val CdfWatermarkStateRemoved: String      = "cdf_watermark_state_removed"
    val ViewDeltaNamespaceRemoved: String     = "view_delta_namespace_removed"
    val ViewDeltaNamespaceCleanupFailed: String =
      "view_delta_namespace_cleanup_failed"
    val CatalogEntryRemoved: String = "catalog_entry_removed"

    val StreamingCascadePlan: String                = "streaming_cascade_plan"
    val StreamingRecoveryStateObserved: String      = "streaming_recovery_state_observed"
    val StreamingTargetCreated: String              = "streaming_target_created"
    val StreamingTargetReused: String               = "streaming_target_reused"
    val StreamingTargetRecreated: String            = "streaming_target_recreated"
    val StreamingTargetDropped: String              = "streaming_target_dropped"
    val StreamingCheckpointArchived: String         = "streaming_checkpoint_archived"
    val StreamingQueryStarted: String               = "streaming_query_started"
    val StreamingQueryResumed: String               = "streaming_query_resumed"
    val StreamingQueryRestarted: String             = "streaming_query_restarted"
    val StreamingQueryStopped: String               = "streaming_query_stopped"
    val StreamingQueryStopTimeout: String           = "streaming_query_stop_timeout"
    val StreamingQueryStopFailed: String            = "streaming_query_stop_failed"
    val StreamingQueryStopRejected: String          = "streaming_query_stop_rejected"
    val StreamingQueryStopInterrupted: String       = "streaming_query_stop_interrupted"
    val StreamingLifecycleLockTimeout: String       = "streaming_lifecycle_lock_timeout"
    val StreamingQueryCancellationFailed: String    = "streaming_query_cancellation_failed"
    val StreamingQueryFirstProgressTimeout: String  = "streaming_query_first_progress_timeout"
    val StreamingDescendantCleanupStarted: String   = "streaming_descendant_cleanup_started"
    val StreamingDescendantCleanupCompleted: String = "streaming_descendant_cleanup_completed"
    val StreamingDescendantCleanupFailed: String    = "streaming_descendant_cleanup_failed"
    val StreamingDescendantArchiveCompleted: String = "streaming_descendant_archive_completed"

    val SourceDeltaConsumed: String          = "source_delta_consumed"
    val FullRecomputeApply: String           = "full_recompute_apply"
    val IncrementalProgramStatement: String  = "incremental_program_statement"
    val SourceChangesMaterialized: String    = "source_changes_materialized"
    val ZeroMultiplicityCleanup: String      = "zero_multiplicity_cleanup"
    val InitialMaterialization: String       = "initial_materialization"
    val CatalogPublication: String           = "catalog_publication"
    val PublicViewPublication: String        = "public_view_publication"
    val CascadeDeltaMaterialized: String     = "cascade_delta_materialized"
    val PostRefreshCleanup: String           = "post_refresh_cleanup"
    val ExplainPlanObserved: String          = "explain_plan_observed"
    val MvQueryHashDecisionAnnotated: String = "mv_query_hash_decision_annotated"
    val RegularSparkCompleted: String        = "regular_spark_completed"
    val RegularSparkFailed: String           = "regular_spark_failed"
    val RegularSparkChangeCapture: String    = "regular_spark_change_capture"
  }

  object FailureCode {
    val CaptureExists: String         = "INSIGHTS_CAPTURE_EXISTS"
    val CaptureLimitExceeded: String  = "INSIGHTS_CAPTURE_LIMIT_EXCEEDED"
    val CaptureMissing: String        = "INSIGHTS_CAPTURE_MISSING"
    val CaptureNotTerminal: String    = "INSIGHTS_CAPTURE_NOT_TERMINAL"
    val EndStatusConflict: String     = "INSIGHTS_END_STATUS_CONFLICT"
    val ConfigInvalid: String         = "INSIGHTS_CONFIG_INVALID"
    val EventLimitExceeded: String    = "INSIGHTS_EVENT_LIMIT_EXCEEDED"
    val ByteLimitExceeded: String     = "INSIGHTS_BYTE_LIMIT_EXCEEDED"
    val EventAfterEnd: String         = "INSIGHTS_EVENT_AFTER_END"
    val EmitFailed: String            = "INSIGHTS_EMIT_FAILED"
    val ActiveCaptureRequired: String = "INSIGHTS_ACTIVE_CAPTURE_REQUIRED"
    val AnnotationContextMismatch: String =
      "INSIGHTS_ANNOTATION_CONTEXT_MISMATCH"
  }

  val QueryLogCategoryCodes: Map[String, String] = Map(
    "drop_cleanup"            -> Code.SourceDeltaConsumed,
    "full_refresh_stmt"       -> Code.FullRecomputeApply,
    "rewritten_stmt"          -> Code.IncrementalProgramStatement,
    "register_source_delta"   -> Code.SourceChangesMaterialized,
    "count_monoid_cleanup"    -> Code.ZeroMultiplicityCleanup,
    "initial_load_ctas"       -> Code.InitialMaterialization,
    "catalog_registration"    -> Code.CatalogPublication,
    "backing_user_view"       -> Code.PublicViewPublication,
    "full_refresh_cascade"    -> Code.CascadeDeltaMaterialized,
    "fused_view_delta_select" -> Code.CascadeDeltaMaterialized,
    "post_cleanup_stage"      -> Code.PostRefreshCleanup,
    "explain_formatted"       -> Code.ExplainPlanObserved,
    "original_query"          -> Code.QueryBodyObserved
  )

  def queryLogCode(category: String): String =
    QueryLogCategoryCodes.getOrElse(
      Option(category).map(_.toLowerCase(java.util.Locale.ROOT)).getOrElse(""),
      Code.QueryLogActionObserved
    )

  final case class OutputColumn(name: String, sqlType: String, nullable: Boolean)

  final case class ModelContext(
      executionMode: String,
      materialization: String,
      targetRelation: String
  )

  object ModelContext {
    def fromMaterialization(materialization: String, targetRelation: String): ModelContext =
      ModelContext(
        ExecutionMode.forMaterialization(materialization),
        materialization,
        targetRelation
      )
  }

  final case class FailureIdentity(errorClass: String, errorCode: String)

  final case class MvQueryHashDecision(
      targetRelation: String,
      oldQueryHash: String,
      newQueryHash: String,
      policy: String,
      decision: String,
      reason: String
  )

  object MvQueryHashDecision {
    val RebuildPolicy: String = "rebuild"
    val FailPolicy: String    = "fail"

    val RebuildDecision: String  = "rebuild"
    val RejectedDecision: String = "rejected"

    val MissingHash: String = "missing"
    val InvalidHash: String = "invalid"
  }

  val OutputColumns: Seq[OutputColumn] = Seq(
    OutputColumn("record_type", "STRING", nullable = false),
    OutputColumn("request_id", "STRING", nullable = false),
    OutputColumn("run_id", "STRING", nullable = true),
    OutputColumn("dbt_node_id", "STRING", nullable = true),
    OutputColumn("capture_status", "STRING", nullable = false),
    OutputColumn("next_sequence", "BIGINT", nullable = false),
    OutputColumn("has_more", "BOOLEAN", nullable = false),
    OutputColumn("sequence", "BIGINT", nullable = true),
    OutputColumn("event_timestamp", "TIMESTAMP", nullable = true),
    OutputColumn("operation_id", "STRING", nullable = true),
    OutputColumn("materialized_view", "STRING", nullable = true),
    OutputColumn("operation", "STRING", nullable = true),
    OutputColumn("stage", "STRING", nullable = true),
    OutputColumn("event_type", "STRING", nullable = true),
    OutputColumn("level", "STRING", nullable = true),
    OutputColumn("code", "STRING", nullable = true),
    OutputColumn("message", "STRING", nullable = true),
    OutputColumn("status", "STRING", nullable = true),
    OutputColumn("duration_ms", "BIGINT", nullable = true),
    OutputColumn("parent_sequence", "BIGINT", nullable = true),
    OutputColumn("details_json", "STRING", nullable = true),
    OutputColumn("terminal", "BOOLEAN", nullable = true)
  )

  final case class EventDraft(
      operation: String,
      stage: String,
      eventType: String,
      level: String,
      code: String,
      message: String,
      operationId: Option[String] = None,
      materializedView: Option[String] = None,
      status: Option[String] = None,
      durationMs: Option[Long] = None,
      parentSequence: Option[Long] = None,
      detailsJson: Option[String] = None,
      terminal: Option[Boolean] = None
  )

  final case class EventRecord(
      sequence: Long,
      eventTimestamp: Timestamp,
      operationId: Option[String],
      materializedView: Option[String],
      operation: String,
      stage: String,
      eventType: String,
      level: String,
      code: String,
      message: String,
      status: Option[String],
      durationMs: Option[Long],
      parentSequence: Option[Long],
      detailsJson: Option[String],
      terminal: Option[Boolean]
  )

  final case class StatusRecord(
      requestId: String,
      runId: Option[String],
      dbtNodeId: Option[String],
      captureStatus: String,
      requestStatus: Option[String],
      code: Option[String],
      message: Option[String],
      terminal: Boolean
  )

  final case class Page(
      status: StatusRecord,
      nextSequence: Long,
      hasMore: Boolean,
      events: Vector[EventRecord]
  )
}
