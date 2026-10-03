package org.openivm.spark.insights

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.insights.OpenIvmInsightsContract.{
  BranchCode,
  CaptureStatus,
  Code,
  DetailField,
  EventDraft,
  EventRecord,
  EventType,
  ExecutionMode,
  FailureCode,
  FailureIdentity,
  Level,
  Materialization,
  ModelContext,
  MvQueryHashDecision,
  Operation,
  Page,
  RequestStatus,
  StatusRecord
}
import org.openivm.spark.insights.OpenIvmRequestMetricsCollector.RequestMetrics

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.control.NonFatal

/** Bounded, application/request-scoped, driver-memory broker for SQL insights. */
object OpenIvmInsightsBroker {

  private val Applications = new ConcurrentHashMap[String, Application]()
  private val Mapper       = new ObjectMapper()

  private val ForbiddenDetailKeys = Set(
    "query",
    "query_sql",
    "raw_sql",
    "sql",
    "sql_text",
    "statement_sql",
    "statement_text"
  )

  private val SqlLike =
    """(?is).*\b(select|insert|update|delete|merge|create|drop|alter|truncate|replace|with)\b\s+.*""".r
  private val SafeIdentity = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:$-]*")
  private val Md5Hash      = Pattern.compile("(?i)[0-9a-f]{32}")
  private val ManagedOperations =
    Set(Operation.Preflight, Operation.Create, Operation.Refresh, Operation.Drop, Operation.Streaming)
  private val ListenerDrainTimeoutMs = 5000L

  private final case class Limits(
      captures: Int,
      eventsPerCapture: Int,
      bytesPerCapture: Long,
      totalEvents: Long,
      totalBytes: Long,
      detailsBytes: Long,
      messageBytes: Long,
      fieldBytes: Long
  )

  private final case class Degradation(code: String, message: String)

  private final class BrokerException(val code: String, val safeMessage: String)
      extends IllegalArgumentException(s"$code: $safeMessage")

  private final class Application(
      val limits: Limits,
      val requestMetrics: OpenIvmRequestMetricsCollector
  ) {
    val captures: mutable.Map[String, Capture] = mutable.HashMap.empty
    var retainedEvents: Long                   = 0L
    var retainedBytes: Long                    = 0L
  }

  private final class Capture(
      val requestId: String,
      val runId: String,
      val dbtNodeId: String,
      val modelContext: Option[ModelContext],
      val previousProperties: Map[String, Option[String]]
  ) {
    val events: mutable.ArrayBuffer[EventRecord]         = mutable.ArrayBuffer.empty
    val operationParents: mutable.Map[String, Long]      = mutable.HashMap.empty
    var nextSequence: Long                               = 0L
    var retainedBytes: Long                              = 0L
    var requestStatus: Option[String]                    = None
    var degradation: Option[Degradation]                 = None
    var mvQueryHashDecision: Option[MvQueryHashDecision] = None
    var managedOperationOwned: Boolean                   = false
    var regularSparkChangeCapture: Boolean               = false
  }

  def apiVersion(): Int = OpenIvmInsightsContract.SchemaVersion

  def begin(
      spark: SparkSession,
      requestId: String,
      runId: String,
      dbtNodeId: String
  ): Unit =
    begin(spark, requestId, runId, dbtNodeId, None, None)

  def begin(
      spark: SparkSession,
      requestId: String,
      runId: String,
      dbtNodeId: String,
      materialization: Option[String],
      targetRelation: Option[String]
  ): Unit = {
    requireIdentity("request-id", requestId)
    requireIdentity("run-id", runId)
    requireIdentity("dbt-node-id", dbtNodeId)
    val modelContext = validateModelContext(materialization, targetRelation)

    val context = spark.sparkContext
    val app     = application(spark)
    val previous = Map(
      OpenIvmInsightsContract.RequestIdProperty -> Option(
        context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty)
      ),
      OpenIvmInsightsContract.RunIdProperty -> Option(
        context.getLocalProperty(OpenIvmInsightsContract.RunIdProperty)
      ),
      OpenIvmInsightsContract.DbtNodeIdProperty -> Option(
        context.getLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty)
      )
    )
    val capture = app.synchronized {
      requireCondition(
        !app.captures.contains(requestId),
        FailureCode.CaptureExists,
        s"request '$requestId' is already reserved"
      )
      requireCondition(
        app.captures.size < app.limits.captures,
        FailureCode.CaptureLimitExceeded,
        "release terminal insight captures before reserving another request"
      )
      val created = new Capture(requestId, runId, dbtNodeId, modelContext, previous)
      app.captures.put(requestId, created)
      created
    }
    app.requestMetrics.begin(requestId)

    try {
      context.setLocalProperty(OpenIvmInsightsContract.RequestIdProperty, requestId)
      context.setLocalProperty(OpenIvmInsightsContract.RunIdProperty, runId)
      context.setLocalProperty(OpenIvmInsightsContract.DbtNodeIdProperty, dbtNodeId)
    } catch {
      case NonFatal(error) =>
        app.synchronized {
          app.captures.remove(requestId)
          ()
        }
        app.requestMetrics.discard(requestId)
        restoreProperties(context, previous)
        throw error
    }

    emitForCapture(app, capture) {
      EventDraft(
        operation = Operation.Request,
        stage = "request",
        eventType = EventType.RequestStarted,
        level = Level.Info,
        code = Code.RequestStarted,
        message = "OpenIVM insight capture started.",
        status = Some(CaptureStatus.Running),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            (Seq(
              "schema_id"      -> OpenIvmInsightsContract.SchemaId,
              "schema_version" -> OpenIvmInsightsContract.SchemaVersion,
              "request_id"     -> requestId,
              "run_id"         -> runId,
              "dbt_node_id"    -> dbtNodeId
            ) ++ modelContextDetails(modelContext)): _*
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  def end(spark: SparkSession, requestId: String, succeeded: Boolean): Unit =
    end(spark, requestId, succeeded, None)

  def end(
      spark: SparkSession,
      requestId: String,
      succeeded: Boolean,
      failureIdentity: Option[FailureIdentity]
  ): Unit = {
    requireIdentity("request-id", requestId)
    val normalizedFailureIdentity = validateFailureIdentity(succeeded, failureIdentity)
    val context                   = spark.sparkContext
    val app = Option(Applications.get(context.applicationId)).getOrElse {
      throw explicit(
        FailureCode.CaptureMissing,
        s"no retained insight capture exists for request '$requestId'"
      )
    }
    val capture = app.synchronized {
      app.captures.getOrElse(
        requestId,
        throw explicit(
          FailureCode.CaptureMissing,
          s"no retained insight capture exists for request '$requestId'"
        )
      )
    }

    val requestedStatus = if (succeeded) RequestStatus.Succeeded else RequestStatus.Failed
    val shouldDrainMetrics = app.synchronized {
      capture.requestStatus match {
        case Some(existing) if existing != requestedStatus =>
          throw explicit(
            FailureCode.EndStatusConflict,
            s"request '$requestId' was already ended with status $existing"
          )
        case Some(_) => false
        case None    => !capture.managedOperationOwned
      }
    }
    if (shouldDrainMetrics)
      OpenIvmRequestMetricsCollector.awaitListenerDrain(context, ListenerDrainTimeoutMs)

    val restoreContext = app.synchronized {
      capture.requestStatus match {
        case Some(existing) if existing != requestedStatus =>
          throw explicit(
            FailureCode.EndStatusConflict,
            s"request '$requestId' was already ended with status $existing"
          )
        case Some(_) => false
        case None =>
          val collectRegularSpark = !capture.managedOperationOwned
          val metrics =
            if (collectRegularSpark) {
              app.requestMetrics.finish(requestId, System.currentTimeMillis())
            } else {
              app.requestMetrics.discard(requestId)
              RequestMetrics.empty
            }
          if (collectRegularSpark) {
            appendTerminalSafely(
              app,
              capture,
              regularSparkTerminalEvent(
                capture,
                succeeded,
                normalizedFailureIdentity,
                metrics
              )
            )
          }
          capture.requestStatus = Some(requestedStatus)
          appendTerminalSafely(
            app,
            capture,
            EventDraft(
              operation = Operation.Request,
              stage = "request",
              eventType = if (succeeded) EventType.RequestCompleted else EventType.RequestFailed,
              level = if (succeeded) Level.Info else Level.Error,
              code = if (succeeded) Code.RequestSucceeded else Code.RequestFailed,
              message =
                if (succeeded) "The outer SQL request succeeded."
                else "The outer SQL request failed.",
              status = Some(requestedStatus),
              detailsJson = Some(
                OpenIvmInsightJson.obj(
                  (Seq(
                    "request_id"  -> requestId,
                    "run_id"      -> capture.runId,
                    "dbt_node_id" -> capture.dbtNodeId
                  ) ++ modelContextDetails(capture.modelContext) ++ Seq(
                    DetailField.ErrorClass -> normalizedFailureIdentity.map(_.errorClass),
                    DetailField.ErrorCode  -> normalizedFailureIdentity.map(_.errorCode)
                  ) ++ regularSparkMetricsDetails(
                    collectRegularSpark && capture.regularSparkChangeCapture,
                    metrics
                  )): _*
                )
              ),
              terminal = Some(true)
            )
          )
          context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty) == requestId
      }
    }
    if (restoreContext) restoreProperties(context, capture.previousProperties)
  }

  def release(spark: SparkSession, requestId: String): Unit = {
    requireIdentity("request-id", requestId)
    Option(Applications.get(spark.sparkContext.applicationId)).foreach { app =>
      app.synchronized {
        app.captures.get(requestId).foreach { capture =>
          requireCondition(
            capture.requestStatus.nonEmpty,
            FailureCode.CaptureNotTerminal,
            s"request '$requestId' must be ended before release"
          )
          app.captures.remove(requestId)
          app.retainedEvents -= capture.events.size.toLong
          app.retainedBytes -= capture.retainedBytes
        }
      }
    }
  }

  def page(
      spark: SparkSession,
      requestId: String,
      afterSequence: Long,
      maxEvents: Int
  ): Page = {
    requireIdentity("request-id", requestId)
    requireCondition(
      afterSequence >= 0L,
      FailureCode.ConfigInvalid,
      "AFTER sequence must be nonnegative"
    )
    requireCondition(
      maxEvents > 0,
      FailureCode.ConfigInvalid,
      "LIMIT max-events must be positive"
    )

    Option(Applications.get(spark.sparkContext.applicationId))
      .flatMap { app =>
        app.synchronized {
          app.captures.get(requestId).map { capture =>
            val remaining = capture.events.iterator.filter(_.sequence > afterSequence).toVector
            val events    = remaining.take(maxEvents)
            val next      = events.lastOption.map(_.sequence).getOrElse(afterSequence)
            val captureStatus = capture.degradation match {
              case Some(_) => CaptureStatus.Degraded
              case None =>
                capture.requestStatus match {
                  case None                          => CaptureStatus.Running
                  case Some(RequestStatus.Failed)    => CaptureStatus.Failed
                  case Some(RequestStatus.Succeeded) => CaptureStatus.Complete
                  case Some(_)                       => CaptureStatus.Degraded
                }
            }
            val statusCode = capture.degradation.map(_.code).orElse {
              capture.requestStatus.collect { case RequestStatus.Failed => Code.RequestFailed }
            }
            val statusMessage = capture.degradation.map(_.message).orElse {
              capture.requestStatus.collect { case RequestStatus.Failed =>
                "The caller reported a failed outer SQL request."
              }
            }
            Page(
              StatusRecord(
                requestId = capture.requestId,
                runId = Some(capture.runId),
                dbtNodeId = Some(capture.dbtNodeId),
                captureStatus = captureStatus,
                requestStatus = capture.requestStatus,
                code = statusCode,
                message = statusMessage,
                terminal = capture.requestStatus.nonEmpty
              ),
              nextSequence = next,
              hasMore = remaining.size > events.size,
              events = events
            )
          }
        }
      }
      .getOrElse(missing(requestId, afterSequence))
  }

  /** Emit only when the calling thread has an active BEGIN request. */
  def emit(spark: SparkSession)(event: => EventDraft): Option[Long] = {
    if (spark == null) return None
    val context   = spark.sparkContext
    val requestId = context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty)
    if (requestId == null) return None
    val app = Applications.get(context.applicationId)
    if (app == null) return None
    val capture = app.synchronized {
      app.captures.get(requestId).filter(value => value.requestStatus.isEmpty && value.degradation.isEmpty)
    }
    capture.flatMap(value => emitForCapture(app, value)(event))
  }

  def hasActiveCapture(spark: SparkSession): Boolean = {
    if (spark == null) false
    else {
      val context   = spark.sparkContext
      val requestId = context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty)
      if (requestId == null) false
      else {
        val app = Applications.get(context.applicationId)
        app != null && app.synchronized {
          app.captures.get(requestId).exists(value => value.requestStatus.isEmpty && value.degradation.isEmpty)
        }
      }
    }
  }

  private[spark] def hasActiveRegularSparkCapture(spark: SparkSession): Boolean =
    currentCapture(spark).exists { case (app, capture) =>
      app.synchronized {
        !capture.managedOperationOwned && capture.degradation.isEmpty
      }
    }

  def currentRequestId(spark: SparkSession): Option[String] =
    if (spark == null) None
    else Option(spark.sparkContext.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty))

  def currentModelContext(spark: SparkSession): Option[ModelContext] =
    currentCapture(spark).flatMap { case (_, capture) => capture.modelContext }

  def currentMvQueryHashDecision(spark: SparkSession): Option[MvQueryHashDecision] =
    currentCapture(spark).flatMap { case (_, capture) => capture.mvQueryHashDecision }

  private[insights] def activeMetricRequestCount(spark: SparkSession): Int =
    if (spark == null) 0
    else
      Option(Applications.get(spark.sparkContext.applicationId))
        .map(_.requestMetrics.activeRequestCount)
        .getOrElse(0)

  def annotateMvQueryHashDecision(
      spark: SparkSession,
      annotation: MvQueryHashDecision
  ): Unit = {
    val normalized = validateMvQueryHashDecision(annotation)
    val (app, capture) = currentCapture(spark).getOrElse {
      throw explicit(
        FailureCode.ActiveCaptureRequired,
        "an active OpenIVM insight request is required for query-hash annotations"
      )
    }
    capture.modelContext.foreach { context =>
      requireCondition(
        context.executionMode == ExecutionMode.MaterializedView &&
          context.targetRelation.equalsIgnoreCase(normalized.targetRelation),
        FailureCode.AnnotationContextMismatch,
        "the query-hash annotation does not match the active materialized-view request"
      )
    }
    val branchCode = mvQueryHashBranchCode(normalized)
    app.synchronized {
      capture.mvQueryHashDecision = Some(normalized)
    }

    emitForCapture(app, capture) {
      EventDraft(
        operation = Operation.Create,
        stage = "query_hash",
        eventType = EventType.DecisionCompleted,
        level =
          if (branchCode == BranchCode.M8) Level.Warn
          else Level.Info,
        code = Code.MvQueryHashDecisionAnnotated,
        message = "Materialized-view query-hash decision recorded.",
        materializedView = Some(normalized.targetRelation),
        status = Some(normalized.decision),
        detailsJson = Some(
          OpenIvmInsightJson.obj(
            "request_id"              -> capture.requestId,
            "run_id"                  -> capture.runId,
            "dbt_node_id"             -> capture.dbtNodeId,
            DetailField.ExecutionMode -> ExecutionMode.MaterializedView,
            DetailField.Materialization -> capture.modelContext
              .map(_.materialization)
              .getOrElse(Materialization.MaterializedView),
            DetailField.TargetRelation -> normalized.targetRelation,
            DetailField.OldQueryHash   -> normalized.oldQueryHash,
            DetailField.NewQueryHash   -> normalized.newQueryHash,
            DetailField.Policy         -> normalized.policy,
            DetailField.Decision       -> normalized.decision,
            DetailField.Reason         -> normalized.reason,
            DetailField.BranchCode     -> branchCode
          )
        ),
        terminal = Some(false)
      )
    }
    ()
  }

  /** Profile details are preserved unless they look like submitted SQL. */
  def safeDiagnosticDetail(value: String): (String, Boolean) = {
    val normalized = Option(value).getOrElse("")
    if (
      normalized.toLowerCase(java.util.Locale.ROOT).contains("rewritten sql:") ||
      SqlLike.pattern.matcher(normalized).matches()
    ) ("[redacted sql-like detail]", true)
    else (normalized, false)
  }

  private def currentCapture(spark: SparkSession): Option[(Application, Capture)] =
    if (spark == null) None
    else {
      val context = spark.sparkContext
      Option(context.getLocalProperty(OpenIvmInsightsContract.RequestIdProperty)).flatMap { requestId =>
        Option(Applications.get(context.applicationId)).flatMap { app =>
          app.synchronized {
            app.captures
              .get(requestId)
              .filter(_.requestStatus.isEmpty)
              .map(capture => app -> capture)
          }
        }
      }
    }

  private def application(spark: SparkSession): Application = {
    val appId = spark.sparkContext.applicationId
    Applications.computeIfAbsent(
      appId,
      _ => {
        val requestMetrics = new OpenIvmRequestMetricsCollector
        val created        = new Application(limits(spark.sparkContext.getConf), requestMetrics)
        spark.sparkContext.addSparkListener(requestMetrics)
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
            Applications.remove(appId, created)
            ()
          }
        })
        created
      }
    )
  }

  private def emitForCapture(
      app: Application,
      capture: Capture
  )(event: => EventDraft): Option[Long] =
    try {
      val draft = event
      app.synchronized {
        if (!app.captures.get(capture.requestId).contains(capture)) None
        else if (capture.requestStatus.nonEmpty) {
          degrade(
            capture,
            FailureCode.EventAfterEnd,
            "An insight event was emitted after the request ended."
          )
          None
        } else {
          observeOperation(capture, draft)
          append(app, capture, draft)
        }
      }
    } catch {
      case error: BrokerException =>
        app.synchronized {
          if (app.captures.get(capture.requestId).contains(capture))
            degrade(capture, error.code, error.safeMessage)
        }
        None
      case NonFatal(error) =>
        app.synchronized {
          if (app.captures.get(capture.requestId).contains(capture))
            degrade(
              capture,
              FailureCode.EmitFailed,
              s"Insight event emission failed (${error.getClass.getSimpleName})."
            )
        }
        None
    }

  private def observeOperation(capture: Capture, draft: EventDraft): Unit = {
    if (ManagedOperations.contains(draft.operation)) capture.managedOperationOwned = true
    else if (draft.operation == Operation.RegularSpark && draft.code == Code.RegularSparkChangeCapture)
      capture.regularSparkChangeCapture = true
  }

  private def regularSparkTerminalEvent(
      capture: Capture,
      succeeded: Boolean,
      failureIdentity: Option[FailureIdentity],
      metrics: RequestMetrics
  ): EventDraft = {
    val branchCode = if (succeeded) BranchCode.R1 else BranchCode.R3
    EventDraft(
      operation = Operation.RegularSpark,
      stage = "execution",
      eventType = if (succeeded) EventType.OperationCompleted else EventType.OperationFailed,
      level = if (succeeded) Level.Info else Level.Error,
      code = if (succeeded) Code.RegularSparkCompleted else Code.RegularSparkFailed,
      message =
        if (succeeded) "Regular Spark execution completed."
        else "Regular Spark execution failed.",
      status = Some(if (succeeded) RequestStatus.Succeeded else RequestStatus.Failed),
      durationMs = metrics.wallDurationMs,
      detailsJson = Some(
        OpenIvmInsightJson.obj(
          (Seq(
            DetailField.BranchCode      -> branchCode,
            DetailField.ExecutionMode   -> ExecutionMode.RegularSpark,
            DetailField.Materialization -> capture.modelContext.map(_.materialization),
            DetailField.TargetRelation  -> capture.modelContext.map(_.targetRelation),
            DetailField.ErrorClass      -> failureIdentity.map(_.errorClass),
            DetailField.ErrorCode       -> failureIdentity.map(_.errorCode)
          ) ++ (if (metrics.nonEmpty) Seq(DetailField.Metrics -> metrics.details) else Seq.empty)): _*
        )
      ),
      terminal = Some(true)
    )
  }

  private def regularSparkMetricsDetails(
      include: Boolean,
      metrics: RequestMetrics
  ): Seq[(String, Any)] =
    if (include && metrics.nonEmpty) Seq(DetailField.Metrics -> metrics.details)
    else Seq.empty

  private def append(
      app: Application,
      capture: Capture,
      draft: EventDraft
  ): Option[Long] = {
    if (capture.degradation.nonEmpty) return None

    val normalized = normalize(draft, app.limits)
    if (
      capture.events.size >= app.limits.eventsPerCapture ||
      app.retainedEvents >= app.limits.totalEvents
    ) {
      degrade(
        capture,
        FailureCode.EventLimitExceeded,
        "Insight capture exceeded its configured event limit; later events were dropped."
      )
      return None
    }

    val sequence = capture.nextSequence + 1L
    val parent = normalized.parentSequence
      .orElse(normalized.operationId.flatMap(capture.operationParents.get))
      .filter(value => value > 0L && value < sequence)
    val record = EventRecord(
      sequence = sequence,
      eventTimestamp = new Timestamp(System.currentTimeMillis()),
      operationId = normalized.operationId,
      materializedView = normalized.materializedView,
      operation = normalized.operation,
      stage = normalized.stage,
      eventType = normalized.eventType,
      level = normalized.level,
      code = normalized.code,
      message = normalized.message,
      status = normalized.status,
      durationMs = normalized.durationMs,
      parentSequence = parent,
      detailsJson = normalized.detailsJson,
      terminal = normalized.terminal
    )
    val bytes = eventBytes(record)
    if (
      bytes > app.limits.bytesPerCapture - capture.retainedBytes ||
      bytes > app.limits.totalBytes - app.retainedBytes
    ) {
      degrade(
        capture,
        FailureCode.ByteLimitExceeded,
        "Insight capture exceeded its configured UTF-8 byte limit; later events were dropped."
      )
      None
    } else {
      capture.nextSequence = sequence
      capture.events += record
      capture.retainedBytes += bytes
      app.retainedEvents += 1L
      app.retainedBytes += bytes
      if (record.eventType == EventType.OperationStarted)
        record.operationId.foreach(capture.operationParents.update(_, sequence))
      Some(sequence)
    }
  }

  private def appendTerminalSafely(
      app: Application,
      capture: Capture,
      draft: EventDraft
  ): Unit =
    try {
      append(app, capture, draft)
      ()
    } catch {
      case error: BrokerException =>
        degrade(capture, error.code, error.safeMessage)
      case NonFatal(error) =>
        degrade(
          capture,
          FailureCode.EmitFailed,
          s"Insight terminal event emission failed (${error.getClass.getSimpleName})."
        )
    }

  private def normalize(draft: EventDraft, limits: Limits): EventDraft = {
    requireCondition(
      Operation.Values.contains(draft.operation),
      FailureCode.EmitFailed,
      s"unsupported insight operation '${draft.operation}'"
    )
    requireCondition(
      EventType.Values.contains(draft.eventType),
      FailureCode.EmitFailed,
      s"unsupported insight event type '${draft.eventType}'"
    )
    requireCondition(
      Level.Values.contains(draft.level),
      FailureCode.EmitFailed,
      s"unsupported insight level '${draft.level}'"
    )
    val normalizedDetails = draft.detailsJson.map { raw =>
      requireCondition(
        utf8Size(raw) <= limits.detailsBytes,
        FailureCode.ByteLimitExceeded,
        "insight details_json exceeds the configured per-event limit"
      )
      val parsed = Mapper.readTree(raw)
      requireCondition(
        parsed != null && parsed.isObject,
        FailureCode.EmitFailed,
        "insight details_json must be a JSON object"
      )
      requireCondition(
        !containsForbiddenDetailKey(parsed),
        FailureCode.EmitFailed,
        "insight details_json contains a raw-SQL field"
      )
      Mapper.writeValueAsString(parsed)
    }
    draft.copy(
      operationId = boundedOptional("operation_id", draft.operationId, limits.fieldBytes),
      materializedView = boundedOptional("materialized_view", draft.materializedView, limits.fieldBytes),
      stage = boundedRequired("stage", draft.stage, limits.fieldBytes),
      code = boundedRequired("code", draft.code, limits.fieldBytes),
      message = truncateUtf8(Option(draft.message).getOrElse(""), limits.messageBytes),
      status = boundedOptional("status", draft.status, limits.fieldBytes),
      durationMs = draft.durationMs.map(math.max(0L, _)),
      detailsJson = normalizedDetails
    )
  }

  private def containsForbiddenDetailKey(node: JsonNode): Boolean = {
    if (node.isObject) {
      val fieldNames = node.fieldNames()
      while (fieldNames.hasNext) {
        val fieldName = fieldNames.next()
        if (
          ForbiddenDetailKeys.contains(fieldName.toLowerCase(java.util.Locale.ROOT)) ||
          containsForbiddenDetailKey(node.get(fieldName))
        ) return true
      }
      false
    } else if (node.isArray) {
      val values = node.elements()
      while (values.hasNext) {
        if (containsForbiddenDetailKey(values.next())) return true
      }
      false
    } else false
  }

  private def degrade(capture: Capture, code: String, message: String): Unit =
    if (capture.degradation.isEmpty)
      capture.degradation = Some(Degradation(code, truncateUtf8(message, 2048L)))

  private def missing(requestId: String, afterSequence: Long): Page =
    Page(
      StatusRecord(
        requestId = requestId,
        runId = None,
        dbtNodeId = None,
        captureStatus = CaptureStatus.Missing,
        requestStatus = None,
        code = Some(Code.CaptureMissing),
        message = Some("No retained OpenIVM insight capture exists for this request."),
        terminal = true
      ),
      nextSequence = afterSequence,
      hasMore = false,
      events = Vector.empty
    )

  private def limits(conf: SparkConf): Limits = {
    def positive(key: String, default: Long, max: Long = Long.MaxValue): Long = {
      val value =
        try conf.getLong(key, default)
        catch {
          case NonFatal(error) =>
            throw explicit(
              FailureCode.ConfigInvalid,
              s"$key is not a valid integer (${error.getClass.getSimpleName})"
            )
        }
      requireCondition(
        value > 0L && value <= max,
        FailureCode.ConfigInvalid,
        s"$key must be in [1, $max]"
      )
      value
    }

    Limits(
      captures = positive(
        OpenIvmInsightsContract.MaxCapturesKey,
        OpenIvmInsightsContract.DefaultMaxCaptures,
        Int.MaxValue.toLong
      ).toInt,
      eventsPerCapture = positive(
        OpenIvmInsightsContract.MaxEventsPerCaptureKey,
        OpenIvmInsightsContract.DefaultMaxEventsPerCapture,
        Int.MaxValue.toLong
      ).toInt,
      bytesPerCapture = positive(
        OpenIvmInsightsContract.MaxBytesPerCaptureKey,
        OpenIvmInsightsContract.DefaultMaxBytesPerCapture
      ),
      totalEvents = positive(
        OpenIvmInsightsContract.MaxTotalEventsKey,
        OpenIvmInsightsContract.DefaultMaxTotalEvents
      ),
      totalBytes = positive(
        OpenIvmInsightsContract.MaxTotalBytesKey,
        OpenIvmInsightsContract.DefaultMaxTotalBytes
      ),
      detailsBytes = positive(
        OpenIvmInsightsContract.MaxDetailsBytesKey,
        OpenIvmInsightsContract.DefaultMaxDetailsBytes
      ),
      messageBytes = positive(
        OpenIvmInsightsContract.MaxMessageBytesKey,
        OpenIvmInsightsContract.DefaultMaxMessageBytes
      ),
      fieldBytes = positive(
        OpenIvmInsightsContract.MaxFieldBytesKey,
        OpenIvmInsightsContract.DefaultMaxFieldBytes
      )
    )
  }

  private def restoreProperties(
      context: org.apache.spark.SparkContext,
      properties: Map[String, Option[String]]
  ): Unit =
    properties.foreach { case (key, value) =>
      context.setLocalProperty(key, value.orNull)
    }

  private def modelContextDetails(context: Option[ModelContext]): Seq[(String, Any)] =
    Seq(
      DetailField.ExecutionMode   -> context.map(_.executionMode),
      DetailField.Materialization -> context.map(_.materialization),
      DetailField.TargetRelation  -> context.map(_.targetRelation)
    )

  private def validateModelContext(
      materialization: Option[String],
      targetRelation: Option[String]
  ): Option[ModelContext] = {
    val safeMaterialization = Option(materialization).flatten
    val safeTargetRelation  = Option(targetRelation).flatten
    requireCondition(
      safeMaterialization.isDefined == safeTargetRelation.isDefined,
      FailureCode.ConfigInvalid,
      "MATERIALIZATION and TARGET must be supplied together"
    )
    safeMaterialization.map { value =>
      ModelContext.fromMaterialization(
        requireSafeIdentity(
          "materialization",
          value,
          OpenIvmInsightsContract.MaxSafeIdentityBytes
        ),
        requireSafeIdentity(
          "target relation",
          safeTargetRelation.get,
          OpenIvmInsightsContract.DefaultMaxFieldBytes
        )
      )
    }
  }

  private def validateFailureIdentity(
      succeeded: Boolean,
      failureIdentity: Option[FailureIdentity]
  ): Option[FailureIdentity] = {
    val supplied = Option(failureIdentity).flatten
    requireCondition(
      !succeeded || supplied.isEmpty,
      FailureCode.ConfigInvalid,
      "failure identity is valid only with STATUS FAILED"
    )
    supplied.map { identity =>
      FailureIdentity(
        requireSafeIdentity(
          "error class",
          identity.errorClass,
          OpenIvmInsightsContract.MaxSafeIdentityBytes
        ),
        requireSafeIdentity(
          "error code",
          identity.errorCode,
          OpenIvmInsightsContract.MaxSafeIdentityBytes
        )
      )
    }
  }

  private def validateMvQueryHashDecision(
      annotation: MvQueryHashDecision
  ): MvQueryHashDecision = {
    requireCondition(
      annotation != null,
      FailureCode.ConfigInvalid,
      "query-hash annotation must be provided"
    )
    val normalized = MvQueryHashDecision(
      targetRelation = requireSafeIdentity(
        "target relation",
        annotation.targetRelation,
        OpenIvmInsightsContract.DefaultMaxFieldBytes
      ),
      oldQueryHash = requireQueryHash(
        "old query hash",
        annotation.oldQueryHash,
        allowSentinel = true
      ),
      newQueryHash = requireQueryHash(
        "new query hash",
        annotation.newQueryHash,
        allowSentinel = false
      ),
      policy = requireSafeIdentity(
        "query-change policy",
        annotation.policy,
        OpenIvmInsightsContract.MaxSafeIdentityBytes
      ),
      decision = requireSafeIdentity(
        "query-change decision",
        annotation.decision,
        OpenIvmInsightsContract.MaxSafeIdentityBytes
      ),
      reason = requireSafeIdentity(
        "query-change reason",
        annotation.reason,
        OpenIvmInsightsContract.MaxSafeIdentityBytes
      )
    )
    mvQueryHashBranchCode(normalized)
    normalized
  }

  private def mvQueryHashBranchCode(annotation: MvQueryHashDecision): String =
    (annotation.policy, annotation.decision) match {
      case (
            OpenIvmInsightsContract.MvQueryHashDecision.RebuildPolicy,
            OpenIvmInsightsContract.MvQueryHashDecision.RebuildDecision
          ) =>
        BranchCode.M7
      case (
            OpenIvmInsightsContract.MvQueryHashDecision.FailPolicy,
            OpenIvmInsightsContract.MvQueryHashDecision.RejectedDecision
          ) =>
        BranchCode.M8
      case _ =>
        throw explicit(
          FailureCode.ConfigInvalid,
          "query-hash annotations require rebuild/rebuild or fail/rejected"
        )
    }

  private def requireQueryHash(label: String, value: String, allowSentinel: Boolean): String = {
    val normalized = Option(value).map(_.trim.toLowerCase(java.util.Locale.ROOT)).getOrElse("")
    val sentinel =
      allowSentinel &&
        Set(
          OpenIvmInsightsContract.MvQueryHashDecision.MissingHash,
          OpenIvmInsightsContract.MvQueryHashDecision.InvalidHash
        ).contains(normalized)
    requireCondition(
      sentinel || Md5Hash.matcher(normalized).matches(),
      FailureCode.ConfigInvalid,
      s"$label must be a 32-character hexadecimal MD5 value" +
        (if (allowSentinel) " or the typed missing/invalid sentinel" else "")
    )
    normalized
  }

  private def requireIdentity(label: String, value: String): Unit = {
    val bytes = utf8Size(Option(value).getOrElse(""))
    requireCondition(
      value != null && value.nonEmpty && bytes <= 4096L,
      FailureCode.ConfigInvalid,
      s"$label must be nonempty and at most 4096 UTF-8 bytes"
    )
  }

  private def requireSafeIdentity(label: String, value: String, maxBytes: Long): String = {
    requireCondition(
      value != null &&
        value.nonEmpty &&
        utf8Size(value) <= maxBytes &&
        SafeIdentity.matcher(value).matches(),
      FailureCode.ConfigInvalid,
      s"$label must be a nonempty safe identity of at most $maxBytes UTF-8 bytes"
    )
    value
  }

  private def boundedRequired(label: String, value: String, maxBytes: Long): String = {
    requireCondition(
      value != null && value.nonEmpty && utf8Size(value) <= maxBytes,
      FailureCode.EmitFailed,
      s"$label must be nonempty and at most $maxBytes UTF-8 bytes"
    )
    value
  }

  private def boundedOptional(
      label: String,
      value: Option[String],
      maxBytes: Long
  ): Option[String] =
    value.map { text =>
      requireCondition(
        text.nonEmpty && utf8Size(text) <= maxBytes,
        FailureCode.EmitFailed,
        s"$label must be nonempty and at most $maxBytes UTF-8 bytes"
      )
      text
    }

  private def eventBytes(event: EventRecord): Long =
    128L + Seq(
      event.operationId,
      event.materializedView,
      Some(event.operation),
      Some(event.stage),
      Some(event.eventType),
      Some(event.level),
      Some(event.code),
      Some(event.message),
      event.status,
      event.detailsJson
    ).flatten.map(utf8Size).sum

  private def utf8Size(value: String): Long =
    value.getBytes(UTF_8).length.toLong

  private def truncateUtf8(value: String, maxBytes: Long): String = {
    if (utf8Size(value) <= maxBytes) value
    else {
      val suffix = "…"
      val budget = math.max(0L, maxBytes - utf8Size(suffix))
      val out    = new StringBuilder
      var bytes  = 0L
      val chars  = value.codePoints().iterator()
      while (chars.hasNext) {
        val point = chars.nextInt()
        val text  = new String(Character.toChars(point))
        val size  = utf8Size(text)
        if (bytes + size > budget) return out.append(suffix).toString()
        out.append(text)
        bytes += size
      }
      out.toString()
    }
  }

  private def requireCondition(
      condition: Boolean,
      code: String,
      message: => String
  ): Unit =
    if (!condition) throw explicit(code, message)

  private def explicit(code: String, message: String): BrokerException =
    new BrokerException(code, message)
}
