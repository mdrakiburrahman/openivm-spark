package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, TimeTravelPinReason, TimeTravelPinStatus}
import org.slf4j.MDC

import java.time.Instant
import java.util.LinkedHashMap
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/** Structured one-line execution-span contract for CREATE / REFRESH MV work. */
final class OpenIvmExecutionSpan private[telemetry] (
    val materializedView: String,
    val operation: String,
    private val requestId: Option[String],
    private val dbtNodeId: Option[String],
    private val startedAtEpochMs: Long,
    private val startedAtNanos: Long,
    private val enabled: Boolean,
    private val configuredExport: Option[ConfiguredTelemetryExport],
    private val oneLakeSink: Option[SpanSink] = None
) {

  private val lock = new Object

  private var refreshClassification            = OpenIvmExecutionSpan.RefreshClassification.empty
  private var timeTravelPinStatus              = Option.empty[String]
  private var timeTravelPinReason              = Option.empty[String]
  private var sameMvLockWaitMs                 = Option.empty[Long]
  private var driverAdmissionMs                = Option.empty[Long]
  private var catalogPublicationAdmissionWidth = Option.empty[Int]
  private var catalogPublicationAdmissionMs    = Option.empty[Long]
  private var compilerMs                       = Option.empty[Long]
  private var catalogMs                        = Option.empty[Long]
  private var rocksDbWriteMs                   = Option.empty[Long]
  private var rocksDbWriteCount                = 0L
  private var analysisMs                       = Option.empty[Long]
  private var watermarkMs                      = Option.empty[Long]
  private var ctasMs                           = Option.empty[Long]
  private var ctasDataWriteMs                  = Option.empty[Long]
  private var hiveCatalogPublishMs             = Option.empty[Long]
  private var metadataPublicationMs            = Option.empty[Long]
  private var deltaVersionLookupMs             = Option.empty[Long]
  private var deltaVersionLookupCount          = 0L
  private var rocksDbFlushMs                   = Option.empty[Long]
  private var rocksDbFlushCount                = 0L
  private var rocksDbFlushFailures             = 0L
  private var rocksDbJvmLockWaitMs             = Option.empty[Long]
  private var rocksDbExternalWaitMs            = Option.empty[Long]
  private var rocksDbBackupMs                  = Option.empty[Long]
  private var sourceVersions                   = Map.empty[String, OpenIvmTelemetryContract.SourceVersion]
  private var pendingDeltaCount                = 0L
  private var completedAtEpochMs               = Option.empty[Long]
  private var completedAtNanos                 = Option.empty[Long]
  private var outcome                          = Option.empty[String]
  private var driverThread                     = Option.empty[String]
  private var completedExportReuseAllowed      = false
  private var emitted                          = false

  private[telemetry] def matchesCommand(expectedView: String, expectedOperation: String): Boolean =
    lock.synchronized {
      !emitted &&
      materializedView == OpenIvmExecutionSpan.normalizeString(expectedView) &&
      operation == OpenIvmExecutionSpan.normalizeOperation(expectedOperation)
    }

  private def addDuration(current: Option[Long], deltaMs: Long): Option[Long] =
    Some(current.getOrElse(0L) + math.max(0L, deltaMs))

  private def recordBeforeComplete(update: => Unit): Unit =
    if (enabled) {
      lock.synchronized {
        if (completedAtNanos.isEmpty) update
      }
    }

  private def recordBeforeEmit(update: => Unit): Unit =
    if (enabled) {
      lock.synchronized {
        if (!emitted) update
      }
    }

  def recordSameMvLockWait(durationMs: Long): Unit =
    recordBeforeComplete {
      sameMvLockWaitMs = addDuration(sameMvLockWaitMs, durationMs)
    }

  def recordDriverAdmissionWait(durationMs: Long): Unit =
    recordBeforeComplete {
      driverAdmissionMs = addDuration(driverAdmissionMs, durationMs)
    }

  def recordCatalogPublicationAdmission(width: Int, durationMs: Long): Unit =
    recordBeforeComplete {
      catalogPublicationAdmissionWidth = Some(width)
      catalogPublicationAdmissionMs = addDuration(catalogPublicationAdmissionMs, durationMs)
    }

  def recordCompiler(durationMs: Long): Unit =
    recordBeforeComplete {
      compilerMs = addDuration(compilerMs, durationMs)
    }

  def recordCatalog(durationMs: Long): Unit =
    recordBeforeComplete {
      catalogMs = addDuration(catalogMs, durationMs)
    }

  def recordRocksDbWrite(durationMs: Long): Unit =
    recordBeforeComplete {
      rocksDbWriteMs = addDuration(rocksDbWriteMs, durationMs)
      rocksDbWriteCount += 1L
    }

  def recordRocksDbWriteLatency(durationMs: Long): Unit =
    recordBeforeComplete {
      rocksDbWriteMs = addDuration(rocksDbWriteMs, durationMs)
    }

  def recordAnalysis(durationMs: Long): Unit =
    recordBeforeComplete {
      analysisMs = addDuration(analysisMs, durationMs)
    }

  def recordWatermark(durationMs: Long): Unit =
    recordBeforeComplete {
      watermarkMs = addDuration(watermarkMs, durationMs)
    }

  def recordCtas(durationMs: Long): Unit =
    recordBeforeComplete {
      ctasMs = addDuration(ctasMs, durationMs)
    }

  def recordCtasDataWrite(durationMs: Long): Unit =
    recordBeforeComplete {
      ctasDataWriteMs = addDuration(ctasDataWriteMs, durationMs)
    }

  def recordHiveCatalogPublication(durationMs: Long): Unit =
    recordBeforeComplete {
      hiveCatalogPublishMs = addDuration(hiveCatalogPublishMs, durationMs)
    }

  def recordMetadataPublication(durationMs: Long): Unit =
    recordBeforeComplete {
      metadataPublicationMs = addDuration(metadataPublicationMs, durationMs)
    }

  def recordDeltaVersionLookup(durationMs: Long): Unit =
    recordBeforeComplete {
      deltaVersionLookupMs = addDuration(deltaVersionLookupMs, durationMs)
      deltaVersionLookupCount += 1L
    }

  def recordRocksDbFlush(durationMs: Long, failed: Boolean = false): Unit =
    recordBeforeComplete {
      rocksDbFlushMs = addDuration(rocksDbFlushMs, durationMs)
      rocksDbFlushCount += 1L
      if (failed) rocksDbFlushFailures += 1L
    }

  def recordRocksDbLockWait(jvmDurationMs: Long, externalDurationMs: Long): Unit =
    recordBeforeComplete {
      if (jvmDurationMs > 0L) rocksDbJvmLockWaitMs = addDuration(rocksDbJvmLockWaitMs, jvmDurationMs)
      if (externalDurationMs > 0L) {
        rocksDbExternalWaitMs = addDuration(rocksDbExternalWaitMs, externalDurationMs)
      }
    }

  def recordRocksDbBackup(durationMs: Long): Unit =
    recordBeforeEmit {
      rocksDbBackupMs = addDuration(rocksDbBackupMs, durationMs)
    }

  def recordSourceVersions(values: => Seq[OpenIvmTelemetryContract.SourceVersion]): Unit =
    if (configuredExport.nonEmpty) {
      recordBeforeEmit {
        OpenIvmTelemetryContract.validatedSourceVersions(values).foreach { value =>
          val relationKey = OpenIvmTelemetryContract.canonicalRelationKey(value.relation)
          val merged = sourceVersions.get(relationKey) match {
            case Some(existing) =>
              OpenIvmTelemetryContract.SourceVersion(
                existing.relation,
                math.min(existing.startVersion, value.startVersion),
                math.max(existing.endVersion, value.endVersion)
              )
            case None => value
          }
          sourceVersions = sourceVersions.updated(relationKey, merged)
        }
      }
    }

  def recordPendingDeltaCount(count: Long): Unit =
    if (configuredExport.nonEmpty) {
      recordBeforeEmit {
        if (count < 0L)
          throw new OpenIvmTelemetryExportException("OpenIVM telemetry pending delta count must be non-negative")
        pendingDeltaCount = count
      }
    }

  def allowCompletedExportReuse(): Unit =
    if (configuredExport.nonEmpty) {
      recordBeforeEmit {
        completedExportReuseAllowed = true
      }
    }

  def recordRefreshClassification(
      compileRefreshType: Option[String] = None,
      effectiveRefreshType: Option[String] = None,
      refreshReason: Option[String] = None
  ): Unit =
    recordBeforeEmit {
      refreshClassification = refreshClassification.merge(
        OpenIvmExecutionSpan.RefreshClassification.fromOptions(
          compileRefreshType = compileRefreshType,
          effectiveRefreshType = effectiveRefreshType,
          refreshReason = refreshReason
        )
      )
    }

  /** Record the [[org.openivm.spark.common.TimeTravelPinStatus]] of the view's
    * user-authored snapshot pins, with the
    * [[org.openivm.spark.common.TimeTravelPinReason]] that explains it.
    *
    * `COMPILE_FAILED` is authoritative once seen — a later, weaker value cannot
    * mask a refused pin. A status that is not a contract value is recorded
    * fail-closed as `COMPILE_FAILED`/`unknown_pin_status` rather than dropped,
    * so a telemetry defect can never surface as a missing field that a
    * coverage guard might read as "this engine does not classify pins".
    */
  def recordTimeTravelPinStatus(status: String, reason: String = null): Unit =
    recordBeforeEmit {
      TimeTravelPinStatus.normalizeOrRefuse(status).foreach { incoming =>
        val refused = timeTravelPinStatus.contains(TimeTravelPinStatus.CompileFailed)
        val unknown = TimeTravelPinStatus.normalize(status).isEmpty
        if (!refused || incoming == TimeTravelPinStatus.CompileFailed) {
          timeTravelPinStatus = Some(incoming)
          timeTravelPinReason =
            if (unknown) Some(TimeTravelPinReason.UnknownPinStatus)
            else TimeTravelPinReason.normalize(reason)
        }
      }
    }

  def recordProfileStep(stepName: String, detail: String, durationMs: Long): Unit =
    if (enabled) {
      if (detail eq null) ()
      stepName match {
        case "acquire_locks"                   => recordSameMvLockWait(durationMs)
        case "create_catalog_lookup"           => recordCatalog(durationMs)
        case "create_analyze_query"            => recordAnalysis(durationMs)
        case "create_capture_watermarks"       => recordWatermark(durationMs)
        case "create_ctas_total"               => recordCtas(durationMs)
        case "create_ctas_data_write"          => recordCtasDataWrite(durationMs)
        case "create_hive_catalog_publication" => recordHiveCatalogPublication(durationMs)
        case "create_mv_publish_metadata"      => recordMetadataPublication(durationMs)
        case _                                 => ()
      }
    }

  def complete(finalOutcome: String, finalDriverThread: String): Unit =
    if (enabled) {
      lock.synchronized {
        if (completedAtNanos.isEmpty) {
          val endNanos = System.nanoTime()
          completedAtEpochMs = Some(System.currentTimeMillis())
          completedAtNanos = Some(endNanos)
          outcome = Some(OpenIvmExecutionSpan.normalizeString(finalOutcome))
          driverThread = Some(OpenIvmExecutionSpan.normalizeString(finalDriverThread))
        }
      }
    }

  def emitIfNeeded(defaultOutcome: String, defaultDriverThread: String): Unit = {
    val payload =
      if (!enabled) None
      else {
        lock.synchronized {
          if (emitted) None
          else {
            if (completedAtNanos.isEmpty) {
              val endNanos = System.nanoTime()
              completedAtEpochMs = Some(System.currentTimeMillis())
              completedAtNanos = Some(endNanos)
              outcome = Some(OpenIvmExecutionSpan.normalizeString(defaultOutcome))
              driverThread = Some(OpenIvmExecutionSpan.normalizeString(defaultDriverThread))
            }
            emitted = true
            val completedEpochMs = completedAtEpochMs.getOrElse(startedAtEpochMs)
            val durationMs =
              TimeUnit.NANOSECONDS.toMillis(completedAtNanos.getOrElse(startedAtNanos) - startedAtNanos)
            val finalDriverThread =
              driverThread.getOrElse(OpenIvmExecutionSpan.normalizeString(defaultDriverThread))
            val finalOutcome = outcome.getOrElse(OpenIvmExecutionSpan.normalizeString(defaultOutcome))
            val logPayload = OpenIvmExecutionSpan.renderJson(
              requestId = requestId,
              dbtNodeId = dbtNodeId,
              materializedView = materializedView,
              operation = operation,
              startedAtEpochMs = startedAtEpochMs,
              completedAtEpochMs = math.max(startedAtEpochMs, completedEpochMs),
              durationMs = math.max(0L, durationMs),
              driverThread = finalDriverThread,
              outcome = finalOutcome,
              compileRefreshType = refreshClassification.compileRefreshType,
              effectiveRefreshType = refreshClassification.effectiveRefreshType,
              refreshReason = refreshClassification.refreshReason,
              timeTravelPinStatus = timeTravelPinStatus,
              timeTravelPinReason = timeTravelPinReason,
              sameMvLockWaitMs = sameMvLockWaitMs,
              driverAdmissionWaitMs = driverAdmissionMs,
              catalogPublicationAdmissionWidth = catalogPublicationAdmissionWidth,
              catalogPublicationAdmissionWaitMs = catalogPublicationAdmissionMs,
              compilerMs = compilerMs,
              catalogMs = catalogMs,
              rocksDbWriteMs = rocksDbWriteMs,
              rocksDbWriteCount = rocksDbWriteCount,
              analysisMs = analysisMs,
              watermarkMs = watermarkMs,
              ctasMs = ctasMs,
              ctasDataWriteMs = ctasDataWriteMs,
              hiveCatalogPublicationMs = hiveCatalogPublishMs,
              metadataPublicationMs = metadataPublicationMs,
              deltaVersionLookupMs = deltaVersionLookupMs,
              deltaVersionLookupCount = deltaVersionLookupCount,
              rocksDbFlushMs = rocksDbFlushMs,
              rocksDbFlushCount = rocksDbFlushCount,
              rocksDbFlushFailures = rocksDbFlushFailures,
              rocksDbJvmLockWaitMs = rocksDbJvmLockWaitMs,
              rocksDbExternalWaitMs = rocksDbExternalWaitMs,
              rocksDbBackupMs = rocksDbBackupMs
            )
            val exportPayload = configuredExport.map { export =>
              export -> OpenIvmExecutionSpan.renderExportJson(
                identity = export.identity,
                requestId = requestId,
                dbtNodeId = dbtNodeId,
                materializedView = materializedView,
                operation = operation,
                startedAtEpochMs = startedAtEpochMs,
                completedAtEpochMs = completedEpochMs,
                durationMs = durationMs,
                driverThread = finalDriverThread,
                outcome = finalOutcome,
                compileRefreshType = refreshClassification.compileRefreshType,
                effectiveRefreshType = refreshClassification.effectiveRefreshType,
                refreshReason = refreshClassification.refreshReason,
                timeTravelPinStatus = timeTravelPinStatus,
                timeTravelPinReason = timeTravelPinReason,
                sameMvLockWaitMs = sameMvLockWaitMs,
                driverAdmissionWaitMs = driverAdmissionMs,
                catalogPublicationAdmissionWidth = catalogPublicationAdmissionWidth,
                catalogPublicationAdmissionWaitMs = catalogPublicationAdmissionMs,
                compilerMs = compilerMs,
                catalogMs = catalogMs,
                rocksDbWriteMs = rocksDbWriteMs,
                rocksDbWriteCount = rocksDbWriteCount,
                analysisMs = analysisMs,
                watermarkMs = watermarkMs,
                ctasMs = ctasMs,
                ctasDataWriteMs = ctasDataWriteMs,
                hiveCatalogPublicationMs = hiveCatalogPublishMs,
                metadataPublicationMs = metadataPublicationMs,
                deltaVersionLookupMs = deltaVersionLookupMs,
                deltaVersionLookupCount = deltaVersionLookupCount,
                rocksDbFlushMs = rocksDbFlushMs,
                rocksDbFlushCount = rocksDbFlushCount,
                rocksDbFlushFailures = rocksDbFlushFailures,
                rocksDbJvmLockWaitMs = rocksDbJvmLockWaitMs,
                rocksDbExternalWaitMs = rocksDbExternalWaitMs,
                rocksDbBackupMs = rocksDbBackupMs,
                sourceVersions = sourceVersions.values.toSeq,
                pendingDeltaCount = pendingDeltaCount
              )
            }
            Some(
              OpenIvmExecutionSpan.RenderedPayload(
                logPayload,
                exportPayload,
                completedExportReuseAllowed
              )
            )
          }
        }
      }

    payload.foreach { rendered =>
      rendered.exportPayload.foreach { case (export, exportJson) =>
        val reused =
          rendered.completedExportReuseAllowed &&
            export.publisher.reuseCompleted(export.identity, exportJson)
        if (!reused) export.publisher.publish(export.identity, exportJson)
      }
      OpenIvmExecutionSpan.logSpan(rendered.logPayload)
      oneLakeSink.foreach { sink =>
        try {
          sink.write(
            requestId.orElse(dbtNodeId).getOrElse(materializedView),
            OpenIvmExecutionSpan.LogPrefix + rendered.logPayload
          )
        } catch {
          // emitIfNeeded runs from finishActive in the CREATE/REFRESH `finally`,
          // AFTER the model's SQL has committed. The span is a pure telemetry
          // mirror of an already-decided classification, so a sink throw here
          // must never fail the committed model nor mask its original exception.
          // Record it loudly + in the retrievable health signal, then swallow —
          // delivery failure only (fatal VM errors still propagate).
          case NonFatal(sinkError) =>
            OneLakeSpanSink.recordCallSiteFailure(sinkError)
            OpenIvmExecutionSpan.logSinkDropAtEmit(materializedView, sinkError)
        }
      }
    }
  }
}

object OpenIvmExecutionSpan extends Logging {

  private final case class RenderedPayload(
      logPayload: String,
      exportPayload: Option[(ConfiguredTelemetryExport, String)],
      completedExportReuseAllowed: Boolean
  )

  private final case class PendingDuration(durationMs: Long, observedAtNanos: Long)
  private final case class RefreshClassification(
      compileRefreshType: Option[String] = None,
      effectiveRefreshType: Option[String] = None,
      refreshReason: Option[String] = None
  ) {
    private def isAuthoritative: Boolean =
      compileRefreshType.contains(CompileFailedRefreshType) || refreshReason.contains(CompileFailedReason)

    def merge(incoming: RefreshClassification): RefreshClassification =
      if (incoming == RefreshClassification.empty) this
      else if (isAuthoritative && !incoming.isAuthoritative) this
      else {
        val preferred =
          if (incoming.isAuthoritative && !isAuthoritative) incoming
          else this
        val fallback =
          if (preferred eq this) incoming
          else this
        RefreshClassification(
          compileRefreshType = preferred.compileRefreshType.orElse(fallback.compileRefreshType),
          effectiveRefreshType = preferred.effectiveRefreshType.orElse(fallback.effectiveRefreshType),
          refreshReason = preferred.refreshReason.orElse(fallback.refreshReason)
        )
      }
  }

  private object RefreshClassification {
    val empty: RefreshClassification = RefreshClassification()

    def fromOptions(
        compileRefreshType: Option[String],
        effectiveRefreshType: Option[String],
        refreshReason: Option[String]
    ): RefreshClassification =
      RefreshClassification(
        compileRefreshType = normalizeOption(compileRefreshType),
        effectiveRefreshType = normalizeOption(effectiveRefreshType),
        refreshReason = normalizeOption(refreshReason)
      )
  }

  private final case class PendingDurations(
      createDriverAdmission: Option[PendingDuration] = None,
      refreshDriverAdmission: Option[PendingDuration] = None
  ) {
    def record(operation: String, durationMs: Long, observedAtNanos: Long): PendingDurations = {
      def merge(current: Option[PendingDuration]): Option[PendingDuration] =
        Some(
          PendingDuration(
            current.fold(0L)(_.durationMs) + math.max(0L, durationMs),
            observedAtNanos
          )
        )

      operation match {
        case "create"  => copy(createDriverAdmission = merge(createDriverAdmission))
        case "refresh" => copy(refreshDriverAdmission = merge(refreshDriverAdmission))
        case _         => this
      }
    }

    def consume(operation: String, nowNanos: Long): (Option[Long], PendingDurations) = {
      def take(current: Option[PendingDuration]): (Option[Long], Option[PendingDuration]) =
        current match {
          case Some(value) if nowNanos - value.observedAtNanos <= PendingTtlNanos => (Some(value.durationMs), None)
          case Some(_)                                                            => (None, None)
          case None                                                               => (None, None)
        }

      operation match {
        case "create" =>
          val (duration, nextCreate) = take(createDriverAdmission)
          duration -> copy(createDriverAdmission = nextCreate)
        case "refresh" =>
          val (duration, nextRefresh) = take(refreshDriverAdmission)
          duration -> copy(refreshDriverAdmission = nextRefresh)
        case _ => None -> this
      }
    }
  }

  private val Json                     = new ObjectMapper()
  private val current                  = new ThreadLocal[OpenIvmExecutionSpan]()
  private val pending                  = new ThreadLocal[PendingDurations]()
  private val RequestIdKeys            = Seq("openivm.request_id", "request_id", "requestId")
  private val DbtNodeIdKeys            = Seq("openivm.node_id", "spark.jobGroup.id")
  private val PendingTtlNanos          = TimeUnit.SECONDS.toNanos(30L)
  private val LogPrefix: String        = "OPENIVM_EXECUTION_SPAN "
  private val CompileFailedRefreshType = "COMPILE_FAILED"
  private val CompileFailedReason      = "compile_failed"

  /** Mirrors `org.openivm.spark.common.DeltaTableVersion.LookupMetric`;
    * duplicated to keep telemetry free of a common → telemetry dependency. */
  private val DeltaVersionLookupMetric = "catalog.delta_version.lookup"

  val NoOp: OpenIvmExecutionSpan =
    new OpenIvmExecutionSpan("", "refresh", None, None, 0L, 0L, enabled = false, configuredExport = None)

  def start(spark: SparkSession, materializedView: String, operation: String): OpenIvmExecutionSpan = {
    Option(current.get()).filter(_.matchesCommand(materializedView, operation)).foreach { existing =>
      return existing
    }
    correlationIdsFromSpark(spark) match {
      case (requestId, dbtNodeId) =>
        startInternal(
          materializedView = materializedView,
          operation = operation,
          requestId = requestId,
          dbtNodeId = dbtNodeId,
          configuredExport = OpenIvmTelemetryExporter.configuredForSpark(
            spark,
            materializedView,
            operation,
            requestId,
            dbtNodeId
          ),
          oneLakeSink = FeatureGate
            .oneLakeTelemetryDir(spark)
            .map(dir => OneLakeSpanSink.forDir(dir, spark.sessionState.newHadoopConf()))
        )
    }
  }

  def start(
      materializedView: String,
      operation: String,
      requestId: Option[String] = None,
      dbtNodeId: Option[String] = None
  ): OpenIvmExecutionSpan =
    startInternal(materializedView, operation, requestId, dbtNodeId, configuredExport = None)

  private[telemetry] def startForTesting(
      identity: OpenIvmTelemetryContract.ExecutionIdentity,
      publisher: CompletedSpanPublisher
  ): OpenIvmExecutionSpan =
    startInternal(
      materializedView = identity.materializedView,
      operation = identity.operation,
      requestId = Some(identity.requestId),
      dbtNodeId = Some(identity.dbtNodeId),
      configuredExport = Some(ConfiguredTelemetryExport(identity.validated, publisher))
    )

  private[telemetry] def startForTesting(
      identity: OpenIvmTelemetryContract.ExecutionIdentity,
      publisher: CompletedSpanPublisher,
      oneLakeSink: SpanSink
  ): OpenIvmExecutionSpan =
    startInternal(
      materializedView = identity.materializedView,
      operation = identity.operation,
      requestId = Some(identity.requestId),
      dbtNodeId = Some(identity.dbtNodeId),
      configuredExport = Some(ConfiguredTelemetryExport(identity.validated, publisher)),
      oneLakeSink = Some(oneLakeSink)
    )

  private def startInternal(
      materializedView: String,
      operation: String,
      requestId: Option[String],
      dbtNodeId: Option[String],
      configuredExport: Option[ConfiguredTelemetryExport],
      oneLakeSink: Option[SpanSink] = None
  ): OpenIvmExecutionSpan = {
    Option(current.get()).filter(_.matchesCommand(materializedView, operation)).foreach { existing =>
      return existing
    }
    val nowEpochMs = System.currentTimeMillis()
    val nowNanos   = System.nanoTime()
    val span = new OpenIvmExecutionSpan(
      materializedView = normalizeString(materializedView),
      operation = normalizeOperation(operation),
      requestId = requestId.map(normalizeString).filter(_.nonEmpty),
      dbtNodeId = dbtNodeId.map(normalizeString).filter(_.nonEmpty),
      startedAtEpochMs = nowEpochMs,
      startedAtNanos = nowNanos,
      enabled = true,
      configuredExport = configuredExport,
      oneLakeSink = oneLakeSink
    )
    consumePending(span, nowNanos)
    current.set(span)
    span
  }

  def finishActive(defaultOutcome: String, defaultDriverThread: String): Unit =
    Option(current.get()).foreach { span =>
      try span.emitIfNeeded(defaultOutcome, defaultDriverThread)
      finally current.remove()
    }

  def hasCurrentSpan: Boolean = current.get() != null

  def hasActiveExport: Boolean =
    Option(current.get()).exists(_.configuredExport.nonEmpty)

  def captureCurrent(): Option[OpenIvmExecutionSpan] = Option(current.get())

  def recordActiveRefreshClassification(
      compileRefreshType: Option[String] = None,
      effectiveRefreshType: Option[String] = None,
      refreshReason: Option[String] = None
  ): Unit =
    Option(current.get()).foreach(
      _.recordRefreshClassification(
        compileRefreshType = compileRefreshType,
        effectiveRefreshType = effectiveRefreshType,
        refreshReason = refreshReason
      )
    )

  def recordActiveTimeTravelPinStatus(status: String, reason: String = null): Unit =
    Option(current.get()).foreach(_.recordTimeTravelPinStatus(status, reason))

  def recordActiveSourceVersions(values: => Seq[OpenIvmTelemetryContract.SourceVersion]): Unit =
    Option(current.get()).foreach(_.recordSourceVersions(values))

  def recordActivePendingDeltaCount(count: Long): Unit =
    Option(current.get()).foreach(_.recordPendingDeltaCount(count))

  def allowActiveCompletedExportReuse(): Unit =
    Option(current.get()).foreach(_.allowCompletedExportReuse())

  def recordActiveCatalogPublicationAdmission(width: Int, nanos: Long): Unit =
    if (nanos >= 0L)
      Option(current.get()).foreach(
        _.recordCatalogPublicationAdmission(
          width,
          TimeUnit.NANOSECONDS.toMillis(nanos)
        )
      )

  def withCaptured[A](captured: Option[OpenIvmExecutionSpan])(body: => A): A = {
    val previous = Option(current.get())
    captured match {
      case Some(span) => current.set(span)
      case None       => current.remove()
    }
    try body
    finally {
      previous match {
        case Some(span) => current.set(span)
        case None       => current.remove()
      }
    }
  }

  def observeTimer(metricName: String, nanos: Long): Unit = {
    if (nanos < 0L) return
    val durationMs = TimeUnit.NANOSECONDS.toMillis(nanos)
    metricName match {
      case "driver_admission.create.wait" =>
        withCurrentOrPending("create", durationMs)
      case "driver_admission.refresh.wait" =>
        withCurrentOrPending("refresh", durationMs)
      case "compiler.compile" =>
        Option(current.get()).foreach(_.recordCompiler(durationMs))
      case DeltaVersionLookupMetric =>
        Option(current.get()).foreach(_.recordDeltaVersionLookup(durationMs))
      case name if isCatalogMetric(name) =>
        Option(current.get()).foreach(_.recordCatalog(durationMs))
      case _ => ()
    }
  }

  def observeRocksDbWrite(nanos: Long): Unit =
    if (nanos >= 0L) Option(current.get()).foreach(_.recordRocksDbWrite(TimeUnit.NANOSECONDS.toMillis(nanos)))

  def observeRocksDbWalSync(nanos: Long): Unit =
    if (nanos >= 0L) Option(current.get()).foreach(_.recordRocksDbWriteLatency(TimeUnit.NANOSECONDS.toMillis(nanos)))

  def observeRocksDbFlush(nanos: Long, failed: Boolean): Unit =
    if (nanos >= 0L) {
      Option(current.get()).foreach(_.recordRocksDbFlush(TimeUnit.NANOSECONDS.toMillis(nanos), failed))
    }

  def observeRocksDbLockWait(jvmNanos: Long, externalNanos: Long): Unit =
    Option(current.get()).foreach(
      _.recordRocksDbLockWait(
        TimeUnit.NANOSECONDS.toMillis(math.max(0L, jvmNanos)),
        TimeUnit.NANOSECONDS.toMillis(math.max(0L, externalNanos))
      )
    )

  def observeRocksDbBackup(nanos: Long): Unit =
    if (nanos >= 0L) Option(current.get()).foreach(_.recordRocksDbBackup(TimeUnit.NANOSECONDS.toMillis(nanos)))

  def needsProfileStepTiming(stepName: String): Boolean =
    stepName == "create_catalog_lookup" ||
      stepName == "create_analyze_query" ||
      stepName == "create_capture_watermarks" ||
      stepName == "create_mv_publish_metadata"

  private[spark] def clearCurrent(span: OpenIvmExecutionSpan): Unit =
    if (current.get() eq span) current.remove()

  private[telemetry] def resetForTesting(): Unit = {
    current.remove()
    pending.remove()
  }

  private def consumePending(span: OpenIvmExecutionSpan, nowNanos: Long): Unit = {
    val currentPending          = Option(pending.get()).getOrElse(PendingDurations())
    val (duration, nextPending) = currentPending.consume(span.operation, nowNanos)
    pending.set(nextPending)
    duration.foreach(span.recordDriverAdmissionWait)
  }

  private def withCurrentOrPending(operation: String, durationMs: Long): Unit =
    Option(current.get()) match {
      case Some(span) => span.recordDriverAdmissionWait(durationMs)
      case None =>
        val next = Option(pending.get()).getOrElse(PendingDurations()).record(operation, durationMs, System.nanoTime())
        pending.set(next)
    }

  private def isCatalogMetric(metricName: String): Boolean =
    metricName.startsWith("catalog.") && !metricName.endsWith(".ensure_tables")

  private def normalizeOperation(operation: String): String =
    normalizeString(operation).toLowerCase(java.util.Locale.ROOT) match {
      case "create"  => "create"
      case "refresh" => "refresh"
      case other     => other
    }

  private[telemetry] def normalizeString(value: String): String =
    Option(value).map(_.trim).getOrElse("")

  private def normalizeOption(value: Option[String]): Option[String] =
    value.map(normalizeString).filter(_.nonEmpty)

  private[telemetry] def correlationIdsFromLookups(
      localPropertyLookup: String => Option[String],
      mdcLookup: String => Option[String],
      confLookup: String => Option[String] = _ => None,
      sysPropLookup: String => Option[String] = _ => None
  ): (Option[String], Option[String]) = {
    val requestId = firstNonBlank(
      valuesFor(RequestIdKeys, localPropertyLookup),
      valuesFor(RequestIdKeys, mdcLookup),
      valuesFor(Seq("spark.openivm.request_id"), confLookup),
      valuesFor(Seq("openivm.request_id"), sysPropLookup)
    )
    val dbtNodeId = firstNonBlank(
      valuesFor(DbtNodeIdKeys, localPropertyLookup),
      valuesFor(DbtNodeIdKeys, mdcLookup)
    )
    requestId -> dbtNodeId
  }

  private def correlationIdsFromSpark(spark: SparkSession): (Option[String], Option[String]) =
    correlationIdsFromLookups(
      localPropertyLookup = key => Option(spark.sparkContext.getLocalProperty(key)),
      mdcLookup = key => Option(MDC.get(key)),
      confLookup = key => spark.conf.getOption(key),
      sysPropLookup = key => sys.props.get(key)
    )

  private def valuesFor(keys: Seq[String], lookup: String => Option[String]): Iterator[String] =
    keys.iterator.flatMap(key => lookup(key).iterator.map(normalizeString).filter(_.nonEmpty))

  private def firstNonBlank(sources: Iterator[String]*): Option[String] =
    sources.iterator.flatten.find(_.nonEmpty)

  private def renderJson(
      requestId: Option[String],
      dbtNodeId: Option[String],
      materializedView: String,
      operation: String,
      startedAtEpochMs: Long,
      completedAtEpochMs: Long,
      durationMs: Long,
      driverThread: String,
      outcome: String,
      compileRefreshType: Option[String],
      effectiveRefreshType: Option[String],
      refreshReason: Option[String],
      timeTravelPinStatus: Option[String],
      timeTravelPinReason: Option[String],
      sameMvLockWaitMs: Option[Long],
      driverAdmissionWaitMs: Option[Long],
      catalogPublicationAdmissionWidth: Option[Int],
      catalogPublicationAdmissionWaitMs: Option[Long],
      compilerMs: Option[Long],
      catalogMs: Option[Long],
      rocksDbWriteMs: Option[Long],
      rocksDbWriteCount: Long,
      analysisMs: Option[Long],
      watermarkMs: Option[Long],
      ctasMs: Option[Long],
      ctasDataWriteMs: Option[Long],
      hiveCatalogPublicationMs: Option[Long],
      metadataPublicationMs: Option[Long],
      deltaVersionLookupMs: Option[Long],
      deltaVersionLookupCount: Long,
      rocksDbFlushMs: Option[Long],
      rocksDbFlushCount: Long,
      rocksDbFlushFailures: Long,
      rocksDbJvmLockWaitMs: Option[Long],
      rocksDbExternalWaitMs: Option[Long],
      rocksDbBackupMs: Option[Long]
  ): String = {
    val fields = new LinkedHashMap[String, AnyRef]()
    requestId.foreach(fields.put("request_id", _))
    dbtNodeId.foreach(fields.put("dbt_node_id", _))
    fields.put("materialized_view", materializedView)
    fields.put("operation", operation)
    fields.put("engine_started_at", Instant.ofEpochMilli(startedAtEpochMs).toString)
    fields.put("engine_completed_at", Instant.ofEpochMilli(completedAtEpochMs).toString)
    fields.put("duration_ms", java.lang.Long.valueOf(durationMs))
    fields.put("driver_thread", driverThread)
    fields.put("outcome", outcome)
    compileRefreshType.foreach(fields.put("compile_refresh_type", _))
    effectiveRefreshType.foreach(fields.put("effective_refresh_type", _))
    refreshReason.foreach(fields.put("refresh_reason", _))
    timeTravelPinStatus.foreach(fields.put("time_travel_pin_status", _))
    timeTravelPinReason.foreach(fields.put("time_travel_pin_reason", _))
    sameMvLockWaitMs.foreach(v => fields.put("same_mv_lock_wait_ms", java.lang.Long.valueOf(v)))
    driverAdmissionWaitMs.foreach(v => fields.put("driver_admission_wait_ms", java.lang.Long.valueOf(v)))
    catalogPublicationAdmissionWidth.foreach(v =>
      fields.put("catalog_publication_admission_width", java.lang.Integer.valueOf(v))
    )
    catalogPublicationAdmissionWaitMs.foreach(v =>
      fields.put("catalog_publication_admission_wait_ms", java.lang.Long.valueOf(v))
    )
    compilerMs.foreach(v => fields.put("compiler_ms", java.lang.Long.valueOf(v)))
    catalogMs.foreach(v => fields.put("catalog_ms", java.lang.Long.valueOf(v)))
    rocksDbWriteMs.foreach(v => fields.put("rocksdb_write_ms", java.lang.Long.valueOf(v)))
    if (rocksDbWriteCount > 0L) fields.put("rocksdb_write_count", java.lang.Long.valueOf(rocksDbWriteCount))
    analysisMs.foreach(v => fields.put("analysis_ms", java.lang.Long.valueOf(v)))
    watermarkMs.foreach(v => fields.put("watermark_ms", java.lang.Long.valueOf(v)))
    ctasMs.foreach(v => fields.put("ctas_ms", java.lang.Long.valueOf(v)))
    ctasDataWriteMs.foreach(v => fields.put("ctas_data_write_ms", java.lang.Long.valueOf(v)))
    hiveCatalogPublicationMs.foreach(v => fields.put("hive_catalog_publication_ms", java.lang.Long.valueOf(v)))
    metadataPublicationMs.foreach(v => fields.put("metadata_publication_ms", java.lang.Long.valueOf(v)))
    deltaVersionLookupMs.foreach(v => fields.put("delta_version_lookup_ms", java.lang.Long.valueOf(v)))
    if (deltaVersionLookupCount > 0L) {
      fields.put("delta_version_lookup_count", java.lang.Long.valueOf(deltaVersionLookupCount))
    }
    rocksDbFlushMs.foreach(v => fields.put("rocksdb_flush_ms", java.lang.Long.valueOf(v)))
    if (rocksDbFlushCount > 0L) fields.put("rocksdb_flush_count", java.lang.Long.valueOf(rocksDbFlushCount))
    if (rocksDbFlushFailures > 0L) {
      fields.put("rocksdb_flush_failed_count", java.lang.Long.valueOf(rocksDbFlushFailures))
    }
    rocksDbJvmLockWaitMs.foreach(v => fields.put("rocksdb_jvm_lock_wait_ms", java.lang.Long.valueOf(v)))
    rocksDbExternalWaitMs.foreach(v => fields.put("rocksdb_external_lock_wait_ms", java.lang.Long.valueOf(v)))
    rocksDbBackupMs.foreach(v => fields.put("rocksdb_backup_ms", java.lang.Long.valueOf(v)))
    Json.writeValueAsString(fields)
  }

  private def renderExportJson(
      identity: OpenIvmTelemetryContract.ExecutionIdentity,
      requestId: Option[String],
      dbtNodeId: Option[String],
      materializedView: String,
      operation: String,
      startedAtEpochMs: Long,
      completedAtEpochMs: Long,
      durationMs: Long,
      driverThread: String,
      outcome: String,
      compileRefreshType: Option[String],
      effectiveRefreshType: Option[String],
      refreshReason: Option[String],
      timeTravelPinStatus: Option[String],
      timeTravelPinReason: Option[String],
      sameMvLockWaitMs: Option[Long],
      driverAdmissionWaitMs: Option[Long],
      catalogPublicationAdmissionWidth: Option[Int],
      catalogPublicationAdmissionWaitMs: Option[Long],
      compilerMs: Option[Long],
      catalogMs: Option[Long],
      rocksDbWriteMs: Option[Long],
      rocksDbWriteCount: Long,
      analysisMs: Option[Long],
      watermarkMs: Option[Long],
      ctasMs: Option[Long],
      ctasDataWriteMs: Option[Long],
      hiveCatalogPublicationMs: Option[Long],
      metadataPublicationMs: Option[Long],
      deltaVersionLookupMs: Option[Long],
      deltaVersionLookupCount: Long,
      rocksDbFlushMs: Option[Long],
      rocksDbFlushCount: Long,
      rocksDbFlushFailures: Long,
      rocksDbJvmLockWaitMs: Option[Long],
      rocksDbExternalWaitMs: Option[Long],
      rocksDbBackupMs: Option[Long],
      sourceVersions: Seq[OpenIvmTelemetryContract.SourceVersion],
      pendingDeltaCount: Long
  ): String = {
    val validatedIdentity = identity.validated
    val fields            = new LinkedHashMap[String, AnyRef]()
    fields.put("schema_id", OpenIvmTelemetryContract.SchemaId)
    fields.put("schema_version", java.lang.Integer.valueOf(OpenIvmTelemetryContract.SchemaVersion))
    fields.put("campaign_id", validatedIdentity.campaignId)
    fields.put("request_id", requestId.getOrElse(validatedIdentity.requestId))
    fields.put("correlation_id", validatedIdentity.correlationId)
    fields.put("dbt_node_id", dbtNodeId.getOrElse(validatedIdentity.dbtNodeId))
    fields.put("materialized_view", materializedView)
    fields.put("operation", operation)
    fields.put("phase", validatedIdentity.phase)
    fields.put("engine_started_at", Instant.ofEpochMilli(startedAtEpochMs).toString)
    fields.put("engine_completed_at", Instant.ofEpochMilli(completedAtEpochMs).toString)
    fields.put("duration_ms", java.lang.Long.valueOf(durationMs))
    fields.put("driver_thread", driverThread)
    fields.put("outcome", outcome)
    compileRefreshType.foreach(fields.put("compile_refresh_type", _))
    effectiveRefreshType.foreach(fields.put("effective_refresh_type", _))
    refreshReason.foreach(fields.put("refresh_reason", _))
    timeTravelPinStatus.foreach(fields.put("time_travel_pin_status", _))
    timeTravelPinReason.foreach(fields.put("time_travel_pin_reason", _))

    val versionArray = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
    OpenIvmTelemetryContract.validatedSourceVersions(sourceVersions).foreach { version =>
      val item = new LinkedHashMap[String, AnyRef]()
      item.put("relation", version.relation)
      item.put("start_version", java.lang.Long.valueOf(version.startVersion))
      item.put("end_version", java.lang.Long.valueOf(version.endVersion))
      versionArray.add(item)
    }
    fields.put("source_versions", versionArray)
    fields.put("pending_delta_count", java.lang.Long.valueOf(math.max(0L, pendingDeltaCount)))

    sameMvLockWaitMs.foreach(v => fields.put("same_mv_lock_wait_ms", java.lang.Long.valueOf(v)))
    driverAdmissionWaitMs.foreach(v => fields.put("driver_admission_wait_ms", java.lang.Long.valueOf(v)))
    catalogPublicationAdmissionWidth.foreach(v =>
      fields.put("catalog_publication_admission_width", java.lang.Integer.valueOf(v))
    )
    catalogPublicationAdmissionWaitMs.foreach(v =>
      fields.put("catalog_publication_admission_wait_ms", java.lang.Long.valueOf(v))
    )
    compilerMs.foreach(v => fields.put("compiler_ms", java.lang.Long.valueOf(v)))
    catalogMs.foreach(v => fields.put("catalog_ms", java.lang.Long.valueOf(v)))
    analysisMs.foreach(v => fields.put("analysis_ms", java.lang.Long.valueOf(v)))
    watermarkMs.foreach(v => fields.put("watermark_ms", java.lang.Long.valueOf(v)))
    ctasMs.foreach(v => fields.put("ctas_ms", java.lang.Long.valueOf(v)))
    ctasDataWriteMs.foreach(v => fields.put("ctas_data_write_ms", java.lang.Long.valueOf(v)))
    hiveCatalogPublicationMs.foreach(v => fields.put("hive_catalog_publication_ms", java.lang.Long.valueOf(v)))
    metadataPublicationMs.foreach(v => fields.put("metadata_publication_ms", java.lang.Long.valueOf(v)))
    deltaVersionLookupMs.foreach(v => fields.put("delta_version_lookup_ms", java.lang.Long.valueOf(v)))
    if (deltaVersionLookupCount > 0L) {
      fields.put("delta_version_lookup_count", java.lang.Long.valueOf(deltaVersionLookupCount))
    }
    rocksDbWriteMs.foreach(v => fields.put("rocksdb_write_ms", java.lang.Long.valueOf(v)))
    if (rocksDbWriteCount > 0L) fields.put("rocksdb_write_count", java.lang.Long.valueOf(rocksDbWriteCount))
    rocksDbFlushMs.foreach(v => fields.put("rocksdb_flush_ms", java.lang.Long.valueOf(v)))
    if (rocksDbFlushCount > 0L) fields.put("rocksdb_flush_count", java.lang.Long.valueOf(rocksDbFlushCount))
    if (rocksDbFlushFailures > 0L) {
      fields.put("rocksdb_flush_failed_count", java.lang.Long.valueOf(rocksDbFlushFailures))
    }
    rocksDbJvmLockWaitMs.foreach(v => fields.put("rocksdb_jvm_lock_wait_ms", java.lang.Long.valueOf(v)))
    rocksDbExternalWaitMs.foreach(v => fields.put("rocksdb_external_lock_wait_ms", java.lang.Long.valueOf(v)))
    rocksDbBackupMs.foreach(v => fields.put("rocksdb_backup_ms", java.lang.Long.valueOf(v)))
    Json.writeValueAsString(fields)
  }

  private def logSpan(payload: String): Unit =
    logInfo(LogPrefix + payload)

  /** Stable-marker ERROR for a span-mirror delivery failure observed at the
    * emit call site. Greppable on `[openivm-telemetry-sink]`; the model itself
    * is unaffected (the span mirrors an already-committed classification). */
  private[telemetry] def logSinkDropAtEmit(model: String, error: Throwable): Unit =
    logError(
      s"[openivm-telemetry-sink] DROPPED span mirror at emit call site " +
        s"(model=$model); telemetry only, model unaffected",
      error
    )
}
