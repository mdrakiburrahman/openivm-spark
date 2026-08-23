package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.slf4j.MDC

import java.time.Instant
import java.util.LinkedHashMap
import java.util.concurrent.TimeUnit

/** Structured one-line execution-span contract for CREATE / REFRESH MV work. */
final class OpenIvmExecutionSpan private[telemetry] (
    val materializedView: String,
    val operation: String,
    private val requestId: Option[String],
    private val dbtNodeId: Option[String],
    private val startedAtEpochMs: Long,
    private val startedAtNanos: Long,
    private val enabled: Boolean
) {

  private val lock = new Object

  private var refreshClassification = OpenIvmExecutionSpan.RefreshClassification.empty
  private var sameMvLockWaitMs      = Option.empty[Long]
  private var driverAdmissionMs     = Option.empty[Long]
  private var compilerMs            = Option.empty[Long]
  private var catalogMs             = Option.empty[Long]
  private var rocksDbWriteMs        = Option.empty[Long]
  private var rocksDbWriteCount     = 0L
  private var rocksDbFlushMs        = Option.empty[Long]
  private var rocksDbFlushCount     = 0L
  private var rocksDbFlushFailures  = 0L
  private var rocksDbJvmLockWaitMs  = Option.empty[Long]
  private var rocksDbExternalWaitMs = Option.empty[Long]
  private var rocksDbBackupMs       = Option.empty[Long]
  private var completedAtEpochMs    = Option.empty[Long]
  private var completedAtNanos      = Option.empty[Long]
  private var outcome               = Option.empty[String]
  private var driverThread          = Option.empty[String]
  private var emitted               = false

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

  def recordProfileStep(stepName: String, detail: String, durationMs: Long): Unit =
    if (enabled) {
      if (detail eq null) ()
      stepName match {
        case "acquire_locks"         => recordSameMvLockWait(durationMs)
        case "create_catalog_lookup" => recordCatalog(durationMs)
        case _                       => ()
      }
    }

  def complete(finalOutcome: String, finalDriverThread: String): Unit =
    if (enabled) {
      lock.synchronized {
        if (completedAtNanos.isEmpty) {
          completedAtEpochMs = Some(System.currentTimeMillis())
          completedAtNanos = Some(System.nanoTime())
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
              completedAtEpochMs = Some(System.currentTimeMillis())
              completedAtNanos = Some(System.nanoTime())
              outcome = Some(OpenIvmExecutionSpan.normalizeString(defaultOutcome))
              driverThread = Some(OpenIvmExecutionSpan.normalizeString(defaultDriverThread))
            }
            emitted = true
            Some(
              OpenIvmExecutionSpan.renderJson(
                requestId = requestId,
                dbtNodeId = dbtNodeId,
                materializedView = materializedView,
                operation = operation,
                startedAtEpochMs = startedAtEpochMs,
                completedAtEpochMs = completedAtEpochMs.getOrElse(startedAtEpochMs),
                durationMs = TimeUnit.NANOSECONDS.toMillis(completedAtNanos.getOrElse(startedAtNanos) - startedAtNanos),
                driverThread = driverThread.getOrElse(OpenIvmExecutionSpan.normalizeString(defaultDriverThread)),
                outcome = outcome.getOrElse(OpenIvmExecutionSpan.normalizeString(defaultOutcome)),
                compileRefreshType = refreshClassification.compileRefreshType,
                effectiveRefreshType = refreshClassification.effectiveRefreshType,
                refreshReason = refreshClassification.refreshReason,
                sameMvLockWaitMs = sameMvLockWaitMs,
                driverAdmissionWaitMs = driverAdmissionMs,
                compilerMs = compilerMs,
                catalogMs = catalogMs,
                rocksDbWriteMs = rocksDbWriteMs,
                rocksDbWriteCount = rocksDbWriteCount,
                rocksDbFlushMs = rocksDbFlushMs,
                rocksDbFlushCount = rocksDbFlushCount,
                rocksDbFlushFailures = rocksDbFlushFailures,
                rocksDbJvmLockWaitMs = rocksDbJvmLockWaitMs,
                rocksDbExternalWaitMs = rocksDbExternalWaitMs,
                rocksDbBackupMs = rocksDbBackupMs
              )
            )
          }
        }
      }

    payload.foreach(OpenIvmExecutionSpan.logSpan)
  }
}

object OpenIvmExecutionSpan extends Logging {

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

  val NoOp: OpenIvmExecutionSpan =
    new OpenIvmExecutionSpan("", "refresh", None, None, 0L, 0L, enabled = false)

  def start(spark: SparkSession, materializedView: String, operation: String): OpenIvmExecutionSpan =
    correlationIdsFromSpark(spark) match {
      case (requestId, dbtNodeId) =>
        start(materializedView, operation, requestId, dbtNodeId)
    }

  def start(
      materializedView: String,
      operation: String,
      requestId: Option[String] = None,
      dbtNodeId: Option[String] = None
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
      enabled = true
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
      case name if isCatalogMetric(name) =>
        Option(current.get()).foreach(_.recordCatalog(durationMs))
      case _ => ()
    }
  }

  def observeRocksDbWrite(nanos: Long): Unit =
    if (nanos >= 0L) Option(current.get()).foreach(_.recordRocksDbWrite(TimeUnit.NANOSECONDS.toMillis(nanos)))

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
    stepName == "create_catalog_lookup"

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
      sameMvLockWaitMs: Option[Long],
      driverAdmissionWaitMs: Option[Long],
      compilerMs: Option[Long],
      catalogMs: Option[Long],
      rocksDbWriteMs: Option[Long],
      rocksDbWriteCount: Long,
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
    sameMvLockWaitMs.foreach(v => fields.put("same_mv_lock_wait_ms", java.lang.Long.valueOf(v)))
    driverAdmissionWaitMs.foreach(v => fields.put("driver_admission_wait_ms", java.lang.Long.valueOf(v)))
    compilerMs.foreach(v => fields.put("compiler_ms", java.lang.Long.valueOf(v)))
    catalogMs.foreach(v => fields.put("catalog_ms", java.lang.Long.valueOf(v)))
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
}
