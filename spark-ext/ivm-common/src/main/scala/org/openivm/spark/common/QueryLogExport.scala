package org.openivm.spark.common

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.util.control.{NoStackTrace, NonFatal}

/** Application/request-scoped export of the existing CREATE/REFRESH SQL logger.
  *
  * Only metadata is retained here. SQL remains in the collector/async queue until
  * persisted in the registry-owned, request-indexed query-log column family.
  * No API uses SHOW, a global flush barrier, or a Spark write job.
  */
object QueryLogExport {
  val RequestIdProperty: String = "openivm.request_id"
  val ConfigPrefix: String      = "spark.openivm.queryLog.export."

  private val applications = new ConcurrentHashMap[String, Application]()
  private val jsonFactory  = new JsonFactory()
  private val emptyOutcomes =
    Set("no_pending_deltas", "noop_fast_exit", "source_versions_already_applied", "runtime_empty_delta_skip")
  private val submittedCategories = Set(
    "initial_load_ctas",
    "catalog_registration",
    "backing_user_view",
    "rewritten_stmt",
    "full_refresh_stmt",
    "count_monoid_cleanup",
    "drop_cleanup",
    "fused_view_delta_select"
  )

  private final case class Limits(
      captures: Int,
      invocations: Int,
      records: Int,
      bytes: Long,
      totalRecords: Long,
      totalBytes: Long
  )

  private def limits(conf: SparkConf): Limits = {
    def positive(name: String, default: Long): Long = {
      val value = conf.getLong(ConfigPrefix + name, default)
      require(value > 0L && value <= Int.MaxValue, s"$ConfigPrefix$name must be in [1, ${Int.MaxValue}]")
      value
    }
    Limits(
      positive("maxCaptures", 1024).toInt,
      positive("maxInvocations", 128).toInt,
      positive("maxRecords", 100000).toInt,
      positive("maxBytes", 64L * 1024 * 1024),
      positive("maxTotalRecords", 1000000),
      positive("maxTotalBytes", 512L * 1024 * 1024)
    )
  }

  private final case class Failure(code: String, message: String)
  private final class Application(val limits: Limits) {
    val captures: mutable.Map[String, Capture] = mutable.HashMap.empty
    var retainedRecords: Long                  = 0L
    var retainedBytes: Long                    = 0L
  }
  private final class Capture(val requestId: String) {
    val invocations: mutable.ArrayBuffer[Invocation] = mutable.ArrayBuffer.empty
    var sqlSucceeded: Option[Boolean]                = None
    var failure: Option[Failure]                     = None
    var recordCount: Int                             = 0
    var bytes: Long                                  = 0L
    var reading: Boolean                             = false
    var releasing: Boolean                           = false
  }

  private def fail(capture: Capture, code: String, message: String): Unit =
    if (capture.failure.isEmpty) capture.failure = Some(Failure(code, message.take(2048)))

  private def errorMessage(error: Throwable): String =
    s"${error.getClass.getSimpleName}: ${Option(error.getMessage).getOrElse("").take(1900)}"

  private def requireRequestId(requestId: String): Unit =
    require(
      requestId != null && requestId.nonEmpty && utf8Size(requestId) <= 4096,
      "requestId must be nonempty and at most 4096 UTF-8 bytes"
    )

  def apiVersion(): Int = 1

  /** Reserve a capture before executing the outer SQL action. Duplicate IDs and
    * exhausted slot/configuration limits throw before SQL starts; captures are
    * never evicted to make room. A disabled logger produces a failed capture.
    */
  def begin(spark: SparkSession, requestId: String): Unit = {
    requireRequestId(requestId)
    val appId = spark.sparkContext.applicationId
    val app = applications.computeIfAbsent(
      appId,
      _ => {
        val created = new Application(limits(spark.sparkContext.getConf))
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
            applications.remove(appId, created)
            ()
          }
        })
        created
      }
    )
    app.synchronized {
      require(!app.captures.contains(requestId), "CAPTURE_EXISTS: requestId is already reserved")
      require(app.captures.size < app.limits.captures, "CAPTURE_LIMIT_EXCEEDED: release exported captures first")
      val capture = new Capture(requestId)
      if (!FeatureGate.queryLogEnabled(spark))
        fail(capture, "QUERY_LOG_DISABLED", s"${FeatureGate.QueryLogEnabledKey} must be enabled at application startup")
      app.captures.put(requestId, capture)
      ()
    }
  }

  /** Seal the outer action, including failures. No persistence or waiting; safe
    * in the caller's finally block without replacing the original SQL error.
    */
  def end(spark: SparkSession, requestId: String, sqlSucceeded: Boolean): Unit =
    Option(applications.get(spark.sparkContext.applicationId)).foreach { app =>
      app.synchronized {
        app.captures.get(requestId).foreach { capture =>
          if (capture.sqlSucceeded.exists(_ != sqlSucceeded))
            fail(capture, "END_CONFLICT", "end was called with conflicting SQL outcomes")
          capture.sqlSucceeded = Some(capture.sqlSucceeded.forall(identity) && sqlSucceeded)
          if (capture.invocations.isEmpty)
            fail(
              capture,
              "NO_LIFECYCLE",
              "The outer action ended without an associated CREATE/REFRESH logger lifecycle"
            )
        }
      }
    }

  /** Called once by the enabled collector, on the SQL worker. All later calls
    * carry this explicit association, never a thread ID or thread-local lookup.
    */
  private[spark] def startInvocation(
      spark: SparkSession,
      refreshId: String,
      viewName: String,
      mode: String
  ): Option[Invocation] = {
    if (!FeatureGate.queryLogEnabled(spark)) return None
    val appId     = spark.sparkContext.applicationId
    val requestId = spark.sparkContext.getLocalProperty(RequestIdProperty)
    Option(applications.get(appId)).flatMap { app =>
      app.synchronized {
        app.captures.get(requestId).map { capture =>
          if (capture.sqlSucceeded.nonEmpty || capture.releasing)
            fail(capture, "LIFECYCLE_AFTER_END", "A logger lifecycle started after the request was sealed")
          if (capture.invocations.size >= app.limits.invocations)
            fail(capture, "CAPTURE_LIMIT_EXCEEDED", "The request exceeded maxInvocations")
          if (
            Seq(refreshId, viewName, mode).exists(s => s == null || s.isEmpty || utf8Size(s) > 4096) ||
            capture.invocations.exists(_.refreshId == refreshId)
          )
            fail(capture, "INVALID_LIFECYCLE", "A native lifecycle identity is invalid or duplicated")
          val invocation =
            new Invocation(appId, app, capture, capture.invocations.size, refreshId, viewName, mode)
          if (capture.failure.isEmpty) capture.invocations += invocation
          invocation
        }
      }
    }
  }

  private[spark] final class Invocation private[QueryLogExport] (
      val applicationId: String,
      private[QueryLogExport] val app: Application,
      private[QueryLogExport] val capture: Capture,
      val ordinal: Int,
      val refreshId: String,
      val viewName: String,
      val mode: String
  ) {
    val requestId: String                               = capture.requestId
    private[QueryLogExport] var recordCount: Int        = 0
    private[QueryLogExport] var scheduledRecords: Int   = 0
    private[QueryLogExport] var persistedRecords: Int   = 0
    private[QueryLogExport] var pendingFlushes: Int     = 0
    private[QueryLogExport] var outcome: Option[String] = None

    def accept(row: RefreshSqlLogRow): Boolean = {
      val bytes = rowBytes(row)
      app.synchronized {
        if (outcome.nonEmpty) fail(capture, "LATE_RECORD", "A row was logged after native lifecycle completion")
        if (row.refreshId != refreshId || row.viewName != viewName || row.mode != mode)
          fail(capture, "ROW_IDENTITY_MISMATCH", "A SQL row does not belong to its registered native lifecycle")
        if (capture.failure.nonEmpty) false
        else if (
          capture.recordCount >= app.limits.records || bytes > app.limits.bytes - capture.bytes ||
          app.retainedRecords >= app.limits.totalRecords || bytes > app.limits.totalBytes - app.retainedBytes
        ) {
          fail(
            capture,
            "CAPTURE_LIMIT_EXCEEDED",
            "The request or application exceeded its SQL row/byte retention limit"
          )
          false
        } else {
          recordCount += 1
          capture.recordCount += 1
          capture.bytes += bytes
          app.retainedRecords += 1L
          app.retainedBytes += bytes
          true
        }
      }
    }

    private[common] def beginFlush(count: Int): FlushTicket = app.synchronized {
      val ticket = new FlushTicket(this, scheduledRecords, count)
      scheduledRecords += count
      pendingFlushes += 1
      if (scheduledRecords > recordCount)
        fail(capture, "FLUSH_ACCOUNTING_FAILED", "More SQL rows were submitted than admitted")
      ticket
    }

    def finish(nativeOutcome: String): Unit = app.synchronized {
      if (outcome.isEmpty) {
        if (nativeOutcome == null || nativeOutcome.isEmpty || utf8Size(nativeOutcome) > 4096)
          fail(capture, "INVALID_LIFECYCLE", "The native lifecycle did not supply a bounded, nonempty outcome")
        outcome = Some(Option(nativeOutcome).getOrElse("").take(4096))
      }
    }

    def failed(code: String, error: Throwable): Unit = app.synchronized {
      fail(capture, code, errorMessage(error))
    }
  }

  private[common] final class FlushTicket private[QueryLogExport] (
      val invocation: Invocation,
      val firstRecord: Int,
      val count: Int
  ) {
    private var acknowledged = false
    def complete(failure: Option[(String, Throwable)]): Unit = invocation.app.synchronized {
      if (!acknowledged) {
        acknowledged = true
        invocation.pendingFlushes -= 1
        failure match {
          case Some((code, error)) => fail(invocation.capture, code, errorMessage(error))
          case None                => invocation.persistedRecords += count
        }
      }
    }
  }

  private final case class InvocationSnapshot(
      refreshId: String,
      viewName: String,
      mode: String,
      outcome: Option[String],
      recordCount: Int
  )
  private final case class Snapshot(
      applicationId: String,
      requestId: String,
      status: String,
      captureComplete: Boolean,
      sqlSucceeded: Option[Boolean],
      recordCount: Int,
      pendingFlushes: Int,
      invocations: Vector[InvocationSnapshot],
      failure: Option[Failure]
  )

  private def snapshot(appId: String, capture: Capture): Snapshot = {
    val invocations = capture.invocations.toVector
    val settled = capture.sqlSucceeded.nonEmpty && invocations.forall(i => i.outcome.nonEmpty && i.pendingFlushes == 0)
    if (settled) {
      if (invocations.exists(i => i.persistedRecords != i.recordCount))
        fail(capture, "MISSING_ROWS", "Not every admitted SQL row was successfully persisted")
      if (
        capture.sqlSucceeded.contains(true) && invocations
          .exists(i => i.recordCount == 0 && !(i.mode == "refresh" && i.outcome.exists(emptyOutcomes.contains)))
      ) fail(capture, "EMPTY_TRACE", "An empty successful trace requires an explicit native refresh no-op outcome")
    }
    val complete = settled && invocations.nonEmpty && capture.failure.isEmpty
    val failure = capture.failure.orElse {
      if (capture.sqlSucceeded.contains(false))
        Some(Failure("SQL_FAILED", "The caller reported an unsuccessful SQL action"))
      else None
    }
    Snapshot(
      appId,
      capture.requestId,
      if (failure.nonEmpty) "failed"
      else if (capture.sqlSucceeded.isEmpty) "running"
      else if (complete) "complete"
      else "pending_flush",
      complete,
      capture.sqlSucceeded,
      capture.recordCount,
      invocations.map(_.pendingFlushes).sum,
      invocations.map(i => InvocationSnapshot(i.refreshId, i.viewName, i.mode, i.outcome, i.recordCount)),
      failure
    )
  }

  private def missing(appId: String, requestId: String): Snapshot =
    Snapshot(
      appId,
      requestId,
      "missing",
      false,
      None,
      0,
      0,
      Vector.empty,
      Some(Failure("CAPTURE_MISSING", "No retained capture exists for this application/request"))
    )

  /** Nonblocking with respect to lifecycle/flush completion. Pending responses
    * contain metadata only; terminal captures fetch only this request's index.
    * maxBytes bounds the entire UTF-8 JSON, not SQL characters or stdout.
    */
  def snapshotJson(spark: SparkSession, requestId: String, maxRows: Int, maxBytes: Int): String = {
    requireRequestId(requestId)
    require(maxRows > 0 && maxBytes > 0, "maxRows and maxBytes must be positive")
    val appId = spark.sparkContext.applicationId
    val app   = Option(applications.get(appId))
    val (state, lease) = app
      .map { a =>
        a.synchronized {
          a.captures.get(requestId).filterNot(_.releasing) match {
            case Some(capture) =>
              val state = snapshot(appId, capture)
              if (state.captureComplete) {
                require(!capture.reading, "CAPTURE_BUSY: another snapshot is reading this request")
                capture.reading = true
              }
              (state, if (state.captureComplete) Some(capture) else None)
            case None => (missing(appId, requestId), None)
          }
        }
      }
      .getOrElse((missing(appId, requestId), None))

    def failedEnvelope(code: String, message: String): String =
      render(
        state.copy(status = "failed", captureComplete = false, failure = Some(Failure(code, message.take(2048)))),
        Seq.empty,
        maxBytes
      )

    try {
      try {
        if (state.captureComplete && state.recordCount > maxRows) throw SnapshotLimit
        val rows =
          if (!state.captureComplete || state.recordCount == 0) Vector.empty
          else RefreshSqlLogCatalog.readExport(spark, appId, requestId, maxRows, maxBytes)
        if (state.captureComplete) {
          val counts = Array.fill(state.invocations.size)(0)
          rows.foreach { indexed =>
            if (indexed.invocationOrder < 0 || indexed.invocationOrder >= counts.length)
              throw new IllegalStateException("Unexpected native invocation in the request index")
            val expected = state.invocations(indexed.invocationOrder)
            val row      = indexed.row
            if (
              row.refreshId != expected.refreshId || row.viewName != expected.viewName || row.mode != expected.mode ||
              indexed.recordOrder != counts(indexed.invocationOrder)
            ) throw new IllegalStateException("Request index identity/order does not match the native capture")
            counts(indexed.invocationOrder) += 1
          }
          if (counts.toVector != state.invocations.map(_.recordCount))
            throw new IllegalStateException("Request index is missing captured SQL rows")
        }
        render(state, rows.map(_.row), maxBytes)
      } catch {
        case SnapshotLimit =>
          failedEnvelope(
            "SNAPSHOT_LIMIT_EXCEEDED",
            "The complete trace does not fit maxRows/maxBytes; no partial trace returned"
          )
        case NonFatal(error) =>
          lease.foreach(c => app.foreach(a => a.synchronized { fail(c, "CAPTURE_READ_FAILED", errorMessage(error)) }))
          failedEnvelope("CAPTURE_READ_FAILED", errorMessage(error))
      }
    } catch {
      case SnapshotLimit =>
        throw new IllegalArgumentException("SNAPSHOT_LIMIT_EXCEEDED: maxBytes cannot hold even the failure envelope")
    } finally {
      lease.foreach(c => app.foreach(a => a.synchronized { c.reading = false }))
    }
  }

  /** Caller acknowledgement after durable local export. Deletes only the export
    * index, never the cumulative legacy SHOW log. Unfinished captures are not
    * released, including failed captures whose own writes are still in flight.
    */
  def release(spark: SparkSession, requestId: String): Unit =
    Option(applications.get(spark.sparkContext.applicationId)).foreach { app =>
      val capture = app.synchronized {
        app.captures.get(requestId).map { c =>
          require(
            c.sqlSucceeded.nonEmpty && c.invocations.forall(i => i.outcome.nonEmpty && i.pendingFlushes == 0),
            "CAPTURE_NOT_TERMINAL: end the request and wait for its own lifecycle/flushes"
          )
          require(!c.reading && !c.releasing, "CAPTURE_BUSY: a snapshot or release is in progress")
          c.releasing = true
          c
        }
      }
      capture.foreach { c =>
        try {
          if (c.invocations.exists(_.scheduledRecords > 0))
            RefreshSqlLogCatalog.removeExport(spark, spark.sparkContext.applicationId, requestId)
          app.synchronized {
            app.captures.remove(requestId)
            app.retainedRecords -= c.recordCount
            app.retainedBytes -= c.bytes
          }
        } finally app.synchronized { c.releasing = false }
      }
    }

  private[common] case object SnapshotLimit extends RuntimeException with NoStackTrace

  // Count UTF-8 without allocating another full SQL byte array on the SQL worker.
  private def utf8Size(value: String): Long = {
    var bytes = 0L
    var index = 0
    while (index < value.length) {
      val char = value.charAt(index)
      if (char <= 0x7f) bytes += 1L
      else if (char <= 0x7ff) bytes += 2L
      else if (
        Character.isHighSurrogate(char) && index + 1 < value.length &&
        Character.isLowSurrogate(value.charAt(index + 1))
      ) {
        bytes += 4L
        index += 1
      } else if (Character.isSurrogate(char)) bytes += 1L
      else bytes += 3L
      index += 1
    }
    bytes
  }

  private[common] def rowBytes(row: RefreshSqlLogRow): Long =
    64L + Seq(row.refreshId, row.viewName, row.mode, row.category, row.stmtKind, row.sqlText).map(utf8Size).sum

  private def representationKind(category: String): String = category match {
    case "original_query"                             => "original_query"
    case "explain_formatted"                          => "explain_plan"
    case known if submittedCategories.contains(known) => "submitted_sql"
    case _                                            => "diagnostic"
  }

  private final class BoundedOutput(limit: Int) extends ByteArrayOutputStream(math.min(limit, 8192)) {
    override def write(value: Int): Unit = {
      if (count >= limit) throw SnapshotLimit
      super.write(value)
    }
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      if (length > limit - count) throw SnapshotLimit
      super.write(bytes, offset, length)
    }
  }

  private def render(state: Snapshot, rows: Seq[RefreshSqlLogRow], maxBytes: Int): String = {
    val output = new BoundedOutput(maxBytes)
    val json   = jsonFactory.createGenerator(output)
    def optionalString(name: String, value: Option[String]): Unit =
      value.fold(json.writeNullField(name))(json.writeStringField(name, _))
    try {
      json.writeStartObject()
      json.writeStringField("schema", "openivm.query-log-export")
      json.writeNumberField("version", apiVersion())
      json.writeStringField("application_id", state.applicationId)
      json.writeStringField("request_id", state.requestId)
      json.writeStringField("status", state.status)
      json.writeBooleanField("capture_complete", state.captureComplete)
      state.sqlSucceeded.fold(json.writeNullField("sql_succeeded"))(json.writeBooleanField("sql_succeeded", _))
      json.writeNumberField("record_count", state.recordCount)
      json.writeNumberField("pending_flushes", state.pendingFlushes)
      json.writeArrayFieldStart("invocations")
      state.invocations.foreach { invocation =>
        json.writeStartObject()
        json.writeStringField("refresh_id", invocation.refreshId)
        json.writeStringField("view_name", invocation.viewName)
        json.writeStringField("mode", invocation.mode)
        optionalString("outcome", invocation.outcome)
        json.writeNumberField("record_count", invocation.recordCount)
        json.writeBooleanField("completed", invocation.outcome.nonEmpty)
        json.writeEndObject()
      }
      json.writeEndArray()
      json.writeArrayFieldStart("records")
      rows.foreach(row => writeRow(json, row))
      json.writeEndArray()
      json.writeFieldName("failure")
      state.failure match {
        case Some(failure) =>
          json.writeStartObject()
          json.writeStringField("code", failure.code)
          json.writeStringField("message", failure.message)
          json.writeEndObject()
        case None => json.writeNull()
      }
      json.writeBooleanField("truncated", false)
      json.writeEndObject()
    } finally json.close()
    new String(output.toByteArray, UTF_8)
  }

  private def writeRow(json: JsonGenerator, row: RefreshSqlLogRow): Unit = {
    json.writeStartObject()
    json.writeStringField("refresh_id", row.refreshId)
    json.writeStringField("view_name", row.viewName)
    json.writeStringField("profile_timestamp", row.profileTimestamp.toInstant.toString)
    json.writeNumberField("stmt_order", row.stmtOrder)
    json.writeNumberField("attempt_idx", row.attemptIdx)
    json.writeStringField("mode", row.mode)
    json.writeStringField("category", row.category)
    json.writeStringField("stmt_kind", row.stmtKind)
    json.writeNumberField("duration_ms", row.durationMs)
    json.writeStringField("sql_text", row.sqlText)
    json.writeStringField("representation_kind", representationKind(row.category))
    json.writeEndObject()
  }
}
