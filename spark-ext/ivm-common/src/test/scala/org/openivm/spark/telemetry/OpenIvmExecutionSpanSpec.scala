package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.slf4j.MDC
import org.openivm.spark.common.{TimeTravelPinReason, TimeTravelPinStatus}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class OpenIvmExecutionSpanSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  private val Json = new ObjectMapper()

  override protected def beforeEach(): Unit = {
    OpenIvmExecutionSpan.resetForTesting()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    OpenIvmExecutionSpan.resetForTesting()
    super.afterEach()
  }

  private final class BufferingAppender(name: String)
      extends AbstractAppender(
        name,
        null,
        PatternLayout.createDefaultLayout(),
        false,
        Property.EMPTY_ARRAY
      ) {
    private val buffer = ArrayBuffer.empty[String]

    override def append(event: LogEvent): Unit =
      buffer.synchronized {
        buffer += event.getMessage.getFormattedMessage
      }

    def messages: Seq[String] = buffer.synchronized(buffer.toVector)
  }

  private def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"openivm-span-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  private def spanPayloads(messages: Seq[String]): Seq[JsonNode] =
    messages.collect {
      case line if line.startsWith("OPENIVM_EXECUTION_SPAN ") =>
        Json.readTree(line.stripPrefix("OPENIVM_EXECUTION_SPAN "))
    }

  private def millis(value: Long): Long = TimeUnit.MILLISECONDS.toNanos(value)

  private final class CapturingPublisher(delayMs: Long = 0L, failure: Option[Throwable] = None)
      extends CompletedSpanPublisher {
    private val captured = ArrayBuffer.empty[(OpenIvmTelemetryContract.ExecutionIdentity, String)]
    @volatile var publishStartedAtEpochMs: Long = 0L

    override def publish(
        identity: OpenIvmTelemetryContract.ExecutionIdentity,
        payload: String
    ): org.apache.hadoop.fs.Path = {
      publishStartedAtEpochMs = System.currentTimeMillis()
      captured.synchronized(captured += identity -> payload)
      if (delayMs > 0L) Thread.sleep(delayMs)
      failure.foreach(throw _)
      new org.apache.hadoop.fs.Path("file:/captured/completed.json")
    }

    def payloads: Seq[JsonNode] =
      captured.synchronized(captured.toVector.map { case (_, payload) => Json.readTree(payload) })
  }

  private def withMdc[A](entries: (String, String)*)(body: => A): A = {
    entries.foreach { case (key, value) => MDC.put(key, value) }
    try body
    finally entries.foreach { case (key, _) => MDC.remove(key) }
  }

  describe("OpenIvmExecutionSpan") {
    it("emits a completed span with isolated metrics and optional backup timing") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start(
          "default.sales_mv",
          "refresh",
          requestId = Some("req-123"),
          dbtNodeId = Some("model.sales_mv")
        )
        span.recordRefreshClassification(
          compileRefreshType = Some("AGGREGATE_GROUP"),
          effectiveRefreshType = Some("AGGREGATE_GROUP"),
          refreshReason = Some("kept")
        )
        span.recordProfileStep("acquire_locks", "thread=driver-1", 3L)
        OpenIvmExecutionSpan.observeTimer("driver_admission.refresh.wait", millis(7L))
        OpenIvmExecutionSpan.observeTimer("compiler.compile", millis(13L))
        OpenIvmExecutionSpan.observeTimer("catalog.mv_catalog.upsert", millis(5L))
        OpenIvmExecutionSpan.observeRocksDbWrite(millis(4L))
        OpenIvmExecutionSpan.observeRocksDbWrite(millis(6L))
        OpenIvmExecutionSpan.observeRocksDbFlush(millis(11L), failed = false)
        OpenIvmExecutionSpan.observeRocksDbLockWait(millis(2L), millis(1L))
        span.complete("refresh_executed", "driver-1")
        OpenIvmExecutionSpan.observeRocksDbWrite(millis(99L))
        OpenIvmExecutionSpan.observeRocksDbFlush(millis(99L), failed = true)
        OpenIvmExecutionSpan.observeRocksDbBackup(millis(17L))
        span.emitIfNeeded("failed_before_end", "fallback-thread")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      val payload = payloads.head
      payload.get("request_id").asText() shouldBe "req-123"
      payload.get("dbt_node_id").asText() shouldBe "model.sales_mv"
      payload.get("materialized_view").asText() shouldBe "default.sales_mv"
      payload.get("operation").asText() shouldBe "refresh"
      payload.get("driver_thread").asText() shouldBe "driver-1"
      payload.get("outcome").asText() shouldBe "refresh_executed"
      payload.get("compile_refresh_type").asText() shouldBe "AGGREGATE_GROUP"
      payload.get("effective_refresh_type").asText() shouldBe "AGGREGATE_GROUP"
      payload.get("refresh_reason").asText() shouldBe "kept"
      payload.get("same_mv_lock_wait_ms").asLong() shouldBe 3L
      payload.get("driver_admission_wait_ms").asLong() shouldBe 7L
      payload.get("compiler_ms").asLong() shouldBe 13L
      payload.get("catalog_ms").asLong() shouldBe 5L
      payload.get("rocksdb_write_ms").asLong() shouldBe 10L
      payload.get("rocksdb_write_count").asLong() shouldBe 2L
      payload.get("rocksdb_flush_ms").asLong() shouldBe 11L
      payload.get("rocksdb_flush_count").asLong() shouldBe 1L
      payload.has("rocksdb_flush_failed_count") shouldBe false
      payload.get("rocksdb_jvm_lock_wait_ms").asLong() shouldBe 2L
      payload.get("rocksdb_external_lock_wait_ms").asLong() shouldBe 1L
      payload.get("rocksdb_backup_ms").asLong() shouldBe 17L
      payload.get("duration_ms").asLong() should be >= 0L
      payload.get("engine_started_at").asText() should endWith("Z")
      payload.get("engine_completed_at").asText() should endWith("Z")
      payload.has("schema_version") shouldBe false
      payload.has("campaign_id") shouldBe false
      payload.has("source_versions") shouldBe false
      payload.has("pending_delta_count") shouldBe false
    }

    it("exports the complete versioned schema with refresh classification and source versions") {
      val identity = OpenIvmTelemetryContract.ExecutionIdentity(
        campaignId = "central-frozen-fast",
        requestId = "request-123",
        correlationId = "correlation-123",
        dbtNodeId = "model.benchmark.sales",
        materializedView = "benchmark.sales",
        operation = "refresh",
        phase = "refresh"
      )
      val publisher = new CapturingPublisher()
      val span      = OpenIvmExecutionSpan.startForTesting(identity, publisher)

      span.recordRefreshClassification(
        compileRefreshType = Some("AGGREGATE_GROUP"),
        effectiveRefreshType = Some("AGGREGATE_GROUP"),
        refreshReason = Some("kept")
      )
      span.recordTimeTravelPinStatus(TimeTravelPinStatus.Applied, TimeTravelPinReason.PinsResolved)
      span.recordSourceVersions(
        Seq(
          OpenIvmTelemetryContract.SourceVersion("benchmark.orders", 41L, 42L),
          OpenIvmTelemetryContract.SourceVersion("benchmark.customers", 17L, 17L)
        )
      )
      span.recordPendingDeltaCount(2L)
      span.recordProfileStep("acquire_locks", "", 3L)
      span.recordProfileStep("create_analyze_query", "", 5L)
      span.recordProfileStep("create_capture_watermarks", "", 7L)
      span.recordProfileStep("create_ctas_total", "", 11L)
      span.recordProfileStep("create_ctas_data_write", "", 9L)
      span.recordProfileStep("create_hive_catalog_publication", "", 2L)
      span.recordProfileStep("create_mv_publish_metadata", "", 4L)
      span.recordDriverAdmissionWait(6L)
      span.recordCompiler(8L)
      span.recordCatalog(10L)
      span.recordRocksDbWrite(12L)
      span.recordRocksDbFlush(14L)
      span.recordRocksDbLockWait(16L, 18L)
      span.complete("refresh_executed", "driver-schema")
      span.emitIfNeeded("failed_before_end", "unused")

      publisher.payloads should have size 1
      val payload = publisher.payloads.head
      payload.path("schema_id").asText() shouldBe OpenIvmTelemetryContract.SchemaId
      payload.path("schema_version").asInt() shouldBe OpenIvmTelemetryContract.SchemaVersion
      payload.path("campaign_id").asText() shouldBe identity.campaignId
      payload.path("request_id").asText() shouldBe identity.requestId
      payload.path("correlation_id").asText() shouldBe identity.correlationId
      payload.path("dbt_node_id").asText() shouldBe identity.dbtNodeId
      payload.path("materialized_view").asText() shouldBe identity.materializedView
      payload.path("operation").asText() shouldBe identity.operation
      payload.path("phase").asText() shouldBe identity.phase
      payload.path("compile_refresh_type").asText() shouldBe "AGGREGATE_GROUP"
      payload.path("effective_refresh_type").asText() shouldBe "AGGREGATE_GROUP"
      payload.path("refresh_reason").asText() shouldBe "kept"
      payload.path("time_travel_pin_status").asText() shouldBe "APPLIED"
      payload.path("time_travel_pin_reason").asText() shouldBe "pins_resolved"
      payload.path("pending_delta_count").asLong() shouldBe 2L
      payload.path("analysis_ms").asLong() shouldBe 5L
      payload.path("ctas_ms").asLong() shouldBe 11L
      payload.path("ctas_data_write_ms").asLong() shouldBe 9L
      payload.path("hive_catalog_publication_ms").asLong() shouldBe 2L
      payload.path("metadata_publication_ms").asLong() shouldBe 4L
      payload.path("compiler_ms").asLong() shouldBe 8L
      payload.path("catalog_ms").asLong() shouldBe 10L
      payload.path("rocksdb_write_ms").asLong() shouldBe 12L
      payload.path("rocksdb_flush_ms").asLong() shouldBe 14L
      payload.path("rocksdb_jvm_lock_wait_ms").asLong() shouldBe 16L
      payload.path("rocksdb_external_lock_wait_ms").asLong() shouldBe 18L

      val versions = payload.path("source_versions")
      versions.size() shouldBe 2
      versions.get(0).path("relation").asText() shouldBe "benchmark.customers"
      versions.get(0).path("start_version").asLong() shouldBe 17L
      versions.get(0).path("end_version").asLong() shouldBe 17L
      versions.get(1).path("relation").asText() shouldBe "benchmark.orders"
      versions.get(1).path("start_version").asLong() shouldBe 41L
      versions.get(1).path("end_version").asLong() shouldBe 42L

      val schemaStream = Option(getClass.getResourceAsStream(OpenIvmTelemetryContract.SchemaResource))
      schemaStream should not be empty
      val schema =
        try Json.readTree(schemaStream.get)
        finally schemaStream.get.close()
      schema.path("$id").asText() shouldBe OpenIvmTelemetryContract.SchemaId
      schema.path("properties").path("schema_version").path("const").asInt() shouldBe
        OpenIvmTelemetryContract.SchemaVersion
      schema
        .path("x-openivm-w6-required-success-fields")
        .elements()
        .asScala
        .map(_.asText())
        .toSet shouldBe OpenIvmTelemetryContract.W6RequiredSuccessFields.toSet
      schema.path("required").elements().asScala.map(_.asText()).toSeq shouldBe
        OpenIvmTelemetryContract.SchemaRequiredFields
      schema.path("properties").fieldNames().asScala.toSet shouldBe OpenIvmTelemetryContract.AllowedFields
      schema
        .path("x-openivm-operation-success-outcomes")
        .path("create")
        .elements()
        .asScala
        .map(_.asText())
        .toSet shouldBe OpenIvmTelemetryContract.CreateSuccessOutcomes
      schema
        .path("x-openivm-operation-success-outcomes")
        .path("refresh")
        .elements()
        .asScala
        .map(_.asText())
        .toSet shouldBe OpenIvmTelemetryContract.RefreshSuccessOutcomes
      schema.path("x-openivm-duration-timestamp-tolerance-ms").asLong() shouldBe
        OpenIvmTelemetryContract.DurationTimestampToleranceMs
      schema
        .path("x-openivm-outcomes-requiring-source-versions")
        .elements()
        .asScala
        .map(_.asText())
        .toSet shouldBe OpenIvmTelemetryContract.OutcomesRequiringSourceVersions
      schema.path("x-openivm-source-version-order").asText() shouldBe "canonical_relation_ascending"
      schema.path("x-openivm-source-version-uniqueness").asText() shouldBe "canonical_relation"
    }

    it("does not evaluate export-only source fields when telemetry is disabled") {
      var evaluated = false
      val span      = OpenIvmExecutionSpan.start("default.disabled_export_mv", "refresh")
      span.recordSourceVersions {
        evaluated = true
        throw new IllegalStateException("disabled telemetry evaluated export-only data")
      }
      span.recordPendingDeltaCount(-1L)
      span.complete("no_pending_deltas", "driver-disabled")
      span.emitIfNeeded("failed_before_end", "unused")

      evaluated shouldBe false
    }

    it("captures the internal end before serialization and excludes export delay from duration") {
      val identity = OpenIvmTelemetryContract.ExecutionIdentity(
        campaignId = "central-frozen-fast",
        requestId = "request-delay",
        correlationId = "correlation-delay",
        dbtNodeId = "model.benchmark.delay",
        materializedView = "benchmark.delay",
        operation = "refresh",
        phase = "refresh"
      )
      val publisher = new CapturingPublisher(delayMs = 150L)
      val wallStart = System.nanoTime()
      val span      = OpenIvmExecutionSpan.startForTesting(identity, publisher)
      Thread.sleep(10L)
      span.complete("refresh_executed", "driver-delay")
      span.emitIfNeeded("failed_before_end", "unused")
      val wallDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart)

      val payload        = publisher.payloads.head
      val startedAt      = Instant.parse(payload.path("engine_started_at").asText())
      val completedAt    = Instant.parse(payload.path("engine_completed_at").asText())
      val engineDuration = payload.path("duration_ms").asLong()

      completedAt should be >= startedAt
      completedAt.toEpochMilli should be <= publisher.publishStartedAtEpochMs
      wallDurationMs should be >= 140L
      engineDuration should be < (wallDurationMs - 100L)
    }

    it("surfaces exporter failures to the command path") {
      val identity = OpenIvmTelemetryContract.ExecutionIdentity(
        campaignId = "central-frozen-fast",
        requestId = "request-failure",
        correlationId = "correlation-failure",
        dbtNodeId = "model.benchmark.failure",
        materializedView = "benchmark.failure",
        operation = "create",
        phase = "full"
      )
      val publisher =
        new CapturingPublisher(failure = Some(new OpenIvmTelemetryExportException("explicit export failure")))
      val span = OpenIvmExecutionSpan.startForTesting(identity, publisher)
      span.complete("create_executed", "driver-failure")

      val error = intercept[OpenIvmTelemetryExportException] {
        span.emitIfNeeded("failed_before_end", "unused")
      }
      error.getMessage shouldBe "explicit export failure"
      publisher.payloads should have size 1
    }

    it("swallows a OneLake sink delivery failure without failing the emitted span") {
      OneLakeSpanSink.resetHealth()
      val identity = OpenIvmTelemetryContract.ExecutionIdentity(
        campaignId = "sink-failure",
        requestId = "request-sink",
        correlationId = "correlation-sink",
        dbtNodeId = "model.benchmark.sink",
        materializedView = "benchmark.sink",
        operation = "create",
        phase = "full"
      )
      val publisher = new CapturingPublisher()
      val throwingSink = new SpanSink {
        override def write(key: String, line: String): Unit =
          throw new RuntimeException("sink delivery boom")
      }
      val span = OpenIvmExecutionSpan.startForTesting(identity, publisher, throwingSink)
      span.complete("create_executed", "driver-sink")

      // The model has committed by the time emitIfNeeded runs; an always-throwing
      // sink must NOT fail the span nor mask anything. The authoritative export
      // still succeeds and the drop is recorded in the retrievable health signal.
      noException should be thrownBy span.emitIfNeeded("failed_before_end", "unused")
      publisher.payloads should have size 1
      OneLakeSpanSink.health.dropped should be >= 1L
    }

    it("keeps delta version lookups out of catalog_ms") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.version_mv", "create")
        OpenIvmExecutionSpan.observeTimer("catalog.delta_version.lookup", millis(4L))
        OpenIvmExecutionSpan.observeTimer("catalog.delta_version.lookup", millis(6L))
        OpenIvmExecutionSpan.observeTimer("catalog.mv_catalog.upsert", millis(5L))
        span.complete("create_executed", "driver-create")
        span.emitIfNeeded("create_executed", "driver-create")

        val untouched = OpenIvmExecutionSpan.start("default.no_version_mv", "create")
        untouched.complete("create_executed", "driver-create")
        untouched.emitIfNeeded("create_executed", "driver-create")
        spanPayloads(appender.messages)
      }

      payloads should have size 2
      val withLookups = payloads.head
      withLookups.get("delta_version_lookup_ms").asLong() shouldBe 10L
      withLookups.get("delta_version_lookup_count").asLong() shouldBe 2L
      withLookups.get("catalog_ms").asLong() shouldBe 5L

      val withoutLookups = payloads(1)
      withoutLookups.has("delta_version_lookup_ms") shouldBe false
      withoutLookups.has("delta_version_lookup_count") shouldBe false
    }

    it("emits failed_before_end when completion is missing") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.fail_mv", "create")
        OpenIvmExecutionSpan.observeTimer("driver_admission.create.wait", millis(2L))
        span.emitIfNeeded("failed_before_end", "driver-create")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      val payload = payloads.head
      payload.has("request_id") shouldBe false
      payload.has("dbt_node_id") shouldBe false
      payload.get("materialized_view").asText() shouldBe "default.fail_mv"
      payload.get("operation").asText() shouldBe "create"
      payload.get("driver_thread").asText() shouldBe "driver-create"
      payload.get("outcome").asText() shouldBe "failed_before_end"
      payload.get("driver_admission_wait_ms").asLong() shouldBe 2L
      payload.has("compiler_ms") shouldBe false
    }

    it("reports failed flush attempts on the failed command span") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.flush_fail_mv", "refresh")
        OpenIvmExecutionSpan.observeRocksDbFlush(millis(3L), failed = true)
        span.complete("refresh_failed", "driver-flush-fail")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      val payload = payloads.head
      payload.get("outcome").asText() shouldBe "refresh_failed"
      payload.get("rocksdb_flush_ms").asLong() shouldBe 3L
      payload.get("rocksdb_flush_count").asLong() shouldBe 1L
      payload.get("rocksdb_flush_failed_count").asLong() shouldBe 1L
    }

    it("emits always-on CREATE phase timing without requiring profile rows") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.create_phases_mv", "create")
        OpenIvmExecutionSpan.recordActiveCatalogPublicationAdmission(7, millis(19L))
        span.recordProfileStep("create_analyze_query", "", 11L)
        span.recordProfileStep("create_capture_watermarks", "", 13L)
        span.recordProfileStep("create_ctas_total", "", 41L)
        span.recordProfileStep("create_ctas_data_write", "", 29L)
        span.recordProfileStep("create_hive_catalog_publication", "", 12L)
        span.recordProfileStep("create_mv_publish_metadata", "", 17L)
        span.complete("create_executed", "driver-create-phases")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      val payload = payloads.head
      payload.get("catalog_publication_admission_width").asInt() shouldBe 7
      payload.get("catalog_publication_admission_wait_ms").asLong() shouldBe 19L
      payload.get("analysis_ms").asLong() shouldBe 11L
      payload.get("watermark_ms").asLong() shouldBe 13L
      payload.get("ctas_ms").asLong() shouldBe 41L
      payload.get("ctas_data_write_ms").asLong() shouldBe 29L
      payload.get("hive_catalog_publication_ms").asLong() shouldBe 12L
      payload.get("metadata_publication_ms").asLong() shouldBe 17L

      OpenIvmExecutionSpan.needsProfileStepTiming("create_analyze_query") shouldBe true
      OpenIvmExecutionSpan.needsProfileStepTiming("create_capture_watermarks") shouldBe true
      OpenIvmExecutionSpan.needsProfileStepTiming("create_mv_publish_metadata") shouldBe true
    }

    it("keeps compile_failed classification sticky against later generic fallbacks") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.sticky_mv", "create")
        span.recordRefreshClassification(
          compileRefreshType = Some("FULL_REFRESH"),
          effectiveRefreshType = Some("FULL_REFRESH"),
          refreshReason = Some("no_real_delta")
        )
        span.recordRefreshClassification(
          compileRefreshType = Some("COMPILE_FAILED"),
          effectiveRefreshType = Some("FULL_REFRESH"),
          refreshReason = Some("compile_failed")
        )
        span.recordRefreshClassification(
          compileRefreshType = Some("FULL_REFRESH"),
          effectiveRefreshType = Some("FULL_REFRESH"),
          refreshReason = Some("no_real_delta")
        )
        span.complete("create_failed", "driver-sticky")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      val payload = payloads.head
      payload.get("compile_refresh_type").asText() shouldBe "COMPILE_FAILED"
      payload.get("effective_refresh_type").asText() shouldBe "FULL_REFRESH"
      payload.get("refresh_reason").asText() shouldBe "compile_failed"
    }

    it("renders the recorded time-travel pin status and reason on the span") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.pinned_mv", "refresh")
        span.recordTimeTravelPinStatus(TimeTravelPinStatus.Applied, TimeTravelPinReason.PinsResolved)
        span.complete("refreshed", "driver-pin")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      payloads.head.get("time_travel_pin_status").asText() shouldBe "APPLIED"
      payloads.head.get("time_travel_pin_reason").asText() shouldBe "pins_resolved"
    }

    it("omits time_travel_pin_status and reason when nothing recorded them") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.unrecorded_mv", "refresh")
        span.complete("refreshed", "driver-pin")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      payloads.head.has("time_travel_pin_status") shouldBe false
      payloads.head.has("time_travel_pin_reason") shouldBe false
    }

    it("reports a refusal reason alongside a COMPILE_FAILED status") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.refused_reason_mv", "create")
        span.recordTimeTravelPinStatus(
          TimeTravelPinStatus.CompileFailed,
          TimeTravelPinReason.UnsupportedPinShape
        )
        span.complete("created", "driver-pin")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      payloads.head.get("time_travel_pin_status").asText() shouldBe "COMPILE_FAILED"
      payloads.head.get("time_travel_pin_reason").asText() shouldBe "unsupported_pin_shape"
    }

    it("keeps a refused pin status and its reason sticky") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.refused_mv", "create")
        span.recordTimeTravelPinStatus(
          TimeTravelPinStatus.CompileFailed,
          TimeTravelPinReason.UnsupportedPinShape
        )
        span.recordTimeTravelPinStatus(TimeTravelPinStatus.NotApplicable, TimeTravelPinReason.NoUserPin)
        span.recordTimeTravelPinStatus(TimeTravelPinStatus.Applied, TimeTravelPinReason.PinsResolved)
        span.recordTimeTravelPinStatus(null)
        span.complete("created", "driver-pin")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      payloads.head.get("time_travel_pin_status").asText() shouldBe "COMPILE_FAILED"
      payloads.head.get("time_travel_pin_reason").asText() shouldBe "unsupported_pin_shape"
    }

    it("fails an unrecognised pin status closed instead of dropping it") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.unknown_pin_mv", "create")
        span.recordTimeTravelPinStatus("SOMETHING_ELSE")
        span.complete("created", "driver-pin")
        span.emitIfNeeded("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      payloads.head.get("time_travel_pin_status").asText() shouldBe "COMPILE_FAILED"
      payloads.head.get("time_travel_pin_reason").asText() shouldBe "unknown_pin_status"
    }

    it("records the pin status and reason on the active span for the current thread") {
      val payloads = withLogCapture { appender =>
        OpenIvmExecutionSpan.start("default.active_pin_mv", "create")
        OpenIvmExecutionSpan.recordActiveTimeTravelPinStatus(
          TimeTravelPinStatus.NotApplicable,
          TimeTravelPinReason.NoUserPin
        )
        OpenIvmExecutionSpan.finishActive("created", "driver-active")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      payloads.head.get("time_travel_pin_status").asText() shouldBe "NOT_APPLICABLE"
      payloads.head.get("time_travel_pin_reason").asText() shouldBe "no_user_pin"
    }

    it("reuses the same command span and does not re-emit for late async backup timing") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start(
          "default.reused_mv",
          "refresh",
          requestId = Some("req-reused"),
          dbtNodeId = Some("model.reused")
        )
        val reused = OpenIvmExecutionSpan.start(
          "default.reused_mv",
          "refresh",
          requestId = Some("ignored-request"),
          dbtNodeId = Some("ignored-node")
        )

        reused should be theSameInstanceAs span

        OpenIvmExecutionSpan.observeTimer("driver_admission.refresh.wait", millis(8L))
        span.complete("refresh_done", "driver-reused")
        span.emitIfNeeded("failed_before_end", "unused")
        OpenIvmExecutionSpan.observeRocksDbBackup(millis(21L))
        OpenIvmExecutionSpan.finishActive("failed_before_end", "unused")
        spanPayloads(appender.messages)
      }

      payloads should have size 1
      val payload = payloads.head
      payload.get("request_id").asText() shouldBe "req-reused"
      payload.get("dbt_node_id").asText() shouldBe "model.reused"
      payload.get("driver_admission_wait_ms").asLong() shouldBe 8L
      payload.has("rocksdb_backup_ms") shouldBe false
    }

    it("prefers openivm.node_id and falls back to spark.jobGroup.id for dbt_node_id") {
      val (requestId, explicitNodeId) = OpenIvmExecutionSpan.correlationIdsFromLookups(
        localPropertyLookup = key =>
          Map(
            "openivm.request_id" -> "req-local",
            "openivm.node_id"    -> "model.explicit",
            "spark.jobGroup.id"  -> "job-group-explicit"
          ).get(key),
        mdcLookup = _ => None
      )
      requestId shouldBe Some("req-local")
      explicitNodeId shouldBe Some("model.explicit")

      val (_, fallbackNodeId) = OpenIvmExecutionSpan.correlationIdsFromLookups(
        localPropertyLookup = key => Map("spark.jobGroup.id" -> "job-group-fallback").get(key),
        mdcLookup = _ => None
      )
      fallbackNodeId shouldBe Some("job-group-fallback")
    }

    it("keeps concurrent spans isolated across threads") {
      val payloads = withLogCapture { appender =>
        val pool = Executors.newFixedThreadPool(2)
        try {
          val refreshTask = new Runnable {
            override def run(): Unit = {
              withMdc("request_id" -> "req-refresh", "openivm.node_id" -> "model.refresh_mv") {
                val (requestId, dbtNodeId) = OpenIvmExecutionSpan.correlationIdsFromLookups(
                  localPropertyLookup = _ => None,
                  mdcLookup = key => Option(MDC.get(key))
                )
                OpenIvmExecutionSpan.observeTimer("driver_admission.refresh.wait", millis(9L))
                val span = OpenIvmExecutionSpan.start(
                  "default.concurrent_refresh_mv",
                  "refresh",
                  requestId = requestId,
                  dbtNodeId = dbtNodeId
                )
                span.recordProfileStep("acquire_locks", "thread=refresh-worker", 4L)
                span.complete("refresh_done", Thread.currentThread().getName)
                span.emitIfNeeded("failed_before_end", "unused")
              }
            }
          }
          val createTask = new Runnable {
            override def run(): Unit = {
              withMdc("openivm.request_id" -> "req-create", "spark.jobGroup.id" -> "job-group-create") {
                val (requestId, dbtNodeId) = OpenIvmExecutionSpan.correlationIdsFromLookups(
                  localPropertyLookup = _ => None,
                  mdcLookup = key => Option(MDC.get(key))
                )
                val span = OpenIvmExecutionSpan.start(
                  "default.concurrent_create_mv",
                  "create",
                  requestId = requestId,
                  dbtNodeId = dbtNodeId
                )
                OpenIvmExecutionSpan.observeTimer("driver_admission.create.wait", millis(6L))
                OpenIvmExecutionSpan.observeTimer("compiler.compile", millis(12L))
                span.complete("create_done", Thread.currentThread().getName)
                span.emitIfNeeded("failed_before_end", "unused")
              }
            }
          }

          val refreshFuture = pool.submit(refreshTask)
          val createFuture  = pool.submit(createTask)
          refreshFuture.get(30L, TimeUnit.SECONDS)
          createFuture.get(30L, TimeUnit.SECONDS)
        } finally {
          pool.shutdownNow()
        }
        spanPayloads(appender.messages)
      }

      payloads should have size 2
      val byView = payloads.map(node => node.get("materialized_view").asText() -> node).toMap

      val refresh = byView("default.concurrent_refresh_mv")
      refresh.get("request_id").asText() shouldBe "req-refresh"
      refresh.get("dbt_node_id").asText() shouldBe "model.refresh_mv"
      refresh.get("driver_admission_wait_ms").asLong() shouldBe 9L
      refresh.get("same_mv_lock_wait_ms").asLong() shouldBe 4L
      refresh.has("compiler_ms") shouldBe false

      val create = byView("default.concurrent_create_mv")
      create.get("request_id").asText() shouldBe "req-create"
      create.get("dbt_node_id").asText() shouldBe "job-group-create"
      create.get("driver_admission_wait_ms").asLong() shouldBe 6L
      create.get("compiler_ms").asLong() shouldBe 12L
      create.has("same_mv_lock_wait_ms") shouldBe false
    }
  }
}
