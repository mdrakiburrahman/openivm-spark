package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.slf4j.MDC
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
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
        OpenIvmExecutionSpan.observeRocksDbCommit(millis(11L))
        span.complete("refresh_executed", "driver-1")
        OpenIvmExecutionSpan.observeRocksDbCommit(millis(99L))
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
      payload.get("rocksdb_flush_ms").asLong() shouldBe 11L
      payload.get("rocksdb_backup_ms").asLong() shouldBe 17L
      payload.get("duration_ms").asLong() should be >= 0L
      payload.get("engine_started_at").asText() should endWith("Z")
      payload.get("engine_completed_at").asText() should endWith("Z")
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
