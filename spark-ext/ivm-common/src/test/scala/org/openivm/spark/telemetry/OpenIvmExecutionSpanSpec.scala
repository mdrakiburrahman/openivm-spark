package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
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

  describe("OpenIvmExecutionSpan") {
    it("emits a completed span with isolated metrics and optional backup timing") {
      val payloads = withLogCapture { appender =>
        val span = OpenIvmExecutionSpan.start("default.sales_mv", "refresh", Some("req-123"))
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
      payload.get("materialized_view").asText() shouldBe "default.sales_mv"
      payload.get("operation").asText() shouldBe "refresh"
      payload.get("driver_thread").asText() shouldBe "driver-1"
      payload.get("outcome").asText() shouldBe "refresh_executed"
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
      payload.get("materialized_view").asText() shouldBe "default.fail_mv"
      payload.get("operation").asText() shouldBe "create"
      payload.get("driver_thread").asText() shouldBe "driver-create"
      payload.get("outcome").asText() shouldBe "failed_before_end"
      payload.get("driver_admission_wait_ms").asLong() shouldBe 2L
      payload.has("compiler_ms") shouldBe false
    }

    it("keeps concurrent spans isolated across threads") {
      val payloads = withLogCapture { appender =>
        val pool = Executors.newFixedThreadPool(2)
        try {
          val refreshTask = new Runnable {
            override def run(): Unit = {
              OpenIvmExecutionSpan.observeTimer("driver_admission.refresh.wait", millis(9L))
              val span = OpenIvmExecutionSpan.start("default.concurrent_refresh_mv", "refresh", Some("req-refresh"))
              span.recordProfileStep("acquire_locks", "thread=refresh-worker", 4L)
              span.complete("refresh_done", Thread.currentThread().getName)
              span.emitIfNeeded("failed_before_end", "unused")
            }
          }
          val createTask = new Runnable {
            override def run(): Unit = {
              val span = OpenIvmExecutionSpan.start("default.concurrent_create_mv", "create", Some("req-create"))
              OpenIvmExecutionSpan.observeTimer("driver_admission.create.wait", millis(6L))
              OpenIvmExecutionSpan.observeTimer("compiler.compile", millis(12L))
              span.complete("create_done", Thread.currentThread().getName)
              span.emitIfNeeded("failed_before_end", "unused")
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
      refresh.get("driver_admission_wait_ms").asLong() shouldBe 9L
      refresh.get("same_mv_lock_wait_ms").asLong() shouldBe 4L
      refresh.has("compiler_ms") shouldBe false

      val create = byView("default.concurrent_create_mv")
      create.get("request_id").asText() shouldBe "req-create"
      create.get("driver_admission_wait_ms").asLong() shouldBe 6L
      create.get("compiler_ms").asLong() shouldBe 12L
      create.has("same_mv_lock_wait_ms") shouldBe false
    }
  }
}
