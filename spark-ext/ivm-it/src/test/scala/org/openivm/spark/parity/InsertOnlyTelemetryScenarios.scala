package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/** Verifies the `[openivm-perf]` breadcrumb emitted when a positive-only
  * SIMPLE_PROJECTION refresh skips the delete MERGE.
  */
abstract class InsertOnlyTelemetryScenarios extends IvmParitySpecBase("insert-only-telemetry") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  private final class BufferingAppender(name: String)
      extends AbstractAppender(
        name,
        null,
        PatternLayout.createDefaultLayout(),
        false,
        Property.EMPTY_ARRAY
      ) {
    protected val buffer = ArrayBuffer.empty[String]

    override def append(event: LogEvent): Unit =
      buffer.synchronized {
        buffer += event.getMessage.getFormattedMessage
      }

    def messages: Seq[String] = buffer.synchronized(buffer.toVector)
  }

  protected def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"iot-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  describe("insert-only SIMPLE_PROJECTION telemetry") {

    it("emits a zero-time merge_skipped statement block and remains correct") {
      sql("CREATE TABLE iot_users_insert(id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO iot_users_insert VALUES (1, 'Alice', 30)")
      sql(
        "CREATE MATERIALIZED VIEW iot_mv_insert AS " +
          "SELECT id, name FROM iot_users_insert WHERE age >= 25"
      )
      sql("INSERT INTO iot_users_insert VALUES (2, 'Bob', 28), (3, 'Carol', 35)")

      val perfLines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW iot_mv_insert").collect()
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`iot_mv_insert`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + perfLines.mkString("\n") + "\n") {
        perfLines.exists { line =>
          line.contains("phase='stmt'") &&
          line.contains("stmt_kind='merge_skipped'") &&
          line.contains("elapsed_ms=0")
        } shouldBe true
      }
      assertMvCorrect("iot_mv_insert", "SELECT id, name FROM iot_users_insert WHERE age >= 25")
    }
  }
}
