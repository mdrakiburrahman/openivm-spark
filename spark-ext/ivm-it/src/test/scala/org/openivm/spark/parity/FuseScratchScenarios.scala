package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

import org.openivm.spark.common.ChangeFeedMode

/** Verifies the scratch-CTAS fuse fast path emits `fused='true'` on
  * stmt_kind='view_delta_ctas' for SIMPLE_PROJECTION MVs, reuses the cached
  * view-delta as an MV-over-MV cascade input, and preserves correctness in
  * both retract and insert-only scenarios.
  */
abstract class FuseScratchScenarios extends IvmParitySpecBase("fuse-scratch") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    if (changeFeedMode == ChangeFeedMode.Cdf)
      Map(
        // CDF must fuse automatically without either intercept-only opt-in.
        "spark.openivm.fuseScratch.enabled"              -> "false",
        "spark.openivm.fuseScratch.cascadeCache.enabled" -> "false"
      )
    else
      Map(
        "spark.openivm.fuseScratch.enabled"              -> "true",
        "spark.openivm.fuseScratch.cascadeCache.enabled" -> "true"
      )

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
    val appender = new BufferingAppender(s"fuse-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  describe("scratch-CTAS fuse fast path") {

    it("uses the fused path for a leaf SIMPLE_PROJECTION MV and preserves correctness on insert-only deltas") {
      sql("CREATE TABLE fuse_users_a(id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO fuse_users_a VALUES (1, 'Alice', 30)")
      sql(
        "CREATE MATERIALIZED VIEW fuse_mv_leaf AS " +
          "SELECT id, name FROM fuse_users_a WHERE age >= 25"
      )
      sql("INSERT INTO fuse_users_a VALUES (2, 'Bob', 28), (3, 'Carol', 35)")

      val lines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW fuse_mv_leaf").collect()
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`fuse_mv_leaf`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + lines.mkString("\n") + "\n") {
        lines.exists { line =>
          line.contains("phase='stmt'") &&
          line.contains("stmt_kind='view_delta_ctas'") &&
          line.contains("fused='true'")
        } shouldBe true
      }
      assertMvCorrect("fuse_mv_leaf", "SELECT id, name FROM fuse_users_a WHERE age >= 25")
    }

    it("uses the fused scratch cache as the cascade input when an MV-over-MV chain depends on it") {
      sql("CREATE TABLE fuse_users_b(id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO fuse_users_b VALUES (1, 'Alice', 30), (2, 'Bob', 22)")
      sql(
        "CREATE MATERIALIZED VIEW fuse_mv_upstream AS " +
          "SELECT id, name, age FROM fuse_users_b WHERE age >= 18"
      )
      sql(
        "CREATE MATERIALIZED VIEW fuse_mv_downstream AS " +
          "SELECT id, name FROM fuse_mv_upstream WHERE age >= 25"
      )
      sql("INSERT INTO fuse_users_b VALUES (3, 'Carol', 40)")

      val lines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW fuse_mv_upstream").collect()
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`fuse_mv_upstream`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + lines.mkString("\n") + "\n") {
        lines.exists { line =>
          line.contains("phase='stmt'") &&
          line.contains("stmt_kind='view_delta_ctas'") &&
          line.contains("fused='true'")
        } shouldBe true
      }
      sql("REFRESH MATERIALIZED VIEW fuse_mv_downstream").collect()
      assertMvCorrect(
        "fuse_mv_upstream",
        "SELECT id, name, age FROM fuse_users_b WHERE age >= 18"
      )
      assertMvCorrect(
        "fuse_mv_downstream",
        "SELECT id, name FROM fuse_users_b WHERE age >= 25"
      )
    }

    it("uses the fused path correctly when a DELETE produces a negative-multiplicity retract") {
      sql("CREATE TABLE fuse_users_c(id INT, name STRING, age INT) USING DELTA")
      sql(
        "INSERT INTO fuse_users_c VALUES (1, 'Alice', 30), (2, 'Bob', 28), (3, 'Carol', 35)"
      )
      sql(
        "CREATE MATERIALIZED VIEW fuse_mv_retract AS " +
          "SELECT id, name FROM fuse_users_c WHERE age >= 25"
      )
      // Trigger refresh #1 (initial baseline already at CREATE time, but cycle deltas)
      sql("INSERT INTO fuse_users_c VALUES (4, 'Dave', 50)")
      sql("DELETE FROM fuse_users_c WHERE id = 2")

      val lines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW fuse_mv_retract").collect()
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`fuse_mv_retract`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + lines.mkString("\n") + "\n") {
        lines.exists { line =>
          line.contains("phase='stmt'") &&
          line.contains("stmt_kind='view_delta_ctas'") &&
          line.contains("fused='true'")
        } shouldBe true
      }
      assertMvCorrect("fuse_mv_retract", "SELECT id, name FROM fuse_users_c WHERE age >= 25")
    }
  }
}
