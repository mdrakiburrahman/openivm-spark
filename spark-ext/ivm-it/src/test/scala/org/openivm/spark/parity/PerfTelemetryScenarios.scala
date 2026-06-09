package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/** Verifies that REFRESH MATERIALIZED VIEW emits the structured
  * `[openivm-perf]` timing lines that the SF10 perf analyser depends on.
  *
  * The exact line shape is contract: a downstream parser splits on the
  * single space between key=value pairs.  If you change the line layout
  * (add fields, change quoting), update the matching parser too.
  */
abstract class PerfTelemetryScenarios extends IvmParitySpecBase("perf-telemetry") {
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
    val appender = new BufferingAppender(s"perftel-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  protected def perfLinesFor(logs: Seq[String], view: String): Seq[String] =
    logs.filter(m => m.startsWith("[openivm-perf] ") && m.contains(s"view='`$view`'"))

  /** Extract `phase='<name>'` token value from a [openivm-perf] line, if any. */
  protected def phaseOf(line: String): Option[String] = {
    val marker = "phase='"
    val s      = line.indexOf(marker)
    if (s < 0) None
    else {
      val from = s + marker.length
      val to   = line.indexOf('\'', from)
      if (to < 0) None else Some(line.substring(from, to))
    }
  }

  describe("[openivm-perf] telemetry emission") {

    it("emits start, schema_resolve, compile, register_views, rewrite, stmt(s), post_cleanup, end") {
      sql("DROP TABLE IF EXISTS perftel_users")
      sql("DROP MATERIALIZED VIEW IF EXISTS perftel_mv")
      sql("CREATE TABLE perftel_users(id INT, name STRING) USING DELTA")
      sql("INSERT INTO perftel_users VALUES (1, 'Alice')")
      sql("CREATE MATERIALIZED VIEW perftel_mv AS SELECT id, name FROM perftel_users")
      sql("INSERT INTO perftel_users VALUES (2, 'Bob'), (3, 'Carol')")

      val perfLines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW perftel_mv").collect()
        perfLinesFor(appender.messages, "perftel_mv")
      }

      withClue("captured [openivm-perf] lines:\n" + perfLines.mkString("\n") + "\n") {
        perfLines should not be empty

        val phases = perfLines.flatMap(phaseOf).toSet
        phases should contain("start")
        phases should contain("collect_staging")
        phases should contain("schema_resolve")
        phases should contain("compile")
        phases should contain("register_views")
        phases should contain("rewrite")
        phases should contain("stmt")
        phases should contain("post_cleanup")
        phases should contain("end")

        // start line carries thread='...'
        perfLines.find(phaseOf(_).contains("start")).get should include("thread='")
        // compile line carries compile_cache_hit=true|false
        perfLines.find(phaseOf(_).contains("compile")).get should include regex
          "compile_cache_hit=(true|false)"
        // every stmt line carries stmt_idx, stmt_kind, elapsed_ms
        perfLines.filter(phaseOf(_).contains("stmt")).foreach { ln =>
          ln should include regex "stmt_idx=\\d+"
          ln should include regex "stmt_kind='\\w+'"
          ln should include regex "elapsed_ms=\\d+"
        }
        // end line carries total_ms, refresh_type, outcome, pending_deltas
        val end = perfLines.find(phaseOf(_).contains("end")).get
        end should include regex "total_ms=\\d+"
        end should include regex "refresh_type='[A-Z_]+'"
        end should include regex "outcome='\\w+'"
        end should include regex "pending_deltas=\\d+"

        // refresh_id is a single UUID-shaped token shared by every line of this refresh.
        val refreshIds = perfLines.flatMap { ln =>
          val m = "refresh_id='"
          val s = ln.indexOf(m)
          if (s < 0) None
          else {
            val from = s + m.length
            val to   = ln.indexOf('\'', from)
            if (to < 0) None else Some(ln.substring(from, to))
          }
        }.toSet
        refreshIds.size shouldBe 1
        // refresh_id is shaped `<view>_<nanotime>` (see RefreshProfile.start).
        refreshIds.head should fullyMatch regex "[A-Za-z0-9_.]+_\\d+"
      }
    }

    it("emits end with outcome='no_pending_deltas' when nothing to refresh") {
      sql("DROP TABLE IF EXISTS perftel_users_nop")
      sql("DROP MATERIALIZED VIEW IF EXISTS perftel_mv_nop")
      sql("CREATE TABLE perftel_users_nop(id INT, name STRING) USING DELTA")
      sql("INSERT INTO perftel_users_nop VALUES (1, 'Alice')")
      sql("CREATE MATERIALIZED VIEW perftel_mv_nop AS SELECT id, name FROM perftel_users_nop")

      val perfLines = withLogCapture { appender =>
        sql("REFRESH MATERIALIZED VIEW perftel_mv_nop").collect()
        perfLinesFor(appender.messages, "perftel_mv_nop")
      }

      withClue("captured [openivm-perf] lines:\n" + perfLines.mkString("\n") + "\n") {
        val end = perfLines.find(phaseOf(_).contains("end"))
        end shouldBe defined
        end.get should include("outcome='no_pending_deltas'")
      }
    }
  }
}
