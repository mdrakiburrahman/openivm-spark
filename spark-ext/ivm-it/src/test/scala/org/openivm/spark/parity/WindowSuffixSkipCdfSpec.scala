package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class WindowSuffixSkipCdfSpec extends IvmParitySpecBase("window-suffix-skip") with CdfMode {

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

  private def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"wss-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("append-only WINDOW_PARTITION suffix skip") {

    it("inserts only strict suffix rows and remains correct") {
      sql("CREATE TABLE wss_sales_suffix(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wss_sales_suffix VALUES " +
          "(1,'east',10), (2,'east',20), (3,'west',5), (4,'west',15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "LAG(amount) OVER (PARTITION BY region ORDER BY id) AS prev_amount " +
          "FROM wss_sales_suffix"
      sql(s"CREATE MATERIALIZED VIEW wss_mv_suffix AS $viewSql")
      mvRefreshType("wss_mv_suffix") shouldBe RefreshTypeCode.WindowPartition

      sql("INSERT INTO wss_sales_suffix VALUES (5,'east',30), (6,'west',25), (7,'north',1)")
      val perfLines = withLogCapture { appender =>
        refreshMv("wss_mv_suffix")
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`wss_mv_suffix`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + perfLines.mkString("\n") + "\n") {
        perfLines.exists(_.contains("stmt_kind='window_suffix_aux_skipped'")) shouldBe true
        perfLines.exists(_.contains("stmt_kind='window_suffix_delete_skipped'")) shouldBe true
      }
      assertMvCorrect("wss_mv_suffix", viewSql)
    }

    it("falls back to partition recompute when any affected partition is not a strict suffix") {
      sql("CREATE TABLE wss_sales_fallback(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wss_sales_fallback VALUES " +
          "(1,'east',10), (3,'east',30), (4,'west',5), (5,'west',15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn " +
          "FROM wss_sales_fallback"
      sql(s"CREATE MATERIALIZED VIEW wss_mv_fallback AS $viewSql")
      mvRefreshType("wss_mv_fallback") shouldBe RefreshTypeCode.WindowPartition

      sql("INSERT INTO wss_sales_fallback VALUES (2,'east',20), (6,'west',25)")
      val perfLines = withLogCapture { appender =>
        refreshMv("wss_mv_fallback")
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`wss_mv_fallback`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + perfLines.mkString("\n") + "\n") {
        perfLines.exists(_.contains("outcome='window_suffix_skip'")) shouldBe false
      }
      assertMvCorrect("wss_mv_fallback", viewSql)
    }
  }
}
