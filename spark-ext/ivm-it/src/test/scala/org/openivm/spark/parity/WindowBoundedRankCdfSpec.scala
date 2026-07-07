package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class WindowBoundedRankCdfSpec extends IvmParitySpecBase("window-bounded-rank") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.BoundedRankEnabledKey -> "true")

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
    val appender = new BufferingAppender(s"wbr-${UUID.randomUUID()}")
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

  describe("bounded top-K WINDOW_PARTITION ranking") {

    it("recomputes only affected top-K ranking candidates and remains correct") {
      sql("CREATE TABLE wbr_scores(id INT, region STRING, score INT) USING DELTA")
      sql(
        "INSERT INTO wbr_scores VALUES " +
          "(1,'east',100), (2,'east',90), (3,'east',80), " +
          "(4,'west',50), (5,'west',40), (6,'west',30)"
      )
      val viewSql =
        "SELECT id, region, score, rn FROM (" +
          "SELECT id, region, score, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY score DESC) AS rn " +
          "FROM wbr_scores" +
          ") ranked WHERE rn <= 2"
      sql(s"CREATE MATERIALIZED VIEW wbr_mv_scores AS $viewSql")
      mvRefreshType("wbr_mv_scores") shouldBe RefreshTypeCode.WindowPartition

      sql("INSERT INTO wbr_scores VALUES (7,'east',95), (8,'west',45)")
      val perfLines = withLogCapture { appender =>
        refreshMv("wbr_mv_scores")
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`wbr_mv_scores`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + perfLines.mkString("\n") + "\n") {
        perfLines.exists(_.contains("outcome='bounded_rank_topk'")) shouldBe true
        perfLines.exists(_.contains("stmt_kind='bounded_rank_aux_skipped'")) shouldBe true
      }
      assertMvCorrect("wbr_mv_scores", viewSql)
    }
  }
}
