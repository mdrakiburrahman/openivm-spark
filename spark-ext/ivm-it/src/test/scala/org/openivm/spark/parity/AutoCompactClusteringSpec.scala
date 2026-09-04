package org.openivm.spark.parity

import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

/** Regression coverage for the Delta Auto Compact post-commit hook on clustered
  * MV data tables.
  *
  * Delta's clustered-table OPTIMIZE path is hard-wired to the hilbert curve
  * (`ClusteringStrategy.curve`), and `HilbertClustering.getClusteringExpression`
  * asserts `cols.size > 1`. Every auto-compaction of a table clustered by exactly
  * one column therefore dies with
  * `AssertionError: Cannot do Hilbert clustering by zero or one column!`. Delta
  * swallows post-commit hook failures, so the commit stands but the compaction
  * never runs — enabling `delta.autoOptimize.autoCompact` on such a table only
  * buys a wasted bin-packing pass and an ERROR per commit (22 of them in the
  * 78-model local canary).
  *
  * These probes pin: single-column `CLUSTER BY` MVs are created without the
  * property and log no hook failure, while zero-column and multi-column MVs keep
  * it; results, schema and commit versions are unchanged either way.
  *
  * The suite forces `spark.openivm.delta.autoCompact=true` (the shipped default,
  * which the test build turns off) and drops `minNumFiles` to 1 so a single CTAS
  * commit is enough to make Delta attempt a compaction.
  */
class AutoCompactClusteringSpec extends IvmParitySpecBase("auto-compact-clustering") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.openivm.delta.autoCompact"                -> "true",
    "spark.databricks.delta.autoCompact.minNumFiles" -> "1",
    "spark.databricks.delta.autoCompact.maxFileSize" -> "1048576",
    "spark.databricks.delta.optimize.minFileSize"    -> "1048576",
    "spark.databricks.delta.optimize.maxFileSize"    -> "1048576"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    // The test JVM pins `spark.databricks.delta.autoCompact.enabled=false`
    // (Settings.scala). That session conf outranks the table property in
    // `AutoCompact.getAutoCompactType`, so clear it and let the per-table
    // property decide — which is exactly what production does.
    spark.conf.unset("spark.databricks.delta.autoCompact.enabled")
  }

  private final class BufferingAppender(name: String)
      extends AbstractAppender(
        name,
        null,
        PatternLayout.createDefaultLayout(),
        false,
        Property.EMPTY_ARRAY
      ) {
    private val events = ArrayBuffer.empty[(String, Option[Throwable])]

    override def append(event: LogEvent): Unit =
      events.synchronized {
        events += ((event.getMessage.getFormattedMessage, Option(event.getThrown)))
      }

    def messages: Seq[String] = events.synchronized(events.toVector.map(_._1))

    def throwables: Seq[Throwable] = events.synchronized(events.toVector.flatMap(_._2))
  }

  @tailrec
  private def causeChain(t: Throwable, acc: List[Throwable] = Nil): List[Throwable] =
    Option(t.getCause) match {
      case Some(cause) if cause ne t => causeChain(cause, t :: acc)
      case _                         => (t :: acc).reverse
    }

  /** Every Auto-Compact post-commit hook failure observed during `body`, as
    * `<log message> :: <flattened throwable chain>` strings.
    */
  private def autoCompactFailures[A](body: => A): (A, Seq[String]) = {
    val appender = new BufferingAppender(s"acc-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    val result =
      try body
      finally {
        root.removeAppender(appender)
        appender.stop()
      }
    val fromMessages = appender.messages.filter { msg =>
      msg.contains("Auto Compaction failed") ||
      (msg.contains("post-commit hook") && msg.contains("Auto Compact"))
    }
    val fromThrowables = appender.throwables.flatMap(causeChain(_)).collect {
      case t if Option(t.getMessage).exists(_.contains("Hilbert clustering")) =>
        s"${t.getClass.getName}: ${t.getMessage}"
    }
    (result, fromMessages ++ fromThrowables)
  }

  private def deltaMetadataConfig(mvName: String): Map[String, String] = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
    DeltaLog.forTable(spark, id).update().metadata.configuration
  }

  private def deltaVersion(mvName: String): Long = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(mvName)
    DeltaLog.forTable(spark, id).update().version
  }

  private val AutoCompactProperty = "delta.autoOptimize.autoCompact"

  describe("Delta Auto Compact on clustered MV data tables") {
    it("creates a single-column CLUSTER BY MV without auto-compact and without a hook failure") {
      sql("CREATE TABLE acc_one_src (region STRING, day STRING, amount INT) USING DELTA")
      sql("INSERT INTO acc_one_src VALUES ('east','d1',10), ('west','d1',20)")
      sql("INSERT INTO acc_one_src VALUES ('east','d2',30), ('north','d2',40)")

      val viewBody =
        "SELECT region, day, SUM(amount) AS total FROM acc_one_src GROUP BY region, day"
      val (_, failures) = autoCompactFailures {
        sql(s"CREATE MATERIALIZED VIEW acc_mv_one CLUSTER BY (region) AS $viewBody")
      }

      withClue(s"Auto Compact hook failures during CREATE: ${failures.mkString(" | ")}") {
        failures shouldBe empty
      }
      deltaMetadataConfig("acc_mv_one") should not contain key(AutoCompactProperty)
      deltaVersion("acc_mv_one") shouldBe 0L
      spark.table("acc_mv_one").schema.fieldNames should contain allOf ("region", "day", "total")
      assertMvCorrect("acc_mv_one", viewBody)
    }

    it("refreshes a single-column CLUSTER BY MV without a hook failure on later commits") {
      sql("CREATE TABLE acc_refresh_src (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO acc_refresh_src VALUES ('east', 10), ('west', 20)")

      val viewBody = "SELECT region, SUM(amount) AS total FROM acc_refresh_src GROUP BY region"
      sql(s"CREATE MATERIALIZED VIEW acc_mv_refresh CLUSTER BY (region) AS $viewBody")
      val versionAfterCreate = deltaVersion("acc_mv_refresh")

      val (_, failures) = autoCompactFailures {
        sql("INSERT INTO acc_refresh_src VALUES ('east', 5), ('north', 40)")
        refreshMv("acc_mv_refresh")
      }

      withClue(s"Auto Compact hook failures during REFRESH: ${failures.mkString(" | ")}") {
        failures shouldBe empty
      }
      deltaMetadataConfig("acc_mv_refresh") should not contain key(AutoCompactProperty)
      deltaVersion("acc_mv_refresh") should be > versionAfterCreate
      assertMvCorrect("acc_mv_refresh", viewBody)
    }

    it("keeps auto-compact enabled for an unclustered MV") {
      sql("CREATE TABLE acc_none_src (region STRING, amount INT) USING DELTA")
      sql("INSERT INTO acc_none_src VALUES ('east', 10), ('west', 20)")

      val viewBody = "SELECT region, SUM(amount) AS total FROM acc_none_src GROUP BY region"
      val (_, failures) = autoCompactFailures {
        sql(s"CREATE MATERIALIZED VIEW acc_mv_none AS $viewBody")
      }

      withClue(s"Auto Compact hook failures during CREATE: ${failures.mkString(" | ")}") {
        failures shouldBe empty
      }
      deltaMetadataConfig("acc_mv_none").get(AutoCompactProperty) shouldBe Some("true")
      assertMvCorrect("acc_mv_none", viewBody)
    }

    it("keeps auto-compact enabled for a multi-column CLUSTER BY MV") {
      sql("CREATE TABLE acc_multi_src (region STRING, day STRING, amount INT) USING DELTA")
      sql("INSERT INTO acc_multi_src VALUES ('east','d1',10), ('west','d1',20)")
      sql("INSERT INTO acc_multi_src VALUES ('east','d2',30), ('north','d2',40)")

      val viewBody =
        "SELECT region, day, SUM(amount) AS total FROM acc_multi_src GROUP BY region, day"
      val (_, failures) = autoCompactFailures {
        sql(s"CREATE MATERIALIZED VIEW acc_mv_multi CLUSTER BY (region, day) AS $viewBody")
      }

      withClue(s"Auto Compact hook failures during CREATE: ${failures.mkString(" | ")}") {
        failures shouldBe empty
      }
      deltaMetadataConfig("acc_mv_multi").get(AutoCompactProperty) shouldBe Some("true")
      assertMvCorrect("acc_mv_multi", viewBody)
    }
  }
}
