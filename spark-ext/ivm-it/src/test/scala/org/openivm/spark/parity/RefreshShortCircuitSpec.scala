package org.openivm.spark.parity

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class RefreshShortCircuitSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-rsc-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-RefreshShortCircuitSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
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

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def withLogCapture[A](body: BufferingAppender => A): A = {
    val appender = new BufferingAppender(s"rsc-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try body(appender)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def timedRefresh(name: String): Long = {
    val started = System.nanoTime()
    refreshMv(name)
    System.nanoTime() - started
  }

  private def noPendingFor(logs: Seq[String], viewName: String): Boolean =
    logs.exists(msg => msg.contains(s"view='`$viewName`'") && msg.contains("outcome='no_pending_deltas'"))

  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  describe("incremental REFRESH no-op short-circuit") {
    it("skips analyzed refresh SQL when no staging rows are pending") {
      spark.sql("CREATE TABLE IF NOT EXISTS rsc_users(id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO rsc_users VALUES (1, 'Alice')")
      spark.sql("CREATE MATERIALIZED VIEW rsc_mv AS SELECT id, name FROM rsc_users")
      spark.sql("INSERT INTO rsc_users VALUES (2, 'Bob')")

      val firstNanos = withLogCapture { appender =>
        val elapsed = timedRefresh("rsc_mv")
        noPendingFor(appender.messages, "rsc_mv") shouldBe false
        elapsed
      }
      assertMvCorrect("rsc_mv", "SELECT id, name FROM rsc_users")

      val secondNanos = withLogCapture { appender =>
        val elapsed = timedRefresh("rsc_mv")
        noPendingFor(appender.messages, "rsc_mv") shouldBe true
        elapsed
      }
      withClue(s"first refresh ${firstNanos}ns, second refresh ${secondNanos}ns: ") {
        secondNanos should be < (firstNanos / 2)
      }

      spark.sql("INSERT INTO rsc_users VALUES (3, 'Carol')")
      withLogCapture { appender =>
        refreshMv("rsc_mv")
        noPendingFor(appender.messages, "rsc_mv") shouldBe false
      }
      assertMvCorrect("rsc_mv", "SELECT id, name FROM rsc_users")
    }
  }
}
