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

/** Verifies the `[openivm-perf]` breadcrumb emitted when a positive-only
  * SIMPLE_PROJECTION refresh skips the delete MERGE.
  */
class InsertOnlyTelemetrySpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-iot-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-InsertOnlyTelemetrySpec")
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

  describe("insert-only SIMPLE_PROJECTION telemetry") {

    it("emits a zero-time merge_skipped statement block and remains correct") {
      spark.sql("CREATE TABLE iot_users_insert(id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO iot_users_insert VALUES (1, 'Alice', 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW iot_mv_insert AS " +
          "SELECT id, name FROM iot_users_insert WHERE age >= 25"
      )
      spark.sql("INSERT INTO iot_users_insert VALUES (2, 'Bob', 28), (3, 'Carol', 35)")

      val perfLines = withLogCapture { appender =>
        spark.sql("REFRESH MATERIALIZED VIEW iot_mv_insert").collect()
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
