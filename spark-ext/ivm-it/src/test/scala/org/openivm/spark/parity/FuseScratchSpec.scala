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

/** Verifies the scratch-CTAS fuse fast path emits `fused='true'` on
  * stmt_kind='view_delta_ctas' for leaf SIMPLE_PROJECTION MVs, falls back to
  * the on-disk scratch path when there is a downstream MV consumer, and
  * preserves correctness in both retract and insert-only scenarios.
  */
class FuseScratchSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-fuse-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FuseScratchSpec")
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

  describe("scratch-CTAS fuse fast path") {

    it("uses the fused path for a leaf SIMPLE_PROJECTION MV and preserves correctness on insert-only deltas") {
      spark.sql("CREATE TABLE fuse_users_a(id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO fuse_users_a VALUES (1, 'Alice', 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW fuse_mv_leaf AS " +
          "SELECT id, name FROM fuse_users_a WHERE age >= 25"
      )
      spark.sql("INSERT INTO fuse_users_a VALUES (2, 'Bob', 28), (3, 'Carol', 35)")

      val lines = withLogCapture { appender =>
        spark.sql("REFRESH MATERIALIZED VIEW fuse_mv_leaf").collect()
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

    it("uses the on-disk scratch path (no fused breadcrumb) when an MV-over-MV chain depends on it") {
      spark.sql("CREATE TABLE fuse_users_b(id INT, name STRING, age INT) USING DELTA")
      spark.sql("INSERT INTO fuse_users_b VALUES (1, 'Alice', 30), (2, 'Bob', 22)")
      spark.sql(
        "CREATE MATERIALIZED VIEW fuse_mv_upstream AS " +
          "SELECT id, name, age FROM fuse_users_b WHERE age >= 18"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW fuse_mv_downstream AS " +
          "SELECT id, name FROM fuse_mv_upstream WHERE age >= 25"
      )
      spark.sql("INSERT INTO fuse_users_b VALUES (3, 'Carol', 40)")

      val lines = withLogCapture { appender =>
        spark.sql("REFRESH MATERIALIZED VIEW fuse_mv_upstream").collect()
        appender.messages.filter(m => m.startsWith("[openivm-perf] ") && m.contains("view='`fuse_mv_upstream`'"))
      }

      withClue("captured [openivm-perf] lines:\n" + lines.mkString("\n") + "\n") {
        val ctasLine = lines.find(l => l.contains("phase='stmt'") && l.contains("stmt_kind='view_delta_ctas'"))
        ctasLine should not be empty
        ctasLine.get should not include "fused='true'"
      }
      spark.sql("REFRESH MATERIALIZED VIEW fuse_mv_downstream").collect()
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
      spark.sql("CREATE TABLE fuse_users_c(id INT, name STRING, age INT) USING DELTA")
      spark.sql(
        "INSERT INTO fuse_users_c VALUES (1, 'Alice', 30), (2, 'Bob', 28), (3, 'Carol', 35)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW fuse_mv_retract AS " +
          "SELECT id, name FROM fuse_users_c WHERE age >= 25"
      )
      // Trigger refresh #1 (initial baseline already at CREATE time, but cycle deltas)
      spark.sql("INSERT INTO fuse_users_c VALUES (4, 'Dave', 50)")
      spark.sql("DELETE FROM fuse_users_c WHERE id = 2")

      val lines = withLogCapture { appender =>
        spark.sql("REFRESH MATERIALIZED VIEW fuse_mv_retract").collect()
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
