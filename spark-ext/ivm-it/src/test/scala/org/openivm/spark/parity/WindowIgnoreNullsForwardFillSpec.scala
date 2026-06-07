package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class WindowIgnoreNullsForwardFillSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-winnf-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-WindowIgnoreNullsForwardFillSpec")
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

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
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

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def mvRefreshType(name: String): Int =
    MvCatalog
      .list(spark)
      .find(_.name.table == name)
      .getOrElse(fail(s"MV $name not found in catalog"))
      .refreshType

  describe("last_value(expr, true) forward-fill over nullable rows") {
    it("stays WindowPartition and bag-equal after initial load and refresh") {
      spark.sql("CREATE TABLE winnf_customer_src (customer_id INT, effective_ts TIMESTAMP, status STRING) USING DELTA")
      spark.sql(
        "INSERT INTO winnf_customer_src VALUES " +
          "(1, TIMESTAMP'2024-01-01 00:00:00', 'bronze'), " +
          "(1, TIMESTAMP'2024-02-01 00:00:00', NULL), " +
          "(1, TIMESTAMP'2024-03-01 00:00:00', 'silver'), " +
          "(2, TIMESTAMP'2024-01-15 00:00:00', NULL), " +
          "(2, TIMESTAMP'2024-02-15 00:00:00', 'starter')"
      )

      val mvName = "winnf_mv_customer"
      val viewSql =
        "SELECT customer_id, effective_ts, status, " +
          "last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts) AS carried_status, " +
          "coalesce(status, last_value(status, true) OVER (PARTITION BY customer_id ORDER BY effective_ts)) AS filled_status " +
          "FROM winnf_customer_src"
      spark.sql(s"CREATE MATERIALIZED VIEW $mvName AS $viewSql")

      mvRefreshType(mvName) shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect(mvName, viewSql)

      spark.sql(
        "INSERT INTO winnf_customer_src VALUES " +
          "(1, TIMESTAMP'2024-04-01 00:00:00', NULL), " +
          "(2, TIMESTAMP'2024-03-15 00:00:00', NULL), " +
          "(3, TIMESTAMP'2024-01-20 00:00:00', 'new')"
      )
      refreshMv(mvName)

      mvRefreshType(mvName) shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect(mvName, viewSql)
    }
  }
}
