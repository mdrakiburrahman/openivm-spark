package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Additive-monoid slice of the SIMPLE_AGGREGATE parity coverage (SUM, COUNT,
  * AVG, STDDEV, STDDEV_POP, VARIANCE / VAR_POP). See
  * [[SimpleAggregateMinMaxSpec]] for MIN/MAX, empty-table, NULL-handling and
  * cancellation scenarios.
  */
class SimpleAggregateSumSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-sa-sum-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleAggregateSumSpec")
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

  // ── Helpers ────────────────────────────────────────────────────────────────

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

  // ── (1) SUM scalar aggregate ───────────────────────────────────────────────

  describe("(1) SELECT SUM(amount) AS total FROM sales") {
    it("incremental refresh correctly updates the single-row SUM aggregate") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_1(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_1 VALUES (1, 100), (2, 200), (3, 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_1 AS SELECT SUM(amount) AS total FROM sa_sales_1"
      )
      assertMvCorrect("mv_sa_1", "SELECT SUM(amount) AS total FROM sa_sales_1")
      spark.sql("INSERT INTO sa_sales_1 VALUES (4, 150)")
      refreshMv("mv_sa_1")
      assertMvCorrect("mv_sa_1", "SELECT SUM(amount) AS total FROM sa_sales_1")
    }
  }

  // ── (2) COUNT(*) scalar aggregate ─────────────────────────────────────────

  describe("(2) SELECT COUNT(*) AS n FROM sales") {
    it("incremental refresh correctly updates the single-row COUNT(*) aggregate") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_2(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_2 VALUES (1, 100), (2, 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_2 AS SELECT COUNT(*) AS n FROM sa_sales_2"
      )
      assertMvCorrect("mv_sa_2", "SELECT COUNT(*) AS n FROM sa_sales_2")
      spark.sql("INSERT INTO sa_sales_2 VALUES (3, 300)")
      spark.sql("DELETE FROM sa_sales_2 WHERE id = 1")
      refreshMv("mv_sa_2")
      assertMvCorrect("mv_sa_2", "SELECT COUNT(*) AS n FROM sa_sales_2")
    }
  }

  // ── (3) COUNT(column) — counts non-NULL values only ───────────────────────

  describe("(3) SELECT COUNT(amount) AS n FROM sales") {
    it("incremental refresh correctly counts non-NULL values after DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_3(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_3 VALUES (1, 100), (2, NULL), (3, 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_3 AS SELECT COUNT(amount) AS n FROM sa_sales_3"
      )
      assertMvCorrect("mv_sa_3", "SELECT COUNT(amount) AS n FROM sa_sales_3")
      spark.sql("INSERT INTO sa_sales_3 VALUES (4, NULL), (5, 75)")
      refreshMv("mv_sa_3")
      assertMvCorrect("mv_sa_3", "SELECT COUNT(amount) AS n FROM sa_sales_3")
    }
  }

  // ── (4) AVG scalar aggregate ───────────────────────────────────────────────

  describe("(4) SELECT AVG(amount) AS avg_amount FROM sales") {
    it("refresh stays correct after INSERT (openivm reformulates AVG as SUM/COUNT)") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_4(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_4 VALUES (1, 100), (2, 200), (3, 300)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_4 AS SELECT AVG(amount) AS avg_amount FROM sa_sales_4"
      )
      assertMvCorrect("mv_sa_4", "SELECT AVG(amount) AS avg_amount FROM sa_sales_4")
      spark.sql("INSERT INTO sa_sales_4 VALUES (4, 400)")
      refreshMv("mv_sa_4")
      assertMvCorrect("mv_sa_4", "SELECT AVG(amount) AS avg_amount FROM sa_sales_4")
    }
  }

  // ── (5) STDDEV sample + population ────────────────────────────────────────

  describe("(5) STDDEV and STDDEV_POP scalar aggregates") {
    it("STDDEV (sample) refresh stays correct after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_5a(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_5a VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_5a AS SELECT STDDEV(amount) AS sd FROM sa_sales_5a"
      )
      assertMvCorrect("mv_sa_5a", "SELECT STDDEV(amount) AS sd FROM sa_sales_5a")
      spark.sql("INSERT INTO sa_sales_5a VALUES (5, 50)")
      refreshMv("mv_sa_5a")
      assertMvCorrect("mv_sa_5a", "SELECT STDDEV(amount) AS sd FROM sa_sales_5a")
    }

    it("STDDEV_POP (population) refresh stays correct after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_5b(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_5b VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_5b AS SELECT STDDEV_POP(amount) AS sd_pop FROM sa_sales_5b"
      )
      assertMvCorrect("mv_sa_5b", "SELECT STDDEV_POP(amount) AS sd_pop FROM sa_sales_5b")
      spark.sql("INSERT INTO sa_sales_5b VALUES (5, 50)")
      refreshMv("mv_sa_5b")
      assertMvCorrect("mv_sa_5b", "SELECT STDDEV_POP(amount) AS sd_pop FROM sa_sales_5b")
    }
  }

  // ── (6) VARIANCE and VAR_POP ──────────────────────────────────────────────

  describe("(6) VARIANCE and VAR_POP scalar aggregates") {
    it("VARIANCE (sample) and VAR_POP (population) refresh correctly after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_6(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_6 VALUES (1, 10), (2, 20), (3, 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_6 AS " +
          "SELECT VARIANCE(amount) AS var_samp, VAR_POP(amount) AS var_pop FROM sa_sales_6"
      )
      assertMvCorrect(
        "mv_sa_6",
        "SELECT VARIANCE(amount) AS var_samp, VAR_POP(amount) AS var_pop FROM sa_sales_6"
      )
      spark.sql("INSERT INTO sa_sales_6 VALUES (4, 40)")
      refreshMv("mv_sa_6")
      assertMvCorrect(
        "mv_sa_6",
        "SELECT VARIANCE(amount) AS var_samp, VAR_POP(amount) AS var_pop FROM sa_sales_6"
      )
    }
  }
}
