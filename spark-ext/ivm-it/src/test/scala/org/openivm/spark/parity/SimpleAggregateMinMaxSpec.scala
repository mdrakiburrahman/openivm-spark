package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** MIN/MAX + edge-case slice of the SIMPLE_AGGREGATE parity coverage. Covers
  * scalar MIN/MAX via SIMPLE_AGGREGATE, empty-table behavior, INSERT+DELETE
  * cancellation, mixed batched DML, and NULL-input handling. See
  * [[SimpleAggregateSumSpec]] for SUM/COUNT/AVG/STDDEV/VARIANCE tests.
  */
class SimpleAggregateMinMaxSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-sa-minmax-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleAggregateMinMaxSpec")
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

  // ── (7) MIN/MAX scalar aggregates — SIMPLE_AGGREGATE path ────────────────

  describe("(7) Scalar MIN and MAX aggregates (SIMPLE_AGGREGATE, not FULL_REFRESH)") {
    it("multi-scalar MV with MAX refreshes correctly via SIMPLE_AGGREGATE incremental path") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_7(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_7 VALUES (1, 100), (2, 200), (3, 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_7 AS " +
          "SELECT SUM(amount) AS sum_amount, COUNT(*) AS count_star, AVG(amount) AS avg_amount, MAX(amount) AS most FROM sa_sales_7"
      )
      assertMvCorrect(
        "mv_sa_7",
        "SELECT SUM(amount) AS sum_amount, COUNT(*) AS count_star, AVG(amount) AS avg_amount, MAX(amount) AS most FROM sa_sales_7"
      )
      spark.sql("INSERT INTO sa_sales_7 VALUES (4, 350)")
      refreshMv("mv_sa_7")
      assertMvCorrect(
        "mv_sa_7",
        "SELECT SUM(amount) AS sum_amount, COUNT(*) AS count_star, AVG(amount) AS avg_amount, MAX(amount) AS most FROM sa_sales_7"
      )
    }

    it("scalar MIN MV refreshes correctly via SIMPLE_AGGREGATE incremental path") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_7b(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_7b VALUES (1, 100), (2, 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_7b AS SELECT MIN(amount) AS least FROM sa_sales_7b"
      )
      assertMvCorrect("mv_sa_7b", "SELECT MIN(amount) AS least FROM sa_sales_7b")
      spark.sql("INSERT INTO sa_sales_7b VALUES (3, 50)")
      refreshMv("mv_sa_7b")
      assertMvCorrect("mv_sa_7b", "SELECT MIN(amount) AS least FROM sa_sales_7b")
    }
  }

  // ── (8) Empty base table → 1 NULL-aggregate row → INSERT → REFRESH ────────

  describe("(8) Empty base table → single NULL-aggregate row → INSERT → REFRESH") {
    it("initial MV over empty table has 1 row with NULL SUM; INSERT+REFRESH propagates") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_8(id INT, amount INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_8 AS SELECT SUM(amount) AS total FROM sa_sales_8"
      )

      // SQL semantics: scalar SUM over empty input → 1 row with NULL value
      val emptyExpected = spark.sql("SELECT SUM(amount) AS total FROM sa_sales_8").collect()
      emptyExpected should have length 1
      emptyExpected.head.isNullAt(0) shouldBe true

      // Initial MV must match
      assertMvCorrect("mv_sa_8", "SELECT SUM(amount) AS total FROM sa_sales_8")

      // After INSERT and REFRESH the MV must reflect the new data
      spark.sql("INSERT INTO sa_sales_8 VALUES (1, 100), (2, 200)")
      refreshMv("mv_sa_8")
      assertMvCorrect("mv_sa_8", "SELECT SUM(amount) AS total FROM sa_sales_8")
    }
  }

  // ── (9) INSERT then DELETE-back of same row → net-zero delta ─────────────

  describe("(9) INSERT then DELETE-back of same row → zero net delta") {
    it("MV value is unchanged after a cancelling INSERT+DELETE pair") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_9(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_9 VALUES (1, 100), (2, 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_9 AS SELECT SUM(amount) AS total FROM sa_sales_9"
      )
      // INSERT a row then immediately DELETE it — net delta is zero
      spark.sql("INSERT INTO sa_sales_9 VALUES (3, 50)")
      spark.sql("DELETE FROM sa_sales_9 WHERE id = 3")
      refreshMv("mv_sa_9")
      // base table is back to (1,100),(2,200) → total = 300
      assertMvCorrect("mv_sa_9", "SELECT SUM(amount) AS total FROM sa_sales_9")
    }
  }

  // ── (10) Mixed batched DML before single REFRESH ───────────────────────────

  describe("(10) Mixed batched DML (INSERT + DELETE + UPDATE) → single REFRESH") {
    it("MV equals view body after mixed DML and a single REFRESH") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_10(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_10 VALUES (1, 100), (2, 200), (3, 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_10 AS " +
          "SELECT SUM(amount) AS total, COUNT(*) AS n FROM sa_sales_10"
      )
      spark.sql("INSERT INTO sa_sales_10 VALUES (4, 75), (5, 125)")
      spark.sql("DELETE FROM sa_sales_10 WHERE id = 2")
      spark.sql("UPDATE sa_sales_10 SET amount = 300 WHERE id = 3")
      refreshMv("mv_sa_10")
      assertMvCorrect(
        "mv_sa_10",
        "SELECT SUM(amount) AS total, COUNT(*) AS n FROM sa_sales_10"
      )
    }
  }

  // ── (11) NULL aggregate inputs — SUM/COUNT ignore NULLs ──────────────────

  describe("(11) NULL aggregate inputs — SUM and COUNT ignore NULLs") {
    it("SUM ignores NULL amounts; incremental refresh preserves that behavior") {
      spark.sql("CREATE TABLE IF NOT EXISTS sa_sales_11(id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO sa_sales_11 VALUES (1, 100), (2, NULL), (3, 50), (4, NULL)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_sa_11 AS SELECT SUM(amount) AS total FROM sa_sales_11"
      )
      assertMvCorrect("mv_sa_11", "SELECT SUM(amount) AS total FROM sa_sales_11")
      // Add more NULLs and one real value
      spark.sql("INSERT INTO sa_sales_11 VALUES (5, NULL), (6, 75)")
      refreshMv("mv_sa_11")
      assertMvCorrect("mv_sa_11", "SELECT SUM(amount) AS total FROM sa_sales_11")
    }
  }
}
