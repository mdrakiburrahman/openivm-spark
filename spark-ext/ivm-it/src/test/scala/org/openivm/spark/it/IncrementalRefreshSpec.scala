package org.openivm.spark.it

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** End-to-end incremental-refresh verification.
  *
  * Each test creates a private base table + MV (so test bodies don't share
  * state), runs DML through Spark's real Delta path (so the DML interceptor
  * captures staging entries automatically), then issues
  * `REFRESH MATERIALIZED VIEW` and asserts the MV is equivalent to a fresh
  * recompute via bidirectional `EXCEPT ALL`.
  *
  * These tests require the OpenIVM DuckDB extension to be present at the
  * path indicated by `OPENIVM_EXTENSION_PATH`.
  */
class IncrementalRefreshSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-incr-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-IncrementalRefreshSpec")
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

  /** Bidirectional `EXCEPT ALL` equivalence check.  Projects `mv` to the same
    * column set as `expected` first so hidden bookkeeping columns (e.g.
    * `openivm_count_star` that openivm's initial-load query emits) don't
    * cause a column-count mismatch.
    */
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

  // ── Test 1: INSERT propagates to a SUM/COUNT MV ───────────────────────────
  describe("(1) Single INSERT → SUM/COUNT MV reflects new row") {
    it("incremental refresh adds the new region with correct sum and count") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_i1(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i1 VALUES ('east', 100), ('west', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i1 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_i1 GROUP BY region"
      )
      spark.sql("INSERT INTO sales_i1 VALUES ('north', 300), ('east', 50)")
      refreshMv("mv_i1")
      assertMvCorrect(
        "mv_i1",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_i1 GROUP BY region"
      )
    }
  }

  // ── Test 2: Multiple sequential INSERTs ──────────────────────────────────
  describe("(2) Multiple INSERTs → MV totals reflect every staged batch") {
    it("each batched INSERT registers a separate staging delta, single refresh consumes all") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_i2(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i2 VALUES ('a', 10)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i2 AS SELECT region, SUM(amount) AS total FROM sales_i2 GROUP BY region"
      )
      spark.sql("INSERT INTO sales_i2 VALUES ('a', 20), ('b', 5)")
      spark.sql("INSERT INTO sales_i2 VALUES ('a', 30), ('c', 7)")
      refreshMv("mv_i2")
      assertMvCorrect(
        "mv_i2",
        "SELECT region, SUM(amount) AS total FROM sales_i2 GROUP BY region"
      )
    }
  }

  // ── Test 3: DELETE removes / zeroes a group ──────────────────────────────
  describe("(3) DELETE → MV no longer reflects deleted rows") {
    it("incremental refresh drops the retracted group via the post-merge cleanup") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_i3(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i3 VALUES ('east', 100), ('west', 200), ('north', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i3 AS SELECT region, SUM(amount) AS total FROM sales_i3 GROUP BY region"
      )
      spark.sql("DELETE FROM sales_i3 WHERE region = 'north'")
      refreshMv("mv_i3")
      assertMvCorrect(
        "mv_i3",
        "SELECT region, SUM(amount) AS total FROM sales_i3 GROUP BY region"
      )
    }
  }

  // ── Test 4: UPDATE rewrites the value of an existing group ───────────────
  describe("(4) UPDATE → MV reflects the new aggregate value") {
    it("incremental refresh applies UPDATE_BEFORE/UPDATE_AFTER and the SUM moves") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_i4(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i4 VALUES ('east', 100), ('west', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i4 AS SELECT region, SUM(amount) AS total FROM sales_i4 GROUP BY region"
      )
      spark.sql("UPDATE sales_i4 SET amount = 250 WHERE region = 'west'")
      refreshMv("mv_i4")
      assertMvCorrect(
        "mv_i4",
        "SELECT region, SUM(amount) AS total FROM sales_i4 GROUP BY region"
      )
    }
  }

  // ── Test 5: Batched mixed DML (INSERT + DELETE + UPDATE) ─────────────────
  describe("(5) Batched INSERT + DELETE + UPDATE → single REFRESH yields the live aggregate") {
    it("incremental refresh reconciles all staged operations against the MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_i5(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i5 VALUES ('east', 100), ('west', 200), ('north', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i5 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_i5 GROUP BY region"
      )
      spark.sql("DELETE FROM sales_i5 WHERE region = 'north'")
      spark.sql("INSERT INTO sales_i5 VALUES ('east', 50), ('south', 300)")
      spark.sql("UPDATE sales_i5 SET amount = 250 WHERE region = 'west'")
      refreshMv("mv_i5")
      assertMvCorrect(
        "mv_i5",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_i5 GROUP BY region"
      )
    }
  }

  // ── Test 6: Empty delta — REFRESH immediately after CREATE ───────────────
  describe("(6) REFRESH with no pending delta → no-op, MV unchanged") {
    it("returns without error and the MV row set is identical to the initial load") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_i6(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i6 VALUES ('east', 100)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i6 AS SELECT region, SUM(amount) AS total FROM sales_i6 GROUP BY region"
      )
      val before = spark.table("mv_i6").collect().toSet
      refreshMv("mv_i6")
      val after = spark.table("mv_i6").collect().toSet
      after shouldBe before
    }
  }

  // ── Test 7: Source schema change → refresh fails fast ────────────────────
  describe("(7) Source schema change → REFRESH throws AnalysisException") {
    it("schema fingerprint mismatch is detected before any rewrite is executed") {
      import org.apache.spark.sql.AnalysisException

      spark.sql("CREATE TABLE IF NOT EXISTS sales_i7(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_i7 VALUES ('east', 100)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_i7 AS SELECT region, SUM(amount) AS total FROM sales_i7 GROUP BY region"
      )

      // Add a column to the source after CREATE — fingerprint will diverge.
      spark.sql("ALTER TABLE sales_i7 ADD COLUMN extra INT")

      // Insert so the staging catalog has at least one row, otherwise the
      // refresh command short-circuits before the fingerprint check.
      spark.sql("INSERT INTO sales_i7 VALUES ('west', 200, 1)")

      val ex = intercept[AnalysisException] { refreshMv("mv_i7") }
      ex.getMessage should include("schema")
    }
  }
}
