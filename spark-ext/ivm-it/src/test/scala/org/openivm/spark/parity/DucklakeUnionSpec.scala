package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_union.test`.
  *
  * UNION ALL under aggregation: per `openivm/CLAUDE.md` rule table,
  * UNION ALL routes through `IncrementalUnionRule` in
  * `src/rules/union.cpp`.  On DuckLake, the snapshot-diff source delta is
  * unioned across both sources and the aggregate-group rule applies.  On
  * Spark+Delta the same shape is reached via the AGGREGATE_GROUP path; the
  * source-side delta is detected via Delta versions (the Delta-equivalent
  * invariant).
  *
  * Sections mirror the source test:
  *   Basic UNION ALL — SUM/COUNT GROUP BY product across two sources;
  *     verify after CREATE and after INSERT into one source.
  *   Stress — batch INSERT into both sources, DELETE from one source,
  *     single refresh.
  */
class DucklakeUnionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dlu-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DucklakeUnionSpec")
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

  // ── Basic UNION ALL with SUM/COUNT aggregate ──────────────────────────────

  describe("Basic UNION ALL: SUM/COUNT GROUP BY product across two source tables") {
    it("INSERT into one source propagates through the UNION ALL to the aggregate") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlu_sales_online(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlu_sales_store(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      spark.sql(
        "INSERT INTO dlu_sales_online VALUES (1, 'Widget', 10.00), (2, 'Gadget', 20.00)"
      )
      spark.sql(
        "INSERT INTO dlu_sales_store VALUES (1, 'Widget', 15.00), (2, 'Gizmo', 30.00)"
      )

      val viewSql =
        "SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM (" +
          "SELECT product, amount FROM dlu_sales_online " +
          "UNION ALL " +
          "SELECT product, amount FROM dlu_sales_store) GROUP BY product"
      spark.sql(s"CREATE MATERIALIZED VIEW dlu_mv_all_sales AS $viewSql")
      refreshMv("dlu_mv_all_sales")
      assertMvCorrect("dlu_mv_all_sales", viewSql)

      // Insert into one source only — Widget total should bump
      spark.sql("INSERT INTO dlu_sales_online VALUES (3, 'Widget', 5.00)")
      refreshMv("dlu_mv_all_sales")
      assertMvCorrect("dlu_mv_all_sales", viewSql)
    }
  }

  // ── Stress: batch ops on both sources ──────────────────────────────────────

  describe("Stress: batch INSERT/DELETE on both UNION sources before one refresh") {
    it("delta consolidation across both UNION arms is correct") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlu_stress_online(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlu_stress_store(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      spark.sql(
        "INSERT INTO dlu_stress_online VALUES (1, 'Widget', 10.00), (2, 'Gadget', 20.00)"
      )
      spark.sql(
        "INSERT INTO dlu_stress_store VALUES (1, 'Widget', 15.00), (2, 'Gizmo', 30.00)"
      )
      val viewSql =
        "SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM (" +
          "SELECT product, amount FROM dlu_stress_online " +
          "UNION ALL " +
          "SELECT product, amount FROM dlu_stress_store) GROUP BY product"
      spark.sql(s"CREATE MATERIALIZED VIEW dlu_mv_stress AS $viewSql")
      refreshMv("dlu_mv_stress")
      assertMvCorrect("dlu_mv_stress", viewSql)

      // Batch on both sources.
      spark.sql("INSERT INTO dlu_stress_store VALUES (3, 'Gadget', 40.00)")
      spark.sql("DELETE FROM dlu_stress_online WHERE id = 2")
      spark.sql("INSERT INTO dlu_stress_online VALUES (4, 'Gizmo', 12.00)")
      refreshMv("dlu_mv_stress")
      assertMvCorrect("dlu_mv_stress", viewSql)
    }
  }
}
