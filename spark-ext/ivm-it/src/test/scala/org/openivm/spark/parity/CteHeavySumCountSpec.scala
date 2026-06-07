package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy-test isolation spin-off of [[CteSpec]] section CTE-4's
  * "SUM + COUNT(*) on a single CTE source refreshes correctly" test.
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `cte_heavy_sumcnt_*` so two
  * parallel JVMs (this one and the host `CteSpec`) cannot collide on Delta
  * paths.
  */
class CteHeavySumCountSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cte-heavy-sumcnt-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-CteHeavySumCountSpec")
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

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── CTE Shape 4: CTE referenced in multiple aggregate expressions ─────────

  describe("CTE-4: CTE referenced in multiple aggregate expressions") {

    it("SUM + COUNT(*) on a single CTE source refreshes correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS cte_heavy_sumcnt_sales(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO cte_heavy_sumcnt_sales VALUES ('east', 100), ('west', 200), ('east', 50)")
      spark.sql(
        """CREATE MATERIALIZED VIEW mv_cte_heavy_sumcnt AS
          |WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales)
          |SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region""".stripMargin
      )

      mvRefreshType("mv_cte_heavy_sumcnt") shouldBe RefreshTypeCode.AggregateGroup

      // INSERT
      spark.sql("INSERT INTO cte_heavy_sumcnt_sales VALUES ('east', 75), ('north', 300)")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )

      // DELETE
      spark.sql("DELETE FROM cte_heavy_sumcnt_sales WHERE region = 'north'")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )

      // UPDATE
      spark.sql("UPDATE cte_heavy_sumcnt_sales SET amount = 500 WHERE region = 'west'")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )

      // Batched mix
      spark.sql("INSERT INTO cte_heavy_sumcnt_sales VALUES ('south', 800)")
      spark.sql("DELETE FROM cte_heavy_sumcnt_sales WHERE region = 'east' AND amount = 50")
      spark.sql("UPDATE cte_heavy_sumcnt_sales SET amount = 100 WHERE region = 'west'")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )
    }
  }
}
