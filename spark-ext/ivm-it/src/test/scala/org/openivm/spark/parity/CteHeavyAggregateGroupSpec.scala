package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy-test isolation spin-off of [[CteSpec]] section CTE-1's slow
  * "classifies as AggregateGroup and refreshes incrementally" test.
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `cte_heavy_aggrp_*` so two
  * parallel JVMs (this one and the host `CteSpec`) cannot collide on Delta
  * paths.
  */
class CteHeavyAggregateGroupSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cte-heavy-aggrp-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-CteHeavyAggregateGroupSpec")
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

  // ── CTE Shape 1: AGGREGATE_GROUP (RefreshType 0) ─────────────────────────

  describe("CTE-1: CTE wrapping AGGREGATE_GROUP → RefreshType 0") {

    it("classifies as AggregateGroup and refreshes incrementally") {
      spark.sql("CREATE TABLE IF NOT EXISTS cte_heavy_aggrp_sales(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO cte_heavy_aggrp_sales VALUES ('east', 50), ('west', 200), ('north', 10)")
      spark.sql(
        """CREATE MATERIALIZED VIEW mv_cte_heavy_aggrp AS
          |WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100)
          |SELECT region, SUM(amount) AS total FROM big GROUP BY region""".stripMargin
      )

      mvRefreshType("mv_cte_heavy_aggrp") shouldBe RefreshTypeCode.AggregateGroup

      // INSERT: new rows above/below the CTE filter
      spark.sql("INSERT INTO cte_heavy_aggrp_sales VALUES ('east', 300), ('east', 20), ('south', 500)")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )

      // DELETE: remove rows that were inside the CTE's filter
      spark.sql("DELETE FROM cte_heavy_aggrp_sales WHERE region = 'south'")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )

      // UPDATE: bring a row from below the filter threshold to above it
      spark.sql("UPDATE cte_heavy_aggrp_sales SET amount = 150 WHERE region = 'north'")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )

      // Batched mix: INSERT + DELETE + UPDATE in one refresh cycle
      spark.sql("INSERT INTO cte_heavy_aggrp_sales VALUES ('west', 999)")
      spark.sql("DELETE FROM cte_heavy_aggrp_sales WHERE region = 'east' AND amount = 20")
      spark.sql("UPDATE cte_heavy_aggrp_sales SET amount = 1 WHERE region = 'north'")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )
    }
  }
}
