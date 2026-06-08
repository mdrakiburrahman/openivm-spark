package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

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
abstract class CteHeavySumCountScenarios extends IvmParitySpecBase("cte-heavy-sum-count") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── CTE Shape 4: CTE referenced in multiple aggregate expressions ─────────

  describe("CTE-4: CTE referenced in multiple aggregate expressions") {

    it("SUM + COUNT(*) on a single CTE source refreshes correctly") {
      sql("CREATE TABLE IF NOT EXISTS cte_heavy_sumcnt_sales(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO cte_heavy_sumcnt_sales VALUES ('east', 100), ('west', 200), ('east', 50)")
      sql(
        """CREATE MATERIALIZED VIEW mv_cte_heavy_sumcnt AS
          |WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales)
          |SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region""".stripMargin
      )

      mvRefreshType("mv_cte_heavy_sumcnt") shouldBe RefreshTypeCode.AggregateGroup

      // INSERT
      sql("INSERT INTO cte_heavy_sumcnt_sales VALUES ('east', 75), ('north', 300)")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )

      // DELETE
      sql("DELETE FROM cte_heavy_sumcnt_sales WHERE region = 'north'")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )

      // UPDATE
      sql("UPDATE cte_heavy_sumcnt_sales SET amount = 500 WHERE region = 'west'")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )

      // Batched mix
      sql("INSERT INTO cte_heavy_sumcnt_sales VALUES ('south', 800)")
      sql("DELETE FROM cte_heavy_sumcnt_sales WHERE region = 'east' AND amount = 50")
      sql("UPDATE cte_heavy_sumcnt_sales SET amount = 100 WHERE region = 'west'")
      refreshMv("mv_cte_heavy_sumcnt")
      assertMvCorrect(
        "mv_cte_heavy_sumcnt",
        "WITH t1 AS (SELECT region, amount FROM cte_heavy_sumcnt_sales) SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM t1 GROUP BY region"
      )
    }
  }
}
