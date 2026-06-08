package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

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
abstract class CteHeavyAggregateGroupScenarios extends IvmParitySpecBase("cte-heavy-aggregate-group") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── CTE Shape 1: AGGREGATE_GROUP (RefreshType 0) ─────────────────────────

  describe("CTE-1: CTE wrapping AGGREGATE_GROUP → RefreshType 0") {

    it("classifies as AggregateGroup and refreshes incrementally") {
      sql("CREATE TABLE IF NOT EXISTS cte_heavy_aggrp_sales(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO cte_heavy_aggrp_sales VALUES ('east', 50), ('west', 200), ('north', 10)")
      sql(
        """CREATE MATERIALIZED VIEW mv_cte_heavy_aggrp AS
          |WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100)
          |SELECT region, SUM(amount) AS total FROM big GROUP BY region""".stripMargin
      )

      mvRefreshType("mv_cte_heavy_aggrp") shouldBe RefreshTypeCode.AggregateGroup

      // INSERT: new rows above/below the CTE filter
      sql("INSERT INTO cte_heavy_aggrp_sales VALUES ('east', 300), ('east', 20), ('south', 500)")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )

      // DELETE: remove rows that were inside the CTE's filter
      sql("DELETE FROM cte_heavy_aggrp_sales WHERE region = 'south'")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )

      // UPDATE: bring a row from below the filter threshold to above it
      sql("UPDATE cte_heavy_aggrp_sales SET amount = 150 WHERE region = 'north'")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )

      // Batched mix: INSERT + DELETE + UPDATE in one refresh cycle
      sql("INSERT INTO cte_heavy_aggrp_sales VALUES ('west', 999)")
      sql("DELETE FROM cte_heavy_aggrp_sales WHERE region = 'east' AND amount = 20")
      sql("UPDATE cte_heavy_aggrp_sales SET amount = 1 WHERE region = 'north'")
      refreshMv("mv_cte_heavy_aggrp")
      assertMvCorrect(
        "mv_cte_heavy_aggrp",
        "WITH big AS (SELECT * FROM cte_heavy_aggrp_sales WHERE amount > 100) SELECT region, SUM(amount) AS total FROM big GROUP BY region"
      )
    }
  }
}
