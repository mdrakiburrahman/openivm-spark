package org.openivm.spark.parity

import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

/** Correctness coverage for the issue #13 per-type Delta layout levers
  * ([[org.openivm.spark.common.MvLayoutPolicy]]): with every `CLUSTER BY`
  * lever + the data-skipping-stats lever enabled, each refresh type must still
  * produce a `CREATE TABLE … CLUSTER BY (…)` DDL that Delta accepts AND stay
  * bag-equal to its view body (bidirectional `EXCEPT ALL`).
  *
  * This de-risks the SF10 `lc-all` experiment: the SIMPLE_PROJECTION probe key
  * is the projected output columns (openivm's rowid is DuckDB-internal, not a
  * stored Spark column), so clustering it exercises a previously-untested DDL
  * shape.
  */
class MvLayoutClusterParitySpec extends IvmParitySpecBase("lclc-smoke") with InterceptMode {

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.openivm.delta.layout.simpleProjection.enabled"  -> "true",
    "spark.openivm.delta.layout.aggregateGroup.enabled"    -> "true",
    "spark.openivm.delta.layout.window.enabled"            -> "true",
    "spark.openivm.delta.layout.recompute.enabled"         -> "true",
    "spark.openivm.delta.dataSkippingStatsColumns.enabled" -> "true"
  )

  describe("per-type CLUSTER BY layout levers (issue #13)") {
    it("SIMPLE_PROJECTION clusters on the projected columns and stays correct") {
      sql("CREATE TABLE IF NOT EXISTS lclc_users(user_id INT, name STRING, age INT) USING DELTA")
      sql("INSERT INTO lclc_users VALUES (1, 'Alice', 30), (2, 'Bob', 28)")
      sql("CREATE MATERIALIZED VIEW lclc_mv_sp AS SELECT name, age FROM lclc_users WHERE age > 20")
      sql("INSERT INTO lclc_users VALUES (3, 'Carol', 35), (4, 'Dave', 18)")
      refreshMv("lclc_mv_sp")
      assertMvCorrect("lclc_mv_sp", "SELECT name, age FROM lclc_users WHERE age > 20")
    }

    it("AGGREGATE_GROUP clusters on the GROUP BY key and stays correct") {
      sql("CREATE TABLE IF NOT EXISTS lclc_sales(k STRING, x INT) USING DELTA")
      sql("INSERT INTO lclc_sales VALUES ('a', 1), ('b', 2)")
      sql("CREATE MATERIALIZED VIEW lclc_mv_agg AS SELECT k, SUM(x) AS total FROM lclc_sales GROUP BY k")
      sql("INSERT INTO lclc_sales VALUES ('a', 5), ('c', 3)")
      refreshMv("lclc_mv_agg")
      assertMvCorrect("lclc_mv_agg", "SELECT k, SUM(x) AS total FROM lclc_sales GROUP BY k")
    }

    it("WINDOW_PARTITION clusters on the PARTITION BY key and stays correct") {
      sql("CREATE TABLE IF NOT EXISTS lclc_wsales(id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO lclc_wsales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 15)")
      sql(
        "CREATE MATERIALIZED VIEW lclc_mv_win AS SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM lclc_wsales"
      )
      sql("INSERT INTO lclc_wsales VALUES (4, 'east', 5), (5, 'west', 25)")
      refreshMv("lclc_mv_win")
      assertMvCorrect(
        "lclc_mv_win",
        "SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM lclc_wsales"
      )
    }

    it("DISTINCT (recompute family) clusters on the distinct key and stays correct") {
      sql("CREATE TABLE IF NOT EXISTS lclc_events(region STRING, kind STRING) USING DELTA")
      sql("INSERT INTO lclc_events VALUES ('east', 'a'), ('east', 'a'), ('west', 'b')")
      sql("CREATE MATERIALIZED VIEW lclc_mv_dist AS SELECT DISTINCT region, kind FROM lclc_events")
      sql("INSERT INTO lclc_events VALUES ('north', 'c'), ('east', 'a')")
      refreshMv("lclc_mv_dist")
      assertMvCorrect("lclc_mv_dist", "SELECT DISTINCT region, kind FROM lclc_events")
    }
  }
}
