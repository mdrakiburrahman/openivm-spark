package org.openivm.spark.parity

import org.apache.spark.sql.AnalysisException
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{IvmParityMode, IvmParitySpecBase}

/** CREATE MATERIALIZED VIEW rejects an incrementally maintained query whose result depends on
  * evaluation time or randomness: maintaining it incrementally re-evaluates the expression only for
  * changed rows, so the view would diverge from a recompute of its query. Views recomputed on every
  * refresh stay accepted. Detection of each random expression is covered by `MvDeterminismSpec`;
  * Spark itself rejects them inside aggregate arguments, and OpenIVM recomputes them elsewhere.
  */
abstract class NonDeterministicCreateScenarios extends IvmParitySpecBase("non-deterministic-create") {
  self: IvmParityMode =>

  private val AllowKey = "spark.openivm.create.allowNonDeterministic"

  private def refreshType(name: String): Int =
    MvCatalog
      .lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name))
      .map(_.refreshType)
      .getOrElse(fail(s"MV $name not found in catalog"))

  private def withNonDeterministicAllowed[T](body: => T): T = {
    spark.conf.set(AllowKey, "true")
    try body
    finally spark.conf.unset(AllowKey)
  }

  /** Creates `mv` with the escape hatch, proves it is maintained incrementally, drops it, and
    * asserts CREATE without the escape hatch rejects it naming `expression`.
    */
  private def assertRejectedIncremental(mv: String, query: String, expression: String): Unit = {
    withNonDeterministicAllowed(sql(s"CREATE MATERIALIZED VIEW $mv AS $query"))
    refreshType(mv) should not be RefreshTypeCode.FullRefresh
    sql(s"DROP MATERIALIZED VIEW $mv")

    val error = intercept[AnalysisException](sql(s"CREATE MATERIALIZED VIEW $mv AS $query"))
    error.getMessage should include(expression)
    error.getMessage should include("diverge from a recompute")
    error.getMessage should include(AllowKey)
    MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(mv)) shouldBe None
    spark.catalog.tableExists(mv) shouldBe false
  }

  describe("CREATE MATERIALIZED VIEW determinism guard") {

    it("rejects a time-dependent expression in an incrementally maintained view") {
      sql("CREATE TABLE nd_date_src (k INT, d DATE, v BIGINT) USING DELTA")
      sql("INSERT INTO nd_date_src VALUES (1, current_date(), 10), (1, DATE '2020-01-01', 20), (2, current_date(), 5)")
      assertRejectedIncremental(
        "nd_date_mv",
        "SELECT k, SUM(CASE WHEN d >= current_date() THEN v ELSE 0 END) AS today_total, COUNT(*) AS n " +
          "FROM nd_date_src GROUP BY k",
        "current_date"
      )
    }

    it("rejects current_timestamp() in an incrementally maintained view") {
      sql("CREATE TABLE nd_ts_src (k INT, ts TIMESTAMP, v BIGINT) USING DELTA")
      sql(
        "INSERT INTO nd_ts_src VALUES (1, TIMESTAMP '2020-01-01 00:00:00', 10), (2, TIMESTAMP '2999-01-01 00:00:00', 5)"
      )
      assertRejectedIncremental(
        "nd_ts_mv",
        "SELECT k, SUM(CASE WHEN ts <= current_timestamp() THEN v ELSE 0 END) AS elapsed_total, COUNT(*) AS n " +
          "FROM nd_ts_src GROUP BY k",
        "current_timestamp"
      )
    }

    it("accepts a random expression in a view that is recomputed on every refresh") {
      sql("CREATE TABLE nd_rand_src (k INT, v BIGINT) USING DELTA")
      sql("INSERT INTO nd_rand_src VALUES (1, 10), (2, 5)")
      sql("CREATE MATERIALIZED VIEW nd_rand_mv AS SELECT k, v, random() AS r FROM nd_rand_src")
      refreshType("nd_rand_mv") shouldBe RefreshTypeCode.FullRefresh
      sql("INSERT INTO nd_rand_src VALUES (3, 1)")
      refreshMv("nd_rand_mv")
      assertMvCorrect("nd_rand_mv", "SELECT k, v FROM nd_rand_src")
    }

    it("accepts deterministic views, including window functions and order-dependent aggregates") {
      sql("CREATE TABLE nd_det_src (k INT, v BIGINT) USING DELTA")
      sql("INSERT INTO nd_det_src VALUES (1, 10), (1, 20), (2, 5)")
      val aggregate =
        "SELECT k, SUM(v) AS total, array_sort(collect_list(v)) AS vs FROM nd_det_src GROUP BY k"
      val window =
        "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn, " +
          "last_value(v, true) OVER (PARTITION BY k ORDER BY v) AS last_v FROM nd_det_src"
      sql(s"CREATE MATERIALIZED VIEW nd_det_agg_mv AS $aggregate")
      sql(s"CREATE MATERIALIZED VIEW nd_det_win_mv AS $window")
      refreshType("nd_det_win_mv") should not be RefreshTypeCode.FullRefresh

      sql("INSERT INTO nd_det_src VALUES (2, 7), (3, 1)")
      refreshMv("nd_det_agg_mv")
      refreshMv("nd_det_win_mv")
      assertMvCorrect("nd_det_agg_mv", aggregate, arrayCols = Set("vs"))
      assertMvCorrect("nd_det_win_mv", window)
    }

    it("accepts a time-dependent view that is recomputed on every refresh") {
      sql("CREATE TABLE nd_full_src (k INT, v BIGINT) USING DELTA")
      sql("INSERT INTO nd_full_src VALUES (1, 10)")
      sql("CREATE MATERIALIZED VIEW nd_full_mv AS SELECT k, v, current_timestamp() AS observed_at FROM nd_full_src")
      refreshType("nd_full_mv") shouldBe RefreshTypeCode.FullRefresh
      sql("INSERT INTO nd_full_src VALUES (2, 20)")
      refreshMv("nd_full_mv")
      assertMvCorrect("nd_full_mv", "SELECT k, v FROM nd_full_src")
    }
  }
}
