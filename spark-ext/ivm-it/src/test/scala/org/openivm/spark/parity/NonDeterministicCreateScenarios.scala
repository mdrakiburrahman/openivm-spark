package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{IvmParityMode, IvmParitySpecBase}

/** Time-dependent queries must use full refresh, so results are reevaluated for every source row.
  * Spark's expression-level rejection of unsafe incremental plans is covered by `MvDeterminismSpec`.
  */
abstract class NonDeterministicCreateScenarios extends IvmParitySpecBase("non-deterministic-create") {
  self: IvmParityMode =>

  private def refreshType(name: String): Int =
    MvCatalog
      .lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name))
      .map(_.refreshType)
      .getOrElse(fail(s"MV $name not found in catalog"))

  private def assertRecomputed(mv: String, query: String, insert: String): Unit = {
    sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
    refreshType(mv) shouldBe RefreshTypeCode.FullRefresh
    assertMvCorrect(mv, query)
    sql(insert)
    refreshMv(mv)
    assertMvCorrect(mv, query)
  }

  describe("CREATE MATERIALIZED VIEW determinism guard") {

    it("fully refreshes a view with a time-dependent date expression") {
      sql("CREATE TABLE nd_date_src (k INT, d DATE, v BIGINT) USING DELTA")
      sql("INSERT INTO nd_date_src VALUES (1, current_date(), 10), (1, DATE '2020-01-01', 20), (2, current_date(), 5)")
      assertRecomputed(
        "nd_date_mv",
        "SELECT k, SUM(CASE WHEN d >= current_date() THEN v ELSE 0 END) AS today_total, COUNT(*) AS n " +
          "FROM nd_date_src GROUP BY k",
        "INSERT INTO nd_date_src VALUES (3, current_date(), 7)"
      )
    }

    it("fully refreshes a view with a time-dependent timestamp expression") {
      sql("CREATE TABLE nd_ts_src (k INT, ts TIMESTAMP, v BIGINT) USING DELTA")
      sql(
        "INSERT INTO nd_ts_src VALUES (1, TIMESTAMP '2020-01-01 00:00:00', 10), (2, TIMESTAMP '2999-01-01 00:00:00', 5)"
      )
      assertRecomputed(
        "nd_ts_mv",
        "SELECT k, SUM(CASE WHEN ts <= current_timestamp() THEN v ELSE 0 END) AS elapsed_total, COUNT(*) AS n " +
          "FROM nd_ts_src GROUP BY k",
        "INSERT INTO nd_ts_src VALUES (3, TIMESTAMP '2020-01-01 00:00:00', 7)"
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
