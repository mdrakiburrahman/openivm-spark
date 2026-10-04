package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** running-window suffix-extend refresh (`windowRunningIncremental`).
  *
  * Exercises the gated cumulative-window fast path: a daily-market-shaped MV
  * (`MIN/MAX(v) OVER (PARTITION BY k ORDER BY d)`) whose insert-only batch adds
  * strictly-later `d` rows per partition is refreshed by extending the
  * persisted running value rather than recomputing the partition; a backdated
  * insert falls back to the full WINDOW_PARTITION recompute. Both paths must
  * stay bag-equal to the live window query (`assertMvCorrect`) and the MV must
  * remain WINDOW_PARTITION (never demoted to FULL_REFRESH).
  */
abstract class WindowRunningIncrementalScenarios extends IvmParitySpecBase("window-running-incremental") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowRunningIncrementalEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey                 -> "true"
    )

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  private val viewSql =
    "SELECT dm_date, dm_s_symb, " +
      "MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_low, " +
      "MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_high " +
      "FROM wri_daily_market"

  describe("WINDOW_PARTITION running-aggregate suffix extension") {
    it("extends the running MIN/MAX from persisted state on a strictly-later insert-only batch") {
      sql("CREATE TABLE wri_daily_market(dm_date DATE, dm_s_symb STRING, dm_low INT, dm_high INT) USING DELTA")
      sql(
        "INSERT INTO wri_daily_market VALUES " +
          "(DATE '2024-01-01','AAA',10,20),(DATE '2024-01-02','AAA',8,22)," +
          "(DATE '2024-01-01','BBB',15,25),(DATE '2024-01-02','BBB',14,26)"
      )
      sql(s"CREATE MATERIALIZED VIEW wri_mv AS $viewSql")
      assertMvCorrect("wri_mv", viewSql)
      mvRefreshType("wri_mv") shouldBe RefreshTypeCode.WindowPartition

      // Suffix-append: later dates for existing partitions + a brand-new partition.
      sql(
        "INSERT INTO wri_daily_market VALUES " +
          "(DATE '2024-01-03','AAA',7,24),(DATE '2024-01-03','BBB',13,27),(DATE '2024-01-01','CCC',30,40)"
      )
      refreshMv("wri_mv")
      assertMvCorrect("wri_mv", viewSql)
      mvRefreshType("wri_mv") shouldBe RefreshTypeCode.WindowPartition
      val executedSql = sql("SHOW OPENIVM QUERY LOG").select("sql_text").collect().map(_.getString(0))
      executedSql.exists(_.contains("CACHE TABLE `openivm_run_result_wri_mv`")) shouldBe true
    }

    it("falls back to full recompute for a backdated partition and stays correct") {
      sql("CREATE TABLE wri_bd_daily_market(dm_date DATE, dm_s_symb STRING, dm_low INT, dm_high INT) USING DELTA")
      sql(
        "INSERT INTO wri_bd_daily_market VALUES " +
          "(DATE '2024-02-01','AAA',10,20),(DATE '2024-02-05','AAA',8,22)"
      )
      val bdSql =
        "SELECT dm_date, dm_s_symb, " +
          "MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_low, " +
          "MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_high " +
          "FROM wri_bd_daily_market"
      sql(s"CREATE MATERIALIZED VIEW wri_bd_mv AS $bdSql")
      assertMvCorrect("wri_bd_mv", bdSql)

      // Backdated row (2024-02-03 lands BEFORE the existing 2024-02-05 max) +
      // a clean suffix row (2024-02-09): per-partition split must stay correct.
      sql(
        "INSERT INTO wri_bd_daily_market VALUES " +
          "(DATE '2024-02-03','AAA',3,30),(DATE '2024-02-09','AAA',9,21)"
      )
      refreshMv("wri_bd_mv")
      assertMvCorrect("wri_bd_mv", bdSql)
      mvRefreshType("wri_bd_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("stays correct across two consecutive suffix-append batches") {
      sql("CREATE TABLE wri_seq_daily_market(dm_date DATE, dm_s_symb STRING, dm_low INT, dm_high INT) USING DELTA")
      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-01','AAA',10,20)")
      val seqSql =
        "SELECT dm_date, dm_s_symb, " +
          "MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_low, " +
          "MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_high " +
          "FROM wri_seq_daily_market"
      sql(s"CREATE MATERIALIZED VIEW wri_seq_mv AS $seqSql")

      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-02','AAA',6,25)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)

      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-03','AAA',12,18)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)
      mvRefreshType("wri_seq_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("handles the real daily_market window-over-window shape (cumulative MIN/MAX -> flag CASE -> cumulative MAX)") {
      sql(
        "CREATE TABLE wri_wow_market(dm_date DATE, dm_s_symb STRING, dm_close INT, dm_high INT, dm_low INT, " +
          "dm_vol INT) USING DELTA"
      )
      sql(
        "INSERT INTO wri_wow_market VALUES " +
          "(DATE '2024-01-01','AAA',10,20,5,100),(DATE '2024-01-02','AAA',11,22,4,110)," +
          "(DATE '2024-01-01','BBB',30,40,15,200),(DATE '2024-01-02','BBB',31,38,16,210)"
      )
      val wowSql =
        "WITH cumulative AS (" +
          "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, " +
          "MIN(dm_low) OVER w AS f52_low, MAX(dm_high) OVER w AS f52_high " +
          "FROM wri_wow_market WINDOW w AS (PARTITION BY dm_s_symb ORDER BY dm_date)), " +
          "flagged AS (SELECT *, " +
          "CASE WHEN dm_low = f52_low THEN dm_date ELSE NULL END AS low_flag, " +
          "CASE WHEN dm_high = f52_high THEN dm_date ELSE NULL END AS high_flag FROM cumulative) " +
          "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, f52_low, f52_high, " +
          "MAX(low_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS f52_low_date, " +
          "MAX(high_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS f52_high_date FROM flagged"
      sql(s"CREATE MATERIALIZED VIEW wri_wow_mv AS $wowSql")
      assertMvCorrect("wri_wow_mv", wowSql)
      mvRefreshType("wri_wow_mv") shouldBe RefreshTypeCode.WindowPartition

      // Strictly-later insert-only batch (fast path) + a brand-new partition.
      sql(
        "INSERT INTO wri_wow_market VALUES " +
          "(DATE '2024-01-03','AAA',9,24,3,120),(DATE '2024-01-03','BBB',33,36,17,220)," +
          "(DATE '2024-01-01','CCC',50,60,25,300)"
      )
      refreshMv("wri_wow_mv")
      assertMvCorrect("wri_wow_mv", wowSql)
      mvRefreshType("wri_wow_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("keeps a downstream MV correct via the window fast path's cascade view-delta") {
      sql("CREATE TABLE wri_casc_market(dm_date DATE, dm_s_symb STRING, dm_low INT, dm_high INT) USING DELTA")
      sql(
        "INSERT INTO wri_casc_market VALUES " +
          "(DATE '2024-01-01','AAA',10,20),(DATE '2024-01-02','AAA',8,22)," +
          "(DATE '2024-01-01','BBB',15,25),(DATE '2024-01-02','BBB',14,26)"
      )
      val upstreamSql =
        "SELECT dm_date, dm_s_symb, " +
          "MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_low, " +
          "MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_high " +
          "FROM wri_casc_market"
      sql(s"CREATE MATERIALIZED VIEW wri_casc_win AS $upstreamSql")
      // Downstream MV consumes the window MV's view-delta (depth-2 MV-over-MV chain).
      val downstreamSql =
        "SELECT dm_s_symb, COUNT(*) AS n, MIN(wk_low) AS lo, MAX(wk_high) AS hi FROM wri_casc_win GROUP BY dm_s_symb"
      sql(s"CREATE MATERIALIZED VIEW wri_casc_agg AS $downstreamSql")
      assertMvCorrect("wri_casc_win", upstreamSql)
      assertMvCorrect("wri_casc_agg", downstreamSql)

      // Strictly-later insert-only batch → window fast path → cascade view-delta → downstream stays incremental.
      sql(
        "INSERT INTO wri_casc_market VALUES " +
          "(DATE '2024-01-03','AAA',7,24),(DATE '2024-01-03','BBB',13,27),(DATE '2024-01-01','CCC',30,40)"
      )
      refreshMv("wri_casc_win")
      refreshMv("wri_casc_agg")
      assertMvCorrect("wri_casc_win", upstreamSql)
      assertMvCorrect("wri_casc_agg", downstreamSql)
      mvRefreshType("wri_casc_win") shouldBe RefreshTypeCode.WindowPartition

      // The fallback delta and materialised suffix must both reach the child.
      sql(
        "INSERT INTO wri_casc_market VALUES " +
          "(DATE '2024-01-01','AAA',2,50),(DATE '2024-01-04','BBB',12,28)"
      )
      refreshMv("wri_casc_win")
      refreshMv("wri_casc_agg")
      assertMvCorrect("wri_casc_win", upstreamSql)
      assertMvCorrect("wri_casc_agg", downstreamSql)
    }

    it("preserves nullable cumulative SUM and AVG seeds across consecutive suffix batches") {
      sql("CREATE TABLE wri_avg_source(k STRING, t INT, v DOUBLE) USING DELTA")
      sql("INSERT INTO wri_avg_source VALUES ('A',1,NULL),('B',1,0.125),('B',2,0.25)")
      val query =
        "SELECT k,t,v,SUM(v) OVER (PARTITION BY k ORDER BY t) AS total," +
          "AVG(v) OVER (PARTITION BY k ORDER BY t) AS mean FROM wri_avg_source"
      sql(s"CREATE MATERIALIZED VIEW wri_avg_mv AS $query")
      assertMvCorrect("wri_avg_mv", query)
      sql("INSERT INTO wri_avg_source VALUES ('A',2,4.0),('B',3,0.375),('C',1,NULL)")
      refreshMv("wri_avg_mv")
      assertMvCorrect("wri_avg_mv", query)
      sql("INSERT INTO wri_avg_source VALUES ('A',3,NULL),('B',4,0.5),('C',2,2.0)")
      refreshMv("wri_avg_mv")
      assertMvCorrect("wri_avg_mv", query)
      mvRefreshType("wri_avg_mv") shouldBe RefreshTypeCode.WindowPartition
    }
  }
}
