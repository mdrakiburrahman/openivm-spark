package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** P5.2 — running-window suffix-extend refresh (`windowRunningIncremental`).
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
    Map(FeatureGate.WindowRunningIncrementalEnabledKey -> "true")

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
  }
}
