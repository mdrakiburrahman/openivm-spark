package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class WindowSinglePassReplaceCdfSpec extends IvmParitySpecBase("window-single-pass-replace") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey                -> "true",
      FeatureGate.ProfileRefreshKey                 -> "true"
    )

  private def ranNetChangeProbe(viewName: String): Boolean =
    RefreshProfileCatalog
      .scanAll(spark)
      .filter(_.viewName.split("\\.").last == viewName)
      .exists(_.detail == "phase=cdf_window_net_change_probe")

  describe("WINDOW_PARTITION single-pass replacement with a named affected-key view") {
    it("creates the affected-key view before collecting keys and remains bag-equal") {
      sql("CREATE TABLE wspr_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wspr_sales VALUES " +
          "(1, 'east', 10), (3, 'east', 30), (4, 'west', 5), (5, 'west', 15)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM wspr_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_mv AS $viewSql")

      val id = spark.sessionState.sqlParser.parseTableIdentifier("wspr_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("missing wspr_mv metadata")).refreshType shouldBe
        RefreshTypeCode.WindowPartition

      // Batch conflicting inserts, updates, and deletes before one refresh so
      // affected-key consolidation and replacement see the complete CDF batch.
      sql("INSERT INTO wspr_sales VALUES (2, 'east', 20), (6, 'west', 25), (7, 'north', 1)")
      sql("UPDATE wspr_sales SET amount = 22 WHERE id = 2")
      sql("DELETE FROM wspr_sales WHERE id = 6")
      sql("UPDATE wspr_sales SET amount = 2 WHERE id = 5")

      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wspr_mv")

      assertMvCorrect("wspr_mv", viewSql)
      val refreshStatements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      val replacement = refreshStatements.find(_.contains("REPLACE WHERE")).getOrElse(fail("missing REPLACE WHERE"))

      replacement should include("openivm_new_wspr_mv")
      replacement should not include "/_ivm/view_deltas/"
      refreshStatements.mkString("\n") should not include "/_ivm/view_deltas/"
      refreshStatements.mkString("\n") should not include "WHEN MATCHED THEN DELETE"
    }

    it("does not commit or cascade when a mixed CDF batch leaves the window bag unchanged") {
      sql("CREATE TABLE wspr_noop_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO wspr_noop_sales VALUES " +
          "(1, 'east', 10), (2, 'east', 20), (3, 'west', 30)"
      )
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM wspr_noop_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_noop_mv AS $viewSql")
      val downstreamSql = "SELECT id, region, amount, rn FROM wspr_noop_mv"
      sql(s"CREATE MATERIALIZED VIEW wspr_noop_downstream AS $downstreamSql")

      val upstreamVersion   = mvDataVersion("wspr_noop_mv")
      val downstreamVersion = mvDataVersion("wspr_noop_downstream")

      // Exercise several conflicting operations before one refresh while
      // restoring the exact source bag.
      sql("INSERT INTO wspr_noop_sales VALUES (4, 'east', 40), (5, 'west', 50)")
      sql("UPDATE wspr_noop_sales SET amount = 11 WHERE id = 1")
      sql("DELETE FROM wspr_noop_sales WHERE id IN (4, 5)")
      sql("UPDATE wspr_noop_sales SET amount = 10 WHERE id = 1")
      sql("DELETE FROM wspr_noop_sales WHERE id = 3")
      sql("INSERT INTO wspr_noop_sales VALUES (3, 'west', 30)")

      refreshMv("wspr_noop_mv")
      assertMvCorrect("wspr_noop_mv", viewSql)
      mvDataVersion("wspr_noop_mv") shouldBe upstreamVersion
      ranNetChangeProbe("wspr_noop_mv") shouldBe true

      refreshMv("wspr_noop_downstream")
      assertMvCorrect("wspr_noop_downstream", downstreamSql)
      mvDataVersion("wspr_noop_downstream") shouldBe downstreamVersion
    }

    it("does not scan CDF for net-zero detection when the window view has no downstream consumer") {
      sql("CREATE TABLE wspr_terminal_sales(id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO wspr_terminal_sales VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 30)")
      val viewSql =
        "SELECT id, region, amount, " +
          "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) AS rn FROM wspr_terminal_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_terminal_mv AS $viewSql")

      sql("UPDATE wspr_terminal_sales SET amount = 15 WHERE id = 1")
      RefreshProfileCatalog.removeAll(spark)
      refreshMv("wspr_terminal_mv")

      assertMvCorrect("wspr_terminal_mv", viewSql)
      ranNetChangeProbe("wspr_terminal_mv") shouldBe false
    }

    it("feeds a downstream view from the target CDF for the TPC-DI daily-market shape") {
      sql(
        "CREATE TABLE wspr_daily_market(" +
          "dm_date DATE, dm_s_symb STRING, dm_close DECIMAL(18,4), " +
          "dm_high DECIMAL(18,4), dm_low DECIMAL(18,4), dm_vol BIGINT) USING DELTA"
      )
      sql(
        "INSERT INTO wspr_daily_market VALUES " +
          "(DATE '2026-01-01', 'A', 10, 12, 8, 100), " +
          "(DATE '2026-01-02', 'A', 11, 13, 7, 110), " +
          "(DATE '2026-01-01', 'B', 20, 22, 18, 200), " +
          "(DATE '2026-01-02', 'B', 21, 23, 17, 210)"
      )
      val viewSql =
        "WITH cumulative AS (" +
          "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, " +
          "MIN(dm_low) OVER w AS fifty_two_week_low, " +
          "MAX(dm_high) OVER w AS fifty_two_week_high " +
          "FROM wspr_daily_market WINDOW w AS (PARTITION BY dm_s_symb ORDER BY dm_date)" +
          "), flagged AS (" +
          "SELECT *, CASE WHEN dm_low = fifty_two_week_low THEN dm_date END AS low_date_flag, " +
          "CASE WHEN dm_high = fifty_two_week_high THEN dm_date END AS high_date_flag FROM cumulative" +
          ") SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, " +
          "fifty_two_week_low, fifty_two_week_high, " +
          "MAX(low_date_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS fifty_two_week_low_date, " +
          "MAX(high_date_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS fifty_two_week_high_date " +
          "FROM flagged"
      sql(s"CREATE MATERIALIZED VIEW wspr_daily_market_mv AS $viewSql")
      val downstreamSql =
        "SELECT dm_s_symb, SUM(dm_close) AS total_close, COUNT(*) AS row_count " +
          "FROM wspr_daily_market_mv GROUP BY dm_s_symb"
      sql(s"CREATE MATERIALIZED VIEW wspr_daily_market_downstream AS $downstreamSql")

      // Consolidate conflicting DML before one refresh.
      sql("INSERT INTO wspr_daily_market VALUES (DATE '2026-01-03', 'A', 12, 14, 6, 120)")
      sql("UPDATE wspr_daily_market SET dm_low = 5 WHERE dm_s_symb = 'A' AND dm_date = DATE '2026-01-02'")
      sql("DELETE FROM wspr_daily_market WHERE dm_s_symb = 'B' AND dm_date = DATE '2026-01-01'")
      sql("INSERT INTO wspr_daily_market VALUES (DATE '2026-01-03', 'transient', 30, 31, 29, 300)")
      sql("UPDATE wspr_daily_market SET dm_high = 32 WHERE dm_s_symb = 'transient'")
      sql("DELETE FROM wspr_daily_market WHERE dm_s_symb = 'transient'")

      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wspr_daily_market_mv")
      assertMvCorrect("wspr_daily_market_mv", viewSql)

      val upstreamStatements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      upstreamStatements.exists(_.contains("REPLACE WHERE")) shouldBe true
      upstreamStatements.mkString("\n") should not include "/_ivm/view_deltas/"
      upstreamStatements.mkString("\n") should not include "WHEN MATCHED THEN DELETE"

      refreshMv("wspr_daily_market_downstream")
      assertMvCorrect("wspr_daily_market_downstream", downstreamSql)
    }
  }
}
