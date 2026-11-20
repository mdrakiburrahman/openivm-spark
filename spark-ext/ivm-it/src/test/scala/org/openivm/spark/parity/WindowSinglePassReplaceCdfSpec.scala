package org.openivm.spark.parity

import org.openivm.spark.common.{
  CdfWatermarkCatalog,
  FeatureGate,
  MvProjectionSource,
  MvCatalog,
  RefreshProfileCatalog,
  RefreshSqlLogCatalog,
  RefreshTypeCode
}
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

    for (changed <- Seq(false, true)) {
      val kind = if (changed) "changed" else "net_zero"
      it(s"handles managed DISTINCT parent CDF [1,1] with $kind public rows") {
        spark.conf.set("spark.openivm.catalogPreservesColumnCase", "true")
        spark.conf.set("spark.openivm.managedTablesRoot", s"$warehouseDir/Tables")
        try {
          val base        = s"wspr_${kind}_base"
          val parent      = s"default.wspr_${kind}_parent"
          val target      = s"default.wspr_${kind}_target"
          val child       = s"default.wspr_${kind}_child"
          val targetShort = target.split("\\.").last
          sql(s"CREATE TABLE $base(id INT, region STRING, item_value INT) USING DELTA")
          sql(s"INSERT INTO $base VALUES (1, 'east', 10), (2, 'east', 10), (3, 'east', NULL), (4, 'west', 20)")
          sql(s"CREATE MATERIALIZED VIEW $parent AS SELECT DISTINCT region, item_value FROM $base")
          val query = s"SELECT region, item_value, ROW_NUMBER() OVER " +
            s"(PARTITION BY region ORDER BY item_value NULLS FIRST) AS rn FROM $parent"
          sql(s"CREATE MATERIALIZED VIEW $target AS $query")
          val childQuery = s"SELECT region, item_value, rn FROM $target"
          sql(s"CREATE MATERIALIZED VIEW $child AS $childQuery")

          MvProjectionSource.isProjection(spark, parent) shouldBe true
          spark.table(parent).columns.toSeq shouldBe Seq("region", "item_value")
          spark.read.format("delta").load(mvDataLocation(parent)).columns.toSeq should contain("openivm_distinct_count")
          val targetId   = spark.sessionState.sqlParser.parseTableIdentifier(target)
          val targetMeta = MvCatalog.lookup(spark, targetId).get
          targetMeta.refreshType shouldBe RefreshTypeCode.WindowPartition
          targetMeta.sourceTables shouldBe Seq(parent)
          mvDataVersion(parent) shouldBe 0L
          val targetBefore = mvDataVersion(target)
          val childBefore  = mvDataVersion(child)
          CdfWatermarkCatalog.get(spark, target, parent) shouldBe None

          if (changed) sql(s"UPDATE $base SET item_value = 7 WHERE id = 1")
          else sql(s"DELETE FROM $base WHERE id = 1")
          refreshMv(parent)
          mvDataVersion(parent) shouldBe 1L
          assertMvCorrect(parent, s"SELECT DISTINCT region, item_value FROM $base")
          RefreshProfileCatalog.removeAll(spark)
          refreshMv(target)
          ranNetChangeProbe(targetShort) shouldBe true
          CdfWatermarkCatalog.get(spark, target, parent) shouldBe Some(1L)
          refreshMv(child)
          val targetRows = spark.table(target)
          val expected   = sql(query)
          targetRows.exceptAll(expected).count() shouldBe 0L
          expected.exceptAll(targetRows).count() shouldBe 0L
          val childRows = spark.table(child)
          childRows.exceptAll(expected).count() shouldBe 0L
          expected.exceptAll(childRows).count() shouldBe 0L
          if (changed) {
            mvDataVersion(target) should be > targetBefore
            mvDataVersion(child) should be > childBefore
          } else {
            mvDataVersion(target) shouldBe targetBefore
            mvDataVersion(child) shouldBe childBefore
          }
        } finally {
          spark.conf.unset("spark.openivm.catalogPreservesColumnCase")
          spark.conf.unset("spark.openivm.managedTablesRoot")
        }
      }
    }

    it("does not suppress a managed-parent signed-zero change visible through a window projection") {
      spark.conf.set("spark.openivm.catalogPreservesColumnCase", "true")
      spark.conf.set("spark.openivm.managedTablesRoot", s"$warehouseDir/Tables")
      try {
        val base   = "wspr_zero_base"
        val parent = "default.wspr_zero_parent"
        val target = "default.wspr_zero_target"
        val child  = "default.wspr_zero_child"
        sql(s"CREATE TABLE $base(id INT, region STRING, item_value DOUBLE) USING DELTA")
        sql(s"INSERT INTO $base VALUES (1, 'east', CAST('0.0' AS DOUBLE)), (2, 'east', 2D)")
        sql(s"CREATE MATERIALIZED VIEW $parent AS SELECT DISTINCT id, region, item_value FROM $base")
        val query = s"SELECT id, region, CAST(item_value AS STRING) AS observed_zero, " +
          s"ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM $parent"
        sql(s"CREATE MATERIALIZED VIEW $target AS $query")
        val childQuery = s"SELECT id, region, observed_zero, rn FROM $target"
        sql(s"CREATE MATERIALIZED VIEW $child AS $childQuery")
        MvProjectionSource.isProjection(spark, parent) shouldBe true
        val targetId = spark.sessionState.sqlParser.parseTableIdentifier(target)
        MvCatalog.lookup(spark, targetId).get.refreshType shouldBe RefreshTypeCode.WindowPartition
        val parentPath = mvDataLocation(parent).replace("`", "``")
        mvDataVersion(parent) shouldBe 0L
        sql(s"UPDATE delta.`$parentPath` SET item_value = CAST('-0.0' AS DOUBLE) WHERE id = 1")
        mvDataVersion(parent) shouldBe 1L
        val signed = sql(
          s"SELECT _change_type, CAST(item_value AS STRING) AS v " +
            s"FROM table_changes_by_path('$parentPath', 1, 1)"
        ).collect()
          .map(row => row.getString(0) -> row.getString(1))
          .toSet
        signed shouldBe Set("update_preimage" -> "0.0", "update_postimage" -> "-0.0")
        sql(query).filter("id = 1").select("observed_zero").head().getString(0) shouldBe "-0.0"
        refreshMv(target)
        assertMvCorrect(target, query)
        CdfWatermarkCatalog.get(spark, target, parent) shouldBe Some(1L)
        refreshMv(child)
        assertMvCorrect(child, query)
      } finally {
        spark.conf.unset("spark.openivm.catalogPreservesColumnCase")
        spark.conf.unset("spark.openivm.managedTablesRoot")
      }
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
