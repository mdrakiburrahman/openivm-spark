package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class WindowSinglePassReplaceCdfSpec extends IvmParitySpecBase("window-single-pass-replace") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey                -> "true"
    )

  describe("WINDOW_PARTITION single-pass replacement with a named affected-key view") {
    it("writes only changed rows when the partition-order key is unique") {
      sql("CREATE TABLE wspr_compact_sales(id INT, region STRING, seq INT, amount INT) USING DELTA")
      sql(
        "INSERT INTO wspr_compact_sales " +
          "SELECT CAST(id AS INT), CONCAT('region_', CAST(id AS STRING)), 1, 10 FROM RANGE(1001)"
      )
      val viewSql =
        "SELECT id, region, seq, amount, " +
          "MAX(amount) OVER (PARTITION BY region ORDER BY seq) AS running_max " +
          "FROM wspr_compact_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_compact_mv AS $viewSql")

      // More affected partitions than the literal REPLACE path can encode,
      // plus conflicting operations before one refresh.
      sql("UPDATE wspr_compact_sales SET amount = 15")
      sql("INSERT INTO wspr_compact_sales VALUES (2000, 'transient', 1, 25)")
      sql("UPDATE wspr_compact_sales SET amount = 26 WHERE id = 2000")
      sql("DELETE FROM wspr_compact_sales WHERE id = 2000")

      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wspr_compact_mv")

      assertMvCorrect("wspr_compact_mv", viewSql)
      val refreshStatements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      val compactCascade = refreshStatements.find { statement =>
        statement.contains("CREATE OR REPLACE TABLE delta.") &&
        statement.contains("/_ivm/view_deltas/") &&
        statement.contains("openivm_changed") &&
        statement.contains("FULL OUTER JOIN openivm_new")
      }
      val compactDelete = refreshStatements.find { statement =>
        statement.contains("MERGE INTO") &&
        statement.contains("wspr_compact_mv") &&
        statement.contains("`openivm_multiplicity` < 0") &&
        statement.contains("WHEN MATCHED THEN DELETE")
      }
      val compactInsert = refreshStatements.find { statement =>
        statement.contains("INSERT INTO") &&
        statement.contains("wspr_compact_mv") &&
        statement.contains("`openivm_multiplicity` > 0")
      }
      compactCascade should not be empty
      compactCascade.get should include("WHERE openivm_old_present IS NOT NULL")
      compactCascade.get should include("WHERE openivm_new_present IS NOT NULL")
      compactDelete should not be empty
      compactInsert should not be empty
      refreshStatements.mkString("\n") should not include "REPLACE WHERE"
    }

    it("falls back to partition replacement when the row key has duplicates") {
      sql("CREATE TABLE wspr_duplicate_sales(id INT, region STRING, seq INT, amount INT) USING DELTA")
      sql(
        "INSERT INTO wspr_duplicate_sales " +
          "SELECT CAST(id AS INT), CONCAT('region_', CAST(id AS STRING)), 1, 10 FROM RANGE(1001)"
      )
      sql("INSERT INTO wspr_duplicate_sales VALUES (2000, 'region_0', 1, 20)")
      val viewSql =
        "SELECT id, region, seq, amount, " +
          "MAX(amount) OVER (PARTITION BY region ORDER BY seq) AS running_max " +
          "FROM wspr_duplicate_sales"
      sql(s"CREATE MATERIALIZED VIEW wspr_duplicate_mv AS $viewSql")
      sql("UPDATE wspr_duplicate_sales SET amount = amount + 1")

      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wspr_duplicate_mv")

      assertMvCorrect("wspr_duplicate_mv", viewSql)
      val refreshStatements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      refreshStatements.exists { statement =>
        statement.contains("MERGE INTO") &&
        statement.contains("wspr_duplicate_mv") &&
        statement.contains("WHEN MATCHED THEN DELETE")
      } shouldBe true
      refreshStatements.exists { statement =>
        statement.contains("CREATE OR REPLACE TABLE delta.") &&
        statement.contains("openivm_changed") &&
        statement.contains("FULL OUTER JOIN openivm_new")
      } shouldBe false
    }

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
      val cascadeIdx = refreshStatements.indexWhere { statement =>
        statement.contains("CREATE OR REPLACE TABLE delta.") &&
        statement.contains("/_ivm/view_deltas/")
      }
      val replacementIdx = refreshStatements.indexWhere(_.contains("REPLACE WHERE"))

      cascadeIdx should be >= 0
      replacementIdx should be > cascadeIdx
      refreshStatements(replacementIdx) should include("/_ivm/view_deltas/")
      refreshStatements(replacementIdx) should include("WHERE `openivm_multiplicity` > 0")
      refreshStatements(replacementIdx) should not include "openivm_new_wspr_mv"
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

      refreshMv("wspr_noop_downstream")
      assertMvCorrect("wspr_noop_downstream", downstreamSql)
      mvDataVersion("wspr_noop_downstream") shouldBe downstreamVersion
    }

    it("keeps the TPC-DI daily-market window incremental and compacts its cascade") {
      sql(
        "CREATE TABLE wspr_daily_market(" +
          "dm_date DATE, dm_s_symb STRING, dm_close DECIMAL(18,4), " +
          "dm_high DECIMAL(18,4), dm_low DECIMAL(18,4), dm_vol BIGINT) USING DELTA"
      )
      sql(
        "INSERT INTO wspr_daily_market " +
          "SELECT DATE '2026-01-01', CONCAT('S', CAST(id AS STRING)), 10, 12, 8, 100 " +
          "FROM RANGE(1001)"
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

      val id = spark.sessionState.sqlParser.parseTableIdentifier("wspr_daily_market_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("missing wspr_daily_market_mv metadata")).refreshType shouldBe
        RefreshTypeCode.WindowPartition

      sql("UPDATE wspr_daily_market SET dm_low = 7")
      sql("INSERT INTO wspr_daily_market VALUES (DATE '2026-01-02', 'transient', 13, 15, 11, 130)")
      sql("UPDATE wspr_daily_market SET dm_high = 16 WHERE dm_s_symb = 'transient'")
      sql("DELETE FROM wspr_daily_market WHERE dm_s_symb = 'transient'")

      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wspr_daily_market_mv")

      assertMvCorrect("wspr_daily_market_mv", viewSql)
      val refreshStatements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      refreshStatements.exists { statement =>
        statement.contains("CREATE OR REPLACE TABLE delta.") &&
        statement.contains("openivm_changed") &&
        statement.contains("FULL OUTER JOIN openivm_new")
      } shouldBe true
      refreshStatements.exists { statement =>
        statement.contains("MERGE INTO") &&
        statement.contains("wspr_daily_market_mv") &&
        statement.contains("WHEN MATCHED THEN DELETE")
      } shouldBe true
      refreshStatements.exists { statement =>
        statement.contains("INSERT INTO") && statement.contains("wspr_daily_market_mv")
      } shouldBe true
    }
  }

}
