package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class WindowMixedDmlCompactRepairCdfSpec extends IvmParitySpecBase("window-mixed-dml-compact-repair") with CdfMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.QueryLogEnabledKey                -> "true",
      FeatureGate.WindowSinglePassReplaceEnabledKey -> "true",
      FeatureGate.WindowCascadeMergeEnabledKey      -> "true"
    )

  describe("WINDOW_PARTITION compact repair") {
    it("repairs an unordered mixed-DML batch and remains exactly bag-equal") {
      sql(
        "CREATE TABLE wmcr_market(" +
          "id INT, dm_date DATE, dm_s_symb STRING, dm_high INT, dm_low INT) USING DELTA"
      )
      Seq(0, 1, 3, 4).foreach { day =>
        sql(
          s"INSERT INTO wmcr_market " +
            s"SELECT CAST(id * 10 + $day AS INT), DATE_ADD(DATE '2026-01-01', $day), " +
            "CONCAT('S', LPAD(CAST(id AS STRING), 4, '0')), " +
            s"CAST(10 + $day AS INT), CAST(9 - $day AS INT) FROM RANGE(1001)"
        )
      }

      val viewSql =
        "WITH cumulative AS (" +
          "SELECT id, dm_date, dm_s_symb, dm_high, dm_low, " +
          "MIN(dm_low) OVER w AS running_low, MAX(dm_high) OVER w AS running_high " +
          "FROM wmcr_market WINDOW w AS (PARTITION BY dm_s_symb ORDER BY dm_date)" +
          "), flagged AS (" +
          "SELECT *, CASE WHEN dm_low = running_low THEN dm_date END AS low_date_flag, " +
          "CASE WHEN dm_high = running_high THEN dm_date END AS high_date_flag FROM cumulative" +
          ") SELECT id, dm_date, dm_s_symb, dm_high, dm_low, running_low, running_high, " +
          "MAX(low_date_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS running_low_date, " +
          "MAX(high_date_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS running_high_date " +
          "FROM flagged"
      sql(s"CREATE MATERIALIZED VIEW wmcr_mv AS $viewSql")
      val id = spark.sessionState.sqlParser.parseTableIdentifier("wmcr_mv")
      MvCatalog.lookup(spark, id).getOrElse(fail("missing wmcr_mv metadata")).refreshType shouldBe
        RefreshTypeCode.WindowPartition

      // One refresh sees an out-of-order insert, repeated updates to those rows,
      // an order-key move, a delete, and a fully cancelled transient row.
      sql(
        "INSERT INTO wmcr_market " +
          "SELECT CAST(id * 10 + 2 AS INT), DATE '2026-01-03', " +
          "CONCAT('S', LPAD(CAST(id AS STRING), 4, '0')), 20, 4 FROM RANGE(1001)"
      )
      sql("UPDATE wmcr_market SET dm_low = 2 WHERE id % 20 = 2")
      sql("UPDATE wmcr_market SET dm_low = 3 WHERE id % 20 = 2")
      sql("UPDATE wmcr_market SET dm_date = DATE '2026-01-07' WHERE id % 110 = 3")
      sql("DELETE FROM wmcr_market WHERE id % 130 = 4")
      sql("INSERT INTO wmcr_market VALUES (20000, DATE '2026-01-03', 'transient', 30, 1)")
      sql("UPDATE wmcr_market SET dm_high = 31 WHERE id = 20000")
      sql("DELETE FROM wmcr_market WHERE id = 20000")

      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wmcr_mv")

      sql(s"CREATE OR REPLACE TEMP VIEW wmcr_expected AS $viewSql")
      sql("SELECT * FROM wmcr_mv EXCEPT ALL SELECT * FROM wmcr_expected").count() shouldBe 0L
      sql("SELECT * FROM wmcr_expected EXCEPT ALL SELECT * FROM wmcr_mv").count() shouldBe 0L

      val statements = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).toSeq
      statements.exists { statement =>
        statement.contains("CREATE OR REPLACE TABLE delta.") &&
        statement.contains("openivm_changed") &&
        statement.contains("FULL OUTER JOIN openivm_new")
      } shouldBe true
      statements.exists { statement =>
        statement.contains("MERGE INTO") &&
        statement.contains("wmcr_mv") &&
        statement.contains("`openivm_multiplicity` < 0") &&
        statement.contains("WHEN MATCHED THEN DELETE")
      } shouldBe true
      statements.exists { statement =>
        statement.contains("INSERT INTO") &&
        statement.contains("wmcr_mv") &&
        statement.contains("`openivm_multiplicity` > 0")
      } shouldBe true
    }
  }
}
