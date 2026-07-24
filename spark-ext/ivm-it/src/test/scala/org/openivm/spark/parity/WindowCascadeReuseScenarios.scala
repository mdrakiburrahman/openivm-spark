package org.openivm.spark.parity

import org.apache.spark.sql.Row
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class WindowCascadeReuseScenarios(compileCacheEnabled: Boolean)
    extends IvmParitySpecBase(
      if (compileCacheEnabled) "window-cascade-reuse-compile-cache" else "window-cascade-reuse"
    ) {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowPartitionSingleDeleteMergeEnabledKey -> "true",
      FeatureGate.CompileClassificationCacheEnabledKey       -> compileCacheEnabled.toString,
      FeatureGate.QueryLogEnabledKey                         -> "true"
    )

  private val ColViewName  = 1
  private val ColStmtOrder = 3
  private val ColCategory  = 6
  private val ColSqlText   = 9

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  private def rewrittenRows(viewName: String): Seq[Row] =
    sql("SHOW OPENIVM QUERY LOG")
      .collect()
      .toSeq
      .filter(_.getString(ColViewName).split("\\.").last == viewName)
      .filter(_.getString(ColCategory) == "rewritten_stmt")
      .sortBy(_.getInt(ColStmtOrder))

  private def assertCascadeFirstTargetWrite(viewName: String): Unit = {
    val rows = rewrittenRows(viewName)
    val cascadeRow = rows
      .find { row =>
        val upper = row.getString(ColSqlText).toUpperCase(java.util.Locale.ROOT)
        upper.startsWith("CREATE OR REPLACE TABLE DELTA.") &&
        upper.contains(s"FROM OPENIVM_OLD_${viewName.toUpperCase(java.util.Locale.ROOT)}") &&
        upper.contains(s"FROM OPENIVM_NEW_${viewName.toUpperCase(java.util.Locale.ROOT)}") &&
        upper.contains("UNION ALL")
      }
      .getOrElse(fail("missing raw signed window cascade CTAS"))
    val cascadeSql = cascadeRow.getString(ColSqlText)
    val cascadePath = "(?is)CREATE\\s+OR\\s+REPLACE\\s+TABLE\\s+delta\\.`([^`]+)`".r
      .findFirstMatchIn(cascadeSql)
      .map(_.group(1))
      .getOrElse(fail("missing cascade Delta path"))

    val targetRows = rows.filter { row =>
      val upper = row.getString(ColSqlText).toUpperCase(java.util.Locale.ROOT)
      upper.startsWith("INSERT INTO DELTA.") && upper.contains("REPLACE WHERE")
    }
    targetRows should have size 1
    val targetRow = targetRows.head
    val targetSql = targetRow.getString(ColSqlText)
    targetSql should include(s"FROM delta.`$cascadePath`")
    targetSql should include("`openivm_multiplicity` > 0")
    targetSql
      .toUpperCase(java.util.Locale.ROOT) should not include s"OPENIVM_NEW_${viewName.toUpperCase(java.util.Locale.ROOT)}"
    targetRow.getInt(ColStmtOrder) should be > cascadeRow.getInt(ColStmtOrder)
    rows.map(_.getString(ColSqlText)).mkString("\n") should not include "WHEN MATCHED THEN DELETE"
  }

  describe("joined WINDOW_PARTITION cascade reuse") {
    it("reuses the persisted signed snapshot for existing and new partitions") {
      sql(
        "CREATE TABLE wcr_trade(" +
          "t_id INT, t_dts TIMESTAMP, t_ca_id INT, t_st_id INT, t_tt_id INT, t_qty INT) USING DELTA"
      )
      sql("CREATE TABLE wcr_trade_history(th_t_id INT, th_dts TIMESTAMP, th_st_id INT) USING DELTA")
      sql("CREATE TABLE wcr_status(st_id INT, st_name STRING) USING DELTA")
      sql("CREATE TABLE wcr_type(tt_id INT, tt_name STRING) USING DELTA")

      sql("INSERT INTO wcr_status VALUES (1, 'new'), (2, 'settled'), (3, 'closed')")
      sql("INSERT INTO wcr_type VALUES (10, 'cash')")
      sql(
        "INSERT INTO wcr_trade VALUES " +
          "(100, TIMESTAMP '2026-01-01 09:00:00', 501, 1, 10, 50), " +
          "(200, TIMESTAMP '2026-01-01 10:00:00', 502, 1, 10, 60)"
      )
      sql(
        "INSERT INTO wcr_trade_history VALUES " +
          "(100, TIMESTAMP '2026-01-01 09:05:00', 1), " +
          "(200, TIMESTAMP '2026-01-01 10:05:00', 1)"
      )

      val viewSql =
        "SELECT " +
          "t_id AS trade_id, t_dts AS trade_timestamp, t_ca_id AS account_id, " +
          "ts.st_name AS trade_status, tt_name AS trade_type, us.st_name AS update_status, " +
          "th_dts AS effective_timestamp, " +
          "COALESCE(LAG(th_dts) OVER (" +
          "  PARTITION BY t_id ORDER BY th_dts DESC, t_dts DESC, t_st_id DESC, th_st_id DESC" +
          "), TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp, " +
          "CASE WHEN ROW_NUMBER() OVER (" +
          "  PARTITION BY t_id ORDER BY th_dts DESC, t_dts DESC, t_st_id DESC, th_st_id DESC" +
          ") = 1 THEN TRUE ELSE FALSE END AS is_current " +
          "FROM wcr_trade " +
          "JOIN wcr_trade_history ON t_id = th_t_id " +
          "JOIN wcr_type ON t_tt_id = tt_id " +
          "JOIN wcr_status ts ON t_st_id = ts.st_id " +
          "JOIN wcr_status us ON th_st_id = us.st_id"
      sql(s"CREATE MATERIALIZED VIEW wcr_mv AS $viewSql")
      mvRefreshType("wcr_mv") shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect("wcr_mv", viewSql)

      sql("UPDATE wcr_trade SET t_st_id = 2 WHERE t_id = 100")
      sql(
        "DELETE FROM wcr_trade_history " +
          "WHERE th_t_id = 200 AND th_dts = TIMESTAMP '2026-01-01 10:05:00'"
      )
      sql("INSERT INTO wcr_trade VALUES (300, TIMESTAMP '2026-01-02 11:00:00', 503, 1, 10, 70)")
      sql(
        "INSERT INTO wcr_trade_history VALUES " +
          "(100, TIMESTAMP '2026-01-02 09:05:00', 2), " +
          "(200, TIMESTAMP '2026-01-01 10:05:00', 1), " +
          "(300, TIMESTAMP '2026-01-02 11:05:00', 1)"
      )
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wcr_mv")

      assertMvCorrect("wcr_mv", viewSql)
      assertCascadeFirstTargetWrite("wcr_mv")

      sql("INSERT INTO wcr_trade VALUES (400, TIMESTAMP '2026-01-03 12:00:00', 504, 1, 10, 80)")
      sql("INSERT INTO wcr_trade_history VALUES (400, TIMESTAMP '2026-01-03 12:05:00', 1)")
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("wcr_mv")

      assertMvCorrect("wcr_mv", viewSql)
      assertCascadeFirstTargetWrite("wcr_mv")
    }
  }
}
