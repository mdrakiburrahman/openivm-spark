package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshSqlLogCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class WindowPartitionSingleDeleteMergeSpec
    extends IvmParitySpecBase("window-partition-single-delete-merge")
    with InterceptMode {

  override protected def extraSparkConf: Map[String, String] =
    Map(
      FeatureGate.WindowPartitionSingleDeleteMergeEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey                         -> "true"
    )

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("trades_history-shaped WINDOW_PARTITION ordered window") {
    it("reuses the cascade snapshot for existing and new joined-window partitions") {
      sql(
        "CREATE TABLE trh_trade(" +
          "t_id INT, t_dts TIMESTAMP, t_ca_id INT, t_st_id INT, t_tt_id INT, t_qty INT) USING DELTA"
      )
      sql("CREATE TABLE trh_trade_history(th_t_id INT, th_dts TIMESTAMP, th_st_id INT) USING DELTA")
      sql("CREATE TABLE trh_status(st_id INT, st_name STRING) USING DELTA")
      sql("CREATE TABLE trh_type(tt_id INT, tt_name STRING) USING DELTA")

      sql("INSERT INTO trh_status VALUES (1, 'new'), (2, 'settled'), (3, 'closed')")
      sql("INSERT INTO trh_type VALUES (10, 'cash')")
      sql(
        "INSERT INTO trh_trade VALUES " +
          "(100, TIMESTAMP '2026-01-01 09:00:00', 501, 1, 10, 50), " +
          "(200, TIMESTAMP '2026-01-01 10:00:00', 502, 1, 10, 60)"
      )
      sql(
        "INSERT INTO trh_trade_history VALUES " +
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
          "FROM trh_trade " +
          "JOIN trh_trade_history ON t_id = th_t_id " +
          "JOIN trh_type ON t_tt_id = tt_id " +
          "JOIN trh_status ts ON t_st_id = ts.st_id " +
          "JOIN trh_status us ON th_st_id = us.st_id"
      sql(s"CREATE MATERIALIZED VIEW trh_mv AS $viewSql")
      mvRefreshType("trh_mv") shouldBe RefreshTypeCode.WindowPartition
      assertMvCorrect("trh_mv", viewSql)

      // Batch conflicting UPDATE + DELETE/reinsert + INSERT operations before
      // one refresh, exercising consolidation as well as both old/new keys.
      sql("UPDATE trh_trade SET t_st_id = 2 WHERE t_id = 100")
      sql(
        "DELETE FROM trh_trade_history " +
          "WHERE th_t_id = 200 AND th_dts = TIMESTAMP '2026-01-01 10:05:00'"
      )
      sql(
        "INSERT INTO trh_trade VALUES " +
          "(300, TIMESTAMP '2026-01-02 11:00:00', 503, 1, 10, 70)"
      )
      sql(
        "INSERT INTO trh_trade_history VALUES " +
          "(100, TIMESTAMP '2026-01-02 09:05:00', 2), " +
          "(200, TIMESTAMP '2026-01-01 10:05:00', 1), " +
          "(300, TIMESTAMP '2026-01-02 11:05:00', 1)"
      )
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("trh_mv")

      assertMvCorrect("trh_mv", viewSql)
      mvRefreshType("trh_mv") shouldBe RefreshTypeCode.WindowPartition

      val refreshSql = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
      refreshSql should include("UNION ALL")
      refreshSql should include("REPLACE WHERE")
      refreshSql should include("`openivm_multiplicity` > 0")
      refreshSql should not include "WHEN MATCHED THEN DELETE"

      // A batch whose affected partition is absent from the target can append
      // the positive cascade snapshot without a target rewrite.
      sql(
        "INSERT INTO trh_trade VALUES " +
          "(400, TIMESTAMP '2026-01-03 12:00:00', 504, 1, 10, 80)"
      )
      sql(
        "INSERT INTO trh_trade_history VALUES " +
          "(400, TIMESTAMP '2026-01-03 12:05:00', 1)"
      )
      RefreshSqlLogCatalog.removeAll(spark)
      refreshMv("trh_mv")

      assertMvCorrect("trh_mv", viewSql)
      val appendSql = sql("SHOW OPENIVM QUERY LOG").collect().map(_.getString(9)).mkString("\n")
      appendSql should include("`openivm_multiplicity` > 0")
      appendSql should not include "REPLACE WHERE"
      appendSql should not include "WHEN MATCHED THEN DELETE"
    }
  }
}
