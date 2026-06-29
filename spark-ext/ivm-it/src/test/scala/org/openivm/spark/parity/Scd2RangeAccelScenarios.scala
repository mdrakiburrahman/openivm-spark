package org.openivm.spark.parity

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

abstract class Scd2RangeAccelScenarios extends IvmParitySpecBase("scd2-range-accel") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.Scd2RangeAccelEnabledKey -> "true")

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("SCD2 range-join acceleration") {
    it("keeps fact_market_history-style BETWEEN joins incremental and result-invariant") {
      sql(
        "CREATE TABLE IF NOT EXISTS p43_fact_market_history(" +
          "trade_id INT, security_id INT, ts TIMESTAMP, price DECIMAL(10,2)) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS p43_dim_security_scd(" +
          "security_id INT, symbol STRING, effective_timestamp TIMESTAMP, end_timestamp TIMESTAMP) USING DELTA"
      )

      sql(
        "INSERT INTO p43_dim_security_scd VALUES " +
          "(1, 'AAA', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-12-31 23:59:59'), " +
          "(1, 'AAB', TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '9999-12-31 00:00:00'), " +
          "(2, 'BBB', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '9999-12-31 00:00:00')"
      )
      sql(
        "INSERT INTO p43_fact_market_history VALUES " +
          "(1, 1, TIMESTAMP '2020-06-01 00:00:00', 10.00), " +
          "(2, 2, TIMESTAMP '2020-06-01 00:00:00', 20.00)"
      )

      sql(
        "CREATE MATERIALIZED VIEW p43_scd2_market_mv AS " +
          "SELECT f.trade_id, d.symbol, f.price " +
          "FROM p43_fact_market_history f " +
          "JOIN p43_dim_security_scd d " +
          "ON f.security_id = d.security_id " +
          "AND f.ts BETWEEN d.effective_timestamp AND d.end_timestamp"
      )

      mvRefreshType("p43_scd2_market_mv") should not be RefreshTypeCode.FullRefresh

      sql(
        "INSERT INTO p43_fact_market_history VALUES " +
          "(3, 1, TIMESTAMP '2021-02-01 00:00:00', 11.00), " +
          "(4, 9, TIMESTAMP '2021-02-01 00:00:00', 99.00)"
      )
      refreshMv("p43_scd2_market_mv")

      assertMvCorrect(
        "p43_scd2_market_mv",
        "SELECT f.trade_id, d.symbol, f.price " +
          "FROM p43_fact_market_history f " +
          "JOIN p43_dim_security_scd d " +
          "ON f.security_id = d.security_id " +
          "AND f.ts BETWEEN d.effective_timestamp AND d.end_timestamp"
      )
    }
  }
}
