package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** ScalaTest port of `openivm/test/sql/ducklake_window_delta.test`.
  *
  * Verifies the WINDOW_PARTITION rewriter's partition-scoped DELETE+INSERT
  * preserves unchanged partition rows: only the partitions whose rows changed
  * (via the source delta) are recomputed; other partitions retain their
  * existing MV rows.  This is the documented WINDOW_PARTITION behaviour from
  * `openivm/CLAUDE.md` (Upsert Compilation table → `WINDOW_PARTITION` →
  * `CompileWindowRecompute()`).
  *
  * The second part of the source test chains a join MV
  * (`mv_market_join = mv_market ⋈ security`) over the window MV.
  *
  * == Window frame caveat ==
  *
  * The source test uses `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` (a
  * 2-row rolling max).  Per `WindowPartitionSpec.scala:68-73` ("All tests
  * below use the default frame (ROWS/RANGE BETWEEN UNBOUNDED PRECEDING
  * AND CURRENT ROW) or no explicit frame at all — both are accepted by
  * lpts"), bounded integer window frames trip up the LPTS→Spark bridge:
  * LPTS emits the bound as `CAST(1 AS BIGINT)` and Spark 3.5 rejects
  * BIGINT bounds in window frames with `DATATYPE_MISMATCH.SPECIFIED_WINDOW
  * _FRAME_UNACCEPTED_TYPE` (it expects INT).  The Delta-equivalent
  * invariant exercised here — partition-scoped recompute preserves
  * unchanged-partition rows — is independent of the frame width, so we
  * use the LPTS-compatible `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
  * ROW` (running max within partition) to exercise the same invariant.
  */
abstract class DucklakeWindowDeltaScenarios extends IvmParitySpecBase("ducklake-window-delta") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.openivm.refresh.windowSuffixSkip.enabled" -> "true")

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("DuckLake window refresh preserves unchanged partition rows + downstream join chain") {
    it(
      "INSERT into one partition of a rolling-max window MV; unchanged partitions keep their rows. " +
        "Then refresh a downstream join MV reading from the window MV."
    ) {
      sql(
        "CREATE TABLE IF NOT EXISTS dlwd_market(symbol STRING, d INT, price INT) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS dlwd_security(symbol STRING, sector STRING) USING DELTA"
      )

      sql(
        "INSERT INTO dlwd_market VALUES " +
          "('A', 1, 10), ('A', 2, 20), ('A', 3, 30), " +
          "('B', 1, 11), ('B', 2, 21), ('B', 3, 31)"
      )
      sql("INSERT INTO dlwd_security VALUES ('A', 'tech'), ('B', 'bank')")

      // Upstream WINDOW_PARTITION MV: running max over (symbol) partition.
      // See class-level caveat: bounded frames (`ROWS BETWEEN 1 PRECEDING`)
      // are not yet supported on the LPTS→Spark bridge; UNBOUNDED PRECEDING
      // exercises the same partition-preservation invariant.
      val mvMarketSql =
        "SELECT symbol, d, price, " +
          "max(price) OVER (" +
          "  PARTITION BY symbol ORDER BY d " +
          "  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" +
          ") AS roll_max " +
          "FROM dlwd_market"
      sql(s"CREATE MATERIALIZED VIEW dlwd_mv_market AS $mvMarketSql")
      mvRefreshType("dlwd_mv_market") shouldBe RefreshTypeCode.WindowPartition

      // Downstream join MV reads from the window MV.
      val mvJoinSql =
        "SELECT m.symbol, m.d, m.roll_max, s.sector " +
          "FROM dlwd_mv_market m JOIN dlwd_security s USING (symbol)"
      sql(s"CREATE MATERIALIZED VIEW dlwd_mv_market_join AS $mvJoinSql")

      // Insert into the 'A' partition only.  After refresh of mv_market, the
      // 'B' partition rows must remain untouched; only the 'A' partition
      // recomputes.  This is the unchanged-partition-preservation invariant.
      sql("INSERT INTO dlwd_market VALUES ('A', 4, 25)")
      refreshMv("dlwd_mv_market")
      assertMvCorrect("dlwd_mv_market", mvMarketSql)

      // Now refresh the downstream join MV that reads from the upstream MV.
      refreshMv("dlwd_mv_market_join")
      assertMvCorrect("dlwd_mv_market_join", mvJoinSql)
    }
  }
}
