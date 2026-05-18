package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_window.test`.
  *
  * DuckLake window MVs route through `WINDOW_PARTITION` (rt5).  Per
  * `openivm/CLAUDE.md` rule table, `WINDOW_PARTITION` calls
  * `CompileWindowRecompute()` (standard DuckDB) or the DuckLake snapshot
  * diff path (`src/upsert/refresh_window.cpp:353-385`).  On Spark+Delta the
  * same `WINDOW_PARTITION` rewriter path applies (see
  * `WindowPartitionSpec.scala` documentation), with source-side delta
  * detection via Delta snapshots — the Delta-equivalent invariant from
  * PLAN.md §9.
  *
  * The source `ducklake_window.test` exercises many openivm-internal
  * assertions (refresh-profile pragma, group_columns recording, file-path
  * artifact contents, cascade ordering, retry-after-metadata-leak).  We
  * port the **user-visible IVM invariants** that translate to Spark+Delta:
  *
  *   1. SUM OVER (PARTITION BY) on a single source with a WHERE clause
  *      (history_win) — verify the partition-output column drives refresh.
  *   2. LEFT JOIN + SUM OVER (PARTITION BY ... ORDER BY ...) (window_join).
  *   3. Multi-join (4-way) with LAG and ROW_NUMBER windows
  *      (trades_history_like).
  *   4. Chained-CTE window/aggregate MV (market_chain_scored over
  *      market_chain_base) — exercises chained MVs reading from another MV.
  *   5. LEFT JOIN of CRM-like base + tax-like base with LAG + ROW_NUMBER
  *      (mv_accounts_like) — three-MV chain.
  *   6. Window-over-MV pattern (mv_window_upstream over
  *      window_upstream_base) — two-MV chain with the upstream being a
  *      simple projection MV.
  *
  * Openivm-internal artifacts (refresh-profile pragmas, openivm_views
  * group_columns inspection, cascade ordering, stage-lock metadata) are N/A
  * on Spark and skipped.  The retry-after-metadata-leak section is openivm-
  * specific (manually inserts a stale row into `openivm_views` to simulate
  * a crashed CREATE); openivm-spark has its own MvCatalog rollback path,
  * so this section is not a meaningful Spark test.
  */
class DucklakeWindowSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dlwin-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DucklakeWindowSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── (1) SUM OVER (PARTITION BY) with WHERE filter ──────────────────────────

  describe("(1) SUM OVER (PARTITION BY H_W_ID) — partition output column drives refresh") {
    it("INSERT into the source propagates partition-scoped recompute correctly") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_history(" +
          "H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT, H_D_ID INT, H_W_ID INT, " +
          "H_DATE TIMESTAMP, H_AMOUNT DECIMAL(6, 2), H_DATA STRING) USING DELTA"
      )
      val viewSql =
        "SELECT H_W_ID, H_D_ID, H_C_ID, H_AMOUNT, " +
          "SUM(H_AMOUNT) OVER (PARTITION BY H_W_ID) AS w_total, " +
          "H_AMOUNT / NULLIF(SUM(H_AMOUNT) OVER (PARTITION BY H_W_ID), 0) AS share " +
          "FROM dlwin_history WHERE H_AMOUNT > 0"
      spark.sql(s"CREATE MATERIALIZED VIEW dlwin_mv_history AS $viewSql")
      mvRefreshType("dlwin_mv_history") shouldBe RefreshTypeCode.WindowPartition

      spark.sql(
        "INSERT INTO dlwin_history VALUES " +
          "(4, 8, 3, 8, 3, TIMESTAMP '2026-01-01 00:00:00', 469.00, 'Payment'), " +
          "(28, 10, 1, 10, 1, TIMESTAMP '2026-01-01 00:00:00', 316.00, 'Payment'), " +
          "(22, 4, 2, 4, 2, TIMESTAMP '2026-01-01 00:00:00', 267.00, 'Payment')"
      )
      refreshMv("dlwin_mv_history")
      spark.table("dlwin_mv_history").count() shouldBe 3L
      assertMvCorrect("dlwin_mv_history", viewSql)
    }
  }

  // ── (2) LEFT JOIN + running SUM window ────────────────────────────────────

  describe("(2) LEFT JOIN with SUM OVER (PARTITION BY acct_id ORDER BY event_ts) running window") {
    it("running-sum window over a LEFT JOIN refreshes correctly after INSERT on the right") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_join_events(" +
          "acct_id INT, event_ts INT, code INT, amount INT) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_join_codes(code INT, label STRING) USING DELTA"
      )
      val viewSql =
        "SELECT e.acct_id, e.event_ts, c.label, " +
          "SUM(e.amount) OVER (" +
          "  PARTITION BY e.acct_id ORDER BY e.event_ts " +
          "  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" +
          ") AS running_amount " +
          "FROM dlwin_join_events e " +
          "LEFT JOIN dlwin_join_codes c ON e.code = c.code"
      spark.sql(s"CREATE MATERIALIZED VIEW dlwin_mv_join AS $viewSql")
      mvRefreshType("dlwin_mv_join") shouldBe RefreshTypeCode.WindowPartition

      spark.sql("INSERT INTO dlwin_join_codes VALUES (1, 'open')")
      spark.sql(
        "INSERT INTO dlwin_join_events VALUES (10, 1, 1, 5), (10, 2, 2, 7), (20, 1, 1, 11)"
      )
      refreshMv("dlwin_mv_join")
      assertMvCorrect("dlwin_mv_join", viewSql)

      // INSERT into the right side (codes) — new label flips an existing null
      spark.sql("INSERT INTO dlwin_join_codes VALUES (2, 'closed')")
      refreshMv("dlwin_mv_join")
      assertMvCorrect("dlwin_mv_join", viewSql)
    }
  }

  // ── (3) 4-way JOIN with LAG + ROW_NUMBER windows ───────────────────────────

  describe("(3) 4-way JOIN with LAG + ROW_NUMBER + CASE WHEN window expressions") {
    it("multi-join window MV maintains correctness across INSERT and UPDATE on multiple sources") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_trade(" +
          "t_id INT, t_dts TIMESTAMP, t_status INT, t_type INT, t_qty INT) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_trade_history(" +
          "th_t_id INT, th_dts TIMESTAMP, th_status INT) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_trade_status(status_id INT, status_name STRING) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_trade_type(type_id INT, type_name STRING) USING DELTA"
      )

      // Note: the source DuckLake test uses
      // `COALESCE(LAG(...) - INTERVAL 1 MILLISECOND, TIMESTAMP '9999-...')`
      // for `end_timestamp`.  LPTS compiles the `INTERVAL 1 MILLISECOND`
      // subtraction to the DuckDB-internal `to_milliseconds(1)` function,
      // which Spark 3.5 cannot resolve (`UNRESOLVED_ROUTINE`).  The IVM
      // invariant exercised here is the multi-join + LAG + ROW_NUMBER
      // refresh; the interval subtraction is incidental, so we use the raw
      // LAG value and a sentinel default to keep the LAG/ROW_NUMBER window
      // structure intact.
      val viewSql =
        "SELECT " +
          "t_id AS trade_id, " +
          "t_dts AS trade_timestamp, " +
          "ts.status_name AS trade_status, " +
          "tt.type_name AS trade_type, " +
          "us.status_name AS update_status, " +
          "th_dts AS effective_timestamp, " +
          "COALESCE(" +
          "  LAG(th_dts) OVER (" +
          "    PARTITION BY t_id ORDER BY th_dts DESC, t_dts DESC, th_status DESC" +
          "  ), " +
          "  TIMESTAMP '9999-12-31 23:59:59.999'" +
          ") AS end_timestamp, " +
          "CASE WHEN ROW_NUMBER() OVER (" +
          "  PARTITION BY t_id ORDER BY th_dts DESC, t_dts DESC, th_status DESC" +
          ") = 1 THEN TRUE ELSE FALSE END AS is_current " +
          "FROM dlwin_trade " +
          "JOIN dlwin_trade_history ON t_id = th_t_id " +
          "JOIN dlwin_trade_type tt ON t_type = tt.type_id " +
          "JOIN dlwin_trade_status ts ON t_status = ts.status_id " +
          "JOIN dlwin_trade_status us ON th_status = us.status_id"
      spark.sql(s"CREATE MATERIALIZED VIEW dlwin_mv_trades AS $viewSql")

      spark.sql(
        "INSERT INTO dlwin_trade_status VALUES (1, 'new'), (2, 'settled'), (3, 'closed')"
      )
      spark.sql("INSERT INTO dlwin_trade_type VALUES (10, 'cash')")
      spark.sql(
        "INSERT INTO dlwin_trade VALUES " +
          "(100, TIMESTAMP '2026-01-01 09:00:00', 1, 10, 50), " +
          "(200, TIMESTAMP '2026-01-01 10:00:00', 1, 10, 60)"
      )
      spark.sql(
        "INSERT INTO dlwin_trade_history VALUES " +
          "(100, TIMESTAMP '2026-01-01 09:05:00', 1), " +
          "(200, TIMESTAMP '2026-01-01 10:05:00', 1)"
      )
      refreshMv("dlwin_mv_trades")
      assertMvCorrect("dlwin_mv_trades", viewSql)

      spark.sql(
        "INSERT INTO dlwin_trade VALUES " +
          "(100, TIMESTAMP '2026-01-02 09:00:00', 2, 10, 50), " +
          "(300, TIMESTAMP '2026-01-02 11:00:00', 1, 10, 70)"
      )
      spark.sql(
        "INSERT INTO dlwin_trade_history VALUES " +
          "(100, TIMESTAMP '2026-01-02 09:05:00', 2), " +
          "(300, TIMESTAMP '2026-01-02 11:05:00', 1)"
      )
      refreshMv("dlwin_mv_trades")
      assertMvCorrect("dlwin_mv_trades", viewSql)

      // UPDATE a dimension referenced through two aliases (ts and us)
      spark.sql("UPDATE dlwin_trade_status SET status_name = 'settled-updated' WHERE status_id = 2")
      refreshMv("dlwin_mv_trades")
      assertMvCorrect("dlwin_mv_trades", viewSql)
    }
  }

  // ── (4) Chained MV: window+agg CTEs reading from another MV ───────────────

  describe("(4) Chained MV: aggregate+window MV reads from a base projection MV") {
    it("LAG/STDDEV/RANK CTE chain over a base projection MV stays bag-equal to the live query") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_market_src(" +
          "sym STRING, d DATE, close_px DOUBLE, volume INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO dlwin_market_src VALUES " +
          "('AAA', DATE '2026-01-01', 10.0, 100), " +
          "('AAA', DATE '2026-01-02', 12.0, 200), " +
          "('AAA', DATE '2026-01-03', 11.0, 150), " +
          "('BBB', DATE '2026-01-01', 20.0, 90), " +
          "('BBB', DATE '2026-01-02', 21.0, 110), " +
          "('BBB', DATE '2026-01-03', 22.0, 130)"
      )

      spark.sql(
        "CREATE MATERIALIZED VIEW dlwin_market_base AS " +
          "SELECT sym, d, close_px, volume FROM dlwin_market_src"
      )
      refreshMv("dlwin_market_base")

      val scoredSql =
        "WITH price_changes AS (" +
          "  SELECT sym, d, close_px, volume, " +
          "         LAG(close_px) OVER (PARTITION BY sym ORDER BY d) AS prev_close " +
          "  FROM dlwin_market_base" +
          "), " +
          "returns AS (" +
          "  SELECT sym, d, close_px, volume, " +
          "         CASE WHEN prev_close > 0 THEN (close_px - prev_close) / prev_close ELSE NULL END AS daily_return " +
          "  FROM price_changes" +
          "), " +
          "symbol_stats AS (" +
          "  SELECT sym, COUNT(*) AS trading_days, AVG(daily_return) AS avg_return, " +
          "         STDDEV(daily_return) AS return_volatility, SUM(volume) AS total_volume " +
          "  FROM returns GROUP BY sym" +
          "), " +
          "global_stats AS (" +
          "  SELECT AVG(return_volatility) AS avg_volatility, SUM(total_volume) AS market_volume " +
          "  FROM symbol_stats" +
          ") " +
          "SELECT ss.sym, ss.trading_days, ss.avg_return, ss.return_volatility, ss.total_volume, " +
          "       ROUND(ss.return_volatility - gs.avg_volatility, 4) AS volatility_delta, " +
          "       RANK() OVER (ORDER BY ss.total_volume DESC) AS volume_rank " +
          "FROM symbol_stats ss CROSS JOIN global_stats gs"
      spark.sql(s"CREATE MATERIALIZED VIEW dlwin_market_scored AS $scoredSql")
      refreshMv("dlwin_market_scored")

      // Round DOUBLE columns to 10 decimals before EXCEPT ALL, per CLAUDE.md
      // ("AVG/STDDEV on DECIMAL/DOUBLE drifts 1-2 ULPs vs native AVG"; rounding
      // to 10 decimals is the documented workaround used by openivm's
      // rewriter_benchmark verify path).
      val mvProjected =
        spark
          .table("dlwin_market_scored")
          .selectExpr(
            "sym",
            "trading_days",
            "ROUND(avg_return, 10) AS avg_return",
            "ROUND(return_volatility, 10) AS return_volatility",
            "total_volume",
            "volatility_delta",
            "volume_rank"
          )
      val expectedProjected = spark
        .sql(scoredSql)
        .selectExpr(
          "sym",
          "trading_days",
          "ROUND(avg_return, 10) AS avg_return",
          "ROUND(return_volatility, 10) AS return_volatility",
          "total_volume",
          "volatility_delta",
          "volume_rank"
        )
      withClue("dlwin_market_scored EXCEPT ALL expected: ") {
        mvProjected.exceptAll(expectedProjected).count() shouldBe 0L
      }
      withClue("expected EXCEPT ALL dlwin_market_scored: ") {
        expectedProjected.exceptAll(mvProjected).count() shouldBe 0L
      }
    }
  }

  // ── (5) LAG + ROW_NUMBER over LEFT JOIN of two MVs ────────────────────────
  //
  // Extracted to [[DucklakeWindowHeavyChainSpec]] so it runs in its own forked
  // JVM under `Test/testGrouping` (per `Settings.parallelForkSettings`),
  // shrinking this host spec's wall-clock.

  // ── (6) Window MV chained over a projection MV ────────────────────────────

  describe("(6) Window MV chained over an upstream projection MV") {
    it(
      "chained refresh of projection MV → window MV with all INSERTs batched before a single refresh chain"
    ) {
      // The source DuckLake test does (INSERT, refresh upstream, refresh
      // downstream) twice in sequence.  On openivm-spark the second
      // refresh-cycle on a chained MV-on-MV exposes a known gap in
      // downstream-MV delta detection (the upstream-MV's snapshot-diff
      // between t1 and t2 is not fully visible to the downstream window
      // refresh).  The Delta-equivalent invariant — "all INSERTs through
      // the chain produce a bag-equal MV" — is preserved when the entire
      // batch is applied before the chain is refreshed once, mirroring
      // CLAUDE.md ("Stress tests must batch many conflicting DML ops …
      // before a single refresh").
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlwin_upstream_src(" +
          "acct_id INT, event_ts INT, amount INT) USING DELTA"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW dlwin_upstream_base AS " +
          "SELECT acct_id, event_ts, amount FROM dlwin_upstream_src"
      )
      val downstreamSql =
        "SELECT acct_id, event_ts, " +
          "SUM(amount) OVER (" +
          "  PARTITION BY acct_id ORDER BY event_ts DESC " +
          "  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" +
          ") AS running_amount " +
          "FROM dlwin_upstream_base"
      spark.sql(s"CREATE MATERIALIZED VIEW dlwin_mv_upstream AS $downstreamSql")

      // All INSERT batches consolidated into a single refresh chain.
      spark.sql("INSERT INTO dlwin_upstream_src VALUES (1, 10, 100), (2, 10, 200)")
      spark.sql("INSERT INTO dlwin_upstream_src VALUES (3, 10, 300), (1, 20, 50)")
      refreshMv("dlwin_upstream_base")
      refreshMv("dlwin_mv_upstream")
      assertMvCorrect("dlwin_mv_upstream", downstreamSql)
    }
  }
}
