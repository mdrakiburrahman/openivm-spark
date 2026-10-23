package org.openivm.spark.parity

import org.openivm.spark.common.{ChangeFeedMode, FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** running-window suffix-extend refresh (`windowRunningIncremental`).
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
    Map(
      FeatureGate.WindowRunningIncrementalEnabledKey -> "true",
      FeatureGate.QueryLogEnabledKey                 -> "true",
      FeatureGate.ProfileRefreshKey                  -> "true"
    )

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  private def executedRefreshSql: Seq[String] =
    sql("SHOW OPENIVM QUERY LOG")
      .where("category = 'rewritten_stmt'")
      .select("sql_text")
      .collect()
      .map(_.getString(0))
      .toSeq

  private def assertCascadeStorageMode(viewName: String): Unit = {
    val statements = sql("SHOW OPENIVM QUERY LOG")
      .where(s"category = 'rewritten_stmt' AND view_name IN ('$viewName', 'default.$viewName')")
      .select("sql_text")
      .collect()
      .map(_.getString(0))
    statements.exists(_.contains("/_ivm/view_deltas/")) shouldBe (changeFeedMode == ChangeFeedMode.Intercept)
    // The real target append must not be confused with the suffix cascade copy.
    statements.exists(s =>
      s.startsWith("INSERT INTO") && s.contains(s"FROM openivm_run_result_$viewName") &&
        !s.contains("/_ivm/view_deltas/")
    ) shouldBe true
  }

  private val viewSql =
    "SELECT dm_date, dm_s_symb, " +
      "MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_low, " +
      "MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_high " +
      "FROM wri_daily_market"

  private def stateKeyDetails(view: String): Seq[String] =
    sql("SHOW OPENIVM REFRESH PROFILE")
      .where(s"view_name IN ('$view', 'default.$view') AND step_name = 'window_running_state_keys'")
      .select("detail")
      .collect()
      .map(_.getString(0))
      .toSeq

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
      val executedSql = executedRefreshSql
      executedSql should contain("CACHE LAZY TABLE `openivm_run_result_wri_mv`")
      executedSql should not contain "CACHE TABLE `openivm_run_result_wri_mv`"
      executedSql should not contain "CACHE TABLE `openivm_run_fast_wri_mv`"
      executedSql should not contain "CACHE TABLE `openivm_run_fallback_wri_mv`"
      executedSql.exists(s => s.startsWith("MERGE INTO") && s.contains("wri_mv")) shouldBe false
      executedSql.exists(s => s.startsWith("INSERT INTO") && s.contains("FROM openivm_new_wri_mv")) shouldBe false
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
      executedRefreshSql should not contain "CACHE TABLE `openivm_run_result_wri_bd_mv`"
      executedRefreshSql.exists(s =>
        s.startsWith("INSERT INTO") && s.contains("FROM openivm_run_result_wri_bd_mv")
      ) shouldBe false
      executedRefreshSql.exists(s => s.startsWith("MERGE INTO") && s.contains("wri_bd_mv")) shouldBe true
    }

    it("reuses versioned state across suffix batches and rejects it after a backdated recompute") {
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
      stateKeyDetails("wri_seq_mv").last shouldBe "hits=0;requested=1"

      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-03','AAA',12,18)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)
      mvRefreshType("wri_seq_mv") shouldBe RefreshTypeCode.WindowPartition
      stateKeyDetails("wri_seq_mv").last shouldBe "hits=1;requested=1"
      executedRefreshSql.count(_ == "CACHE LAZY TABLE `openivm_run_result_wri_seq_mv`") shouldBe 2

      // Partial hit: an already cached partition and one unseen key. The new
      // partition must still be seeded by the authoritative historical scan.
      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-04','AAA',5,26),(DATE '2024-03-01','BBB',7,19)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)
      stateKeyDetails("wri_seq_mv").last shouldBe "hits=1;requested=2"
      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-05','AAA',4,27),(DATE '2024-03-02','BBB',6,20)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)
      stateKeyDetails("wri_seq_mv").last shouldBe "hits=2;requested=2"

      // A non-suffix refresh advances Delta without advancing this cache. The
      // next suffix must miss rather than reusing the pre-recompute tail.
      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-02-01','AAA',1,40)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)
      sql("INSERT INTO wri_seq_daily_market VALUES (DATE '2024-03-06','AAA',9,21)")
      refreshMv("wri_seq_mv")
      assertMvCorrect("wri_seq_mv", seqSql)
      stateKeyDetails("wri_seq_mv").last shouldBe "hits=0;requested=1"
    }

    it("preserves nullable keys, exact decimal seeds and microsecond timestamp bounds in cached state") {
      sql("CREATE TABLE wri_typed_source(k STRING, p INT, t TIMESTAMP, v DECIMAL(30,12)) USING DELTA")
      sql(
        "INSERT INTO wri_typed_source VALUES (NULL,1,TIMESTAMP '2024-01-01 00:00:00.123456',123456789012.123456789012)"
      )
      val query = "SELECT k,p,t,v,MIN(v) OVER (PARTITION BY k ORDER BY t) AS lo," +
        "MAX(v) OVER (PARTITION BY k ORDER BY t) AS hi FROM wri_typed_source"
      sql(s"CREATE MATERIALIZED VIEW wri_typed_mv AS $query")
      sql(
        "INSERT INTO wri_typed_source VALUES (NULL,1,TIMESTAMP '2024-01-01 00:00:00.123457',123456789011.123456789011)"
      )
      refreshMv("wri_typed_mv")
      assertMvCorrect("wri_typed_mv", query)
      sql(
        "INSERT INTO wri_typed_source VALUES (NULL,1,TIMESTAMP '2024-01-01 00:00:00.123458',123456789010.123456789010)"
      )
      refreshMv("wri_typed_mv")
      assertMvCorrect("wri_typed_mv", query)
      stateKeyDetails("wri_typed_mv").last shouldBe "hits=1;requested=1"
      // A sub-millisecond backdated row must not be mistaken for a suffix.
      sql(
        "INSERT INTO wri_typed_source VALUES (NULL,1,TIMESTAMP '2024-01-01 00:00:00.123455',123456789009.123456789009)"
      )
      refreshMv("wri_typed_mv")
      assertMvCorrect("wri_typed_mv", query)
    }

    it("handles the real daily_market window-over-window shape (cumulative MIN/MAX -> flag CASE -> cumulative MAX)") {
      sql(
        "CREATE TABLE wri_wow_market(dm_date DATE, dm_s_symb STRING, dm_close INT, dm_high INT, dm_low INT, " +
          "dm_vol INT) USING DELTA"
      )
      sql(
        "INSERT INTO wri_wow_market VALUES " +
          "(DATE '2024-01-01','AAA',10,20,5,100),(DATE '2024-01-02','AAA',11,22,4,110)," +
          "(DATE '2024-01-01','BBB',30,40,15,200),(DATE '2024-01-02','BBB',31,38,16,210)"
      )
      val wowSql =
        "WITH cumulative AS (" +
          "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, " +
          "MIN(dm_low) OVER w AS f52_low, MAX(dm_high) OVER w AS f52_high " +
          "FROM wri_wow_market WINDOW w AS (PARTITION BY dm_s_symb ORDER BY dm_date)), " +
          "flagged AS (SELECT *, " +
          "CASE WHEN dm_low = f52_low THEN dm_date ELSE NULL END AS low_flag, " +
          "CASE WHEN dm_high = f52_high THEN dm_date ELSE NULL END AS high_flag FROM cumulative) " +
          "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, f52_low, f52_high, " +
          "MAX(low_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS f52_low_date, " +
          "MAX(high_flag) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS f52_high_date FROM flagged"
      sql(s"CREATE MATERIALIZED VIEW wri_wow_mv AS $wowSql")
      assertMvCorrect("wri_wow_mv", wowSql)
      mvRefreshType("wri_wow_mv") shouldBe RefreshTypeCode.WindowPartition

      // Strictly-later insert-only batch (fast path) + a brand-new partition.
      sql(
        "INSERT INTO wri_wow_market VALUES " +
          "(DATE '2024-01-03','AAA',9,24,3,120),(DATE '2024-01-03','BBB',33,36,17,220)," +
          "(DATE '2024-01-01','CCC',50,60,25,300)"
      )
      refreshMv("wri_wow_mv")
      assertMvCorrect("wri_wow_mv", wowSql)
      mvRefreshType("wri_wow_mv") shouldBe RefreshTypeCode.WindowPartition
      sql(
        "INSERT INTO wri_wow_market VALUES " +
          "(DATE '2024-01-04','AAA',9,23,4,130),(DATE '2024-01-04','BBB',33,42,14,230)"
      )
      refreshMv("wri_wow_mv")
      assertMvCorrect("wri_wow_mv", wowSql)
      stateKeyDetails("wri_wow_mv").last shouldBe "hits=2;requested=2"
    }

    it("keeps a downstream MV correct via the window fast path's cascade view-delta") {
      sql("CREATE TABLE wri_casc_market(dm_date DATE, dm_s_symb STRING, dm_low INT, dm_high INT) USING DELTA")
      sql(
        "INSERT INTO wri_casc_market VALUES " +
          "(DATE '2024-01-01','AAA',10,20),(DATE '2024-01-02','AAA',8,22)," +
          "(DATE '2024-01-01','BBB',15,25),(DATE '2024-01-02','BBB',14,26)"
      )
      val upstreamSql =
        "SELECT dm_date, dm_s_symb, " +
          "MIN(dm_low) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_low, " +
          "MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS wk_high " +
          "FROM wri_casc_market"
      sql(s"CREATE MATERIALIZED VIEW wri_casc_win AS $upstreamSql")
      // Downstream reads the target CDF or the persisted intercept cascade.
      val downstreamSql =
        "SELECT dm_s_symb, COUNT(*) AS n, MIN(wk_low) AS lo, MAX(wk_high) AS hi FROM wri_casc_win GROUP BY dm_s_symb"
      sql(s"CREATE MATERIALIZED VIEW wri_casc_agg AS $downstreamSql")
      assertMvCorrect("wri_casc_win", upstreamSql)
      assertMvCorrect("wri_casc_agg", downstreamSql)

      // Strictly-later insert-only batch → window fast path → cascade view-delta → downstream stays incremental.
      sql(
        "INSERT INTO wri_casc_market VALUES " +
          "(DATE '2024-01-03','AAA',7,24),(DATE '2024-01-03','BBB',13,27)," +
          "(DATE '2024-01-01','CCC',30,40),(DATE '2024-01-01','CCC',30,40)"
      )
      refreshMv("wri_casc_win")
      refreshMv("wri_casc_agg")
      assertMvCorrect("wri_casc_win", upstreamSql)
      assertMvCorrect("wri_casc_agg", downstreamSql)
      mvRefreshType("wri_casc_win") shouldBe RefreshTypeCode.WindowPartition
      executedRefreshSql.exists(_.contains("openivm_empty_fallback WHERE false")) shouldBe
        (changeFeedMode == ChangeFeedMode.Intercept)
      executedRefreshSql should contain("CACHE LAZY TABLE `openivm_run_result_wri_casc_win`")
      assertCascadeStorageMode("wri_casc_win")

      // The fallback delta and materialised suffix must both reach the child.
      sql(
        "INSERT INTO wri_casc_market VALUES " +
          "(DATE '2024-01-01','AAA',2,50),(DATE '2024-01-04','BBB',12,28)"
      )
      refreshMv("wri_casc_win")
      refreshMv("wri_casc_agg")
      assertMvCorrect("wri_casc_win", upstreamSql)
      assertMvCorrect("wri_casc_agg", downstreamSql)
      // A mixed fallback/suffix batch does not publish a seed update and must
      // retain eager materialization before either target mutation.
      executedRefreshSql should contain("CACHE TABLE `openivm_run_result_wri_casc_win`")
      executedRefreshSql.count(_ == "CACHE LAZY TABLE `openivm_run_result_wri_casc_win`") shouldBe 1
      assertCascadeStorageMode("wri_casc_win")
    }

    itCdf("retries a rejected suffix append without publishing its prepared state or losing the downstream change") {
      sql("CREATE TABLE wri_retry_source(k STRING, t INT, v INT) USING DELTA")
      sql("INSERT INTO wri_retry_source VALUES ('A',1,10)")
      val query = "SELECT k,t,v,MIN(v) OVER (PARTITION BY k ORDER BY t) AS lo," +
        "MAX(v) OVER (PARTITION BY k ORDER BY t) AS hi FROM wri_retry_source"
      sql(s"CREATE MATERIALIZED VIEW wri_retry_mv AS $query")
      val childQuery = "SELECT k,COUNT(*) AS n,MIN(lo) AS lo,MAX(hi) AS hi FROM wri_retry_mv GROUP BY k"
      sql(s"CREATE MATERIALIZED VIEW wri_retry_child AS $childQuery")
      val path = mvDataLocation("wri_retry_mv").replace("`", "``")
      sql(s"ALTER TABLE delta.`$path` ADD CONSTRAINT wri_retry_order CHECK (t < 3)")

      // Consume the constraint's metadata version before exercising the cache's
      // prepare-before-mutation path on the next refresh.
      sql("INSERT INTO wri_retry_source VALUES ('A',2,8)")
      refreshMv("wri_retry_mv")
      refreshMv("wri_retry_child")
      val beforeVersion = mvDataVersion("wri_retry_mv")
      sql("INSERT INTO wri_retry_source VALUES ('A',3,6)")
      intercept[Exception] { refreshMv("wri_retry_mv") }
      mvDataVersion("wri_retry_mv") shouldBe beforeVersion
      sql("SELECT COUNT(*) FROM wri_retry_mv").head().getLong(0) shouldBe 2L
      assertMvCorrect("wri_retry_child", childQuery)
      val failedProfile = RefreshProfileCatalog
        .scanAll(spark)
        .filter(_.viewName.split("\\.").last == "wri_retry_mv")
      failedProfile.count(_.stepName == "window_running_state_prepare") shouldBe 1
      failedProfile.count(_.stepName == "window_running_state_commit") shouldBe 0

      sql(s"ALTER TABLE delta.`$path` DROP CONSTRAINT wri_retry_order")
      refreshMv("wri_retry_mv")
      refreshMv("wri_retry_child")
      // Dropping the constraint and the successful append each commit once.
      mvDataVersion("wri_retry_mv") shouldBe beforeVersion + 2L
      assertMvCorrect("wri_retry_mv", query)
      assertMvCorrect("wri_retry_child", childQuery)
      assertCascadeStorageMode("wri_retry_mv")
    }

    it("preserves ROWS positions while skipping empty fallback and suffix branches") {
      sql("CREATE TABLE wri_rows_source(k STRING, t INT, v INT) USING DELTA")
      sql("INSERT INTO wri_rows_source VALUES ('A',1,5),('A',3,7),('B',1,NULL)")
      val query = "SELECT k,t,v,SUM(v) OVER (PARTITION BY k ORDER BY t " +
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total FROM wri_rows_source"
      sql(s"CREATE MATERIALIZED VIEW wri_rows_mv AS $query")
      sql("INSERT INTO wri_rows_source VALUES ('A',4,2),('B',2,6),('C',1,NULL)")
      refreshMv("wri_rows_mv")
      assertMvCorrect("wri_rows_mv", query)
      executedRefreshSql.count(_ == "CACHE TABLE `openivm_run_suffix_wri_rows_mv`") shouldBe 1
      sql("INSERT INTO wri_rows_source VALUES ('A',2,4)")
      refreshMv("wri_rows_mv")
      assertMvCorrect("wri_rows_mv", query)
      // The second batch is entirely backdated: peer-position and result caches
      // must not be populated again, while the persisted positions stay correct.
      executedRefreshSql.count(_ == "CACHE TABLE `openivm_run_suffix_wri_rows_mv`") shouldBe 1
      executedRefreshSql.count(_ == "CACHE LAZY TABLE `openivm_run_result_wri_rows_mv`") shouldBe 1
    }

    it("handles a NULL-order suffix followed by a mixed backdated batch without reusing stale seeds") {
      sql("CREATE TABLE wri_null_order_source(k STRING, t INT, v INT) USING DELTA")
      sql("INSERT INTO wri_null_order_source VALUES ('A',1,10),('B',1,20)")
      val query = "SELECT k,t,v,MIN(v) OVER (PARTITION BY k ORDER BY t NULLS LAST) AS lo," +
        "MAX(v) OVER (PARTITION BY k ORDER BY t NULLS LAST) AS hi FROM wri_null_order_source"
      sql(s"CREATE MATERIALIZED VIEW wri_null_order_mv AS $query")

      // NULL follows every non-NULL order key. This suffix seeds the cache's
      // null counter, not just its maximum non-NULL order value.
      sql("INSERT INTO wri_null_order_source VALUES ('A',2,7),('B',NULL,8)")
      refreshMv("wri_null_order_mv")
      assertMvCorrect("wri_null_order_mv", query)
      executedRefreshSql should contain("CACHE LAZY TABLE `openivm_run_result_wri_null_order_mv`")

      // B's non-NULL row precedes its cached NULL tail and must recompute B;
      // A still extends its suffix. Both changes must remain exact.
      sql("INSERT INTO wri_null_order_source VALUES ('A',3,5),('B',2,6)")
      refreshMv("wri_null_order_mv")
      assertMvCorrect("wri_null_order_mv", query)
      stateKeyDetails("wri_null_order_mv").last shouldBe "hits=2;requested=2"
      executedRefreshSql should contain("CACHE TABLE `openivm_run_result_wri_null_order_mv`")

      // The mixed refresh invalidates the old cache generation, including A.
      sql("INSERT INTO wri_null_order_source VALUES ('A',4,4)")
      refreshMv("wri_null_order_mv")
      assertMvCorrect("wri_null_order_mv", query)
      stateKeyDetails("wri_null_order_mv").last shouldBe "hits=0;requested=1"
      mvRefreshType("wri_null_order_mv") shouldBe RefreshTypeCode.WindowPartition
    }

    it("preserves nullable cumulative SUM and AVG seeds across consecutive suffix batches") {
      sql("CREATE TABLE wri_avg_source(k STRING, t INT, v DOUBLE) USING DELTA")
      sql("INSERT INTO wri_avg_source VALUES ('A',1,NULL),('B',1,0.125),('B',2,0.25)")
      val query =
        "SELECT k,t,v,SUM(v) OVER (PARTITION BY k ORDER BY t) AS total," +
          "AVG(v) OVER (PARTITION BY k ORDER BY t) AS mean FROM wri_avg_source"
      sql(s"CREATE MATERIALIZED VIEW wri_avg_mv AS $query")
      assertMvCorrect("wri_avg_mv", query)
      sql("INSERT INTO wri_avg_source VALUES ('A',2,4.0),('B',3,0.375),('C',1,NULL)")
      refreshMv("wri_avg_mv")
      assertMvCorrect("wri_avg_mv", query)
      sql("INSERT INTO wri_avg_source VALUES ('A',3,NULL),('B',4,0.5),('C',2,2.0)")
      refreshMv("wri_avg_mv")
      assertMvCorrect("wri_avg_mv", query)
      mvRefreshType("wri_avg_mv") shouldBe RefreshTypeCode.WindowPartition
    }
  }
}
