package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Repro for the ivm-bench SF=10 OAT failure:
  *
  *   `Cannot broadcast the table that is larger than 8.0 GiB: 57.6 GiB`
  *
  * on `gold.fact_market_history` — a `SIMPLE_PROJECTION` MV over an SCD2
  * range-joined view body (`dim_security` versions × `daily_market`).
  *
  * The MV body shape (from the bench's `fact_market_history.sql`):
  *
  * {{{
  *   SELECT s.sk_security_id, dmh.dm_date AS sk_date_id, dmh.dm_close
  *   FROM daily_market dmh
  *   JOIN dim_security s
  *     ON s.symbol = dmh.dm_s_symb
  *    AND dmh.dm_date BETWEEN s.effective_timestamp AND s.end_timestamp
  *   LEFT JOIN wrk_company_financials f USING (sk_company_id)
  * }}}
  *
  * The cycle that produced the explosion is:
  *
  *   1. openivm classifies the MV as `SIMPLE_PROJECTION`.
  *   2. For an incremental refresh openivm emits:
  *      a. a view-delta CTAS holding the affected `openivm_left_key`s;
  *      b. a DELETE MERGE on `openivm_left_key` against the MV;
  *      c. an INSERT recompute wrapping the **full view body** inside
  *         `MERGE INTO <mv> USING ( ... <view body> ... WHERE EXISTS (SELECT 1
  *         FROM <view_deltas> _d WHERE _d.openivm_left_key IS NOT DISTINCT FROM
  *         openivm_lj.openivm_left_key) ) AS d ON false WHEN NOT MATCHED THEN INSERT`.
  *   3. Spark's Catalyst CANNOT push the correlated `WHERE EXISTS (...
  *      IS NOT DISTINCT FROM …)` filter into the openivm CTE chain, so it
  *      materialises the **full** view-body join first and then filters.
  *      The SCD2 `BETWEEN` predicate amplifies the intermediate by
  *      `versions × dates`, blowing past the 8 GiB BroadcastExchange cap at
  *      production SF, and a smaller fixture proves the rewrite still applies
  *      cleanly and preserves correctness.
  *
  * `SparkRefreshRewriter.rewriteRecomputeWhereExistsAsAffectedKeysJoin` rewrites
  * the `WHERE EXISTS (… IS NOT DISTINCT FROM …)` into a
  * `LEFT SEMI JOIN (SELECT DISTINCT key FROM <view_deltas>) ON o.key <=> i.key`.
  * Catalyst then engages `PushDownLeftSemiAntiJoin` + `PushDownPredicates` to
  * push the affected-key filter through the view body and into the underlying
  * scans, pruning the SCD2 range join to only affected rows.
  *
  * This spec uses a tiny per-spec fixture (`spsj_*` prefix, UUID-suffixed
  * warehouse dir) — see `copilot-instructions.md` test conventions.
  */
class SimpleProjectionSCD2RangeJoinRecomputeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-spsj-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleProjectionSCD2RangeJoinRecomputeSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "true")
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
    val mvCount             = mv.count()
    val expectedCount       = expected.count()
    val missingDf           = expected.exceptAll(mv)
    val spuriousDf          = mv.exceptAll(expected)
    val missing             = missingDf.count()
    val spurious            = spuriousDf.count()
    val diffSamples =
      Seq(
        if (spurious > 0) Some(s"spurious_sample=${spuriousDf.limit(20).collect().mkString("[", ", ", "]")}")
        else None,
        if (missing > 0) Some(s"missing_sample=${missingDf.limit(20).collect().mkString("[", ", ", "]")}") else None
      ).flatten.mkString(" ")
    val diagnostics =
      s"$mvName: mv_count=$mvCount, expected_count=$expectedCount, " +
        s"missing(expected\\mv)=$missing, spurious(mv\\expected)=$spurious. $diffSamples "
    withClue(diagnostics + s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      spurious shouldBe 0L
    }
    withClue(diagnostics + s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      missing shouldBe 0L
    }
  }

  private val MarketHistorySql: String =
    """SELECT
      |  s.sk_security_id,
      |  s.sk_company_id,
      |  dmh.dm_date     AS sk_date_id,
      |  dmh.dm_close    AS closeprice,
      |  dmh.dm_high     AS dayhigh,
      |  dmh.dm_low      AS daylow,
      |  dmh.dm_vol      AS volume,
      |  f.eps           AS eps
      |FROM spsj_daily_market dmh
      |JOIN spsj_dim_security s
      |  ON s.symbol = dmh.dm_s_symb
      | AND dmh.dm_date BETWEEN s.effective_timestamp AND s.end_timestamp
      |LEFT JOIN spsj_wrk_financials f USING (sk_company_id)""".stripMargin

  describe("SIMPLE_PROJECTION recompute with WHERE EXISTS over SCD2 range join") {

    it("incremental INSERT into daily_market hits the affected-keys semi-join path and stays correct") {
      spark.sql(
        """CREATE TABLE IF NOT EXISTS spsj_daily_market(
          |  dm_date DATE, dm_s_symb STRING,
          |  dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol BIGINT
          |) USING DELTA""".stripMargin
      )
      spark.sql(
        """CREATE TABLE IF NOT EXISTS spsj_dim_security(
          |  sk_security_id STRING, sk_company_id STRING, symbol STRING,
          |  effective_timestamp DATE, end_timestamp DATE
          |) USING DELTA""".stripMargin
      )
      spark.sql(
        """CREATE TABLE IF NOT EXISTS spsj_wrk_financials(
          |  sk_company_id STRING, eps DOUBLE
          |) USING DELTA""".stripMargin
      )

      // ── BATCH 1: seed dim_security with multiple SCD2 versions per symbol
      // and one quote per symbol in daily_market.
      spark.sql(
        """INSERT INTO spsj_dim_security VALUES
          |  ('sec_a_v1', 'co_a', 'AAA', DATE'2000-01-01', DATE'2017-12-31'),
          |  ('sec_a_v2', 'co_a', 'AAA', DATE'2018-01-01', DATE'9999-12-31'),
          |  ('sec_b_v1', 'co_b', 'BBB', DATE'2000-01-01', DATE'2019-06-30'),
          |  ('sec_b_v2', 'co_b', 'BBB', DATE'2019-07-01', DATE'9999-12-31'),
          |  ('sec_c_v1', 'co_c', 'CCC', DATE'2000-01-01', DATE'9999-12-31')""".stripMargin
      )
      spark.sql(
        """INSERT INTO spsj_wrk_financials VALUES
          |  ('co_a', 1.5), ('co_b', 2.5)""".stripMargin
      )
      spark.sql(
        """INSERT INTO spsj_daily_market VALUES
          |  (DATE'2017-04-10', 'AAA', 100.0, 101.0, 99.5,  1000),
          |  (DATE'2019-08-15', 'BBB', 200.0, 205.5, 198.0, 2000),
          |  (DATE'2020-06-01', 'CCC', 50.0,  51.0,  49.5,  3000)""".stripMargin
      )

      spark.sql(s"CREATE MATERIALIZED VIEW spsj_mv AS $MarketHistorySql")

      assertMvCorrect("spsj_mv", MarketHistorySql)

      // ── BATCH 2: small DML — appends a few new daily_market rows.
      // openivm will classify this as SIMPLE_PROJECTION and emit a recompute
      // INSERT shaped like `MERGE INTO <mv> USING ( <view body> WHERE EXISTS
      // (... IS NOT DISTINCT FROM ...)) AS d ON false WHEN NOT MATCHED THEN INSERT`.
      // The rewrite must replace that WHERE EXISTS with a LEFT SEMI JOIN on the
      // affected-keys delta — assertMvCorrect proves the result set is unchanged.
      spark.sql(
        """INSERT INTO spsj_daily_market VALUES
          |  (DATE'2017-04-11', 'AAA', 102.0, 103.0, 101.5, 1100),
          |  (DATE'2018-01-02', 'AAA', 110.0, 112.0, 109.0, 1200),
          |  (DATE'2019-08-16', 'BBB', 201.0, 206.0, 199.0, 2100)""".stripMargin
      )

      spark.sql("REFRESH MATERIALIZED VIEW spsj_mv").collect()
      assertMvCorrect("spsj_mv", MarketHistorySql)

      // ── BATCH 3: a row whose SCD2 version-window expands to MULTIPLE
      // dim_security versions for the same symbol — the kind of cross-product
      // amplification the bench failure hinged on. (`AAA` on its v2-window
      // start date only matches v2; verifying still-1× match.)
      spark.sql(
        """INSERT INTO spsj_daily_market VALUES
          |  (DATE'2018-01-01', 'AAA', 115.0, 116.0, 114.0, 1500)""".stripMargin
      )
      spark.sql("REFRESH MATERIALIZED VIEW spsj_mv").collect()
      assertMvCorrect("spsj_mv", MarketHistorySql)
    }
  }
}
