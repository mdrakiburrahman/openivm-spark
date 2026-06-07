package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Regression spec for the Cartesian-on-NULL MERGE explosion in
  * `SparkRefreshRewriter.deduplicateNullSafeMergeSource`.
  *
  * == What is being regressed ==
  *
  * OpenIVM emits a delete-only MERGE on `openivm_left_key` (or
  * `openivm_right_key`) with `IS NOT DISTINCT FROM` semantics for FULL OUTER
  * projection partial recompute (see `.temp/openivm/docs/operators/full-outer-join.md`).
  *
  * If the view-delta source contains many rows sharing the same key value
  * (e.g. multiple NULL-keyed dangling rows, or a hot non-NULL key duplicated
  * many times), the MERGE's join with the MV produces an N×M intermediate.
  * Spark's broadcast estimator under-counts the result, attempts to broadcast
  * the source, then fails with `Cannot broadcast the table that is larger
  * than 8.0 GiB` on real-world data shapes (reproduced on
  * `fact_market_history` in PR mdrakiburrahman/ivm-bench#29).
  *
  * The fix wraps the USING source with `SELECT DISTINCT <key_cols> FROM
  * (<orig_source>)` — collapses duplicate keys to one row per affected key,
  * preserves NULL-safe equality, and gives Spark accurate source-row
  * statistics for join planning.
  *
  * == Tests ==
  *
  * Each test creates a LEFT JOIN MV whose hidden left-key column resolves to
  * a column with many NULL-keyed and many duplicate-keyed rows.  We then
  * push a delta with **duplicate** keys to verify the refresh is both
  * correct (bidirectional `EXCEPT ALL`) and does not OOM / Cartesian-explode.
  * Spec uses small data — the structural fix (DISTINCT in source) is
  * verifiable at any scale; the Cartesian risk is N×M where N and M come
  * from raw delta and MV cardinalities.
  */
class LeftJoinNullKeyDedupSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-ljnkd-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-LeftJoinNullKeyDedupSpec")
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
    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  describe("LEFT JOIN MV with NULL left-key and duplicate-key view-delta (Cartesian-dedup fix)") {
    it("retracts and re-inserts NULL-keyed and hot-keyed rows without Cartesian explosion") {
      // Schema: `ljnkd_security` is a LEFT-join "preserved" side whose key
      // column `sk_company_id` can be NULL (companies without a financial
      // ledger).  `ljnkd_financials` carries metric rows joined by company.
      spark.sql(
        "CREATE TABLE IF NOT EXISTS ljnkd_security " +
          "(sk_security_id INT, sk_company_id INT, ticker STRING) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS ljnkd_financials " +
          "(sk_company_id INT, period STRING, revenue INT) USING DELTA"
      )

      // Initial security data: 6 rows, 3 with sk_company_id=NULL (dangling LEFT)
      // and 3 with non-NULL keys.
      spark.sql(
        """INSERT INTO ljnkd_security VALUES
          |  (1, 10, 'AAA'),
          |  (2, 20, 'BBB'),
          |  (3, 20, 'CCC'),
          |  (4, NULL, 'DDD'),
          |  (5, NULL, 'EEE'),
          |  (6, NULL, 'FFF')""".stripMargin
      )
      spark.sql(
        """INSERT INTO ljnkd_financials VALUES
          |  (10, 'Q1', 100),
          |  (20, 'Q1', 200),
          |  (20, 'Q2', 250)""".stripMargin
      )

      // MV body: LEFT JOIN preserves null-keyed rows on the left side.
      val viewBody =
        "SELECT s.sk_security_id, s.sk_company_id, s.ticker, f.period, f.revenue " +
          "FROM ljnkd_security s LEFT JOIN ljnkd_financials f ON s.sk_company_id = f.sk_company_id"

      spark.sql(s"CREATE MATERIALIZED VIEW ljnkd_mv AS $viewBody")
      assertMvCorrect("ljnkd_mv", viewBody)

      // Delta 1: insert MANY new financial rows under the SAME (hot) key
      // and under NULL company_id (which never matches any left-side
      // dangling row in a LEFT JOIN, but openivm's affected-keys retract
      // logic must still handle the duplicate-key delta source).
      // 10 duplicates of (10, 'Q3', x) and 8 duplicates of (NULL, 'Q3', x)
      // — i.e., the delta has duplicate keys that previously triggered the
      // Cartesian explosion at MERGE time.
      val dupRowsForKey10 = (1 to 10).map(i => s"(10, 'Q3', ${300 + i})").mkString(", ")
      val dupRowsForNull  = (1 to 8).map(i => s"(NULL, 'Q3', ${400 + i})").mkString(", ")
      spark.sql(s"INSERT INTO ljnkd_financials VALUES $dupRowsForKey10")
      spark.sql(s"INSERT INTO ljnkd_financials VALUES $dupRowsForNull")

      refreshMv("ljnkd_mv")
      assertMvCorrect("ljnkd_mv", viewBody)

      // Delta 2: also bash with duplicate-non-NULL via UPDATE (delete+insert).
      spark.sql("UPDATE ljnkd_financials SET revenue = 999 WHERE period = 'Q1' AND sk_company_id = 20")
      refreshMv("ljnkd_mv")
      assertMvCorrect("ljnkd_mv", viewBody)

      // Delta 3: insert several NEW null-keyed security rows AND null-keyed
      // financial rows in one batch — exercises bidirectional NULL flow
      // through both source tables in a single refresh.
      val moreNullSec = (7 to 15).map(i => s"($i, NULL, 'X$i')").mkString(", ")
      val moreNullFin = (1 to 6).map(i => s"(NULL, 'Q4', ${500 + i})").mkString(", ")
      spark.sql(s"INSERT INTO ljnkd_security VALUES $moreNullSec")
      spark.sql(s"INSERT INTO ljnkd_financials VALUES $moreNullFin")
      refreshMv("ljnkd_mv")
      assertMvCorrect("ljnkd_mv", viewBody)

      // Delta 4: delete several null-keyed left-side rows.
      spark.sql("DELETE FROM ljnkd_security WHERE sk_company_id IS NULL AND sk_security_id IN (4, 5, 7, 8, 9)")
      refreshMv("ljnkd_mv")
      assertMvCorrect("ljnkd_mv", viewBody)

      spark.sql("DROP MATERIALIZED VIEW ljnkd_mv")
      spark.sql("DROP TABLE ljnkd_security")
      spark.sql("DROP TABLE ljnkd_financials")
    }

  }
}

// NB: FULL OUTER JOIN with bare projection and bulk duplicate-NULL-keyed
// delta exposes a pre-existing openivm classification limitation (verified
// against baseline without the dedup pass — same failure mode).  That is a
// separate engine-side issue tracked outside this spec.  The dedup pass is
// validated end-to-end here for the LEFT JOIN duplicate-key path and via
// `SparkRefreshRewriterSpec` unit tests for the FULL OUTER MERGE shape
// directly.
