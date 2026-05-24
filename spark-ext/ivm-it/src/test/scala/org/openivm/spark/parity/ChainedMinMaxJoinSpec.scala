package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Repro for `.research/OPENIVM-BUG.md`: chained refresh from an upstream
  * AGGREGATE_GROUP MV (MIN/MAX over a conditional-NULL projection) into a
  * downstream SIMPLE_PROJECTION MV that JOINs the upstream MV against a SCD2
  * dim on `upstream.placed BETWEEN dim.eff AND dim.endts`.
  *
  * Mirrors §8.1 of the bug report exactly:
  *
  * {{{
  *   CREATE MATERIALIZED VIEW cmmj_upstream AS
  *     SELECT k, MIN(p) AS placed, MAX(r) AS removed
  *     FROM cmmj_events GROUP BY k;
  *
  *   CREATE MATERIALIZED VIEW cmmj_downstream AS
  *     SELECT u.k, u.placed, u.removed
  *     FROM cmmj_upstream u
  *     JOIN cmmj_dim d
  *       ON u.k = d.k
  *      AND u.placed BETWEEN d.eff AND d.endts;
  * }}}
  *
  * Determinism harness: `local[1]` + `shuffle.partitions=1` +
  * `adaptive.enabled=false` so any partition-iteration-order race the bug
  * report mentions cannot mask the failure.
  */
class ChainedMinMaxJoinSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cmmj-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ChainedMinMaxJoinSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.adaptive.enabled", "false")
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

  // ── Helpers ────────────────────────────────────────────────────────────────

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

  private def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => spark.sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (1) Update-miss: batch 1 ACTV (p, NULL) then batch 2 CNCL (NULL, r) for
  //     the same key — silver.watches MIN/MAX merges them into (p, r); the
  //     downstream SCD2 BETWEEN join must DELETE the stale (p, NULL) row
  //     from cmmj_downstream AND INSERT (p, r) under the same dim version.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(1) Update-miss: chained MIN/MAX → JOIN BETWEEN SCD2 dim") {

    it("downstream stays consistent when an upstream row mutates only its non-key columns") {
      spark.sql("CREATE TABLE IF NOT EXISTS cmmj_events(k INT, p TIMESTAMP, r TIMESTAMP) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS cmmj_dim(k INT, eff TIMESTAMP, endts TIMESTAMP) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW cmmj_upstream AS " +
          "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW cmmj_downstream AS " +
          "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM cmmj_upstream u " +
          "JOIN cmmj_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 1: insert ACTV (p, NULL) and the dim version that spans
      // the entire range we care about. Refresh both MVs.
      spark.sql(
        "INSERT INTO cmmj_events VALUES " +
          "(1, TIMESTAMP '2016-01-01 00:00:00', NULL)"
      )
      spark.sql(
        "INSERT INTO cmmj_dim VALUES " +
          "(1, TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00')"
      )
      refreshChain("cmmj_upstream", "cmmj_downstream")

      assertMvCorrect(
        "cmmj_upstream",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k"
      )
      assertMvCorrect(
        "cmmj_downstream",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k) u " +
          "JOIN cmmj_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 2: insert CNCL (NULL, r) for the SAME key. The upstream MV
      // MIN/MAX rolls (1, '2016-01-01', NULL) into (1, '2016-01-01', '2017-01-01').
      // The downstream SCD2 BETWEEN match on placed=2016-01-01 is unchanged
      // (still falls inside the [2000,2030] window) but the row's `removed`
      // projection column changes from NULL to '2017-01-01'.
      spark.sql(
        "INSERT INTO cmmj_events VALUES " +
          "(1, NULL, TIMESTAMP '2017-01-01 00:00:00')"
      )
      refreshChain("cmmj_upstream", "cmmj_downstream")

      assertMvCorrect(
        "cmmj_upstream",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k"
      )
      assertMvCorrect(
        "cmmj_downstream",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k) u " +
          "JOIN cmmj_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) Insert-miss: brand-new upstream group in batch 2. No prior row to
  //     mutate — the downstream should pick up the brand-new joined row.
  //     Bug report §3 calls this "pure-insert miss" on customer 6e228a…
  // ──────────────────────────────────────────────────────────────────────────

  describe("(2) Insert-miss: brand-new upstream key in batch 2") {

    it("downstream picks up a brand-new key whose first event is an ACTV") {
      spark.sql("CREATE TABLE IF NOT EXISTS cmmj_events2(k INT, p TIMESTAMP, r TIMESTAMP) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS cmmj_dim2(k INT, eff TIMESTAMP, endts TIMESTAMP) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW cmmj_upstream2 AS " +
          "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW cmmj_downstream2 AS " +
          "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM cmmj_upstream2 u " +
          "JOIN cmmj_dim2 d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 1: ACTV for k=1 + dim versions covering both k=1 and k=2.
      spark.sql(
        "INSERT INTO cmmj_events2 VALUES " +
          "(1, TIMESTAMP '2016-01-01 00:00:00', NULL)"
      )
      spark.sql(
        "INSERT INTO cmmj_dim2 VALUES " +
          "(1, TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00')," +
          "(2, TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00')"
      )
      refreshChain("cmmj_upstream2", "cmmj_downstream2")
      assertMvCorrect(
        "cmmj_downstream2",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k) u " +
          "JOIN cmmj_dim2 d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 2: brand-new key 2 with a single ACTV. Insert miss check:
      // the new group must propagate through both MVs.
      spark.sql(
        "INSERT INTO cmmj_events2 VALUES " +
          "(2, TIMESTAMP '2017-07-08 00:00:00', NULL)"
      )
      refreshChain("cmmj_upstream2", "cmmj_downstream2")
      assertMvCorrect(
        "cmmj_upstream2",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k"
      )
      assertMvCorrect(
        "cmmj_downstream2",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k) u " +
          "JOIN cmmj_dim2 d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )
    }
  }
}
