package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Split from the original parity spec.  Scope:
  * Pending depth>2 markers plus the depth-2 cascade / off-mode scenarios from `pipeline.test`.  Sections (H) and (K) share tables and are co-resident.
  *
  * Includes sections: (A–F), (H), (K), (J).
  */
class PipelineDepth2Spec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-pipe-d2-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-PipelineDepth2Spec")
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

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  /** Bidirectional `EXCEPT ALL` equivalence between the MV and the recomputed
    * view body, projecting the MV onto the expected column list to drop any
    * `openivm_*` bookkeeping columns. */
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

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  private def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => spark.sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (A–F) Out of scope per PLAN.md §12 — three-level chain pipeline setup
  //
  // pipeline.test sections A through F operate on a three-level chain:
  //     events → user_totals → {global_total, user_count}
  // and verify various refresh-ordering invariants (correct order, out-of-
  // order, skip-a-level, leaf-only refresh, idempotent refresh, mixed
  // INSERT/DELETE → cascade refresh).  Per PLAN §11 Risk #5 / §12 those
  // depth-3 chains are intentionally out of scope for the openivm-spark MVP.
  // The corresponding test bodies remain `pending` so they show up in the
  // inventory and can be promoted later.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(A–F) Out of scope: depth-3 chain events → user_totals → {global_total, user_count}") {

    it(
      "(A) baseline correct ordered refresh on a 3-level chain " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }

    it(
      "(B) out-of-order refresh: leaf before intermediate " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }

    it(
      "(C) skip-a-level: refresh root and leaf, skip middle " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }

    it(
      "(D) leaf-only refresh without any upstream refresh " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }

    it(
      "(E) idempotent double refresh on a 3-level chain " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }

    it(
      "(F) mixed INSERT + DELETE → cascade refresh on a 3-level chain " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (H) Downstream cascade equivalent — manual ordering
  //     pipeline.test L367–L428: cascade_base → cascade_l1 → cascade_l2.
  //     openivm uses `SET openivm_cascade_refresh = 'downstream'` so a single
  //     refresh of the root pulls in the leaf.  openivm-spark has no cascade
  //     analogue, so we issue both refreshes explicitly and verify the
  //     final data is correct (the manual-order shape from pipeline.test
  //     section K).
  // ──────────────────────────────────────────────────────────────────────────

  describe("(H) Manual downstream order: cascade_base → cascade_l1 → cascade_l2") {

    it("first batch: explicit ordered refresh keeps both MVs consistent") {
      spark.sql("CREATE TABLE IF NOT EXISTS pld_cascade_base(grp STRING, val INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW pld_cascade_l1 AS " +
          "SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW pld_cascade_l2 AS " +
          "SELECT SUM(total) AS grand FROM pld_cascade_l1"
      )
      spark.sql("INSERT INTO pld_cascade_base VALUES ('a',10),('b',20)")
      refreshChain("pld_cascade_l1", "pld_cascade_l2")
      assertMvCorrect(
        "pld_cascade_l1",
        "SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp"
      )
      assertMvCorrect(
        "pld_cascade_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp) t"
      )
    }

    it("second batch: additional groups propagate through both MVs") {
      spark.sql("INSERT INTO pld_cascade_base VALUES ('a',5),('c',100)")
      refreshChain("pld_cascade_l1", "pld_cascade_l2")
      assertMvCorrect(
        "pld_cascade_l1",
        "SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp"
      )
      assertMvCorrect(
        "pld_cascade_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (K) "Off" mode equivalent — explicit control over which MV refreshes
  //     pipeline.test L512–L538.  openivm uses `SET cascade=off` to force
  //     manual control; we already operate that way by default.  After
  //     refreshing the root, the leaf is intentionally NOT refreshed yet
  //     so we can verify the staleness assertion; then a second refresh
  //     of the leaf brings it up to date.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(K) Off-mode equivalent: per-MV refresh control") {

    it("root-only refresh: leaf stays stale until manually refreshed") {
      // Reuse the cascade base table set up in section (H) so we can assert
      // the staleness invariant exactly as pipeline.test does at L518–L538.
      spark.sql("INSERT INTO pld_cascade_base VALUES ('d',50)")
      refreshChain("pld_cascade_l1")

      // l1 is up to date.
      assertMvCorrect(
        "pld_cascade_l1",
        "SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp"
      )

      // l2 is stale: it still reflects the pre-insert state of l1.  pipeline
      // expects `135` (the value before the new row).  We verify the same
      // shape with a count check: the staleness means l2's grand is strictly
      // less than the live grand-total.
      val staleGrand: Long = spark.table("pld_cascade_l2").as("t").collect().head.getLong(0)
      val liveGrand: Long = spark
        .sql("SELECT SUM(val) AS g FROM pld_cascade_base")
        .collect()
        .head
        .getLong(0)
      withClue("pld_cascade_l2 should be stale before its explicit REFRESH: ") {
        staleGrand should be < liveGrand
      }

      // Now bring it up to date and verify.
      refreshChain("pld_cascade_l2")
      assertMvCorrect(
        "pld_cascade_l2",
        "SELECT SUM(total) AS grand FROM (SELECT grp, SUM(val) AS total FROM pld_cascade_base GROUP BY grp) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (J) Both-cascade equivalent — manual full-chain refresh
  //     pipeline.test L479–L510: both_base → both_l1 → both_l2.
  //     openivm uses `SET openivm_cascade_refresh = 'both'`; here we issue
  //     every refresh explicitly.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(J) Manual full-chain order: both_base → both_l1 → both_l2") {

    it("refresh in order: both MVs converge with the source") {
      spark.sql("CREATE TABLE IF NOT EXISTS pld_both_base(k STRING, v INT) USING DELTA")
      spark.sql(
        "CREATE MATERIALIZED VIEW pld_both_l1 AS " +
          "SELECT k, SUM(v) AS total FROM pld_both_base GROUP BY k"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW pld_both_l2 AS " +
          "SELECT SUM(total) AS grand FROM pld_both_l1"
      )
      spark.sql("INSERT INTO pld_both_base VALUES ('x',100)")
      refreshChain("pld_both_l1", "pld_both_l2")
      assertMvCorrect("pld_both_l1", "SELECT k, SUM(v) AS total FROM pld_both_base GROUP BY k")
      assertMvCorrect(
        "pld_both_l2",
        "SELECT SUM(total) AS grand FROM (SELECT k, SUM(v) AS total FROM pld_both_base GROUP BY k) t"
      )
    }
  }
}
