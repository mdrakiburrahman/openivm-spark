package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Regression spec for the SF=10 `gold.fact_holdings diff=18` failure.
  *
  * Symptom: `SIMPLE_PROJECTION` MV ends up MISSING rows after an incremental
  * refresh whose source-delta retracts only SOME of a set of source rows that
  * project to the SAME value-tuple (i.e. value-equal duplicates in the MV).
  *
  * Root cause: `SparkRefreshRewriter.rewriteSimpleProjectionDataInsert`
  * emits a `MERGE INTO <mv> AS v USING <neg-mult rows> AS d ON v.col IS NOT
  * DISTINCT FROM d.col WHEN MATCHED THEN DELETE`. Delta Lake MERGE
  * `WHEN MATCHED THEN DELETE` deletes ALL matching MV rows per source row,
  * not exactly `|openivm_multiplicity|`. When the MV has N > |mult| value-
  * equal copies of a tuple that the delta partially retracts, this
  * over-deletes by `N - |mult|` rows — exactly the SF=10 fact_holdings
  * `diff=18` shape.
  *
  * Reference DuckDB implementation uses `rowid` + `ROW_NUMBER()` to delete
  * exactly `|_net|` copies per (cols) group
  * (`.temp/openivm/src/upsert/refresh_compiler.cpp:828-852`). The Spark fix
  * (this spec's expected post-fix behaviour) emits a 3-statement rewrite
  * that uses `delta.<mvLocation> VERSION AS OF <pre-refresh-version>` to
  * snapshot the pre-DELETE bag-count and re-INSERT `max(0, _cur + _net)`
  * copies after the over-deleting MERGE.
  *
  * Determinism harness: `local[1]` + `shuffle.partitions=1` +
  * `adaptive.enabled=false`, matching the documented prescription in
  * `.research/OPENIVM-BUG.md` §6.
  */
class SimpleProjectionBagDeleteSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-spbd-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-SimpleProjectionBagDeleteSpec")
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

  // ─────────────────────────────────────────────────────────────────────────
  // (1) Partial retract of value-equal duplicates — the SF=10 fact_holdings
  //     `diff=18` shape. Source has 3 rows projecting to `k=1`; batch 2
  //     deletes ONE of them. MV must end up with 2 copies, not 0 (the
  //     pre-fix MERGE-DELETE over-deletes ALL 3).
  // ─────────────────────────────────────────────────────────────────────────

  describe("(1) SIMPLE_PROJECTION partial retract of value-equal duplicates") {

    it("preserves bag-count on incremental DELETE (net=-1, 3 MV copies → 2 copies)") {
      spark.sql("CREATE TABLE IF NOT EXISTS spbd_src1(k INT, payload STRING) USING DELTA")
      spark.sql("CREATE MATERIALIZED VIEW spbd_mv1 AS SELECT k FROM spbd_src1")

      // Batch 1: three rows projecting to value-tuple `k=1`, different payloads.
      spark.sql("INSERT INTO spbd_src1 VALUES (1, 'a'), (1, 'b'), (1, 'c')")
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv1").collect()
      assertMvCorrect("spbd_mv1", "SELECT k FROM spbd_src1")

      // Batch 2: retract ONE source row. View-delta should carry mult=-1 for `k=1`.
      // Pre-fix bug: MERGE-DELETE drops ALL 3 MV copies of `k=1` → MV ends up
      // with 0 rows. Expected: 2 rows.
      spark.sql("DELETE FROM spbd_src1 WHERE payload = 'a'")
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv1").collect()
      assertMvCorrect("spbd_mv1", "SELECT k FROM spbd_src1")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (2) Multi-key partial retract — multiple distinct value-tuples each
  //     have N > |mult| copies in the MV and the delta retracts a strict
  //     subset of each.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(2) SIMPLE_PROJECTION partial retract across multiple value-tuples") {

    it("preserves bag-count when several distinct keys are partially retracted") {
      spark.sql("CREATE TABLE IF NOT EXISTS spbd_src2(k INT, payload STRING) USING DELTA")
      spark.sql("CREATE MATERIALIZED VIEW spbd_mv2 AS SELECT k FROM spbd_src2")

      // Batch 1: k=1 → 4 copies; k=2 → 3 copies; k=3 → 2 copies; k=4 → 1 copy.
      spark.sql(
        "INSERT INTO spbd_src2 VALUES " +
          "(1,'a'),(1,'b'),(1,'c'),(1,'d')," +
          "(2,'a'),(2,'b'),(2,'c')," +
          "(3,'a'),(3,'b')," +
          "(4,'a')"
      )
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv2").collect()
      assertMvCorrect("spbd_mv2", "SELECT k FROM spbd_src2")

      // Batch 2: retract one copy of k=1 and two copies of k=2.
      // Expected post-state: k=1 → 3, k=2 → 1, k=3 → 2, k=4 → 1.
      spark.sql("DELETE FROM spbd_src2 WHERE (k=1 AND payload='a') OR (k=2 AND payload IN ('a','b'))")
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv2").collect()
      assertMvCorrect("spbd_mv2", "SELECT k FROM spbd_src2")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (3) NULL-column value-equality — matches the TPC-DI fact_holdings shape
  //     where PNDG/SBMT trades produce NULL `current_price`,
  //     `current_fee`, `current_commission`. The pre-fix value-equality
  //     MERGE (`IS NOT DISTINCT FROM`) over-collapses NULL-bearing tuples.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(3) SIMPLE_PROJECTION partial retract with NULL-bearing value-tuples") {

    it("preserves bag-count when retracted value-tuple has NULL columns") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS spbd_src3(k INT, price DOUBLE, fee DOUBLE, payload STRING) USING DELTA"
      )
      spark.sql("CREATE MATERIALIZED VIEW spbd_mv3 AS SELECT k, price, fee FROM spbd_src3")

      // Batch 1: 5 rows projecting to (k=1, price=NULL, fee=NULL) and 1 row to (k=2, …).
      spark.sql(
        "INSERT INTO spbd_src3 VALUES " +
          "(1, NULL, NULL, 'p1')," +
          "(1, NULL, NULL, 'p2')," +
          "(1, NULL, NULL, 'p3')," +
          "(1, NULL, NULL, 'p4')," +
          "(1, NULL, NULL, 'p5')," +
          "(2, 1.0, 0.1, 'p6')"
      )
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv3").collect()
      assertMvCorrect("spbd_mv3", "SELECT k, price, fee FROM spbd_src3")

      // Batch 2: retract 2 of the 5 NULL-priced rows. MV must end up with
      // 3 NULL-priced rows; the pre-fix bug deletes ALL 5 because the
      // value-equality `IS NOT DISTINCT FROM` MERGE matches every NULL copy.
      spark.sql("DELETE FROM spbd_src3 WHERE payload IN ('p1', 'p2')")
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv3").collect()
      assertMvCorrect("spbd_mv3", "SELECT k, price, fee FROM spbd_src3")
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (4) Insert + delete mixed batch — non-conflicting (no value-tuple has
  //     both positive and negative net). Pre-fix logic does
  //     `hasConflictingSimpleProjectionRows = false` so no FULL_REFRESH
  //     fallback, and the over-delete fires on the negative-net side.
  // ─────────────────────────────────────────────────────────────────────────

  describe("(4) SIMPLE_PROJECTION mixed insert + partial-retract batch") {

    it("preserves bag-count on a batch that both inserts and partially retracts") {
      spark.sql("CREATE TABLE IF NOT EXISTS spbd_src4(k INT, payload STRING) USING DELTA")
      spark.sql("CREATE MATERIALIZED VIEW spbd_mv4 AS SELECT k FROM spbd_src4")

      // Batch 1: k=1 → 3 copies, k=2 → 2 copies.
      spark.sql("INSERT INTO spbd_src4 VALUES (1,'a'),(1,'b'),(1,'c'),(2,'a'),(2,'b')")
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv4").collect()
      assertMvCorrect("spbd_mv4", "SELECT k FROM spbd_src4")

      // Batch 2: retract one copy of k=1 (net=-1) AND insert 2 new copies of
      // k=3 (net=+2). Different value-tuples; no conflict. Pre-fix: k=1
      // drops to 0 (over-delete), k=3 gets 2 (correct). Expected k=1 → 2.
      spark.sql("DELETE FROM spbd_src4 WHERE k=1 AND payload='a'")
      spark.sql("INSERT INTO spbd_src4 VALUES (3,'a'),(3,'b')")
      spark.sql("REFRESH MATERIALIZED VIEW spbd_mv4").collect()
      assertMvCorrect("spbd_mv4", "SELECT k FROM spbd_src4")
    }
  }
}
