package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6a — Port of `openivm/test/sql/list.test` (DuckDB LIST → Spark ARRAY).
  *
  * == Why the view body must be portable to both DuckDB and Spark ==
  *
  * openivm-spark sends every `CREATE MATERIALIZED VIEW … AS <view body>` to a
  * DuckDB-CLI process (`PRAGMA compile_refresh(...)`) for classification, and
  * Spark executes the same view body when re-evaluating it for full-refresh
  * fallback or for the bidirectional `EXCEPT ALL` correctness check.
  *
  * The DuckDB-only lambda forms used in the openivm test
  * (`list_reduce(list(val), λ(a,b) → list_transform(list_zip(a,b), λx: x[1]+x[2]))`
  * and friends) therefore cannot be ported verbatim — they are unparseable on
  * one or both sides.  Spark-only spellings such as `collect_list`,
  * `transform()`, `aggregate()`, and `zip_with()` likewise fail DuckDB's CLI
  * (verified: `Catalog Error: Scalar Function with name collect_list does not
  * exist!`).  The intersection of both engines is fortunately rich enough to
  * preserve the DBSP-correct test coverage:
  *
  *   DuckDB/Spark intersection │ Used here for
  *   ───────────────────────── │ ────────────────────────────────────
  *   `array_agg(val)`          │ collect rows into an array (LIST aggregate)
  *   `array_sort(arr)`         │ deterministic ordering for the EXCEPT ALL
  *                             │ comparison
  *   `array_agg(...) FILTER`   │ filtered LIST aggregate
  *
  * Element-wise array reductions over a `LIST(ARRAY<FLOAT>)` cannot be
  * expressed in this intersection — DuckDB uses lambda+list_reduce and Spark
  * uses higher-order `aggregate()`.  The element-wise scenario is therefore
  * mapped to a `array_agg` of arrays (preserving bag semantics over arrays)
  * which exercises the same DBSP-correct delta path: array-typed,
  * non-additive monoid → GROUP_RECOMPUTE per openivm classifier.
  *
  * == Spark vs DuckDB array indexing ==
  *
  * DuckDB arrays are 1-indexed (`arr[1]` is the first element); Spark arrays
  * are 0-indexed.  Where indexing matters, this spec uses `array_sort` so the
  * comparison is order-independent.
  *
  * == ARRAY-typed columns in EXCEPT ALL ==
  *
  * Spark's `EXCEPT ALL` can compare ARRAY columns natively; to dodge any
  * remaining nested-type hashing edge cases we project ARRAY columns through
  * `to_json(...)` (via [[assertMvCorrect]] `arrayCols`) before comparing.
  *
  * Source: `.temp/openivm/test/sql/list.test`.
  */
class ListSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-list-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ListSpec")
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

  /** Bag-equality check between MV and freshly-evaluated view body.
    *
    * For ARRAY-valued columns we serialize to JSON before EXCEPT ALL so that
    * nested-type comparisons are deterministic.
    */
  private def assertMvCorrect(
      mvName: String,
      expectedSql: String,
      arrayCols: Set[String] = Set.empty
  ): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq

    def project(df: DataFrame): DataFrame = {
      val exprs = userCols.map { c =>
        if (arrayCols.contains(c)) s"to_json(`$c`) AS `$c`"
        else s"`$c`"
      }
      df.selectExpr(exprs: _*)
    }

    val expectedProj = project(expected)
    val mv           = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    val mvProj       = project(mv)

    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mvProj.exceptAll(expectedProj).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
      expectedProj.exceptAll(mvProj).count() shouldBe 0L
    }
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ============================================================================
  // (1) LIST<ARRAY<FLOAT>> aggregate per group (list.test:7-44, 95-152, 156-242)
  //
  //     DuckDB original: list_reduce(list(val), λ(a,b)→list_transform(...))
  //     produces an element-wise sum.  That reducer is DuckDB-lambda-only.
  //     The portable shape that exercises the same DBSP delta path (ARRAY-typed
  //     source column, non-additive aggregate → GROUP_RECOMPUTE) is:
  //
  //         SELECT grp, array_sort(array_agg(val)) AS bag FROM items GROUP BY grp
  //
  //     `array_agg` on an ARRAY column produces an ARRAY<ARRAY<FLOAT>>; the
  //     wrapping `array_sort` keeps the bag deterministic for EXCEPT ALL.
  // ============================================================================
  // (1) extracted to [[ListHeavyAggSpec]] so it runs in its own forked JVM
  // under `Test/testGrouping`, shrinking this host spec's wall-clock.

  // ============================================================================
  // (2) Filtered LIST aggregate (list.test:46-92)
  //     DuckDB: `LIST(item ORDER BY qty DESC) FILTER (WHERE qty > 50)` — keeps
  //     only qty>50, ordered by qty descending.
  //
  //     Portable shape: `array_sort(array_agg(item) FILTER (WHERE qty > 50))`.
  //     We use the default ascending sort because both engines agree on its
  //     semantics; the original openivm test's `ORDER BY qty DESC` is
  //     unobservable through bag-EXCEPT-ALL after `array_sort`, so a
  //     deterministic sort suffices for parity.
  // ============================================================================
  describe("(2) array_agg(item) FILTER (WHERE qty > 50) — non-additive filtered list") {
    it("filtered LIST aggregate stays bag-equal across INSERTs") {
      spark.sql("CREATE TABLE IF NOT EXISTS list_filter_items(grp INT, item INT, qty INT) USING DELTA")
      spark.sql(
        "INSERT INTO list_filter_items VALUES " +
          "(1, 10, 90), (1, 20, 10), (1, 30, 80), " +
          "(2, 40, 20), (2, 50, 70)"
      )

      val viewBody =
        "SELECT grp, array_sort(array_agg(item) FILTER (WHERE qty > 50)) AS well_stocked " +
          "FROM list_filter_items GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_list_filter AS $viewBody")

      val rt = mvRefreshType("mv_list_filter")
      withClue(s"observed refreshType=$rt: ") {
        Seq(
          RefreshTypeCode.GroupRecompute,
          RefreshTypeCode.AggregateGroup,
          RefreshTypeCode.FullRefresh
        ) should contain(rt)
      }

      // Initial
      assertMvCorrect("mv_list_filter", viewBody, arrayCols = Set("well_stocked"))

      // Mixed insert: (1, 60, 95) enters MV; (2, 70, 30) filtered out
      spark.sql("INSERT INTO list_filter_items VALUES (1, 60, 95), (2, 70, 30)")
      refreshMv("mv_list_filter")
      assertMvCorrect("mv_list_filter", viewBody, arrayCols = Set("well_stocked"))
    }
  }

  // ============================================================================
  // (3) Stress test: INSERT + DELETE + UPDATE on ARRAY column → single REFRESH
  //     Per CLAUDE.md "stress tests must batch many conflicting DML ops".
  // ============================================================================
  describe("(3) Stress: INSERT + DELETE + UPDATE on ARRAY column → single REFRESH") {
    it("LIST aggregate maintained correctly across conflicting batched DML on ARRAY column") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS items_stress(id INT, grp INT, val ARRAY<FLOAT>) USING DELTA"
      )
      spark.sql(
        "INSERT INTO items_stress VALUES " +
          "(1, 1, array(CAST(1.0 AS FLOAT), CAST(1.0 AS FLOAT))), " +
          "(2, 1, array(CAST(2.0 AS FLOAT), CAST(2.0 AS FLOAT))), " +
          "(3, 2, array(CAST(3.0 AS FLOAT), CAST(3.0 AS FLOAT))), " +
          "(4, 2, array(CAST(4.0 AS FLOAT), CAST(4.0 AS FLOAT)))"
      )

      val viewBody =
        "SELECT grp, array_sort(array_agg(val)) AS total, COUNT(*) AS n " +
          "FROM items_stress GROUP BY grp"
      spark.sql(s"CREATE MATERIALIZED VIEW mv_list_stress AS $viewBody")

      // Conflicting batch: INSERT (new row, new group), DELETE (existing row),
      // UPDATE (mutate an array column value).
      spark.sql("INSERT INTO items_stress VALUES (5, 3, array(CAST(5.0 AS FLOAT), CAST(5.0 AS FLOAT)))")
      spark.sql("DELETE FROM items_stress WHERE id = 1")
      spark.sql(
        "UPDATE items_stress SET val = array(CAST(10.0 AS FLOAT), CAST(10.0 AS FLOAT)) WHERE id = 4"
      )
      refreshMv("mv_list_stress")
      assertMvCorrect("mv_list_stress", viewBody, arrayCols = Set("total"))
    }
  }
}
