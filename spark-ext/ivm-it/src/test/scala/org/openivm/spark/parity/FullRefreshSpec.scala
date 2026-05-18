package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog, StagingDelta}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P5.rt3 — Comprehensive coverage of RefreshType 3 (FULL_REFRESH).
  *
  * FULL_REFRESH applies whenever the openivm classifier marks a view as
  * non-incrementally-maintainable.  In those cases `RefreshMaterializedViewCommand`
  * emits a single `INSERT OVERWRITE TABLE mv SELECT * FROM (<view_body>)` rather
  * than the incremental MERGE/DELETE+INSERT program.  The MV is always correct
  * after every refresh by construction.
  *
  * == RESEARCH.md §9 vs. actual openivm classifier (src/core/parser.cpp) ==
  *
  * RESEARCH.md §9 listed five FULL_REFRESH categories.  Cross-checking against
  * the actual openivm source reveals the following corrections:
  *
  *   RESEARCH.md claim              │ Actual classification  │ Explanation
  *   ────────────────────────────── │ ─────────────────────  │ ────────────────────────────────────────
  *   Unkeyed MIN/MAX (no GROUP BY)  │ SimpleAggregate (1)    │ parser.cpp:872 — any `found_aggregation
  *                                  │                        │ && aggregate_columns.empty()` hits
  *                                  │                        │ SIMPLE_AGGREGATE, not FULL_REFRESH.
  *                                  │                        │ RESEARCH.md description was incorrect.
  *   SEMI/ANTI + aggregation        │ FULL_REFRESH (3) ✓     │ parser.cpp:700-704: `found_semi_anti_join
  *                                  │                        │ && found_aggregation` → FULL_REFRESH.
  *   Window function over JOIN      │ WindowPartition (5)    │ parser.cpp:692-694 — any `found_window`
  *                                  │                        │ hits WINDOW_PARTITION regardless of join.
  *                                  │                        │ RESEARCH.md description was incorrect.
  *   Recursive CTE (WITH RECURSIVE) │ FULL_REFRESH (3) ✓     │ incremental_checker.cpp:382-385 —
  *                                  │                        │ LOGICAL_RECURSIVE_CTE default branch
  *                                  │                        │ sets incremental_compatible=false.
  *                                  │                        │ Untestable: Spark 3.5 parser rejects
  *                                  │                        │ WITH RECURSIVE in DDL context.
  *   Volatile / non-deterministic   │ FULL_REFRESH (3) ✓     │ incremental_checker.cpp:134-164 —
  *   UDFs                           │                        │ HasVolatileExpression sets
  *                                  │                        │ incremental_compatible=false.
  *                                  │                        │ Note: rand() not in DuckDB; use random().
  *
  * == Actual FULL_REFRESH classifier paths exercised by this test suite ==
  *
  *   (a) INTERSECT ALL set operation — `parser_plan_helpers.cpp:164-177`
  *       (`HasUnsupportedSetOperation` detects `LOGICAL_INTERSECT` → FULL_REFRESH).
  *       Replaces "Unkeyed MIN/MAX" from RESEARCH.md which is actually SimpleAggregate.
  *       Note: openivm normalizes `FILTER` aggregates (parser.cpp:354-357) before
  *       classification so non-list FILTER aggregates never reach incremental_checker.
  *
  *   (b) SEMI join + aggregation (via WHERE EXISTS) — `parser.cpp:700-704`
  *       (`found_semi_anti_join && found_aggregation` → FULL_REFRESH).
  *       Requires the `collectSourceSchemas` fix: the original `.collect` only
  *       traversed plan children, missing tables inside SubqueryExpression nodes.
  *       `collectAllPairs` now also recurses into SubqueryExpression plans.
  *
  *   (c) EXCEPT ALL set operation — `parser_plan_helpers.cpp:164-177`
  *       (`HasUnsupportedSetOperation` detects `LOGICAL_EXCEPT` → FULL_REFRESH).
  *       Replaces "Window over JOIN" which is actually WindowPartition (5).
  *
  *   (d) Volatile function `random()` — `incremental_checker.cpp:63-83`
  *       (`HasVolatileExpression` on PROJECTION → `incremental_compatible=false`).
  *       Note: Spark SQL `rand()` is called `random()` in DuckDB's function catalog.
  *       Both Spark SQL 3.5 and DuckDB accept `random()` as a built-in function.
  *
  * Tests 1-5 cover each category; tests 6-8 cover mixed-inventory,
  * performance, and resilience to corrupt staging entries respectively.
  */
class FullRefreshSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-fr-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FullRefreshSpec")
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

  /** Bidirectional EXCEPT ALL equivalence check.
    * Projects `mv` to the same column set as `expected` so that hidden
    * bookkeeping columns (e.g. `openivm_count_star`) don't cause a mismatch.
    */
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

  /** Looks up the recorded refresh type for `name` via the MV catalog. */
  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── Test 1: INTERSECT ALL set operation → FULL_REFRESH ───────────────────
  // RESEARCH.md §9 claimed "Unkeyed MIN/MAX (no GROUP BY)" triggers FULL_REFRESH.
  // The actual openivm parser routes ANY unkeyed aggregate (including MIN/MAX) to
  // SIMPLE_AGGREGATE (type 1) via parser.cpp:872.
  //
  // Note: openivm normalizes `SUM(x) FILTER (WHERE cond)` to
  // `SUM(CASE WHEN cond THEN x END)` before plan analysis (parser.cpp:354-357,
  // RewriteAggregateFilters), so the FILTER branch in incremental_checker.cpp:241
  // is never reached for non-list aggregates; the view becomes a SIMPLE_AGGREGATE.
  //
  // The actual FULL_REFRESH trigger tested here is an INTERSECT ALL set operation.
  // `HasUnsupportedSetOperation` (parser_plan_helpers.cpp:164-177) detects
  // `LOGICAL_INTERSECT` nodes and sets `has_unsupported_set_operation = true` →
  // parser.cpp:690-691 → FULL_REFRESH.  This is the INTERSECT variant of the
  // EXCEPT ALL path used in test 3.
  describe("(1) INTERSECT ALL set operation → FULL_REFRESH (classifier: unsupported set op)") {

    it("classifier assigns FULL_REFRESH; INSERT+DELETE then REFRESH rewrites the MV correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr1(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_fr1 VALUES ('east', 100), ('west', 50), ('north', 200)")
      // Self-INTERSECT: rows in the high-value set that also appear in the full table
      // (i.e., rows with amount > 80) — deterministic result that assertMvCorrect can verify.
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr1 AS " +
          "SELECT region, amount FROM sales_fr1 WHERE amount > 80 " +
          "INTERSECT ALL " +
          "SELECT region, amount FROM sales_fr1"
      )

      mvRefreshType("mv_fr1") shouldBe RefreshTypeCode.FullRefresh

      spark.sql("INSERT INTO sales_fr1 VALUES ('south', 500)")
      spark.sql("DELETE FROM sales_fr1 WHERE region = 'west'")
      refreshMv("mv_fr1")

      assertMvCorrect(
        "mv_fr1",
        "SELECT region, amount FROM sales_fr1 WHERE amount > 80 " +
          "INTERSECT ALL " +
          "SELECT region, amount FROM sales_fr1"
      )
    }
  }

  // ── Test 2: SEMI join + aggregation → FULL_REFRESH ────────────────────────
  // openivm classifier path: parser.cpp:700-704 — `found_semi_anti_join &&
  // found_aggregation` → FULL_REFRESH.
  //
  // `WHERE EXISTS (SELECT ...)` is the natural SQL spelling of a SEMI JOIN.
  // Spark's analyzed plan represents this as Filter(Exists(innerPlan), child),
  // where the inner plan is inside a SubqueryExpression — not a first-class
  // LogicalPlan child.  The original `collectSourceSchemas` used `.collect` which
  // traverses only plan children, missing the inner table.
  //
  // The fix adds `collectAllPairs` which also descends into SubqueryExpression
  // instances found in each plan node's expressions, ensuring that both
  // `sales_fr2` and `promotions_fr2` are registered with DuckDB before the
  // openivm compiler runs.  DuckDB routes `WHERE EXISTS + GROUP BY` to
  // FULL_REFRESH via parser.cpp:700-704.
  describe("(2) SEMI join (WHERE EXISTS) + GROUP BY aggregation → FULL_REFRESH (classifier: SEMI+aggregate)") {

    it("classifier assigns FULL_REFRESH; new promotion entry is reflected after REFRESH") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr2(region STRING, amount INT) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS promotions_fr2(region STRING) USING DELTA")
      spark.sql(
        "INSERT INTO sales_fr2 VALUES ('east', 100), ('west', 200), ('north', 50), ('south', 80)"
      )
      spark.sql("INSERT INTO promotions_fr2 VALUES ('east'), ('west')")
      // WHERE EXISTS is standard SQL understood by both Spark and DuckDB.
      // The collectSourceSchemas fix ensures promotions_fr2 (inside the EXISTS
      // subquery expression) is registered in DuckDB before compilation.
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr2 AS " +
          "SELECT s.region, COUNT(*) AS cnt " +
          "FROM sales_fr2 s " +
          "WHERE EXISTS (SELECT 1 FROM promotions_fr2 p WHERE s.region = p.region) " +
          "GROUP BY s.region"
      )

      mvRefreshType("mv_fr2") shouldBe RefreshTypeCode.FullRefresh

      spark.sql("INSERT INTO sales_fr2 VALUES ('east', 300), ('north', 120)")
      spark.sql("INSERT INTO promotions_fr2 VALUES ('north')")
      refreshMv("mv_fr2")

      assertMvCorrect(
        "mv_fr2",
        "SELECT s.region, COUNT(*) AS cnt FROM sales_fr2 s " +
          "WHERE EXISTS (SELECT 1 FROM promotions_fr2 p WHERE s.region = p.region) " +
          "GROUP BY s.region"
      )
    }
  }

  // ── Test 3: EXCEPT ALL set operation → FULL_REFRESH ───────────────────────
  // RESEARCH.md §9 claimed "Window function over JOIN" triggers FULL_REFRESH.
  // The actual openivm classifier routes any query with a window function to
  // WINDOW_PARTITION (type 5) via parser.cpp:692-694, before the FULL_REFRESH
  // evaluation path is reached.
  //
  // The FULL_REFRESH trigger tested here is an EXCEPT ALL set operation.
  // `HasUnsupportedSetOperation` (parser_plan_helpers.cpp:164-177) detects
  // `LOGICAL_INTERSECT` or `LOGICAL_EXCEPT` nodes and sets
  // `has_unsupported_set_operation = true` → parser.cpp:690-691 → FULL_REFRESH.
  describe("(3) EXCEPT ALL set operation → FULL_REFRESH (classifier: unsupported set op)") {

    it("classifier assigns FULL_REFRESH; INSERT then REFRESH rewrites the MV correctly") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr3(region STRING, amount INT) USING DELTA")
      // A self-EXCEPT using two different predicates.  Both branches scan the
      // same source table; collectSourceSchemas de-duplicates to one entry.
      spark.sql("INSERT INTO sales_fr3 VALUES ('east', 150), ('west', 60), ('north', 210)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr3 AS " +
          "SELECT region, amount FROM sales_fr3 WHERE amount > 100 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr3 WHERE amount > 300"
      )

      mvRefreshType("mv_fr3") shouldBe RefreshTypeCode.FullRefresh

      spark.sql("INSERT INTO sales_fr3 VALUES ('south', 500), ('east', 80)")
      refreshMv("mv_fr3")

      assertMvCorrect(
        "mv_fr3",
        "SELECT region, amount FROM sales_fr3 WHERE amount > 100 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr3 WHERE amount > 300"
      )
    }
  }

  // ── Test 4: Recursive CTE — intentionally omitted ─────────────────────────
  // Spark SQL 3.5 does not expose the `WITH RECURSIVE` keyword in DDL context
  // (the SQL parser raises AnalysisException).  The openivm classifier path
  // (src/core/incremental_checker.cpp:382-385, LOGICAL_RECURSIVE_CTE branch →
  // incremental_compatible=false → FULL_REFRESH) is therefore unreachable from
  // openivm-spark until Spark exposes the syntax.  No test is registered.

  // ── Test 5: Volatile function random() → FULL_REFRESH ─────────────────────
  // `HasVolatileExpression` in incremental_checker.cpp:63-83 iterates the
  // PROJECTION node's expressions.  `random()` has `FunctionStability::VOLATILE`
  // in DuckDB's function catalog → `incremental_compatible = false` →
  // parser.cpp:747-748 → FULL_REFRESH.
  //
  // Note: Spark SQL `rand()` (Spark's spelling) has no counterpart in DuckDB
  // (`Scalar Function with name rand does not exist!`).  Both Spark SQL 3.5 and
  // DuckDB accept `random()` — Spark registers it as an alias for rand(Seed)
  // and DuckDB exposes it as its native random-float function.
  //
  // Bidirectional EXCEPT ALL is skipped: `random()` evaluates to different
  // values on each invocation, so a re-run of the view body produces different
  // rows from those frozen in the MV.  Correctness is verified by row count and
  // by asserting r stays in [0.0, 1.0].
  describe("(5) Volatile function random() → FULL_REFRESH (classifier: HasVolatileExpression)") {

    it("classifier assigns FULL_REFRESH; MV row count matches base table after REFRESH") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr5(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_fr5 VALUES ('east', 100), ('west', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr5 AS " +
          "SELECT region, random() AS r FROM sales_fr5"
      )

      mvRefreshType("mv_fr5") shouldBe RefreshTypeCode.FullRefresh

      spark.sql("INSERT INTO sales_fr5 VALUES ('north', 300)")
      refreshMv("mv_fr5")

      val baseCount = spark.table("sales_fr5").count()
      spark.table("mv_fr5").count() shouldBe baseCount

      // Verify the r column is a valid double in [0.0, 1.0].
      spark.table("mv_fr5").where("r < 0.0 OR r > 1.0").count() shouldBe 0L
    }
  }

  // ── Test 6: Mixed MV inventory ────────────────────────────────────────────
  // Two AGGREGATE_GROUP MVs (RefreshType 0) and one FULL_REFRESH MV (RefreshType 3)
  // coexist in the same SparkSession.  Each receives the correct refresh path:
  // the incremental MVs are maintained via MERGE; the FULL_REFRESH MV is
  // rewritten from scratch via INSERT OVERWRITE.
  describe("(6) Mixed inventory: two AGGREGATE_GROUP + one FULL_REFRESH MV in the same session") {

    it("each MV gets its own refresh path; all three are correct after REFRESH") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr6(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_fr6 VALUES ('east', 100), ('west', 200), ('north', 50)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr6_agg1 AS " +
          "SELECT region, SUM(amount) AS total FROM sales_fr6 GROUP BY region"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr6_agg2 AS " +
          "SELECT region, COUNT(*) AS cnt FROM sales_fr6 GROUP BY region"
      )
      // FULL_REFRESH via EXCEPT ALL set operation (same path as test 3).
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr6_full AS " +
          "SELECT region, amount FROM sales_fr6 WHERE amount > 100 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr6 WHERE amount > 400"
      )

      mvRefreshType("mv_fr6_agg1") shouldBe RefreshTypeCode.AggregateGroup
      mvRefreshType("mv_fr6_agg2") shouldBe RefreshTypeCode.AggregateGroup
      mvRefreshType("mv_fr6_full") shouldBe RefreshTypeCode.FullRefresh

      spark.sql("INSERT INTO sales_fr6 VALUES ('south', 400), ('east', 75)")
      spark.sql("DELETE FROM sales_fr6 WHERE region = 'north'")

      refreshMv("mv_fr6_agg1")
      refreshMv("mv_fr6_agg2")
      refreshMv("mv_fr6_full")

      assertMvCorrect(
        "mv_fr6_agg1",
        "SELECT region, SUM(amount) AS total FROM sales_fr6 GROUP BY region"
      )
      assertMvCorrect(
        "mv_fr6_agg2",
        "SELECT region, COUNT(*) AS cnt FROM sales_fr6 GROUP BY region"
      )
      assertMvCorrect(
        "mv_fr6_full",
        "SELECT region, amount FROM sales_fr6 WHERE amount > 100 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr6 WHERE amount > 400"
      )
    }
  }

  // ── Test 7: FULL_REFRESH performance baseline ──────────────────────────────
  // Inserts 10,000 rows into the base table and asserts that REFRESH completes
  // in under 60 seconds.  The bound is intentionally loose — the goal is to
  // confirm the path does not loop, hang, or perform O(n²) staging processing.
  describe("(7) FULL_REFRESH performance baseline: 10,000-row base table") {

    it("REFRESH completes in under 60 seconds and the MV is correct") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr7(region STRING, amount INT) USING DELTA")
      val rows = (1 to 10000).map(i => s"('region_${i % 20}', $i)").mkString(", ")
      spark.sql(s"INSERT INTO sales_fr7 VALUES $rows")
      // FULL_REFRESH via EXCEPT ALL set operation (same path as test 3).
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr7 AS " +
          "SELECT region, amount FROM sales_fr7 WHERE amount > 5000 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr7 WHERE amount > 9000"
      )

      mvRefreshType("mv_fr7") shouldBe RefreshTypeCode.FullRefresh

      // Add a small batch so there is at least one pending staging delta.
      spark.sql("INSERT INTO sales_fr7 VALUES ('region_0', 99999)")

      val t0 = System.currentTimeMillis()
      refreshMv("mv_fr7")
      val elapsed = (System.currentTimeMillis() - t0) / 1000.0

      elapsed should be < 60.0

      assertMvCorrect(
        "mv_fr7",
        "SELECT region, amount FROM sales_fr7 WHERE amount > 5000 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr7 WHERE amount > 9000"
      )
    }
  }

  // ── Test 8: FULL_REFRESH after simulated partial staging failure ───────────
  // FULL_REFRESH assembles INSERT OVERWRITE from the live view body — it never
  // reads the contents of the staged Delta paths.  A corrupt staging entry
  // (pointing to a non-existent path) therefore does NOT break correctness.
  //
  // This test injects such an entry via StagingCatalog.record() directly and
  // verifies that REFRESH still produces the correct result.  An incremental
  // refresh type would fail here because it tries to read the staged rows;
  // FULL_REFRESH is immune because the assembler bypasses staging data entirely.
  describe("(8) FULL_REFRESH after simulated partial staging failure") {

    it("corrupt staging entry does not affect correctness — FULL_REFRESH rewrites from base") {
      spark.sql("CREATE TABLE IF NOT EXISTS sales_fr8(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO sales_fr8 VALUES ('east', 100), ('west', 200)")
      // FULL_REFRESH via EXCEPT ALL set operation (same path as test 3).
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fr8 AS " +
          "SELECT region, amount FROM sales_fr8 WHERE amount > 50 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr8 WHERE amount > 1000"
      )

      mvRefreshType("mv_fr8") shouldBe RefreshTypeCode.FullRefresh

      spark.sql("INSERT INTO sales_fr8 VALUES ('north', 50)")

      // Inject a staging entry pointing to a non-existent Delta path.
      // For the incremental path this would produce a AnalysisException when
      // the assembler tries to read delta.<bad_path>; for FULL_REFRESH the
      // staging entries are only used as a "pending work exists" signal —
      // their paths are never dereferenced during the INSERT OVERWRITE.
      val fakeDelta = StagingDelta(
        baseTable = "sales_fr8",
        opType = "INSERT",
        stagingPath = s"$warehouseDir/_ivm/_staging/FAKE_${UUID.randomUUID()}",
        txnTs = new java.sql.Timestamp(System.currentTimeMillis()),
        consumedBy = Seq.empty
      )
      StagingCatalog.record(spark, fakeDelta)

      refreshMv("mv_fr8")

      assertMvCorrect(
        "mv_fr8",
        "SELECT region, amount FROM sales_fr8 WHERE amount > 50 " +
          "EXCEPT ALL " +
          "SELECT region, amount FROM sales_fr8 WHERE amount > 1000"
      )
    }
  }
}
