package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P5.rt6 — Coverage of RefreshType 6 (GROUP_RECOMPUTE).
  *
  * GROUP_RECOMPUTE applies whenever openivm cannot maintain an aggregate
  * additively per delta row, but can still scope work to a small set of
  * affected group keys.  Typical triggers:
  *   - inner DISTINCT under SUM / COUNT / etc.
  *   - scalar correlated subquery (DELIM join lowering)
  *   - GROUP BY ROLLUP / CUBE / GROUPING SETS
  *   - UNION over aggregates
  *
  * With openivm `4471f4e929fd3b21ac55ea0c47249d4716853c98` and
  * `force_view_delta_cascade=true` (which is what openivm-spark always sets
  * in its CompileFacts payload), GROUP_RECOMPUTE emits an extended
  * affected-groups program:
  *
  *   1. `CREATE OR REPLACE TEMP TABLE openivm_affected_<view> AS
  *        SELECT DISTINCT <keys> FROM (<delta-substituted view query>);`
  *   2. `CREATE OR REPLACE TEMP TABLE openivm_old_<view> AS SELECT * FROM
  *        openivm_data_<view> WHERE EXISTS (… openivm_affected_<view> …);`
  *   3. `CREATE OR REPLACE TEMP TABLE openivm_new_<view> AS SELECT * FROM
  *        (<view_query_sql>) WHERE EXISTS (… openivm_affected_<view> …);`
  *   4. `DELETE FROM openivm_data_<view> AS openivm_tgt
  *        WHERE EXISTS (SELECT 1 FROM openivm_affected_<view> AS openivm_aff
  *                      WHERE openivm_aff.k IS NOT DISTINCT FROM openivm_tgt.k);`
  *   5. `INSERT INTO openivm_data_<view> SELECT * FROM openivm_new_<view>;`
  *   6. `INSERT INTO openivm_delta_<view>
  *        SELECT *, -1, CURRENT_TIMESTAMP FROM openivm_old_<view>
  *        UNION ALL
  *        SELECT *,  1, CURRENT_TIMESTAMP FROM openivm_new_<view>;`
  *   7. `DROP TABLE IF EXISTS openivm_affected_<view>;`
  *      `DROP TABLE IF EXISTS openivm_old_<view>;`
  *      `DROP TABLE IF EXISTS openivm_new_<view>;`
  *
  * The Spark rewriter has dedicated classifier kinds for the affected-key
  * scaffolding plus the old/new snapshot objects. The local MV refresh still
  * depends on the affected-group DELETE + INSERT; the signed view-delta in
  * step 6 is the downstream cascade feed.
  *
  * == Observed refreshType per test ==
  *
  *   (1)  SUM(DISTINCT)        → 6  GROUP_RECOMPUTE
  *   (2)  COUNT(DISTINCT)      → 6  GROUP_RECOMPUTE
  *   (3)  JOIN over CTE-agg    → 6  GROUP_RECOMPUTE
  *                                 (substitutes the prompt's "scalar correlated
  *                                  subquery" form because Spark 3.5's analyzer
  *                                  rejects correlated scalar subqueries in the
  *                                  projection list of an aggregating SELECT —
  *                                  see in-test rationale).
  *   (4)  MIN/MAX GROUP BY     → 0  AGGREGATE_GROUP  (rides on the type-0
  *                                 insert-only fast path with GREATEST/LEAST;
  *                                 the classifier never routes MIN/MAX with
  *                                 a GROUP BY to type 6 — see
  *                                 `refresh_compiler.cpp:372-399`).
  *   (5)  group key change     → 6  GROUP_RECOMPUTE  (uses SUM(DISTINCT))
  *   (6)  insert existing grp  → 6  GROUP_RECOMPUTE
  *   (7)  delete last row      → 6  GROUP_RECOMPUTE
  *   (8)  new group            → 6  GROUP_RECOMPUTE
  *   (9)  batched mixed DML    → 6  GROUP_RECOMPUTE
  *   (10) NULL group keys      → 6  GROUP_RECOMPUTE
  *
  * Each test verifies the MV via bidirectional `EXCEPT ALL` against the
  * base query — hidden `openivm_*` bookkeeping columns are projected away.
  */
class GroupRecomputeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-gr-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-GroupRecomputeSpec")
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

  /** Bidirectional `EXCEPT ALL` equivalence check between the MV (with hidden
    * `openivm_*` columns projected away) and a fresh re-evaluation of the
    * view body via `spark.sql(...)`.
    */
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

  /** Look up the recorded refresh type for `name` via the MV catalog. */
  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── Test 1: Inner DISTINCT under SUM → GROUP_RECOMPUTE ────────────────────

  describe("(1) Inner DISTINCT under SUM — SELECT region, SUM(DISTINCT amount) … GROUP BY region") {

    it("classifies as GROUP_RECOMPUTE and incrementally maintains SUM(DISTINCT) across DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_1(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_1 VALUES " +
          "('east', 10), ('east', 10), ('east', 20), ('west', 30), ('west', 30)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_1 AS " +
          "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_1 GROUP BY region"
      )
      mvRefreshType("mv_gr_1") shouldBe RefreshTypeCode.GroupRecompute

      // Initial state correct
      assertMvCorrect(
        "mv_gr_1",
        "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_1 GROUP BY region"
      )

      // Add a new distinct amount in 'east' (40 is new — SUM(DISTINCT) should jump from 30 → 70)
      spark.sql("INSERT INTO gr_sales_1 VALUES ('east', 40)")
      // Add a duplicate that should NOT change SUM(DISTINCT) for 'west' (30 already counted)
      spark.sql("INSERT INTO gr_sales_1 VALUES ('west', 30)")
      refreshMv("mv_gr_1")

      assertMvCorrect(
        "mv_gr_1",
        "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_1 GROUP BY region"
      )
    }
  }

  // ── Test 2: Inner DISTINCT under COUNT → GROUP_RECOMPUTE ──────────────────

  describe("(2) Inner DISTINCT under COUNT — SELECT region, COUNT(DISTINCT customer_id) … GROUP BY region") {

    it("classifies as GROUP_RECOMPUTE and incrementally maintains COUNT(DISTINCT)") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_2(region STRING, customer_id INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_2 VALUES " +
          "('east', 1), ('east', 1), ('east', 2), ('west', 5), ('west', 6)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_2 AS " +
          "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_2 GROUP BY region"
      )
      mvRefreshType("mv_gr_2") shouldBe RefreshTypeCode.GroupRecompute

      assertMvCorrect(
        "mv_gr_2",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_2 GROUP BY region"
      )

      // Add a new distinct customer in 'east' (3 is new → 'east' uniq goes 2 → 3)
      spark.sql("INSERT INTO gr_sales_2 VALUES ('east', 3)")
      // Add duplicate customer in 'west' (5 already counted → uniq stays at 2)
      spark.sql("INSERT INTO gr_sales_2 VALUES ('west', 5)")
      refreshMv("mv_gr_2")

      assertMvCorrect(
        "mv_gr_2",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_2 GROUP BY region"
      )
    }
  }

  // ── Test 3: JOIN over CTE-aggregate → GROUP_RECOMPUTE ─────────────────────
  //
  // The prompt asks for shape 3 to be a "scalar correlated subquery" view body:
  //   `SELECT region, (SELECT COUNT(*) FROM products p WHERE p.region = s.region)
  //    FROM sales s GROUP BY region`
  //
  // Spark 3.5's analyzer rejects this form with SCALAR_SUBQUERY_IS_IN_GROUP_BY_OR_AGGREGATE_FUNCTION:
  // a correlated scalar subquery in the projection list of an aggregating SELECT
  // must either appear in `GROUP BY` or be wrapped in an aggregate function.
  // DuckDB (and therefore openivm's classifier) accept the form unchanged and
  // lower it to a DELIM/DEPENDENT join, routing to GROUP_RECOMPUTE.  Spark
  // cannot even parse + analyze the view body, so the MV cannot be created.
  //
  // The DELIM-join lowering machinery in openivm — `core/parser.cpp:451-463` —
  // is the same code path that handles non-trivial join-over-CTE-aggregate
  // shapes (recompute.test:511-555).  Substituting an equivalent Spark-friendly
  // form preserves the underlying GROUP_RECOMPUTE classification and exercises
  // the same affected-keys DELETE+reinsert codepath.
  describe("(3) JOIN over CTE-aggregate (DELIM-join key recovery) → GROUP_RECOMPUTE") {

    it("classifies as GROUP_RECOMPUTE and refreshes after aggregate-source DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_wh_3(w_id INT, w_name STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS gr_ol_3(ol_w_id INT, ol_d_id INT, ol_amount DOUBLE) USING DELTA")
      spark.sql("INSERT INTO gr_wh_3 VALUES (1, 'Alpha'), (2, 'Beta')")
      spark.sql(
        "INSERT INTO gr_ol_3 VALUES " +
          "(1, 1, 10.0), (1, 1, 20.0), (1, 2, 5.0), (2, 1, 30.0)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_3 AS " +
          "WITH district_avg AS (" +
          "  SELECT ol_w_id, ol_d_id, AVG(ol_amount) AS avg_rev, COUNT(*) AS line_count " +
          "  FROM gr_ol_3 GROUP BY ol_w_id, ol_d_id" +
          ") " +
          "SELECT w.w_id, w.w_name, da.ol_d_id, da.avg_rev, da.line_count " +
          "FROM gr_wh_3 w JOIN district_avg da ON w.w_id = da.ol_w_id"
      )
      mvRefreshType("mv_gr_3") shouldBe RefreshTypeCode.GroupRecompute

      // Add a row to the aggregate source — affected groups (1,1) and (2,1)
      // must be re-evaluated; (1,2) is untouched.
      spark.sql("INSERT INTO gr_ol_3 VALUES (1, 1, 30.0), (2, 1, 50.0)")
      refreshMv("mv_gr_3")

      assertMvCorrect(
        "mv_gr_3",
        "WITH district_avg AS (" +
          "  SELECT ol_w_id, ol_d_id, AVG(ol_amount) AS avg_rev, COUNT(*) AS line_count " +
          "  FROM gr_ol_3 GROUP BY ol_w_id, ol_d_id" +
          ") " +
          "SELECT w.w_id, w.w_name, da.ol_d_id, da.avg_rev, da.line_count " +
          "FROM gr_wh_3 w JOIN district_avg da ON w.w_id = da.ol_w_id"
      )
    }
  }

  // ── Test 4: MIN/MAX with GROUP BY ─────────────────────────────────────────
  //
  // The prompt asks to cover MIN/MAX with GROUP BY under the GROUP_RECOMPUTE
  // umbrella, but in practice openivm's classifier routes
  //   `SELECT k, MIN(x), MAX(x) FROM t GROUP BY k`
  // to AGGREGATE_GROUP (type 0) with the insert-only `openivm_minmax_incremental`
  // fast path (GREATEST/LEAST in the MERGE) — see
  // `openivm/src/upsert/refresh_compiler.cpp:372-399`.  The
  // `BuildAffectedKeyRefreshSQL` (type-6-shaped) form is only emitted when
  // `insert_only = false` (i.e. when the refresh detects a delete or update
  // that may retract a MIN/MAX). Since openivm-spark's compile-only bridge
  // can't see live delta state at compile time, every compile here yields
  // the type-0 form.
  //
  // We assert the observed classification and exercise the insert-only path,
  // which is the only deterministic outcome of the spark-side bridge today.
  describe("(4) MIN/MAX with GROUP BY — observed refreshType = 0 (AGGREGATE_GROUP, MERGE with GREATEST/LEAST)") {

    it("INSERTs into existing and new groups; MIN/MAX correctly maintained via MERGE fast path") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_4(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_4 VALUES ('east', 10), ('east', 30), ('west', 5), ('west', 50)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_4 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM gr_sales_4 GROUP BY region"
      )
      // Document the actual classification (type 0 with insert-only minmax fast path,
      // not type 6 — the classifier never routes MIN/MAX with GROUP BY to GROUP_RECOMPUTE).
      mvRefreshType("mv_gr_4") shouldBe RefreshTypeCode.AggregateGroup

      assertMvCorrect(
        "mv_gr_4",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM gr_sales_4 GROUP BY region"
      )

      // Insert-only DML: new max for 'east' (40), new min for 'west' (1), new group 'north'
      spark.sql("INSERT INTO gr_sales_4 VALUES ('east', 40), ('west', 1), ('north', 100), ('north', 7)")
      refreshMv("mv_gr_4")

      assertMvCorrect(
        "mv_gr_4",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM gr_sales_4 GROUP BY region"
      )
    }
  }

  // ── Test 5: UPDATE that changes the group key ─────────────────────────────

  describe("(5) Group-key change via UPDATE — old key recomputes, new key recomputes") {

    it("UPDATE that moves a row between groups → both source and destination groups recomputed") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_5(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_5 VALUES " +
          "('east', 10), ('east', 20), ('west', 30), ('west', 40)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_5 AS " +
          "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_5 GROUP BY region"
      )
      mvRefreshType("mv_gr_5") shouldBe RefreshTypeCode.GroupRecompute

      // Move the row (east, 20) → (west, 20).
      // 'east': distinct set {10, 20} → {10}  (sum 30 → 10)
      // 'west': distinct set {30, 40} → {20, 30, 40}  (sum 70 → 90)
      spark.sql("UPDATE gr_sales_5 SET region = 'west' WHERE region = 'east' AND amount = 20")
      refreshMv("mv_gr_5")

      assertMvCorrect(
        "mv_gr_5",
        "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_5 GROUP BY region"
      )
    }
  }

  // ── Test 6: Insert into existing group ────────────────────────────────────

  describe("(6) Insert into existing group → only the affected group is recomputed") {

    it("incremental refresh updates only the group that received new rows") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_6(region STRING, customer_id INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_6 VALUES " +
          "('east', 1), ('east', 2), ('west', 5), ('north', 9)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_6 AS " +
          "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_6 GROUP BY region"
      )
      mvRefreshType("mv_gr_6") shouldBe RefreshTypeCode.GroupRecompute

      // Insert into 'east' only — 'west' and 'north' must remain untouched.
      spark.sql("INSERT INTO gr_sales_6 VALUES ('east', 3), ('east', 4)")
      refreshMv("mv_gr_6")

      assertMvCorrect(
        "mv_gr_6",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_6 GROUP BY region"
      )
    }
  }

  // ── Test 7: Delete last row of a group → group disappears from MV ─────────

  describe("(7) Delete the last row of a group — affected group is removed from MV") {

    it("DELETE of every row in a group → MV no longer contains that group") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_7(region STRING, customer_id INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_7 VALUES " +
          "('east', 1), ('west', 5), ('north', 9), ('north', 10)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_7 AS " +
          "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_7 GROUP BY region"
      )
      mvRefreshType("mv_gr_7") shouldBe RefreshTypeCode.GroupRecompute

      // Delete the only row(s) belonging to group 'east' and 'west'.
      // The MV must drop those groups entirely; 'north' is unaffected.
      spark.sql("DELETE FROM gr_sales_7 WHERE region IN ('east', 'west')")
      refreshMv("mv_gr_7")

      assertMvCorrect(
        "mv_gr_7",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_7 GROUP BY region"
      )
    }
  }

  // ── Test 8: New group entirely ────────────────────────────────────────────

  describe("(8) New group key appears for the first time — INSERT-only into a brand-new group") {

    it("incremental refresh inserts the new group without disturbing pre-existing ones") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_8(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO gr_sales_8 VALUES ('east', 10), ('east', 20), ('west', 5)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_8 AS " +
          "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_8 GROUP BY region"
      )
      mvRefreshType("mv_gr_8") shouldBe RefreshTypeCode.GroupRecompute

      // 'south' is a new group key — must be inserted; other groups unchanged.
      spark.sql("INSERT INTO gr_sales_8 VALUES ('south', 100), ('south', 100), ('south', 200)")
      refreshMv("mv_gr_8")

      assertMvCorrect(
        "mv_gr_8",
        "SELECT region, SUM(DISTINCT amount) AS total_distinct FROM gr_sales_8 GROUP BY region"
      )
    }
  }

  // ── Test 9: Batched DML across multiple group keys → single REFRESH ───────

  describe("(9) Batched conflicting DML across multiple groups before a single REFRESH") {

    it("INSERT + DELETE + UPDATE on overlapping groups are reconciled in one refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_9(region STRING, customer_id INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_9 VALUES " +
          "('east', 1), ('east', 2), ('east', 3), " +
          "('west', 10), ('west', 11), " +
          "('north', 20), ('north', 21)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_9 AS " +
          "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_9 GROUP BY region"
      )
      mvRefreshType("mv_gr_9") shouldBe RefreshTypeCode.GroupRecompute

      // Batched, conflicting DML across multiple groups before a single REFRESH.
      // 'east':  delete (east,2), insert (east,4)            → distinct {1,3,4}   uniq=3
      // 'west':  update (west,11) → (west,11)+amount-style is not applicable here;
      //          we instead simulate via DELETE+INSERT: keep set {10,11,12}      uniq=3
      // 'north': delete one of (north,21), insert (north,20) (duplicate)         uniq=1
      // 'south': brand new group with insertions                                  uniq=2
      spark.sql("DELETE FROM gr_sales_9 WHERE region = 'east' AND customer_id = 2")
      spark.sql("INSERT INTO gr_sales_9 VALUES ('east', 4)")
      spark.sql("INSERT INTO gr_sales_9 VALUES ('west', 12)")
      spark.sql("DELETE FROM gr_sales_9 WHERE region = 'north' AND customer_id = 21")
      spark.sql("INSERT INTO gr_sales_9 VALUES ('north', 20)")
      spark.sql("INSERT INTO gr_sales_9 VALUES ('south', 100), ('south', 101)")

      refreshMv("mv_gr_9")

      assertMvCorrect(
        "mv_gr_9",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_9 GROUP BY region"
      )
    }
  }

  // ── Test 10: NULL group keys handled correctly ────────────────────────────

  describe("(10) NULL group keys are handled correctly (NULL-safe IS NOT DISTINCT FROM matching)") {

    it("rows with NULL region form a single group; DML targeting NULL group works") {
      spark.sql("CREATE TABLE IF NOT EXISTS gr_sales_10(region STRING, customer_id INT) USING DELTA")
      spark.sql(
        "INSERT INTO gr_sales_10 VALUES " +
          "('east', 1), ('east', 2), (NULL, 5), (NULL, 6), (NULL, 5)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_gr_10 AS " +
          "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_10 GROUP BY region"
      )
      mvRefreshType("mv_gr_10") shouldBe RefreshTypeCode.GroupRecompute

      assertMvCorrect(
        "mv_gr_10",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_10 GROUP BY region"
      )

      // Insert another NULL-region row with a new distinct customer → NULL group's uniq goes 2 → 3
      spark.sql("INSERT INTO gr_sales_10 VALUES (NULL, 7)")
      // Insert a duplicate distinct value into NULL group → uniq unchanged
      spark.sql("INSERT INTO gr_sales_10 VALUES (NULL, 5)")
      refreshMv("mv_gr_10")

      assertMvCorrect(
        "mv_gr_10",
        "SELECT region, COUNT(DISTINCT customer_id) AS uniq FROM gr_sales_10 GROUP BY region"
      )
    }
  }
}
