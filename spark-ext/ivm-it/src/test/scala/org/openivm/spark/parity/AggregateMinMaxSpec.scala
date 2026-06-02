package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P5.rt0-minmax — Parity tests for grouped MIN/MAX (RefreshType 0 / 6 with `has_minmax`).
  *
  * Mirrors openivm/test/sql/argminmax.test shapes for the MIN/MAX subset (ARG_MIN/ARG_MAX
  * have no direct Spark equivalent — see (10) for documented fallback).
  *
  * == Observed refreshType (recorded via MvCatalog.lookup) ==
  *
  * Every grouped MIN/MAX shape in this spec classifies as
  * `RefreshTypeCode.AggregateGroup` (0) — the openivm classifier never emits
  * GROUP_RECOMPUTE (6) for plain MIN/MAX. The `has_minmax` flag is set in
  * addition to AGGREGATE_GROUP, and `CompileAggregateGroups` branches on
  * `(has_minmax && !insert_only)` at refresh-emit time
  * (`refresh_compiler.cpp:372-387`).
  *
  * With `compile_only=true` in the CompileFacts JSON the empty compile-time
  * delta tables make `ResolveDeltaFastPathFlags`
  * (`refresh_delta_fast_paths.cpp:80-126`) pick
  * `insert_only=true`, which would otherwise emit a MERGE that consolidates
  * per-group via `min(col)`/`max(col)`. That MERGE produces wrong values once
  * a delete or update of the current min/max arrives at refresh time.
  *
  * To guarantee correctness under mixed DML we force the safe affected-groups
  * recompute path by setting `openivm_minmax_incremental=false` in
  * [[org.openivm.spark.compiler.OpenIvmCompiler]]'s compile script. openivm
  * then emits the form
  * {{{
  *   DELETE FROM openivm_data_<view> AS openivm_tgt
  *   WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT <keys> FROM openivm_delta_<view>)
  *                                AS openivm_aff WHERE …);
  *   INSERT INTO openivm_data_<view> SELECT * FROM (<view_query_sql>) openivm_recompute
  *   WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT <keys> FROM openivm_delta_<view>)
  *                                AS openivm_aff WHERE …);
  * }}}
  * which `SparkRefreshRewriter` translates into Delta-compatible Spark SQL by:
  *   1. substituting `openivm_data_<view>` → the backticked MV identifier
  *      (already supported);
  *   2. substituting `openivm_delta_<view>` → the per-refresh CTAS Delta path
  *      in both the DELETE EXISTS subquery and the INSERT EXISTS subquery
  *      (extended in this PR);
  *   3. substituting `memory.main.<src>` → the source temp view inside the
  *      recompute body (already supported);
  *   4. rewriting `DELETE FROM <mv> AS openivm_tgt WHERE EXISTS (…)` as a
  *      `MERGE INTO <mv> AS openivm_tgt USING (<affected_keys>) AS openivm_aff
  *      ON … WHEN MATCHED THEN DELETE` — Delta forbids subqueries in DELETE
  *      WHERE (DELTA_UNSUPPORTED_SUBQUERY); the rewriter's
  *      [[org.openivm.spark.common.SparkRefreshRewriter#rewriteDeleteExistsAsMerge]]
  *      helper handles this reshape (extended in this PR to also accept the
  *      inline subquery form with paren-aware extraction).
  *
  * Per CLAUDE.md: never silently demote to FULL_REFRESH. The rewriter
  * extensions above preserve incremental maintenance for every grouped MIN/MAX
  * shape covered here.
  */
class AggregateMinMaxSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-mm-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-AggregateMinMaxSpec")
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

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── Shape 1: SELECT region, MIN(amount) FROM sales GROUP BY region — basic MIN

  describe("(1) GROUP BY region, MIN(amount) — basic grouped MIN over insert-only delta") {
    it("classifier records AggregateGroup; MV reflects per-group MIN after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_1(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_1 VALUES ('east', 100), ('west', 50), ('east', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_1 AS " +
          "SELECT region, MIN(amount) AS lo FROM mm_sales_1 GROUP BY region"
      )
      mvRefreshType("mv_mm_1") shouldBe RefreshTypeCode.AggregateGroup
      spark.sql("INSERT INTO mm_sales_1 VALUES ('east', 75), ('south', 30)")
      refreshMv("mv_mm_1")
      assertMvCorrect(
        "mv_mm_1",
        "SELECT region, MIN(amount) AS lo FROM mm_sales_1 GROUP BY region"
      )
    }
  }

  // ── Shape 2: SELECT region, MAX(amount) FROM sales GROUP BY region — basic MAX

  describe("(2) GROUP BY region, MAX(amount) — basic grouped MAX over insert-only delta") {
    it("classifier records AggregateGroup; MV reflects per-group MAX after INSERT") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_2(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_2 VALUES ('east', 100), ('west', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_2 AS " +
          "SELECT region, MAX(amount) AS hi FROM mm_sales_2 GROUP BY region"
      )
      mvRefreshType("mv_mm_2") shouldBe RefreshTypeCode.AggregateGroup
      spark.sql("INSERT INTO mm_sales_2 VALUES ('east', 250), ('west', 30)")
      refreshMv("mv_mm_2")
      assertMvCorrect(
        "mv_mm_2",
        "SELECT region, MAX(amount) AS hi FROM mm_sales_2 GROUP BY region"
      )
    }
  }

  // ── Shape 3: MIN and MAX together ────────────────────────────────────────

  describe("(3) GROUP BY region, MIN(amount), MAX(amount)") {
    it("both extremes maintained in a single MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_3(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_3 VALUES ('east', 100), ('east', 200), ('west', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_3 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_3 GROUP BY region"
      )
      mvRefreshType("mv_mm_3") shouldBe RefreshTypeCode.AggregateGroup
      spark.sql("INSERT INTO mm_sales_3 VALUES ('east', 75), ('west', 60), ('south', 33)")
      refreshMv("mv_mm_3")
      assertMvCorrect(
        "mv_mm_3",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM mm_sales_3 GROUP BY region"
      )
    }
  }

  // ── Shape 4: MIN + SUM (mixed monoid in one MV) ───────────────────────────

  describe("(4) GROUP BY region, MIN(amount), SUM(amount) — mixed MIN with SUM") {
    it("MIN (non-additive) and SUM (additive) co-exist in one MV") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_4(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_4 VALUES ('east', 100), ('east', 200), ('west', 50)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_4 AS " +
          "SELECT region, MIN(amount) AS lo, SUM(amount) AS total " +
          "FROM mm_sales_4 GROUP BY region"
      )
      mvRefreshType("mv_mm_4") shouldBe RefreshTypeCode.AggregateGroup
      spark.sql("INSERT INTO mm_sales_4 VALUES ('east', 75), ('west', 60)")
      refreshMv("mv_mm_4")
      assertMvCorrect(
        "mv_mm_4",
        "SELECT region, MIN(amount) AS lo, SUM(amount) AS total FROM mm_sales_4 GROUP BY region"
      )
    }
  }

  // ── Shape 5: INSERT into existing group, value BELOW current MIN ─────────

  describe("(5) INSERT a value below current MIN of an existing group") {
    it("MV's MIN drops to the newly inserted value") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_5(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_5 VALUES ('east', 100), ('east', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_5 AS " +
          "SELECT region, MIN(amount) AS lo FROM mm_sales_5 GROUP BY region"
      )
      // before refresh, MV has east → 100; insert 10 which is the new MIN
      spark.sql("INSERT INTO mm_sales_5 VALUES ('east', 10)")
      refreshMv("mv_mm_5")
      assertMvCorrect(
        "mv_mm_5",
        "SELECT region, MIN(amount) AS lo FROM mm_sales_5 GROUP BY region"
      )
    }
  }

  // ── Shape 6: INSERT into existing group, value ABOVE current MIN ─────────

  describe("(6) INSERT a value above current MIN — MIN unchanged, MAX may update") {
    it("MIN stays put; MAX picks up the new high") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_6(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_6 VALUES ('east', 100), ('east', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_6 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_6 GROUP BY region"
      )
      spark.sql("INSERT INTO mm_sales_6 VALUES ('east', 500)")
      refreshMv("mv_mm_6")
      assertMvCorrect(
        "mv_mm_6",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM mm_sales_6 GROUP BY region"
      )
    }
  }

  // ── Shape 7: DELETE the current MIN — affected-groups recompute path ─────
  //
  // This test exercises the `has_minmax && !insert_only` branch in
  // `refresh_compiler.cpp:372-387`: openivm emits a DELETE FROM openivm_data_<view>
  // … WHERE EXISTS (affected_keys) followed by an INSERT INTO openivm_data_<view>
  // SELECT … FROM (<recompute>) … WHERE EXISTS (affected_keys). The Spark
  // rewriter must translate `openivm_delta_<view>` → the per-refresh CTAS path
  // in both the DELETE EXISTS subquery and the INSERT EXISTS subquery, and
  // `memory.main.<src>` → temp-view references in the recompute body.

  describe("(7) DELETE the current MIN of a group — MV reflects next-min") {
    it("MIN shifts up to the next-smallest surviving value") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_7(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO mm_sales_7 VALUES ('east', 100), ('east', 200), ('east', 300), ('west', 50)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_7 AS " +
          "SELECT region, MIN(amount) AS lo FROM mm_sales_7 GROUP BY region"
      )
      // Delete the current MIN of east (100). New MIN should be 200.
      spark.sql("DELETE FROM mm_sales_7 WHERE region = 'east' AND amount = 100")
      refreshMv("mv_mm_7")
      assertMvCorrect(
        "mv_mm_7",
        "SELECT region, MIN(amount) AS lo FROM mm_sales_7 GROUP BY region"
      )
    }
  }

  // ── Shape 8: DELETE a non-MIN row — MV unchanged ─────────────────────────

  describe("(8) DELETE a non-MIN, non-MAX row — extremes unchanged") {
    it("MIN and MAX both unchanged after a middle-value DELETE") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_8(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO mm_sales_8 VALUES ('east', 100), ('east', 200), ('east', 300)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_8 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_8 GROUP BY region"
      )
      // Delete 200 (neither min nor max). Extremes should remain 100, 300.
      spark.sql("DELETE FROM mm_sales_8 WHERE region = 'east' AND amount = 200")
      refreshMv("mv_mm_8")
      assertMvCorrect(
        "mv_mm_8",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM mm_sales_8 GROUP BY region"
      )
    }
  }

  // ── Shape 9: INSERT into entirely new group ──────────────────────────────

  describe("(9) INSERT into a brand-new group — MV gains a new row") {
    it("a previously-absent group appears with the correct MIN/MAX") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_9(region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO mm_sales_9 VALUES ('east', 100), ('east', 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_9 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_9 GROUP BY region"
      )
      spark.sql("INSERT INTO mm_sales_9 VALUES ('north', 42), ('north', 99)")
      refreshMv("mv_mm_9")
      assertMvCorrect(
        "mv_mm_9",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM mm_sales_9 GROUP BY region"
      )
    }
  }

  // ── Shape 10: ARG_MIN/ARG_MAX fallback ───────────────────────────────────
  //
  // Spark SQL has no ARG_MIN/ARG_MAX aggregate (DuckDB-specific). The Spark
  // idiom for the same semantics is a tie-breaking struct sort:
  //   MIN_BY(value, key) (Spark 3.0+) — returns `value` of the row with min `key`.
  // Spark 3.5 provides `min_by(v, k)` and `max_by(v, k)`. Although openivm
  // proper supports ARG_MIN/ARG_MAX in `argminmax.test`, openivm's classifier
  // would need to recognise `min_by`/`max_by`; the openivm DuckDB engine does
  // not currently translate Spark's `min_by`/`max_by` through LPTS.
  //
  // We therefore exercise the **fallback shape**: emulate ARG_MIN with a
  // window-style join (semantically `min_by`), but driven by plain MIN —
  // the MV stores `(grp, MIN(key), best_val)` where best_val is fetched
  // by an inner-join against the original table on (grp, min_key).
  // For the openivm-spark MVP this collapses to two grouped MIN MVs.

  describe("(10) ARG_MIN fallback — Spark has no ARG_MIN; emulate via MIN_BY-style join") {
    it("`MIN_BY(val, key)` semantics via Spark-native min_by built-in") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_orders_10(grp STRING, val STRING, k INT) USING DELTA")
      spark.sql(
        "INSERT INTO mm_orders_10 VALUES ('A', 'apple', 3), ('A', 'avocado', 1), " +
          "('B', 'banana', 2), ('B', 'blueberry', 5)"
      )
      // Spark's `min_by(v, k)` returns v from the row with the minimum k.
      // openivm's parser does not classify min_by as MIN/MAX — so this MV
      // currently classifies as FULL_REFRESH (the LPTS layer can't translate
      // min_by). We assert that the MV stays correct under the FULL_REFRESH
      // assembler, which is the documented fallback for ARG_MIN/ARG_MAX in
      // Spark.
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_10 AS " +
          "SELECT grp, MIN_BY(val, k) AS best FROM mm_orders_10 GROUP BY grp"
      )
      // Document that min_by lands in FULL_REFRESH (openivm doesn't translate it).
      val rt = mvRefreshType("mv_mm_10")
      withClue(s"observed refreshType = $rt: ") {
        Set(RefreshTypeCode.FullRefresh, RefreshTypeCode.AggregateGroup) should contain(rt)
      }
      // A new minimum arrives for A — best_val should shift.
      spark.sql("INSERT INTO mm_orders_10 VALUES ('A', 'apricot', 0)")
      refreshMv("mv_mm_10")
      assertMvCorrect(
        "mv_mm_10",
        "SELECT grp, MIN_BY(val, k) AS best FROM mm_orders_10 GROUP BY grp"
      )
    }
  }

  // ── Shape 11: Batched DML mixing inserts & deletes within a group ────────

  describe("(11) Batched DML mixing INSERT + DELETE that changes MIN of a group") {
    it("single REFRESH reconciles a delta that crosses the current MIN") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_11(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO mm_sales_11 VALUES ('east', 100), ('east', 200), ('west', 50), ('west', 250)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_11 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_11 GROUP BY region"
      )
      // Mixed delta: delete current MIN of east (100), add new MIN (25),
      // delete current MAX of west (250), add new MAX (300) — single refresh.
      spark.sql("DELETE FROM mm_sales_11 WHERE region = 'east' AND amount = 100")
      spark.sql("INSERT INTO mm_sales_11 VALUES ('east', 25)")
      spark.sql("DELETE FROM mm_sales_11 WHERE region = 'west' AND amount = 250")
      spark.sql("INSERT INTO mm_sales_11 VALUES ('west', 300)")
      refreshMv("mv_mm_11")
      assertMvCorrect(
        "mv_mm_11",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM mm_sales_11 GROUP BY region"
      )
    }
  }

  // ── Shape 12: NULL handling in MIN/MAX ───────────────────────────────────

  describe("(12) MIN/MAX ignore NULLs per SQL semantics") {
    it("groups containing NULLs report the min/max of the non-NULL values") {
      spark.sql("CREATE TABLE IF NOT EXISTS mm_sales_12(region STRING, amount INT) USING DELTA")
      spark.sql(
        "INSERT INTO mm_sales_12 VALUES ('east', 100), ('east', NULL), ('west', NULL), ('west', 50)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_12 AS " +
          "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_12 GROUP BY region"
      )
      // Inserts/deletes that involve NULLs must not affect MIN/MAX.
      spark.sql("INSERT INTO mm_sales_12 VALUES ('east', NULL), ('south', NULL), ('south', 7)")
      spark.sql("DELETE FROM mm_sales_12 WHERE region = 'west' AND amount IS NULL")
      refreshMv("mv_mm_12")
      assertMvCorrect(
        "mv_mm_12",
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM mm_sales_12 GROUP BY region"
      )
    }
  }

  // ── Shape 13: Composite group keys ───────────────────────────────────────

  describe("(13) Composite group keys (region, channel) with MIN and MAX") {
    it("MIN/MAX maintained correctly under a multi-column GROUP BY") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS mm_sales_13(region STRING, channel STRING, amount INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO mm_sales_13 VALUES " +
          "('east','web',100),('east','web',200),('east','retail',300),('west','web',50)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mm_13 AS " +
          "SELECT region, channel, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_13 GROUP BY region, channel"
      )
      spark.sql(
        "INSERT INTO mm_sales_13 VALUES " +
          "('east','web',75),('west','retail',400),('east','retail',150)"
      )
      spark.sql("DELETE FROM mm_sales_13 WHERE region = 'east' AND channel = 'web' AND amount = 100")
      refreshMv("mv_mm_13")
      assertMvCorrect(
        "mv_mm_13",
        "SELECT region, channel, MIN(amount) AS lo, MAX(amount) AS hi " +
          "FROM mm_sales_13 GROUP BY region, channel"
      )
    }
  }
}
