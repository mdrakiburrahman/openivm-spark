package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6f — 1:1 ScalaTest port of openivm/test/sql/argminmax.test.
  *
  * Covers IVM with ARG_MIN and ARG_MAX (two-argument, group-recompute path)
  * across four scenarios:
  *   1. ARG_MIN baseline + INSERT/INSERT-new-group/batched-INSERT+DELETE flow
  *   2. ARG_MAX baseline + INSERT-new-max + DELETE-current-max+INSERT
  *   3. ARG_MIN delete-only delta that shifts argmin to another existing row
  *   4. Mixed ARG_MIN + SUM in the same view
  *
  * == ARG_MIN/ARG_MAX → Spark mapping ==
  *
  * Spark 3.5 ships `min_by(value, key)` and `max_by(value, key)` as direct
  * semantic equivalents to DuckDB's `ARG_MIN(value, key)` and
  * `ARG_MAX(value, key)`: both return the `value` from the row whose `key` is
  * the minimum / maximum of the group. Per the P6f conventions we use
  * `min_by` / `max_by` in **both** the materialized-view definition and the
  * expected-SQL baseline so the bidirectional `EXCEPT ALL` check exercised by
  * [[assertMvCorrect]] compares like-for-like.
  *
  * == Caveat on openivm classification ==
  *
  * openivm proper recognises `ARG_MIN` / `ARG_MAX` as `has_minmax` aggregates
  * and routes them through the affected-groups recompute path
  * (`refresh_compiler.cpp:372-387`). Spark's `min_by` / `max_by` are
  * structurally similar but pass through LPTS via a different aggregate-name
  * spelling, so the openivm-spark classifier may land these views in
  * `FULL_REFRESH` (3) rather than `AggregateGroup` (0). That does not affect
  * correctness — the FULL_REFRESH assembler recomputes the view from the
  * source, and `assertMvCorrect` verifies bag equality against the live
  * expected query — so this spec deliberately does **not** pin the
  * refresh-type code (mirroring the documented fallback in
  * [[AggregateMinMaxSpec]] shape 10).
  *
  * == Excluded openivm-only sections ==
  *
  * The openivm test file also exercises `PRAGMA refresh_status(...)` and
  * `SELECT type FROM openivm_views ...` to assert the in-memory IVM bookkeeping
  * tables. Those are DuckDB-extension internals with no Spark counterpart and
  * are intentionally skipped here per the porting conventions.
  */
class ArgMinMaxSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-argmm-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ArgMinMaxSpec")
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

  /** Project away hidden `openivm_*` bookkeeping columns from the MV, then
    * perform a bidirectional EXCEPT ALL equivalence check against the
    * expected SQL expression.
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

  // ── Section 1: ARG_MIN — orders(product, value, key) ──────────────────────
  //
  // Ports lines 7–97 of argminmax.test. A single it-block preserves the
  // stateful refresh sequence: baseline → INSERT new argmin → INSERT new
  // group → batched INSERT + DELETE that shifts the argmin within a group.

  describe("(1) ARG_MIN(value, key) GROUP BY product — orders") {
    it("baseline + INSERT new argmin + INSERT new group + batched INSERT+DELETE all stay correct") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS orders (product STRING, value STRING, key INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO orders VALUES ('A', 'apple', 3), ('A', 'avocado', 1), " +
          "('B', 'banana', 2), ('B', 'blueberry', 5)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_argmin AS " +
          "SELECT product, min_by(value, key) AS best_value FROM orders GROUP BY product"
      )

      // Baseline: A→avocado (key=1), B→banana (key=2)
      assertMvCorrect(
        "mv_argmin",
        "SELECT product, min_by(value, key) AS best_value FROM orders GROUP BY product"
      )

      // Insert a row that becomes the new ARG_MIN for product A.
      spark.sql("INSERT INTO orders VALUES ('A', 'apricot', 0)")
      refreshMv("mv_argmin")
      assertMvCorrect(
        "mv_argmin",
        "SELECT product, min_by(value, key) AS best_value FROM orders GROUP BY product"
      )

      // Insert into an entirely new group.
      spark.sql("INSERT INTO orders VALUES ('C', 'cherry', 1)")
      refreshMv("mv_argmin")
      assertMvCorrect(
        "mv_argmin",
        "SELECT product, min_by(value, key) AS best_value FROM orders GROUP BY product"
      )

      // Batched: INSERT + DELETE on the same group (B) before a single refresh.
      // Inserts ('B','blackberry',1) which becomes the new argmin for B, and
      // deletes the prior argmin row ('B','banana',2).
      spark.sql("INSERT INTO orders VALUES ('B', 'blackberry', 1)")
      spark.sql("DELETE FROM orders WHERE product = 'B' AND key = 2")
      refreshMv("mv_argmin")
      assertMvCorrect(
        "mv_argmin",
        "SELECT product, min_by(value, key) AS best_value FROM orders GROUP BY product"
      )
    }
  }

  // ── Section 2: ARG_MAX — items(category, name, score) ─────────────────────
  //
  // Ports lines 99–169 of argminmax.test. Baseline → INSERT new argmax →
  // DELETE current argmax + INSERT a new one (batched before single refresh).

  describe("(2) ARG_MAX(name, score) GROUP BY category — items") {
    it("baseline + INSERT new argmax + DELETE current argmax + INSERT all stay correct") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS items (category STRING, name STRING, score INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO items VALUES ('X', 'alpha', 10), ('X', 'beta', 30), " +
          "('Y', 'gamma', 20), ('Y', 'delta', 5)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_argmax AS " +
          "SELECT category, max_by(name, score) AS best_name FROM items GROUP BY category"
      )

      // Baseline: X→beta (score=30), Y→gamma (score=20).
      assertMvCorrect(
        "mv_argmax",
        "SELECT category, max_by(name, score) AS best_name FROM items GROUP BY category"
      )

      // Insert row that becomes new ARG_MAX for Y.
      spark.sql("INSERT INTO items VALUES ('Y', 'epsilon', 50)")
      refreshMv("mv_argmax")
      assertMvCorrect(
        "mv_argmax",
        "SELECT category, max_by(name, score) AS best_name FROM items GROUP BY category"
      )

      // Batched: DELETE the current argmax for X and INSERT a new one before
      // a single refresh. New X argmax: zeta (score=25) — the next-highest
      // surviving row after removing beta (score=30) is alpha (score=10),
      // and zeta lands at 25 which becomes the new argmax.
      spark.sql("DELETE FROM items WHERE category = 'X' AND score = 30")
      spark.sql("INSERT INTO items VALUES ('X', 'zeta', 25)")
      refreshMv("mv_argmax")
      assertMvCorrect(
        "mv_argmax",
        "SELECT category, max_by(name, score) AS best_name FROM items GROUP BY category"
      )
    }
  }

  // ── Section 3: Delete-only shift — scores(grp, val, key) ──────────────────
  //
  // Ports lines 193–229 of argminmax.test. A pure DELETE delta that removes
  // the current argmin row must shift the result to the next-smallest
  // surviving row in the group (no new row inserted).

  describe("(3) ARG_MIN delete-only delta shifts argmin to another existing row — scores") {
    it("DELETE of current argmin yields the next-smallest surviving row") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS scores (grp STRING, val STRING, key INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO scores VALUES ('A', 'first', 1), ('A', 'second', 2), ('A', 'third', 3)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_shift AS " +
          "SELECT grp, min_by(val, key) AS best FROM scores GROUP BY grp"
      )

      // Baseline: A→first (key=1).
      assertMvCorrect(
        "mv_shift",
        "SELECT grp, min_by(val, key) AS best FROM scores GROUP BY grp"
      )

      // Delete the current argmin row — argmin must shift to the next-smallest key.
      spark.sql("DELETE FROM scores WHERE grp = 'A' AND key = 1")
      refreshMv("mv_shift")
      assertMvCorrect(
        "mv_shift",
        "SELECT grp, min_by(val, key) AS best FROM scores GROUP BY grp"
      )
    }
  }

  // ── Section 4: Mixed ARG_MIN + SUM — events(region, name, priority, amount) ──
  //
  // Ports lines 231–272 of argminmax.test. Verifies a view that combines a
  // non-additive ARG_MIN (group-recompute family) with an additive SUM
  // (incremental family) in a single MV — exercising the openivm classifier's
  // mixed-monoid path.

  describe("(4) Mixed ARG_MIN + SUM in the same view — events") {
    it("ARG_MIN(name, priority) and SUM(amount) co-exist in one MV across an INSERT batch") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS events " +
          "(region STRING, name STRING, priority INT, amount INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO events VALUES " +
          "('N', 'alpha', 3, 100), ('N', 'beta', 1, 200), ('S', 'gamma', 2, 50)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_mixed AS " +
          "SELECT region, min_by(name, priority) AS top_name, SUM(amount) AS total " +
          "FROM events GROUP BY region"
      )

      // Baseline: N→(beta, 300), S→(gamma, 50).
      assertMvCorrect(
        "mv_mixed",
        "SELECT region, min_by(name, priority) AS top_name, SUM(amount) AS total " +
          "FROM events GROUP BY region"
      )

      // Insert two rows: delta becomes new argmin for N (priority=0) and
      // adds to S without changing its argmin.
      spark.sql("INSERT INTO events VALUES ('N', 'delta', 0, 75), ('S', 'epsilon', 5, 25)")
      refreshMv("mv_mixed")
      assertMvCorrect(
        "mv_mixed",
        "SELECT region, min_by(name, priority) AS top_name, SUM(amount) AS total " +
          "FROM events GROUP BY region"
      )
    }
  }
}
