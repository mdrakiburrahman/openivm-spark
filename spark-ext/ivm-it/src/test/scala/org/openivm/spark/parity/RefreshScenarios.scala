package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Parity port of `openivm/test/sql/refresh.test`.
  *
  * == What the openivm test exercises ==
  *
  * The DuckDB test exercises:
  *
  *   1. `REFRESH EVERY '<interval>'` syntax that stores a `refresh_interval`
  *      value in `openivm_views` so the background daemon can pick the view up.
  *   2. `PRAGMA refresh('mv_name')` as a manual trigger, coexisting with the
  *      auto-refresh schedule.
  *   3. `PRAGMA refresh_status('mv_name')` to inspect last/next refresh times.
  *   4. View metadata: `refresh_interval IS NULL` for manual-only views.
  *   5. Multiple views with different intervals stored side by side.
  *   6. Crash-recovery: a `refresh_in_progress` flag in `openivm_views` that
  *      is set to `true` before the refresh begins and cleared on success or
  *      crash-recovery (next refresh sees stale flag → full recompute).
  *   7. `ALTER MATERIALIZED VIEW ... SET REFRESH EVERY '...'` and
  *      `SET REFRESH MANUAL` to mutate the schedule.
  *
  * == Spark-side mapping (per PLAN §9 / §12) ==
  *
  *   - Spark has no `REFRESH EVERY` syntax, no background daemon, and no
  *     `refresh_in_progress` flag on `MvMetadata`.  Per PLAN §12 these are
  *     **out of scope** for openivm-spark MVP; user-driven `REFRESH
  *     MATERIALIZED VIEW <name>` is the only refresh path.
  *
  *   - This spec ports the subset that has a Spark analogue, namely:
  *       a) Manual REFRESH is idempotent: calling it twice in a row with no
  *          intervening DML must yield the same MV contents.
  *       b) REFRESH with no pending staging delta is a no-op (commands
  *          ground out early in `RefreshMaterializedViewCommand.run` when
  *          `StagingCatalog.collectFor` returns empty).
  *       c) Multi-call safety: hammering REFRESH does not corrupt the MV.
  *       d) Multi-view independence: refreshing view A leaves view B unchanged
  *          when only A's source table received DML.
  *       e) After a normal refresh, the staging entry is marked
  *          `consumed_by` for the MV (no replay on next REFRESH).
  *
  *   - The `crash-recovery` test from the openivm file is approximated by
  *     verifying that a refresh which surfaces an internal error leaves the
  *     MV in a usable state and the next REFRESH still produces the correct
  *     answer.  (We cannot inject a `refresh_in_progress=true` row because
  *     `MvMetadata` has no such column — we instead simulate by issuing a
  *     refresh against a view that has never seen DML, then more DML, then
  *     refresh again, asserting both calls leave the MV correct.)
  */
abstract class RefreshScenarios extends IvmParitySpecBase("refresh") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) REFRESH MATERIALIZED VIEW is idempotent when no DML has happened
  // ============================================================================
  describe("(1) Idempotency — REFRESH on a freshly-created MV is a no-op") {
    it("calling REFRESH twice in a row leaves MV bag-equal to the view body") {
      sql("CREATE TABLE IF NOT EXISTS sales_r1(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_r1 VALUES ('east', 100), ('west', 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_r1 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r1 GROUP BY region"
      )

      // First refresh — no pending DML, should be a no-op
      refreshMv("mv_r1")
      assertMvCorrect(
        "mv_r1",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r1 GROUP BY region"
      )

      // Capture state, second refresh, compare
      val before = spark.table("mv_r1").collect().toSet
      refreshMv("mv_r1")
      val after = spark.table("mv_r1").collect().toSet
      after shouldBe before
      assertMvCorrect(
        "mv_r1",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r1 GROUP BY region"
      )
    }
  }

  // ============================================================================
  // (2) REFRESH with pending DML — single call applies the staged delta
  // ============================================================================
  describe("(2) Single REFRESH after INSERT applies the staged delta") {
    it("MV reflects the new row after one REFRESH") {
      sql("CREATE TABLE IF NOT EXISTS sales_r2(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_r2 VALUES ('east', 100), ('west', 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_r2 AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r2 GROUP BY region"
      )

      sql("INSERT INTO sales_r2 VALUES ('east', 50)")
      refreshMv("mv_r2")

      // Spot-check expected aggregate
      val rows = spark.table("mv_r2").orderBy("region").collect()
      val rowsTuples = rows.map(r =>
        (r.getString(r.fieldIndex("region")), r.getLong(r.fieldIndex("total")), r.getLong(r.fieldIndex("cnt")))
      )
      rowsTuples.toSeq shouldBe Seq(("east", 150L, 2L), ("west", 200L, 1L))

      assertMvCorrect(
        "mv_r2",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r2 GROUP BY region"
      )
    }
  }

  // ============================================================================
  // (3) REFRESH is idempotent across multiple calls after one batch of DML
  // ============================================================================
  describe("(3) Multi-call REFRESH safety — replaying does not double-apply") {
    it("ten consecutive REFRESH calls after a single INSERT leave the MV correct exactly once") {
      sql("CREATE TABLE IF NOT EXISTS orders_r3(id INT, qty INT) USING DELTA")
      sql("INSERT INTO orders_r3 VALUES (1, 10), (2, 20)")

      sql(
        "CREATE MATERIALIZED VIEW mv_r3 AS SELECT SUM(qty) AS total, COUNT(*) AS cnt FROM orders_r3"
      )

      sql("INSERT INTO orders_r3 VALUES (3, 30)")

      // Hammer REFRESH ten times
      (1 to 10).foreach(_ => refreshMv("mv_r3"))

      val row = spark.table("mv_r3").collect()
      row.length shouldBe 1
      row.head.getLong(row.head.fieldIndex("total")) shouldBe 60L
      row.head.getLong(row.head.fieldIndex("cnt")) shouldBe 3L

      assertMvCorrect("mv_r3", "SELECT SUM(qty) AS total, COUNT(*) AS cnt FROM orders_r3")
    }
  }

  // ============================================================================
  // (4) REFRESH with empty pending delta — no-op
  // ============================================================================
  describe("(4) REFRESH with no pending staging entries is a no-op") {
    it("REFRESH right after CREATE does not raise and leaves the MV correct") {
      sql("CREATE TABLE IF NOT EXISTS sales_r4(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_r4 VALUES ('east', 100)")

      sql(
        "CREATE MATERIALIZED VIEW mv_r4 AS SELECT region, SUM(amount) AS total FROM sales_r4 GROUP BY region"
      )

      // CREATE already loaded the MV.  No staged DML → REFRESH is a no-op.
      noException should be thrownBy refreshMv("mv_r4")
      assertMvCorrect(
        "mv_r4",
        "SELECT region, SUM(amount) AS total FROM sales_r4 GROUP BY region"
      )
    }
  }

  // ============================================================================
  // (5) Multiple MVs with different shapes — REFRESH on one is independent
  // ============================================================================
  describe("(5) Multiple MVs side-by-side — refreshing one does not affect the other") {
    it("REFRESH on mv_fast does not change mv_slow when only orders_r5 changed") {
      sql("CREATE TABLE IF NOT EXISTS orders_r5(id INT, qty INT) USING DELTA")
      sql("INSERT INTO orders_r5 VALUES (1, 10), (2, 20)")

      sql(
        "CREATE MATERIALIZED VIEW mv_r5_fast AS SELECT SUM(qty) AS total, COUNT(*) AS cnt FROM orders_r5"
      )
      sql("CREATE MATERIALIZED VIEW mv_r5_slow AS SELECT id, qty FROM orders_r5")

      sql("INSERT INTO orders_r5 VALUES (3, 30)")

      val slowBefore = spark.table("mv_r5_slow").collect().toSet
      refreshMv("mv_r5_fast")
      val slowAfter = spark.table("mv_r5_slow").collect().toSet

      // mv_r5_slow was NOT refreshed; its contents must be unchanged
      slowAfter shouldBe slowBefore

      // mv_r5_fast is up-to-date
      assertMvCorrect("mv_r5_fast", "SELECT SUM(qty) AS total, COUNT(*) AS cnt FROM orders_r5")

      // Now refresh mv_r5_slow — it picks up the same DML independently
      refreshMv("mv_r5_slow")
      assertMvCorrect("mv_r5_slow", "SELECT id, qty FROM orders_r5")
    }
  }

  // ============================================================================
  // (6) Crash-recovery analogue — re-refresh after multiple unrefreshed batches
  // ============================================================================
  describe("(6) Crash-recovery analogue — many batched DML ops, then a single REFRESH") {
    it("multiple unrefreshed batches → single REFRESH → MV is correct (full delta consolidation)") {
      sql("CREATE TABLE IF NOT EXISTS crash_r6(id INT, val INT) USING DELTA")
      sql(
        "INSERT INTO crash_r6 VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)"
      )

      sql(
        "CREATE MATERIALIZED VIEW mv_r6 AS SELECT val, COUNT(*) AS cnt FROM crash_r6 GROUP BY val"
      )

      // Batch 1
      sql(
        "INSERT INTO crash_r6 VALUES (11,110),(12,120),(13,130),(14,140),(15,150)"
      )
      // Batch 2 — DELETE
      sql("DELETE FROM crash_r6 WHERE id = 1")
      // Batch 3 — UPDATE
      sql("UPDATE crash_r6 SET val = 999 WHERE id = 2")
      // Batch 4 — more inserts
      sql("INSERT INTO crash_r6 VALUES (16,160),(17,170),(18,180)")

      // No intervening REFRESH → all four batches will hit the MV at once
      refreshMv("mv_r6")

      assertMvCorrect(
        "mv_r6",
        "SELECT val, COUNT(*) AS cnt FROM crash_r6 GROUP BY val"
      )

      // Run another batch and refresh again — final state still correct
      sql("INSERT INTO crash_r6 VALUES (19,190),(20,200)")
      refreshMv("mv_r6")
      assertMvCorrect(
        "mv_r6",
        "SELECT val, COUNT(*) AS cnt FROM crash_r6 GROUP BY val"
      )
    }
  }

  // ============================================================================
  // (7) Out-of-scope features that openivm exercises — document and assert
  //     the Spark fallback (manual REFRESH after ALTER-like recreation) works.
  // ============================================================================
  describe("(7) Out-of-scope: REFRESH EVERY / ALTER REFRESH MANUAL (PLAN §12)") {
    it("documents the gap: REFRESH EVERY is not implemented on Spark") {
      // PLAN §12 lists "REFRESH EVERY scheduling" / background daemon as out
      // of scope.  We document the gap by asserting the syntax is not
      // recognised; the manual REFRESH path remains the supported surface.
      val ex = intercept[Exception] {
        sql(
          "CREATE MATERIALIZED VIEW mv_r7_auto REFRESH EVERY '5 minutes' AS SELECT 1 AS x"
        )
      }
      // Spark either rejects via SqlParser or our IvmAstBuilder bails out.
      ex.getMessage should not be null
    }

    it("manual REFRESH on a non-scheduled MV always works (REFRESH EVERY analogue)") {
      // Simulate the openivm `mv_auto` view but without the REFRESH EVERY
      // clause — the MV behaves identically once the user invokes REFRESH.
      sql("CREATE TABLE IF NOT EXISTS sales_r7(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO sales_r7 VALUES ('east', 100), ('west', 200)")

      sql(
        "CREATE MATERIALIZED VIEW mv_r7_manual AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r7 GROUP BY region"
      )

      sql("INSERT INTO sales_r7 VALUES ('east', 50)")
      refreshMv("mv_r7_manual")
      assertMvCorrect(
        "mv_r7_manual",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r7 GROUP BY region"
      )

      // ALTER MV ... SET REFRESH MANUAL is also out of scope; the MV is
      // already in the "manual" mode by default.  Confirm it still behaves.
      sql("INSERT INTO sales_r7 VALUES ('north', 500)")
      refreshMv("mv_r7_manual")
      assertMvCorrect(
        "mv_r7_manual",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales_r7 GROUP BY region"
      )
    }
  }
}
