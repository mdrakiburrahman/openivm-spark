package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Parity port of `openivm/test/sql/refresh_hooks.test`.
  *
  * == What the openivm test exercises ==
  *
  * The DuckDB test installs custom-SQL hooks against the system table
  * `openivm_refresh_hooks` that fire before, after, or *instead of* the IVM
  * refresh program for a given MV.  Each hook row is `(view_name, hook_sql,
  * mode)` with mode ∈ {before, after, replace}.  When a refresh runs, openivm:
  *
  *   - mode = 'before':  run `hook_sql`, then the IVM refresh program.
  *   - mode = 'after':   run the IVM refresh program, then `hook_sql`.
  *   - mode = 'replace': run `hook_sql` instead of the IVM refresh program.
  *
  * == Spark-side mapping (per PLAN §12) ==
  *
  * Custom refresh hooks are an **openivm-only** feature; they have no Spark
  * analogue and openivm-spark does NOT implement them.  Per PLAN §12 this
  * surface is out of scope.  This spec:
  *
  *   1. Documents the gap explicitly with a test asserting there is no
  *      `openivm_refresh_hooks` table on the Spark side.
  *   2. Confirms that, since the hook subsystem does not exist, the IVM
  *      refresh program runs unconditionally — i.e. no rogue hooks
  *      "replace" the refresh and leave the MV stale.  This is the inverse
  *      of the openivm "replace-hook" assertion: in openivm the hook
  *      *did* run instead of IVM; on Spark there is no hook subsystem, so
  *      IVM *always* runs.
  *   3. Re-runs the openivm hook test's scenario — INSERT + REFRESH leaves
  *      the MV bag-equal to the live view body — to confirm normal refresh
  *      semantics are unaffected.
  *
  * If a future Spark version of refresh-hooks lands, this spec should be
  * extended to mirror openivm's before/after/replace semantics.
  */
abstract class RefreshHooksScenarios extends IvmParitySpecBase("refresh-hooks") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) Out-of-scope documentation — no openivm_refresh_hooks system table
  // ============================================================================
  describe("(1) Refresh hooks are out of scope (PLAN §12)") {
    it("openivm_refresh_hooks does not exist as a Spark table") {
      val all = spark.catalog.listTables().collect().map(_.name.toLowerCase).toSet
      all should not contain "openivm_refresh_hooks"
    }
  }

  // ============================================================================
  // (2) IVM refresh runs unconditionally — no rogue "replace" hook intervenes
  // ============================================================================
  describe("(2) IVM refresh runs unconditionally — no replace-hook exists to short-circuit it") {

    it("base case: INSERT + REFRESH → MV reflects the change (mirrors openivm test 1)") {
      sql("CREATE TABLE IF NOT EXISTS t_hook2(id INT, val INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_hook2 AS SELECT SUM(val) AS total FROM t_hook2"
      )
      sql("INSERT INTO t_hook2 VALUES (1, 100)")
      refreshMv("mv_hook2")

      val row = spark.table("mv_hook2").collect()
      row.length shouldBe 1
      row.head.getLong(row.head.fieldIndex("total")) shouldBe 100L
      assertMvCorrect("mv_hook2", "SELECT SUM(val) AS total FROM t_hook2")
    }

    it("successive inserts + REFRESH cycles each propagate without any hook firing") {
      sql("CREATE TABLE IF NOT EXISTS t_hook3(id INT, val INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_hook3 AS SELECT SUM(val) AS total FROM t_hook3"
      )
      sql("INSERT INTO t_hook3 VALUES (1, 100)")
      refreshMv("mv_hook3")
      assertMvCorrect("mv_hook3", "SELECT SUM(val) AS total FROM t_hook3")

      sql("INSERT INTO t_hook3 VALUES (2, 200)")
      refreshMv("mv_hook3")
      spark.table("mv_hook3").collect().head.getLong(0) shouldBe 300L

      sql("INSERT INTO t_hook3 VALUES (3, 300)")
      refreshMv("mv_hook3")
      spark.table("mv_hook3").collect().head.getLong(0) shouldBe 600L

      sql("INSERT INTO t_hook3 VALUES (4, 400)")
      refreshMv("mv_hook3")
      spark.table("mv_hook3").collect().head.getLong(0) shouldBe 1000L

      assertMvCorrect("mv_hook3", "SELECT SUM(val) AS total FROM t_hook3")
    }
  }

  // ============================================================================
  // (3) User-side "hook" emulation: run user SQL around REFRESH manually
  // ============================================================================
  describe("(3) User-driven hook analogue — sequence user SQL around REFRESH") {

    it("after-hook analogue: user INSERTs into a log table after REFRESH; MV is correct") {
      // Mirrors openivm Test 1 (after-hook).  The "hook" is just the user
      // writing the log row themselves; we verify the MV is updated AND the
      // log row landed.
      sql("CREATE TABLE IF NOT EXISTS t_hook4(id INT, val INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS hook4_log(msg STRING) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_hook4 AS SELECT SUM(val) AS total FROM t_hook4"
      )

      sql("INSERT INTO t_hook4 VALUES (1, 100)")
      refreshMv("mv_hook4")
      sql("INSERT INTO hook4_log VALUES ('after-hook ran')")

      val logRows = spark.table("hook4_log").collect()
      logRows.length shouldBe 1
      logRows.head.getString(0) shouldBe "after-hook ran"
      assertMvCorrect("mv_hook4", "SELECT SUM(val) AS total FROM t_hook4")
    }

    it("before-hook analogue: user INSERTs into the log BEFORE REFRESH; MV is correct") {
      sql("CREATE TABLE IF NOT EXISTS t_hook5(id INT, val INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS hook5_log(msg STRING) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_hook5 AS SELECT SUM(val) AS total FROM t_hook5"
      )

      sql("INSERT INTO t_hook5 VALUES (1, 100)")
      sql("INSERT INTO hook5_log VALUES ('before-hook ran')")
      refreshMv("mv_hook5")

      spark.table("hook5_log").collect().head.getString(0) shouldBe "before-hook ran"
      assertMvCorrect("mv_hook5", "SELECT SUM(val) AS total FROM t_hook5")
    }

    // The "replace-hook" mode has no Spark analogue — the user cannot opt
    // out of openivm-spark's incremental refresh program — and the inverse
    // assertion is the contract we verify in describe(2).
    it("no replace-hook subsystem exists — REFRESH always runs the IVM program") {
      sql("CREATE TABLE IF NOT EXISTS t_hook6(id INT, val INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_hook6 AS SELECT SUM(val) AS total FROM t_hook6"
      )
      sql("INSERT INTO t_hook6 VALUES (1, 100), (2, 200), (3, 300)")
      refreshMv("mv_hook6")
      // If a replace-hook had short-circuited refresh, total would still be NULL/0.
      val total = spark.table("mv_hook6").collect().head.getLong(0)
      total shouldBe 600L
      assertMvCorrect("mv_hook6", "SELECT SUM(val) AS total FROM t_hook6")
    }
  }

  // ============================================================================
  // (4) Mirror openivm Test 4: "No hook (default behavior)" — same as base.
  // ============================================================================
  describe("(4) Default behavior — no hook → normal IVM refresh") {
    it("after a long sequence of DML, the MV stays correct without any hook subsystem") {
      sql("CREATE TABLE IF NOT EXISTS t_hook7(id INT, val INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_hook7 AS SELECT SUM(val) AS total FROM t_hook7"
      )
      sql("INSERT INTO t_hook7 VALUES (1, 100), (2, 200), (3, 300), (4, 400)")
      refreshMv("mv_hook7")
      spark.table("mv_hook7").collect().head.getLong(0) shouldBe 1000L
      assertMvCorrect("mv_hook7", "SELECT SUM(val) AS total FROM t_hook7")
    }
  }
}
