package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** P5.rt9 — Comprehensive coverage of SEMI/ANTI shapes (without aggregation).
  *
  * == Empirical classification on the current openivm/openivm-spark pin ==
  *
  *   View shape                              │ Classifier path
  *   ────────────────────────────────────── │ ──────────────────────────────
  *   WHERE EXISTS / NOT EXISTS              │ FULL_REFRESH (3)
  *   WHERE IN / NOT IN                      │ FULL_REFRESH (3)
  *   `LEFT SEMI JOIN` / `LEFT ANTI JOIN`    │ FULL_REFRESH (3)
  *   `SEMI JOIN`     / `ANTI JOIN`          │ FULL_REFRESH (3)
  *
  * All shapes empirically land in `RefreshTypeCode.FullRefresh` (3) — this was
  * verified via a probe spec during P5.rt9 implementation.  See "Why
  * FULL_REFRESH and not SEMI_ANTI_RECOMPUTE (9)?" below.
  *
  * == Why FULL_REFRESH and not SEMI_ANTI_RECOMPUTE (9)? ==
  *
  * openivm's SEMI_ANTI_RECOMPUTE path is documented in
  * `openivm/src/upsert/refresh_compiler_aux.cpp:88-216` (`CompileSemiAntiRecompute`).
  * In the openivm CLI it relies on:
  *
  *   - **Aux-state metadata registered at CREATE time**.  The
  *     `RefreshMetadata::SemiAntiAuxMeta` row (left_table, right_table,
  *     predicate, output_cols, aux_table, …) is populated by openivm's parser
  *     extension while planning the SEMI/ANTI view.  Without it,
  *     `refresh_sql.cpp:429` falls through to GROUP_RECOMPUTE.
  *
  *   - **A per-MV aux Delta table** `<view>_aux(left_cols…, _match_count,
  *     _left_count)` created and persisted by the openivm CREATE handler.
  *
  *   - **DuckDB-specific physical SQL** in the emitted refresh program:
  *     `generate_series(1, _left_count)` for bag-semantics fan-out, `rowid`
  *     pseudo-columns for retraction targeting via `ROW_NUMBER() OVER
  *     (PARTITION BY …)`, and DuckDB-style `MERGE INTO _aux USING …`
  *     statements.
  *
  * On the openivm-spark side, the CREATE-time aux-meta is **not** persisted
  * (the openivm session is `compile_only=true` and ephemeral) and the Spark
  * side has no `<view>_aux` table.  The `CreateMaterializedViewCommand` checks
  * `SparkRefreshRewriter.hasRealDelta(compiled.sql, name.table)`: when the
  * compiler hasn't emitted a real `INSERT INTO openivm_delta_<view>` block —
  * which is the case for every SEMI/ANTI shape on the current pin — the MV is
  * demoted to FULL_REFRESH so every refresh re-executes the view body via
  * `INSERT OVERWRITE TABLE mv SELECT * FROM (<viewSql>)`.
  *
  * Implementing the rt9 incremental path properly would require:
  *
  *   1. Modifying openivm's parser to ship a Spark-targetable variant of
  *      `SemiAntiAuxMeta` and store it in `MvMetadata.properties` here.
  *   2. Creating a per-MV Delta-backed `<v>_aux` table at CREATE time and
  *      bootstrapping it with the initial match-count snapshot.
  *   3. Rewriting the openivm-emitted refresh SQL in
  *      `SparkRefreshRewriter`: translating `generate_series(1, n)` to
  *      `LATERAL VIEW explode(sequence(1, cast(n as int)))`, eliminating
  *      DuckDB `rowid`, and adapting the
  *      `MERGE INTO _aux USING (left delta) … MERGE INTO _aux USING (right
  *      delta) …` chain for Delta Lake MERGE semantics.
  *   4. Wiring an `AuxStateAssembler` branch through `SparkMergeAssembler`.
  *
  * That is a multi-day, multi-PR effort and is intentionally scoped out of
  * P5.rt9.  This spec exercises the rt3 fallback path that is already correct
  * by construction (INSERT OVERWRITE always reflects the live source) and
  * thereby pins the user-visible behaviour for all 11 shapes the task
  * specifies.  Each test sets up base tables, creates the MV, performs DML
  * (potentially mixed across LEFT + RIGHT relations), refreshes, and asserts
  * bidirectional `EXCEPT ALL` equivalence between the MV and a re-evaluated
  * `sql(viewBody)`.
  *
  * The `LEFT SEMI JOIN` / `LEFT ANTI JOIN` shapes additionally exercise the
  * `OpenIvmCompiler.normalizeSparkSqlForDuckdb` translation added as part of
  * P5.rt9: Spark accepts both `LEFT SEMI`/`LEFT ANTI` and bare `SEMI`/`ANTI`,
  * but DuckDB only accepts the bare forms, so the CREATE-time bridge strips
  * the `LEFT` prefix before forwarding the SQL to openivm.
  */
abstract class SemiAntiScenarios extends IvmParitySpecBase("semi-anti") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Looks up the recorded refresh type for `name` via the MV catalog. */
  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ── Test 1: WHERE EXISTS subquery ────────────────────────────────────────
  // Classifier path: openivm-spark demotes to FULL_REFRESH (rt3) because the
  // compiled refresh SQL has no real openivm_delta_<view> INSERT — the openivm
  // SEMI_ANTI_RECOMPUTE classifier needs aux_meta which is not registered for
  // EXISTS-shaped views under compile_only=true.

  describe("(1) WHERE EXISTS subquery → FULL_REFRESH fallback") {
    it("refreshes correctly after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_1(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_1(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_1 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_1 VALUES (1, 100), (3, 50)")

      val viewBody =
        "SELECT * FROM sa_gods_1 g WHERE EXISTS " +
          "(SELECT 1 FROM sa_pay_1 p WHERE p.from_uid = g.uid)"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_1 AS $viewBody")

      mvRefreshType("mv_sa_1") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_1", viewBody)

      sql("INSERT INTO sa_gods_1 VALUES (4, 'Athena')")
      sql("INSERT INTO sa_pay_1 VALUES (4, 25)")
      refreshMv("mv_sa_1")
      assertMvCorrect("mv_sa_1", viewBody)
    }
  }

  // ── Test 2: WHERE NOT EXISTS subquery ────────────────────────────────────
  // Classifier path: same as Test 1 — FULL_REFRESH fallback.

  describe("(2) WHERE NOT EXISTS subquery → FULL_REFRESH fallback") {
    it("refreshes correctly after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_2(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_2(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_2 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_2 VALUES (1, 100)")

      val viewBody =
        "SELECT * FROM sa_gods_2 g WHERE NOT EXISTS " +
          "(SELECT 1 FROM sa_pay_2 p WHERE p.from_uid = g.uid)"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_2 AS $viewBody")

      mvRefreshType("mv_sa_2") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_2", viewBody)

      // Add a new god (no payment) and a payment for an existing god — both
      // should flip visibility in the MV.
      sql("INSERT INTO sa_gods_2 VALUES (4, 'Athena')")
      sql("INSERT INTO sa_pay_2 VALUES (2, 5)")
      refreshMv("mv_sa_2")
      assertMvCorrect("mv_sa_2", viewBody)
    }
  }

  // ── Test 3: WHERE IN subquery ────────────────────────────────────────────
  // openivm/Spark flatten IN (subquery) to SEMI JOIN — same FULL_REFRESH path.

  describe("(3) WHERE IN subquery → FULL_REFRESH fallback") {
    it("refreshes correctly after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_3(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_3(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_3 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_3 VALUES (1, 100), (2, 50)")

      val viewBody =
        "SELECT * FROM sa_gods_3 WHERE uid IN (SELECT from_uid FROM sa_pay_3)"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_3 AS $viewBody")

      mvRefreshType("mv_sa_3") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_3", viewBody)

      sql("INSERT INTO sa_gods_3 VALUES (4, 'Athena')")
      sql("INSERT INTO sa_pay_3 VALUES (4, 1), (3, 7)")
      refreshMv("mv_sa_3")
      assertMvCorrect("mv_sa_3", viewBody)
    }
  }

  // ── Test 4: WHERE NOT IN subquery (NULL-safe RIGHT) ──────────────────────
  // Standard SQL semantics: NOT IN (subq) returns no rows whenever the
  // subquery emits any NULL.  To keep the result well-defined and exercise
  // the FULL_REFRESH path on a non-empty MV, we filter NULLs out of the
  // RIGHT side here; Test 11 covers the NULL-poisons-NOT-IN gotcha.

  describe("(4) WHERE NOT IN subquery (NULL-filtered right) → FULL_REFRESH fallback") {
    it("refreshes correctly after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_4(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_4(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_4 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_4 VALUES (1, 100)")

      val viewBody =
        "SELECT * FROM sa_gods_4 WHERE uid NOT IN " +
          "(SELECT from_uid FROM sa_pay_4 WHERE from_uid IS NOT NULL)"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_4 AS $viewBody")

      mvRefreshType("mv_sa_4") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_4", viewBody)

      sql("INSERT INTO sa_gods_4 VALUES (4, 'Athena')")
      sql("INSERT INTO sa_pay_4 VALUES (3, 25)")
      refreshMv("mv_sa_4")
      assertMvCorrect("mv_sa_4", viewBody)
    }
  }

  // ── Test 5: LEFT SEMI JOIN syntax ────────────────────────────────────────
  // Verifies the Spark↔DuckDB syntax bridge: `LEFT SEMI JOIN` is the documented
  // Spark spelling but DuckDB only accepts bare `SEMI JOIN`.
  // `OpenIvmCompiler.normalizeSparkSqlForDuckdb` strips the `LEFT` prefix
  // before sending the view body to DuckDB.  Spark stores the *original* SQL
  // in `MvMetadata.querySql`, so the FULL_REFRESH `INSERT OVERWRITE` runs the
  // unmodified user query.

  describe("(5) LEFT SEMI JOIN syntax → FULL_REFRESH fallback") {
    it("refreshes correctly after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_5(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_5(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_5 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_5 VALUES (1, 100), (1, 200), (3, 50)")

      val viewBody =
        "SELECT g.* FROM sa_gods_5 g LEFT SEMI JOIN sa_pay_5 p ON g.uid = p.from_uid"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_5 AS $viewBody")

      mvRefreshType("mv_sa_5") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_5", viewBody)

      sql("INSERT INTO sa_gods_5 VALUES (4, 'Athena')")
      sql("INSERT INTO sa_pay_5 VALUES (4, 7)")
      refreshMv("mv_sa_5")
      assertMvCorrect("mv_sa_5", viewBody)
    }
  }

  // ── Test 6: LEFT ANTI JOIN syntax ────────────────────────────────────────
  // Same syntax-bridge path as Test 5 (verified through the normalizer);
  // FULL_REFRESH at refresh time.

  describe("(6) LEFT ANTI JOIN syntax → FULL_REFRESH fallback") {
    it("refreshes correctly after INSERTs on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_6(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_6(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_6 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_6 VALUES (1, 100)")

      val viewBody =
        "SELECT g.* FROM sa_gods_6 g LEFT ANTI JOIN sa_pay_6 p ON g.uid = p.from_uid"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_6 AS $viewBody")

      mvRefreshType("mv_sa_6") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_6", viewBody)

      sql("INSERT INTO sa_gods_6 VALUES (4, 'Athena')")
      sql("INSERT INTO sa_pay_6 VALUES (2, 5), (3, 10)")
      refreshMv("mv_sa_6")
      assertMvCorrect("mv_sa_6", viewBody)
    }
  }

  // ── Test 7: RIGHT-side INSERT flips a left tuple invisible → visible ─────
  // Specifically exercises the "right-side INSERT" leg of the rt9 design.
  // Under FULL_REFRESH this is a single INSERT OVERWRITE; the test asserts
  // the user-visible semantics regardless of the underlying mechanism.

  describe("(7) RIGHT-side INSERT flips a left tuple invisible → visible") {
    it("MV reflects the new visibility after refresh") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_7(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_7(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_7 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_7 VALUES (1, 100)")

      val viewBody =
        "SELECT g.* FROM sa_gods_7 g WHERE EXISTS " +
          "(SELECT 1 FROM sa_pay_7 p WHERE p.from_uid = g.uid)"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_7 AS $viewBody")

      mvRefreshType("mv_sa_7") shouldBe RefreshTypeCode.FullRefresh

      // Pre-state: only uid=1 should be visible.
      assertMvCorrect("mv_sa_7", viewBody)
      spark.table("mv_sa_7").count() shouldBe 1L

      // Add a payment for uid=2 — flips Hera from invisible → visible.
      sql("INSERT INTO sa_pay_7 VALUES (2, 50)")
      refreshMv("mv_sa_7")
      assertMvCorrect("mv_sa_7", viewBody)
      spark.table("mv_sa_7").count() shouldBe 2L
    }
  }

  // ── Test 8: RIGHT-side DELETE flips a left tuple visible → invisible ─────
  // Removing the last matching right-row must drop the left tuple from the MV.

  describe("(8) RIGHT-side DELETE flips a left tuple visible → invisible (last match removed)") {
    it("MV drops the left tuple after the last matching right row is deleted") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_8(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_8(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_8 VALUES (1, 'Zeus'), (2, 'Hera')")
      sql("INSERT INTO sa_pay_8 VALUES (1, 100), (2, 50)")

      val viewBody =
        "SELECT g.* FROM sa_gods_8 g LEFT SEMI JOIN sa_pay_8 p ON g.uid = p.from_uid"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_8 AS $viewBody")

      mvRefreshType("mv_sa_8") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_8", viewBody)
      spark.table("mv_sa_8").count() shouldBe 2L

      sql("DELETE FROM sa_pay_8 WHERE from_uid = 2")
      refreshMv("mv_sa_8")
      assertMvCorrect("mv_sa_8", viewBody)
      spark.table("mv_sa_8").count() shouldBe 1L
    }
  }

  // ── Test 9: Multiple matches per left tuple ──────────────────────────────
  // Removing ONE of N matches must NOT drop the left tuple — only when the
  // last match is removed should the row flip invisible.

  describe("(9) Multiple matches per left tuple — partial vs full retraction") {
    it("partial RIGHT-side DELETE keeps the row; final DELETE drops it") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_9(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_9(pid INT, from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_9 VALUES (1, 'Zeus'), (2, 'Hera')")
      // uid=1 has 3 payments, uid=2 has 1 payment
      sql(
        "INSERT INTO sa_pay_9 VALUES (10, 1, 100), (11, 1, 200), (12, 1, 300), (13, 2, 50)"
      )

      val viewBody =
        "SELECT g.* FROM sa_gods_9 g LEFT SEMI JOIN sa_pay_9 p ON g.uid = p.from_uid"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_9 AS $viewBody")
      mvRefreshType("mv_sa_9") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_9", viewBody)
      spark.table("mv_sa_9").count() shouldBe 2L

      // Remove one of three matches for uid=1 — still visible.
      sql("DELETE FROM sa_pay_9 WHERE pid = 10")
      refreshMv("mv_sa_9")
      assertMvCorrect("mv_sa_9", viewBody)
      spark.table("mv_sa_9").count() shouldBe 2L

      // Remove the remaining two — uid=1 must disappear.
      sql("DELETE FROM sa_pay_9 WHERE pid IN (11, 12)")
      refreshMv("mv_sa_9")
      assertMvCorrect("mv_sa_9", viewBody)
      spark.table("mv_sa_9").count() shouldBe 1L
    }
  }

  // ── Test 10: Batched DML on LEFT and RIGHT → single REFRESH ──────────────
  // Mirrors openivm/test/sql/semi_anti_join.test "batch conflicting changes
  // before one refresh".  Stresses delta consolidation: a single REFRESH
  // applies LEFT inserts, LEFT deletes, RIGHT inserts, and RIGHT deletes.

  describe("(10) Batched LEFT+RIGHT DML mix → single REFRESH") {
    it("one refresh reconciles inserts and deletes on both sides") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_10(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_10(pid INT, from_uid INT, amt INT) USING DELTA")
      sql(
        "INSERT INTO sa_gods_10 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares'), (4, 'Apollo')"
      )
      // uid=1, uid=4 have payments; uid=2, uid=3 don't.
      sql("INSERT INTO sa_pay_10 VALUES (100, 1, 50), (101, 4, 75)")

      val viewBody =
        "SELECT g.* FROM sa_gods_10 g LEFT SEMI JOIN sa_pay_10 p ON g.uid = p.from_uid"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_10 AS $viewBody")
      mvRefreshType("mv_sa_10") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_10", viewBody)

      // Batch of mixed DML across LEFT and RIGHT, all consumed by ONE refresh.
      sql("INSERT INTO sa_pay_10 VALUES (102, 2, 10)")   // RIGHT insert: Hera flips ON
      sql("DELETE FROM sa_pay_10 WHERE pid = 101")       // RIGHT delete: Apollo flips OFF
      sql("INSERT INTO sa_gods_10 VALUES (5, 'Hermes')") // LEFT insert, no match
      sql("INSERT INTO sa_pay_10 VALUES (103, 5, 1)")    // RIGHT insert: Hermes flips ON
      sql("DELETE FROM sa_gods_10 WHERE uid = 1")        // LEFT delete: Zeus removed
      refreshMv("mv_sa_10")
      assertMvCorrect("mv_sa_10", viewBody)
    }
  }

  // ── Test 11: NULL in RIGHT poisons NOT IN ────────────────────────────────
  // Standard SQL: if the RIGHT-side of NOT IN contains a NULL, the predicate
  // is UNKNOWN for every LEFT row → no rows are returned.  This is a classic
  // SQL gotcha; the test pins the MV's behaviour to the base query's
  // behaviour, regardless of refresh strategy.

  describe("(11) NULL in RIGHT poisons NOT IN — MV is empty") {
    it("inserting a NULL into the right side empties the MV after refresh") {
      sql("CREATE TABLE IF NOT EXISTS sa_gods_11(uid INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS sa_pay_11(from_uid INT, amt INT) USING DELTA")
      sql("INSERT INTO sa_gods_11 VALUES (1, 'Zeus'), (2, 'Hera'), (3, 'Ares')")
      sql("INSERT INTO sa_pay_11 VALUES (1, 100)")

      val viewBody =
        "SELECT * FROM sa_gods_11 WHERE uid NOT IN (SELECT from_uid FROM sa_pay_11)"
      sql(s"CREATE MATERIALIZED VIEW mv_sa_11 AS $viewBody")
      mvRefreshType("mv_sa_11") shouldBe RefreshTypeCode.FullRefresh
      assertMvCorrect("mv_sa_11", viewBody)
      spark.table("mv_sa_11").count() shouldBe 2L

      // Inserting a NULL into the right poisons every comparison.
      sql("INSERT INTO sa_pay_11 VALUES (CAST(NULL AS INT), 0)")
      refreshMv("mv_sa_11")
      assertMvCorrect("mv_sa_11", viewBody)
      spark.table("mv_sa_11").count() shouldBe 0L
    }
  }
}
