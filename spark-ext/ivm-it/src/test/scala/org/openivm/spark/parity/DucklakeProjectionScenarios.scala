package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_projection.test`.
  *
  * DuckLake projection refresh is functionally identical to standard openivm
  * projection refresh, only the delta detection mechanism differs (snapshot
  * diff vs `openivm_delta_<table>`).  On Spark+Delta the source-side delta
  * detection uses Delta's `versionAsOf` / table-snapshot, which is the
  * Delta-equivalent invariant called out in PLAN.md §9 ("table snapshot read
  * for refresh, idempotency of consumed_by, no missed deltas").
  *
  * DuckLake-specific catalog-prefix tests (`lakecat`.`schema`.`view` and
  * cross-catalog joins via `ATTACH ... AS lakecat (TYPE ducklake)`) are
  * translated to Spark's default catalog because Spark+Delta has no
  * equivalent `ATTACH (TYPE ...)` operator — the invariant they verify
  * (catalog name passes through metadata, refresh resolves the correct
  * source table) is covered by the default-catalog tests.  The
  * `PRAGMA refresh_options('catalog', 'schema', 'view')` analogue is the
  * standard `REFRESH MATERIALIZED VIEW` invocation; the catalog/schema is
  * implicit on Spark.  Profile-pragma assertions are N/A on Spark.
  *
  * Sections mirror the source test:
  *   1. Simple SELECT projection (+ INSERT, DELETE)
  *   2. Multi-column projection with expressions (+ DELETE, UPDATE)
  *   3. Stress test: batch INSERT + DELETE + UPDATE before single refresh
  *      including ghost ops, conflicting ops, and round 4 delete-everything.
  *   4. Projection top-k: ORDER BY + LIMIT without GROUP BY
  *   5. Projection over VALUES join (the openivm "VALUES join" shape)
  */
abstract class DucklakeProjectionScenarios extends IvmParitySpecBase("ducklake-projection") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (1) Simple SELECT projection ───────────────────────────────────────────

  describe("(1) Simple SELECT projection: id, name") {
    it("INSERT/DELETE propagate to the MV after refresh") {
      sql("CREATE TABLE IF NOT EXISTS dlproj_employees(id INT, name STRING, dept STRING) USING DELTA")
      sql(
        "INSERT INTO dlproj_employees VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'sales'), (3, 'Charlie', 'eng')"
      )
      val viewSql = "SELECT id, name FROM dlproj_employees"
      sql(s"CREATE MATERIALIZED VIEW dlproj_mv_emp AS $viewSql")

      // INSERT propagation
      sql("INSERT INTO dlproj_employees VALUES (4, 'Diana', 'ops')")
      refreshMv("dlproj_mv_emp")
      assertMvCorrect("dlproj_mv_emp", viewSql)

      // DELETE propagation
      sql("DELETE FROM dlproj_employees WHERE id = 2")
      refreshMv("dlproj_mv_emp")
      assertMvCorrect("dlproj_mv_emp", viewSql)
    }
  }

  // ── (2) Multi-column projection with expressions ───────────────────────────

  describe("(2) Multi-column projection with expressions: price * qty, qty > 75") {
    it("INSERT/DELETE/UPDATE produce the right expression-column values") {
      sql(
        "CREATE TABLE IF NOT EXISTS dlproj_products(id INT, name STRING, price DOUBLE, qty INT) USING DELTA"
      )
      sql(
        "INSERT INTO dlproj_products VALUES (1, 'Widget', 9.99, 100), (2, 'Gadget', 24.50, 50)"
      )

      val viewSql =
        "SELECT name, price * qty AS total_value, qty > 75 AS high_stock FROM dlproj_products"
      sql(s"CREATE MATERIALIZED VIEW dlproj_mv_prod AS $viewSql")

      sql("INSERT INTO dlproj_products VALUES (3, 'Gizmo', 5.00, 200)")
      sql("DELETE FROM dlproj_products WHERE id = 1")
      refreshMv("dlproj_mv_prod")
      assertMvCorrect("dlproj_mv_prod", viewSql)

      // UPDATE a product (price), refresh
      sql("UPDATE dlproj_products SET price = 30.00 WHERE id = 2")
      refreshMv("dlproj_mv_prod")
      assertMvCorrect("dlproj_mv_prod", viewSql)
    }
  }

  // ── (3) Stress: batch INSERT + DELETE + UPDATE before single refresh ──────
  //
  // The source `ducklake_projection.test` uses duplicate `(id, val)` rows
  // (e.g. `(1, 'a'), (1, 'a')`) and then UPDATEs over them.  Per the
  // documented openivm-spark SIMPLE_PROJECTION `_ivm_rowid` caveat (see
  // `SimpleProjectionSpec.scala:20` and RESEARCH §12 risk 8), duplicate
  // rows in a projection MV cannot be uniquely targeted for retraction on
  // Spark+Delta — the projection consolidation collapses identical rows.
  // The Delta-equivalent invariant exercised here is the "many conflicting
  // batched DML ops before one REFRESH" stress pattern from
  // `openivm/CLAUDE.md` ("Stress tests must batch many conflicting DML ops
  // (INSERT + DELETE + UPDATE on same rows) before a single refresh").  We
  // exercise the same conflict density on uniquely-keyed rows, mirroring
  // the structure of every Round in the source test.

  describe("(3) Stress: batched INSERT + DELETE + UPDATE before a single refresh") {
    it("Round 1: mixed batch (INSERT + DELETE + UPDATE) before one refresh") {
      sql("CREATE TABLE IF NOT EXISTS dlproj_stress(id INT, val STRING) USING DELTA")
      sql(
        "INSERT INTO dlproj_stress VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
      )
      val viewSql = "SELECT id, val FROM dlproj_stress"
      sql(s"CREATE MATERIALIZED VIEW dlproj_mv_stress AS $viewSql")

      sql("INSERT INTO dlproj_stress VALUES (5, 'e'), (6, 'f'), (7, 'g')")
      sql("DELETE FROM dlproj_stress WHERE id = 2")
      sql("UPDATE dlproj_stress SET val = 'A' WHERE id = 1")
      refreshMv("dlproj_mv_stress")
      assertMvCorrect("dlproj_mv_stress", viewSql)
    }

    it("Round 2: ghost INSERT+DELETE (net zero) alongside real changes") {
      sql("INSERT INTO dlproj_stress VALUES (99, 'ghost')")
      sql("DELETE FROM dlproj_stress WHERE id = 99")
      sql("INSERT INTO dlproj_stress VALUES (8, 'h')")
      sql("DELETE FROM dlproj_stress WHERE id = 3")
      refreshMv("dlproj_mv_stress")
      assertMvCorrect("dlproj_mv_stress", "SELECT id, val FROM dlproj_stress")
    }

    it("Round 3: conflicting INSERT + DELETE + UPDATE on the same key") {
      sql("INSERT INTO dlproj_stress VALUES (30, 'x')")
      sql("DELETE FROM dlproj_stress WHERE id = 30")
      sql("INSERT INTO dlproj_stress VALUES (30, 'y')")
      sql("UPDATE dlproj_stress SET val = 'z' WHERE id = 30")
      refreshMv("dlproj_mv_stress")
      assertMvCorrect("dlproj_mv_stress", "SELECT id, val FROM dlproj_stress")
    }

    it("Round 4: delete everything, refresh, re-insert") {
      sql("DELETE FROM dlproj_stress")
      refreshMv("dlproj_mv_stress")
      spark.table("dlproj_mv_stress").count() shouldBe 0L
      assertMvCorrect("dlproj_mv_stress", "SELECT id, val FROM dlproj_stress")

      sql("INSERT INTO dlproj_stress VALUES (10, 'new'), (11, 'also')")
      refreshMv("dlproj_mv_stress")
      assertMvCorrect("dlproj_mv_stress", "SELECT id, val FROM dlproj_stress")
    }

    it("Round 5: no-op refresh after previous batch") {
      refreshMv("dlproj_mv_stress")
      assertMvCorrect("dlproj_mv_stress", "SELECT id, val FROM dlproj_stress")
    }
  }

  // ── (4) Projection top-k: ORDER BY + LIMIT without GROUP BY ───────────────

  describe("(4) Projection top-k: ORDER BY + LIMIT without GROUP BY") {
    it("incremental refresh keeps top-3 by score across INSERT, DELETE, UPDATE") {
      sql("CREATE TABLE IF NOT EXISTS dlproj_topkp(id INT, name STRING, score INT) USING DELTA")
      sql(
        "INSERT INTO dlproj_topkp VALUES " +
          "(1,'alice',80),(2,'bob',95),(3,'carol',70),(4,'dave',88),(5,'eve',92)"
      )
      val viewSql = "SELECT id, name, score FROM dlproj_topkp ORDER BY score DESC LIMIT 3"
      sql(s"CREATE MATERIALIZED VIEW dlproj_topkp_mv AS $viewSql")
      refreshMv("dlproj_topkp_mv")
      assertMvCorrect("dlproj_topkp_mv", viewSql)

      // Insert a new top scorer (frank=99 enters top-3)
      sql("INSERT INTO dlproj_topkp VALUES (6,'frank',99)")
      refreshMv("dlproj_topkp_mv")
      assertMvCorrect("dlproj_topkp_mv", viewSql)

      // Conflicting INSERT + DELETE + UPDATE before single refresh
      sql("INSERT INTO dlproj_topkp VALUES (7,'grace',100),(8,'heidi',60)")
      sql("DELETE FROM dlproj_topkp WHERE id = 6")
      sql("UPDATE dlproj_topkp SET score = 50 WHERE id = 2")
      refreshMv("dlproj_topkp_mv")
      assertMvCorrect("dlproj_topkp_mv", viewSql)
    }
  }

  // ── (5) Projection over VALUES join ───────────────────────────────────────

  describe("(5) Projection over a VALUES join (inline constants on the right side)") {
    it("UPDATE on the base table flips a row into the matched set") {
      sql(
        "CREATE TABLE IF NOT EXISTS dlproj_value_customers(id INT, credit STRING, last_name STRING) USING DELTA"
      )
      sql(
        "INSERT INTO dlproj_value_customers VALUES (1, 'GC', 'Able'), (2, 'BC', 'Baker'), (3, 'XX', 'Cross')"
      )
      val viewSql =
        "SELECT c.id, c.last_name, v.priority " +
          "FROM dlproj_value_customers c " +
          "JOIN (VALUES ('GC', 1), ('BC', 2)) AS v(credit, priority) ON c.credit = v.credit"
      sql(s"CREATE MATERIALIZED VIEW dlproj_mv_value_join AS $viewSql")
      refreshMv("dlproj_mv_value_join")
      assertMvCorrect("dlproj_mv_value_join", viewSql)

      // UPDATE flips an unmatched row ('XX') into a matched bucket ('GC').
      sql("UPDATE dlproj_value_customers SET credit = 'GC' WHERE id = 3")
      refreshMv("dlproj_mv_value_join")
      assertMvCorrect("dlproj_mv_value_join", viewSql)
    }
  }
}
