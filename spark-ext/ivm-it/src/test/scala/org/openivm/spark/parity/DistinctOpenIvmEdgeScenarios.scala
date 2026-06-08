package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** 1:1 ScalaTest port of openivm `test/sql/distinct.test` — join / union /
  * mark-join / nested-aggregate sections (6), (7+8), (10) and (11).
  *
  * Split out of the original `DistinctOpenIvmSpec` so each file contributes
  * ≤ 10 `it(...)` cases per forked JVM. Covers:
  *
  *   6.   DISTINCT over 2-table INNER JOIN  (distinct.test:352‒422)
  *   7+8. UNION (implicit DISTINCT) shapes   (distinct.test:424‒524)
  *   10.  IN (SELECT DISTINCT …)             (distinct.test:595‒621)
  *   11.  Inner DISTINCT under outer SUM     (distinct.test:623‒711)
  *
  * Skipped (no Spark analogue, retained in upstream):
  *   12. `SET openivm_distinct_aux_state` + `openivm_views.type`
  *       (distinct.test:713‒777)
  *   13. `openivm_distinct_count_<view>` aux-state row inspection
  *       (distinct.test:779‒882)
  */
abstract class DistinctOpenIvmEdgeScenarios extends IvmParitySpecBase("distinct-open-ivm-edge") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ══════════════════════════════════════════════════════════════════════════
  // (6) DISTINCT on a 2-table INNER JOIN
  //     openivm distinct.test:352‒422  (dist_j_orders ⋈ dist_j_products)
  // ══════════════════════════════════════════════════════════════════════════

  describe("(6) DISTINCT over an INNER JOIN of two tables (dist_j_* / mv_dist_join)") {

    val ordersTable   = "oie_dist_j_orders"
    val productsTable = "oie_dist_j_products"
    val mvName        = "oie_mv_dist_join"
    val body =
      s"""SELECT DISTINCT p.category
         |FROM $ordersTable o INNER JOIN $productsTable p ON o.product = p.name""".stripMargin

    it("(6a) initial join — distinct categories = {X, Y}") {
      sql(s"CREATE TABLE IF NOT EXISTS $ordersTable(id INT, product STRING) USING DELTA")
      sql(s"CREATE TABLE IF NOT EXISTS $productsTable(name STRING, category STRING) USING DELTA")
      sql(s"INSERT INTO $productsTable VALUES ('A', 'X'), ('B', 'X'), ('C', 'Y')")
      sql(s"INSERT INTO $ordersTable VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'C')")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(6b) INSERT more orders for same categories — MV unchanged") {
      sql(s"INSERT INTO $ordersTable VALUES (5, 'B'), (6, 'A')")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(6c) DELETE all orders for category Y (product='C') — MV loses Y") {
      sql(s"DELETE FROM $ordersTable WHERE product = 'C'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // (7+8) UNION (implicit DISTINCT) over three sources, then a UNION view
  //       with a literal column forcing a projection above the DISTINCT.
  //       openivm distinct.test:424‒524  (u_dist_a/b/c / mv_union_distinct,
  //                                        mv_union_labeled)
  //
  //       Same describe block because mv_union_labeled reuses u_dist_a/b.
  // ══════════════════════════════════════════════════════════════════════════

  describe("(7+8) UNION DISTINCT regression — projection above DISTINCT (u_dist_* / mv_union_*)") {

    val aTable          = "oie_u_dist_a"
    val bTable          = "oie_u_dist_b"
    val cTable          = "oie_u_dist_c"
    val mvUnionDistinct = "oie_mv_union_distinct"
    val mvUnionLabeled  = "oie_mv_union_labeled"

    val unionBody =
      s"""SELECT id FROM $aTable
         |UNION DISTINCT SELECT id FROM $bTable
         |UNION DISTINCT SELECT id FROM $cTable""".stripMargin

    val labeledBody =
      s"""SELECT id, 'a' AS src FROM $aTable
         |UNION DISTINCT
         |SELECT id, 'b' AS src FROM $bTable""".stripMargin

    it("(7a) batched DML across all 3 sources — single REFRESH; cancelling DELETE/INSERT") {
      sql(s"CREATE TABLE IF NOT EXISTS $aTable(id INT) USING DELTA")
      sql(s"CREATE TABLE IF NOT EXISTS $bTable(id INT) USING DELTA")
      sql(s"CREATE TABLE IF NOT EXISTS $cTable(id INT) USING DELTA")
      sql(s"INSERT INTO $aTable VALUES (1), (2), (3)")
      sql(s"INSERT INTO $bTable VALUES (2), (3), (4)")
      sql(s"INSERT INTO $cTable VALUES (3), (4), (5)")
      sql(s"CREATE MATERIALIZED VIEW $mvUnionDistinct AS $unionBody")

      // id=1 is deleted from a and simultaneously inserted into c — net effect
      // on the UNION is zero because c already carries it.
      sql(s"INSERT INTO $aTable VALUES (10)")
      sql(s"INSERT INTO $bTable VALUES (5), (5)")
      sql(s"INSERT INTO $cTable VALUES (1)")
      sql(s"DELETE FROM $aTable WHERE id = 1")
      refreshMv(mvUnionDistinct)
      assertMvCorrect(mvUnionDistinct, unionBody)
    }

    it("(8a) UNION with a literal column ('a','b') — projection above DISTINCT") {
      sql(s"CREATE MATERIALIZED VIEW $mvUnionLabeled AS $labeledBody")
      sql(s"INSERT INTO $aTable VALUES (100)")
      sql(s"DELETE FROM $bTable WHERE id = 4")
      refreshMv(mvUnionLabeled)
      assertMvCorrect(mvUnionLabeled, labeledBody)
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // (10) IN (SELECT DISTINCT …) — MARK JOIN regression
  //      openivm distinct.test:595‒621  (dist_mark / mv_dist_mark)
  //
  //      DuckDB:  generate_series(1, 10) t(n)
  //      Spark :  SELECT id AS n FROM range(1, 11)   (range is half-open)
  //
  //      TODO: The MV body uses a TVF (range / generate_series) rather than a
  //      tracked base table. OpenIVM compile (which runs the body through an
  //      internal DuckDB binder before classification) chokes on Spark's
  //      `range` TVF — DuckDB's own `range(start, end)` exposes a column
  //      called `range`, not `id`, so `SELECT id AS n FROM range(1, 11)`
  //      fails to bind:
  //          Binder Error: Referenced column "id" not found in FROM clause!
  //          Candidate bindings: "range"
  //      Marked `ignore` until the compile pipeline either recognises the
  //      Spark `range` TVF schema (`id BIGINT`) or accepts an opaque
  //      TVF-as-source.
  // ══════════════════════════════════════════════════════════════════════════

  describe("(10) IN (SELECT DISTINCT …) MARK-JOIN binding (dist_mark / mv_dist_mark)") {

    val baseTable = "oie_dist_mark"
    val mvName    = "oie_mv_dist_mark"
    val body =
      s"""SELECT n FROM (SELECT id AS n FROM range(1, 11)) t
         |WHERE n IN (SELECT DISTINCT w_id FROM $baseTable)""".stripMargin

    // The MV reads from the `range` TVF — not a tracked base table — and the
    // openivm DuckDB-based compile pipeline rejects it. See the TODO above.
    ignore("(10a) MV body uses a TVF and a MARK JOIN — initial check") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(w_id INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES (1), (2), (3), (3), (5)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // (11) Inner DISTINCT under an outer aggregate — regression for the
  //      double-count bug where IncrementalDistinctRule turned the inner
  //      DISTINCT into GROUP BY all_cols COUNT(*), causing the parent SUM
  //      to count duplicates. openivm now routes such views to
  //      GROUP_RECOMPUTE (RefreshType 6).
  //      openivm distinct.test:623‒711  (idist_u / idist_mv)
  // ══════════════════════════════════════════════════════════════════════════

  describe("(11) Inner DISTINCT under outer SUM (idist_u / idist_mv) → GROUP_RECOMPUTE shape") {

    val baseTable = "oie_idist_u"
    val mvName    = "oie_idist_mv"
    val body =
      s"""SELECT grp, SUM(cores) AS total_cores
         |FROM (SELECT DISTINCT grp, machine, cores
         |      FROM $baseTable
         |      WHERE machine IS NOT NULL) d
         |GROUP BY grp""".stripMargin

    it("(11a) initial INSERT — duplicate (1,'m1',8) contributes once to SUM") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(grp INT, machine STRING, cores INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES (1, 'm1', 8), (1, 'm1', 8), (1, 'm2', 16), (2, 'm3', 4)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(11b) INSERT duplicate of existing distinct tuple — SUM must NOT change") {
      sql(s"INSERT INTO $baseTable VALUES (2, 'm3', 4)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(11c) batched mixed deltas (new tuples, dupes, NULL-filtered row)") {
      sql(s"INSERT INTO $baseTable VALUES (1, 'm4', 32), (1, 'm1', 8), (2, NULL, 999), (3, 'm5', 1)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }
  }
}
