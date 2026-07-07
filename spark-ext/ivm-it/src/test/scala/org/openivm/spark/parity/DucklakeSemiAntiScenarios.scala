package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** ScalaTest port of `openivm/test/sql/ducklake_semi_anti.test`.
  *
  * In openivm the DuckLake SEMI/ANTI shape with `WHERE col IN (subq)`
  * classifies as `SEMI_ANTI_RECOMPUTE` (type 9) because the parser registers
  * an aux-meta row at CREATE time.  On openivm-spark, per
  * `SemiAntiSpec.scala` documentation, the compile-only bridge does not yet
  * persist that aux-meta and the view is demoted to `FULL_REFRESH` (rt3).
  * The MV is still correct because every refresh re-executes the live view
  * body over the current source snapshot — exactly the Delta-equivalent
  * invariant called out in PLAN.md §9.
  *
  * This spec mirrors the single section of `ducklake_semi_anti.test`:
  *   - 2-table SEMI shape with `WHERE c_id IN (subq)`
  *   - Multi-op batch: INSERT customer, INSERT orders, UPDATE order amount
  *     across the predicate threshold, UPDATE last_name (predicate-neutral),
  *     DELETE order, DELETE customer.
  *   - Bidirectional EXCEPT ALL verifies bag equality after a single refresh.
  */
abstract class DucklakeSemiAntiScenarios extends IvmParitySpecBase("ducklake-semi-anti") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── 2-table SEMI shape — IN (subquery with WHERE predicate) ───────────────

  describe("DuckLake SEMI-style view: WHERE c_id IN (SELECT o_c_id FROM orders WHERE o_ol_cnt > 3)") {
    it("multi-op batch (INSERT/UPDATE/DELETE on both sides) refreshes to a bag-equal MV") {
      sql(
        "CREATE TABLE IF NOT EXISTS dlsa_customer(c_id INT, c_w_id INT, c_last STRING) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS dlsa_order(o_id INT, o_c_id INT, o_ol_cnt INT) USING DELTA"
      )
      sql(
        "INSERT INTO dlsa_customer VALUES (1, 1, 'alpha'), (2, 1, 'beta'), (3, 1, 'gamma')"
      )
      sql("INSERT INTO dlsa_order VALUES (10, 1, 5), (11, 2, 2)")

      val viewSql =
        "SELECT c.c_w_id, c.c_id, c.c_last " +
          "FROM dlsa_customer c " +
          "WHERE c.c_id IN (SELECT o.o_c_id FROM dlsa_order o WHERE o.o_ol_cnt > 3)"
      sql(s"CREATE MATERIALIZED VIEW mv_dlsa_in AS $viewSql")
      assertMvCorrect("mv_dlsa_in", viewSql)

      // Multi-op batch — covers every interesting transition for SEMI semantics:
      //   - INSERT new customer (id 4) that the new order makes match
      //   - INSERT new order (12) above threshold for customer 4 → match
      //   - INSERT new order (13) above threshold for customer 3 → match
      //   - UPDATE order 11 above threshold → customer 2 now matches
      //   - UPDATE customer 3 last name (predicate-neutral, projection-affecting)
      //   - DELETE order 10 → customer 1 stops matching
      //   - DELETE customer 1 → row drops anyway
      sql("INSERT INTO dlsa_customer VALUES (4, 1, 'delta')")
      sql("INSERT INTO dlsa_order VALUES (12, 4, 6), (13, 3, 8)")
      sql("UPDATE dlsa_order SET o_ol_cnt = 7 WHERE o_id = 11")
      sql("UPDATE dlsa_customer SET c_last = 'gamma-x' WHERE c_id = 3")
      sql("DELETE FROM dlsa_order WHERE o_id = 10")
      sql("DELETE FROM dlsa_customer WHERE c_id = 1")

      refreshMv("mv_dlsa_in")
      assertMvCorrect("mv_dlsa_in", viewSql)
    }
  }
}
