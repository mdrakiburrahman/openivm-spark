package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_left_join.test`.
  *
  * LEFT JOIN aggregates on DuckLake use `openivm_left_join_merge=true`
  * (default) per `openivm/CLAUDE.md` ("LEFT JOIN aggregates use incremental
  * MERGE by default and can fall back to group-recompute").  The Delta
  * equivalent path on Spark is the same: openivm-spark dispatches the
  * compiled refresh SQL to a MERGE-or-recompute branch and the final MV
  * stays bag-equal to the live view body.
  *
  * Sections mirror the source test:
  *   Basic LEFT JOIN — INSERTs + cross-checks
  *   Stress: batch INSERT + DELETE on both sides before a single refresh
  */
abstract class DucklakeLeftJoinScenarios extends IvmParitySpecBase("ducklake-left-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── Basic LEFT JOIN with SUM + COUNT aggregate ─────────────────────────────

  describe("Basic LEFT JOIN: SUM + COUNT preserves NULL-extended rows") {
    it("incrementally maintains LEFT JOIN aggregate across INSERT into the right side") {
      sql("CREATE TABLE IF NOT EXISTS dllj_customers(id INT, name STRING) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS dllj_orders(id INT, cust_id INT, amount DECIMAL(10,2)) USING DELTA"
      )
      sql("INSERT INTO dllj_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      sql(
        "INSERT INTO dllj_orders VALUES (1, 1, 100.00), (2, 1, 50.00), (3, 2, 75.00)"
      )

      val viewSql =
        "SELECT c.name, SUM(o.amount) AS total, COUNT(o.amount) AS cnt " +
          "FROM dllj_customers c LEFT JOIN dllj_orders o ON c.id = o.cust_id " +
          "GROUP BY c.name"
      sql(s"CREATE MATERIALIZED VIEW dllj_mv_cust_orders AS $viewSql")
      refreshMv("dllj_mv_cust_orders")
      assertMvCorrect("dllj_mv_cust_orders", viewSql)

      // Insert order for customer with no orders — Carol's row flips from NULL/0 to amount/1.
      sql("INSERT INTO dllj_orders VALUES (4, 3, 200.00)")
      refreshMv("dllj_mv_cust_orders")
      assertMvCorrect("dllj_mv_cust_orders", viewSql)
    }
  }

  // ── Stress: batch INSERT + DELETE on both sides before single refresh ─────

  describe("Stress: batch INSERT + DELETE on both customer and order sides") {
    it("delta consolidation is correct under conflicting batched ops before one refresh") {
      sql("CREATE TABLE IF NOT EXISTS dllj_stress_c(id INT, name STRING) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS dllj_stress_o(id INT, cust_id INT, amount DECIMAL(10,2)) USING DELTA"
      )
      sql("INSERT INTO dllj_stress_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      sql(
        "INSERT INTO dllj_stress_o VALUES (1, 1, 100.00), (2, 1, 50.00), (3, 2, 75.00)"
      )

      val viewSql =
        "SELECT c.name, SUM(o.amount) AS total, COUNT(o.amount) AS cnt " +
          "FROM dllj_stress_c c LEFT JOIN dllj_stress_o o ON c.id = o.cust_id " +
          "GROUP BY c.name"
      sql(s"CREATE MATERIALIZED VIEW dllj_mv_stress AS $viewSql")
      refreshMv("dllj_mv_stress")
      assertMvCorrect("dllj_mv_stress", viewSql)

      // Add new customer + order, delete existing order, add another order.
      sql("INSERT INTO dllj_stress_c VALUES (4, 'Dave')")
      sql("INSERT INTO dllj_stress_o VALUES (5, 4, 300.00)")
      sql("DELETE FROM dllj_stress_o WHERE id = 2")
      sql("INSERT INTO dllj_stress_o VALUES (6, 1, 25.00)")

      refreshMv("dllj_mv_stress")
      assertMvCorrect("dllj_mv_stress", viewSql)
    }
  }
}
