package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_union.test`.
  *
  * UNION ALL under aggregation: per `openivm/CLAUDE.md` rule table,
  * UNION ALL routes through `IncrementalUnionRule` in
  * `src/rules/union.cpp`.  On DuckLake, the snapshot-diff source delta is
  * unioned across both sources and the aggregate-group rule applies.  On
  * Spark+Delta the same shape is reached via the AGGREGATE_GROUP path; the
  * source-side delta is detected via Delta versions (the Delta-equivalent
  * invariant).
  *
  * Sections mirror the source test:
  *   Basic UNION ALL — SUM/COUNT GROUP BY product across two sources;
  *     verify after CREATE and after INSERT into one source.
  *   Stress — batch INSERT into both sources, DELETE from one source,
  *     single refresh.
  */
abstract class DucklakeUnionScenarios extends IvmParitySpecBase("ducklake-union") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── Basic UNION ALL with SUM/COUNT aggregate ──────────────────────────────

  describe("Basic UNION ALL: SUM/COUNT GROUP BY product across two source tables") {
    it("INSERT into one source propagates through the UNION ALL to the aggregate") {
      sql(
        "CREATE TABLE IF NOT EXISTS dlu_sales_online(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS dlu_sales_store(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      sql(
        "INSERT INTO dlu_sales_online VALUES (1, 'Widget', 10.00), (2, 'Gadget', 20.00)"
      )
      sql(
        "INSERT INTO dlu_sales_store VALUES (1, 'Widget', 15.00), (2, 'Gizmo', 30.00)"
      )

      val viewSql =
        "SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM (" +
          "SELECT product, amount FROM dlu_sales_online " +
          "UNION ALL " +
          "SELECT product, amount FROM dlu_sales_store) GROUP BY product"
      sql(s"CREATE MATERIALIZED VIEW dlu_mv_all_sales AS $viewSql")
      refreshMv("dlu_mv_all_sales")
      assertMvCorrect("dlu_mv_all_sales", viewSql)

      // Insert into one source only — Widget total should bump
      sql("INSERT INTO dlu_sales_online VALUES (3, 'Widget', 5.00)")
      refreshMv("dlu_mv_all_sales")
      assertMvCorrect("dlu_mv_all_sales", viewSql)
    }
  }

  // ── Stress: batch ops on both sources ──────────────────────────────────────

  describe("Stress: batch INSERT/DELETE on both UNION sources before one refresh") {
    it("delta consolidation across both UNION arms is correct") {
      sql(
        "CREATE TABLE IF NOT EXISTS dlu_stress_online(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS dlu_stress_store(id INT, product STRING, amount DECIMAL(10,2)) USING DELTA"
      )
      sql(
        "INSERT INTO dlu_stress_online VALUES (1, 'Widget', 10.00), (2, 'Gadget', 20.00)"
      )
      sql(
        "INSERT INTO dlu_stress_store VALUES (1, 'Widget', 15.00), (2, 'Gizmo', 30.00)"
      )
      val viewSql =
        "SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM (" +
          "SELECT product, amount FROM dlu_stress_online " +
          "UNION ALL " +
          "SELECT product, amount FROM dlu_stress_store) GROUP BY product"
      sql(s"CREATE MATERIALIZED VIEW dlu_mv_stress AS $viewSql")
      refreshMv("dlu_mv_stress")
      assertMvCorrect("dlu_mv_stress", viewSql)

      // Batch on both sources.
      sql("INSERT INTO dlu_stress_store VALUES (3, 'Gadget', 40.00)")
      sql("DELETE FROM dlu_stress_online WHERE id = 2")
      sql("INSERT INTO dlu_stress_online VALUES (4, 'Gizmo', 12.00)")
      refreshMv("dlu_mv_stress")
      assertMvCorrect("dlu_mv_stress", viewSql)
    }
  }
}
