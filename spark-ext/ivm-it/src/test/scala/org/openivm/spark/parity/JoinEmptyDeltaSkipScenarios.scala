package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Parity coverage for per-term empty-delta pruning.
  *
  * A 3-way star join normally emits 2^N-1 inclusion-exclusion delta terms. When
  * only the fact table changes, both dimensions are `delta_shape=UNCHANGED`;
  * any branch selecting `openivm_delta_<dim>` is result-invariant empty and may
  * be skipped. Correctness remains the bidirectional EXCEPT ALL oracle, and the
  * MV must stay on an incremental refresh type rather than regressing to
  * FULL_REFRESH.
  */
abstract class JoinEmptyDeltaSkipScenarios extends IvmParitySpecBase("join-empty-delta-skip") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  describe("3-way star join with unchanged dimensions") {
    it("refreshes fact-only loads incrementally and remains bag-equal to the base query") {
      sql("CREATE TABLE IF NOT EXISTS p14_dim_product(product_id INT, name STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS p14_dim_region(region_id INT, region_name STRING) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS p14_fact_sales(" +
          "sale_id INT, product_id INT, region_id INT, amount INT) USING DELTA"
      )

      sql("INSERT INTO p14_dim_product VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')")
      sql("INSERT INTO p14_dim_region VALUES (10, 'North'), (20, 'South')")
      sql(
        "INSERT INTO p14_fact_sales VALUES " +
          "(1, 1, 10, 100), (2, 2, 20, 200), (3, 1, 20, 150)"
      )

      sql(
        "CREATE MATERIALIZED VIEW p14_mv_star AS " +
          "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM p14_fact_sales f " +
          "JOIN p14_dim_product p ON f.product_id = p.product_id " +
          "JOIN p14_dim_region r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )

      mvRefreshType("p14_mv_star") should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("p14_mv_star", starQuery)

      sql(
        "INSERT INTO p14_fact_sales VALUES " +
          "(4, 2, 10, 80), (5, 3, 20, 25), (6, 1, 10, 5)"
      )
      refreshMv("p14_mv_star")

      mvRefreshType("p14_mv_star") should not be RefreshTypeCode.FullRefresh
      assertMvCorrect("p14_mv_star", starQuery)
    }
  }

  private def starQuery: String =
    "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
      "FROM p14_fact_sales f " +
      "JOIN p14_dim_product p ON f.product_id = p.product_id " +
      "JOIN p14_dim_region r ON f.region_id = r.region_id " +
      "GROUP BY p.name, r.region_name"
}
