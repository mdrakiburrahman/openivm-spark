package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split from the original parity spec.  Scope:
  * Depth-≤2 chained-MV scenarios driven by INSERTs and the depth>2 pending placeholder from `chained.test`.
  *
  * Includes sections: (A), (B), (K).
  */
abstract class ChainedDepth2Scenarios extends IvmParitySpecBase("chained-depth2") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Issue refreshes in dependency order: every name in `mvs` is refreshed
    * after the ones before it. */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (A) Two-level chain: table → MV1 (group-agg) → MV2 (simple-agg)
  // chained.test L11–L76
  // ──────────────────────────────────────────────────────────────────────────

  describe("(A) Two-level chain: sales → sales_by_region → grand_total") {

    it("propagates the first batch of INSERTs through both MVs") {
      sql("CREATE TABLE IF NOT EXISTS chd_sales(region STRING, product STRING, amount INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW chd_sales_by_region AS " +
          "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region"
      )
      sql(
        "CREATE MATERIALIZED VIEW chd_grand_total AS " +
          "SELECT SUM(total) AS grand_total FROM chd_sales_by_region"
      )
      sql("INSERT INTO chd_sales VALUES ('US','A',100),('US','B',200),('EU','A',300)")
      refreshChain("chd_sales_by_region", "chd_grand_total")
      assertMvCorrect(
        "chd_sales_by_region",
        "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region"
      )
      assertMvCorrect(
        "chd_grand_total",
        "SELECT SUM(total) AS grand_total FROM (" +
          "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region) t"
      )
    }

    it("propagates the second batch of INSERTs through both MVs") {
      sql("INSERT INTO chd_sales VALUES ('EU','B',400),('US','C',500)")
      refreshChain("chd_sales_by_region", "chd_grand_total")
      assertMvCorrect(
        "chd_sales_by_region",
        "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region"
      )
      assertMvCorrect(
        "chd_grand_total",
        "SELECT SUM(total) AS grand_total FROM (" +
          "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region) t"
      )
    }

    it("propagates a brand-new group (JP) through both MVs") {
      sql("INSERT INTO chd_sales VALUES ('JP','D',1000)")
      refreshChain("chd_sales_by_region", "chd_grand_total")
      assertMvCorrect(
        "chd_sales_by_region",
        "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region"
      )
      assertMvCorrect(
        "chd_grand_total",
        "SELECT SUM(total) AS grand_total FROM (" +
          "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region) t"
      )
    }

    it("idempotent: double refresh is a no-op") {
      refreshChain("chd_sales_by_region", "chd_grand_total")
      refreshChain("chd_sales_by_region", "chd_grand_total")
      assertMvCorrect(
        "chd_grand_total",
        "SELECT SUM(total) AS grand_total FROM (" +
          "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region) t"
      )
    }

    it("empty insert (zero rows) is a safe no-op refresh") {
      sql("INSERT INTO chd_sales SELECT * FROM chd_sales WHERE 1=0")
      refreshChain("chd_sales_by_region", "chd_grand_total")
      assertMvCorrect(
        "chd_grand_total",
        "SELECT SUM(total) AS grand_total FROM (" +
          "SELECT region, SUM(amount) AS total FROM chd_sales GROUP BY region) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (B) Two-level chain: COUNT(*) over a group-agg MV
  // chained.test L170–L261 (orders_by_customer → customer_count) — depth-2 slice
  // The COUNT(*) chain exercises openivm's "virtual column" handling for the
  // ivm_helpers.cpp count fix.  The sibling `total_orders` view in chained.test
  // is also a depth-2 chain over the same MV1 — both are kept here as they
  // share the same source MV.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(B) Two-level chain: orders → orders_by_customer → {customer_count, total_orders}") {

    it("initial-batch refresh: SUM and COUNT(*) chains both stay consistent") {
      sql("CREATE TABLE IF NOT EXISTS chd_orders(customer STRING, category STRING, qty INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW chd_orders_by_customer AS " +
          "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer"
      )
      sql(
        "CREATE MATERIALIZED VIEW chd_total_orders AS " +
          "SELECT SUM(total_qty) AS all_orders FROM chd_orders_by_customer"
      )
      sql(
        "CREATE MATERIALIZED VIEW chd_customer_count AS " +
          "SELECT COUNT(*) AS num_customers FROM chd_orders_by_customer"
      )
      sql(
        "INSERT INTO chd_orders VALUES " +
          "('alice','books',3),('bob','books',5),('alice','games',2),('charlie','books',1)"
      )
      // Refresh in dependency order: MV1 first, then the two downstream MVs.
      refreshChain("chd_orders_by_customer", "chd_total_orders", "chd_customer_count")
      assertMvCorrect(
        "chd_orders_by_customer",
        "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer"
      )
      assertMvCorrect(
        "chd_total_orders",
        "SELECT SUM(total_qty) AS all_orders FROM (" +
          "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer) t"
      )
      assertMvCorrect(
        "chd_customer_count",
        "SELECT COUNT(*) AS num_customers FROM (" +
          "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer) t"
      )
    }

    it("second batch (new customer dave + repeat alice) propagates through both downstreams") {
      sql("INSERT INTO chd_orders VALUES ('dave','games',7),('alice','books',4)")
      refreshChain("chd_orders_by_customer", "chd_total_orders", "chd_customer_count")
      assertMvCorrect(
        "chd_orders_by_customer",
        "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer"
      )
      assertMvCorrect(
        "chd_total_orders",
        "SELECT SUM(total_qty) AS all_orders FROM (" +
          "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer) t"
      )
      assertMvCorrect(
        "chd_customer_count",
        "SELECT COUNT(*) AS num_customers FROM (" +
          "SELECT customer, SUM(qty) AS total_qty FROM chd_orders GROUP BY customer) t"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (K) Out-of-scope per PLAN.md §12: MV-over-MV cascade depth > 2
  //
  // openivm's `chained.test` includes a four-level chain
  //     transactions → store_dept_totals → store_totals → chain_grand_total
  //     (chained.test L452–L550)
  // which exercises an MV depending on an MV depending on an MV.  Per PLAN
  // §11 Risk #5 / §12, openivm-spark's MVP enforces depth ≤ 2.  The current
  // `RefreshMaterializedViewCommand` has no topo-sort, no cycle check, and
  // no automatic re-staging of the writes its MERGE emits into the
  // intermediate MV, so a manual three-step `REFRESH MV1; REFRESH MV2;
  // REFRESH MV3` would either no-op on the leaf (empty staging for MV2) or
  // observe stale data.  The cases are kept as `pending` so they show up in
  // the test inventory and can be promoted once depth-3 support is added.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(K) Out of scope: depth-3+ MV-over-MV chain (PLAN.md §12, Risk #5)") {

    it(
      "three-level chain transactions → store_dept_totals → store_totals → chain_grand_total " +
        "— out of scope: MV-over-MV depth > 2 per PLAN §12"
    ) {
      pending
    }
  }
}
