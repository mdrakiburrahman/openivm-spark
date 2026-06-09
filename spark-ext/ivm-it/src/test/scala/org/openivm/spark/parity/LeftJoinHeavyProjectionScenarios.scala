package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy-test isolation spin-off of [[LeftJoinSpec]] section (1).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `ljh_*` / `ljh_mv_*` so two
  * parallel JVMs (this one and the host `LeftJoinSpec`) cannot collide on
  * Delta paths.
  */
abstract class LeftJoinHeavyProjectionScenarios extends IvmParitySpecBase("left-join-heavy-projection") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1) LEFT JOIN customers/orders — INSERT right, DELETE right, INSERT left,
  //     and a batched mixed-DML refresh.
  // ============================================================================
  describe("(1) LEFT JOIN customer_orders: nullable right side, full narrative") {
    it("incrementally maintains the LEFT JOIN projection through insert/delete/batch DML") {
      sql("CREATE TABLE IF NOT EXISTS ljh_customers(id INT, name STRING) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS ljh_orders(customer_id INT, product STRING, amount INT) USING DELTA"
      )
      sql("INSERT INTO ljh_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      sql("INSERT INTO ljh_orders VALUES (1, 'Widget', 100), (1, 'Gadget', 200)")

      val viewBody =
        "SELECT c.name, o.product, o.amount " +
          "FROM ljh_customers c LEFT JOIN ljh_orders o ON c.id = o.customer_id"

      sql(s"CREATE MATERIALIZED VIEW ljh_mv_customer_orders AS $viewBody")

      // Initial: Alice has 2 orders, Bob and Charlie have NULL
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Insert into right side: Bob gets an order (NULL row should be replaced)
      sql("INSERT INTO ljh_orders VALUES (2, 'Bolt', 50)")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Delete from right side: remove Alice's only remaining 'Widget' order
      // (she gets a NULL-extended row back if all orders are removed)
      sql("DELETE FROM ljh_orders WHERE customer_id = 1 AND product = 'Widget'")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Insert into left side (new customer with no orders → NULL extended)
      sql("INSERT INTO ljh_customers VALUES (4, 'Dave')")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)

      // Mixed batched DML: insert + delete in same refresh
      sql("INSERT INTO ljh_orders VALUES (3, 'Screw', 10), (4, 'Nail', 5)")
      sql("DELETE FROM ljh_orders WHERE customer_id = 2")
      refreshMv("ljh_mv_customer_orders")
      assertMvCorrect("ljh_mv_customer_orders", viewBody)
    }
  }
}
