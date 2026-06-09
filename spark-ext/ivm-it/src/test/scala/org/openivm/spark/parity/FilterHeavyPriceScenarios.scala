package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy-test isolation spin-off of [[FilterSpec]] section (1).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `fhp_*` so two parallel JVMs
  * (this one and the host `FilterSpec`) cannot collide on Delta paths.
  */
abstract class FilterHeavyPriceScenarios extends IvmParitySpecBase("filter-heavy-price") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  describe("(1) WHERE price < 20: INSERT/DELETE/boundary/mixed batch") {
    it("incrementally maintains the cheap_products MV across all DML shapes") {
      sql("CREATE TABLE IF NOT EXISTS fhp_products(id INT, name STRING, price INT, category STRING) USING DELTA")
      sql(
        "INSERT INTO fhp_products VALUES " +
          "(1, 'Widget', 10, 'A'), (2, 'Gadget', 25, 'B'), (3, 'Doohickey', 5, 'A')"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_fhp_cheap_products AS " +
          "SELECT id, name, price FROM fhp_products WHERE price < 20"
      )

      // Initial state: ids {1,3} present
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Insert matching filter
      sql("INSERT INTO fhp_products VALUES (4, 'Thingamajig', 8, 'A')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Insert NOT matching filter (price=50 >= 20)
      sql("INSERT INTO fhp_products VALUES (5, 'Expensive', 50, 'B')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Delete row that was in the view
      sql("DELETE FROM fhp_products WHERE id = 1")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // No-op refresh (no DML since last refresh)
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Insert at boundary: price=19 matches (strict <), price=20 fails
      sql("INSERT INTO fhp_products VALUES (6, 'Boundary', 19, 'C')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      sql("INSERT INTO fhp_products VALUES (7, 'TooExpensive', 20, 'C')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Mixed INSERT + DELETE in one batch
      sql("INSERT INTO fhp_products VALUES (8, 'Cheap', 1, 'A')")
      sql("DELETE FROM fhp_products WHERE id = 3")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Delete row NOT in MV (id=5 is filtered out) — MV unchanged
      sql("DELETE FROM fhp_products WHERE id = 5")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")
    }
  }
}
