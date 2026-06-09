package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy-test isolation spin-off of [[RecomputeSpec]] section (A).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `rcd_*` so two parallel JVMs
  * (this one and the host `RecomputeSpec`) cannot collide on Delta paths.
  */
abstract class RecomputeHeavyDistinctOverJoinScenarios extends IvmParitySpecBase("recompute-heavy-distinct-over-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ────────────────────────────────────────────────────────────────────────────
  // (A) COUNT(DISTINCT) — group-recompute
  //     openivm: recompute.test:7–158
  // ────────────────────────────────────────────────────────────────────────────

  describe("(A) COUNT(DISTINCT) over GROUP BY (warehouse, district)") {

    it("maintains unique_items / total_lines / total_amount across INSERT, INSERT-dup, DELETE, and batched stress") {
      sql(
        "CREATE TABLE IF NOT EXISTS rcd_items(" +
          "id INT, warehouse INT, district INT, item INT, amount DOUBLE) USING DELTA"
      )
      sql(
        "INSERT INTO rcd_items VALUES " +
          "(1, 1, 1, 10, 5.0), " +
          "(2, 1, 1, 20, 3.0), " +
          "(3, 1, 1, 10, 2.0), " +
          "(4, 1, 2, 30, 8.0), " +
          "(5, 2, 1, 40, 1.0)"
      )
      sql(
        "CREATE MATERIALIZED VIEW rcd_items_distinct AS " +
          "SELECT warehouse, district, " +
          "       COUNT(DISTINCT item) AS unique_items, " +
          "       COUNT(*) AS total_lines, " +
          "       SUM(amount) AS total_amount " +
          "FROM rcd_items GROUP BY warehouse, district"
      )

      val viewBody =
        "SELECT warehouse, district, " +
          "COUNT(DISTINCT item) AS unique_items, " +
          "COUNT(*) AS total_lines, " +
          "SUM(amount) AS total_amount " +
          "FROM rcd_items GROUP BY warehouse, district"

      // Initial state matches recompute.test:31–36
      assertMvCorrect("rcd_items_distinct", viewBody)

      // INSERT a new distinct item for an existing group (50 is distinct → unique_items++)
      sql("INSERT INTO rcd_items VALUES (6, 1, 1, 50, 4.0)")
      refreshMv("rcd_items_distinct")
      assertMvCorrect("rcd_items_distinct", viewBody)

      // INSERT a duplicate item for an existing group (10 already present → unique_items unchanged)
      sql("INSERT INTO rcd_items VALUES (7, 1, 1, 10, 9.0)")
      refreshMv("rcd_items_distinct")
      assertMvCorrect("rcd_items_distinct", viewBody)

      // DELETE a row (id=2 → item 20 disappears from group (1,1); unique_items--)
      sql("DELETE FROM rcd_items WHERE id = 2")
      refreshMv("rcd_items_distinct")
      assertMvCorrect("rcd_items_distinct", viewBody)

      // Stress: batched INSERT + DELETE + UPDATE across multiple groups, single refresh
      sql(
        "INSERT INTO rcd_items VALUES " +
          "(8, 1, 1, 60, 1.0), (9, 2, 1, 10, 7.0), (10, 3, 1, 99, 2.0)"
      )
      sql("DELETE FROM rcd_items WHERE id IN (3, 5)")
      sql("UPDATE rcd_items SET amount = 100.0 WHERE id = 4")
      refreshMv("rcd_items_distinct")
      assertMvCorrect("rcd_items_distinct", viewBody)
    }
  }
}
