package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Port of `openivm/test/sql/update_computed.test`.
  *
  * Exercises UPDATEs whose SET clause uses computed expressions referencing
  * the column being updated (`val = val + 5`, `price = price - 10.0`,
  * `val = val * 2`, …) and asserts they propagate correctly through both
  * SIMPLE_PROJECTION (full table image) and predicate-filtered views.
  *
  * The openivm test exercises:
  *
  *   1. `col + constant`
  *   2. `col - constant`
  *   3. `col * constant`
  *   4. Multi-column computed UPDATE in a single statement
  *      (`SET val = val + 1, price = price - 10.0`)
  *   5. Computed UPDATE on every row (no WHERE clause)
  *   6. Mandatory stress: computed UPDATE + constant UPDATE + DELETE + INSERT
  *      batched into a single REFRESH
  *   7. Computed UPDATE that moves a row across a filter boundary (in or out
  *      of the predicate)
  *
  * Per CLAUDE.md, every refresh assertion uses bidirectional `EXCEPT ALL`;
  * the stress test in shape (6) batches conflicting DML.
  *
  * Source: `.temp/openivm/test/sql/update_computed.test`.
  */
abstract class UpdateComputedScenarios extends IvmParitySpecBase("update-computed") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1)-(5) Computed UPDATEs walked through on a SIMPLE_PROJECTION view
  //     mirroring update_computed.test:7-149
  //     Spark's DECIMAL default precision differs from DuckDB's, so we declare
  //     `price DECIMAL(10,3)` to match the test's printed value `90.000`.
  //
  //     Extracted to [[UpdateComputedHeavySetVariantsSpec]] (~4m01 wall) so it
  //     runs in its own forked JVM and does not bottleneck the rest of this
  //     spec.
  // ============================================================================

  // ============================================================================
  // (6) Mandatory stress test (update_computed.test:151-181)
  //     INSERT + computed UPDATE + constant UPDATE + DELETE batched → one REFRESH
  // ============================================================================
  describe("(6) Stress: computed UPDATE + constant UPDATE + DELETE + INSERT → single REFRESH") {
    it("MV is bag-equal to view body after a single refresh covering all four DML kinds") {
      sql(
        "CREATE TABLE IF NOT EXISTS comp_upd_stress(" +
          "  id INT, val INT, price DECIMAL(10,3), label STRING" +
          ") USING DELTA"
      )
      sql(
        "INSERT INTO comp_upd_stress VALUES " +
          "(1, 10, 100.0, 'alpha'), (2, 20, 200.0, 'beta'), (3, 30, 300.0, 'gamma')"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_comp_upd_stress AS " +
          "SELECT id, val, price, label FROM comp_upd_stress"
      )
      val expected = "SELECT id, val, price, label FROM comp_upd_stress"

      // Conflicting batch — exercises delta consolidation
      sql("INSERT INTO comp_upd_stress VALUES (4, 40, 400.0, 'delta')")
      sql("UPDATE comp_upd_stress SET val = val - 50 WHERE id = 1") // computed UPDATE
      sql("UPDATE comp_upd_stress SET label = 'BETA' WHERE id = 2") // constant UPDATE
      sql("DELETE FROM comp_upd_stress WHERE id = 3")
      refreshMv("mv_comp_upd_stress")
      assertMvCorrect("mv_comp_upd_stress", expected)
    }
  }

  // ============================================================================
  // (7) Computed UPDATE moves rows across a filter boundary
  //     (update_computed.test:183-238)
  // ============================================================================
  describe("(7) Computed UPDATE crosses the filter boundary in both directions") {
    it("row entering the predicate appears in MV; row leaving the predicate disappears") {
      sql("CREATE TABLE IF NOT EXISTS filt_upd(id INT, val INT) USING DELTA")
      sql("INSERT INTO filt_upd VALUES (1, 10), (2, 50), (3, 100)")
      sql("CREATE MATERIALIZED VIEW mv_filt_upd AS SELECT id, val FROM filt_upd WHERE val > 40")
      val expected = "SELECT id, val FROM filt_upd WHERE val > 40"

      // Initial: ids {2,3} (val > 40)
      assertMvCorrect("mv_filt_upd", expected)

      // (7a) UPDATE id=1: val = 10 + 35 = 45 → enters MV
      sql("UPDATE filt_upd SET val = val + 35 WHERE id = 1")
      refreshMv("mv_filt_upd")
      assertMvCorrect("mv_filt_upd", expected)

      // (7b) UPDATE id=2: val = 50 - 20 = 30 → leaves MV
      sql("UPDATE filt_upd SET val = val - 20 WHERE id = 2")
      refreshMv("mv_filt_upd")
      assertMvCorrect("mv_filt_upd", expected)
    }
  }
}
