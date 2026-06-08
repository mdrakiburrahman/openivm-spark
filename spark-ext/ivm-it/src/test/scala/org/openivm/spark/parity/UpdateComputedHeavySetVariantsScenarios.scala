package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `UpdateComputedSpec.scala` §(1)-(5) — the mv_comp_upd
  * SET-variant walk through col±constant, col*constant, multi-col SET, and
  * no-WHERE-update over a SIMPLE_PROJECTION view (~4m01).  Lives in its own
  * forked JVM so the rest of the parity suite is not blocked by this monster
  * test.
  *
  * Table / MV names are prefixed `upd_heavy_set_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
abstract class UpdateComputedHeavySetVariantsScenarios extends IvmParitySpecBase("update-computed-heavy-set-variants") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (1)-(5) Computed UPDATEs walked through on a SIMPLE_PROJECTION view
  //     mirroring update_computed.test:7-149
  //     Spark's DECIMAL default precision differs from DuckDB's, so we declare
  //     `price DECIMAL(10,3)` to match the test's printed value `90.000`.
  // ============================================================================
  describe("(1)-(5) Computed UPDATE expressions on a SIMPLE_PROJECTION view") {
    it("col±constant, col*constant, multi-col SET, no-WHERE-update all propagate after REFRESH") {
      sql(
        "CREATE TABLE IF NOT EXISTS upd_heavy_set_comp_upd(" +
          "  id INT, val INT, price DECIMAL(10,3), label STRING" +
          ") USING DELTA"
      )
      sql(
        "INSERT INTO upd_heavy_set_comp_upd VALUES " +
          "(1, 10, 100.0, 'alpha'), (2, 20, 200.0, 'beta'), (3, 30, 300.0, 'gamma')"
      )
      sql(
        "CREATE MATERIALIZED VIEW upd_heavy_set_mv_comp_upd AS SELECT id, val, price, label FROM upd_heavy_set_comp_upd"
      )
      val expected = "SELECT id, val, price, label FROM upd_heavy_set_comp_upd"
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)

      // (1) col + constant
      sql("UPDATE upd_heavy_set_comp_upd SET val = val + 5 WHERE id = 1")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      sql("SELECT val FROM upd_heavy_set_mv_comp_upd WHERE id = 1").collect().head.getInt(0) shouldBe 15

      // (2) col - constant
      sql("UPDATE upd_heavy_set_comp_upd SET val = val - 3 WHERE id = 2")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      sql("SELECT val FROM upd_heavy_set_mv_comp_upd WHERE id = 2").collect().head.getInt(0) shouldBe 17

      // (3) col * constant
      sql("UPDATE upd_heavy_set_comp_upd SET val = val * 2 WHERE id = 3")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      sql("SELECT val FROM upd_heavy_set_mv_comp_upd WHERE id = 3").collect().head.getInt(0) shouldBe 60

      // (4) multi-column computed UPDATE
      sql("UPDATE upd_heavy_set_comp_upd SET val = val + 1, price = price - 10.0 WHERE id = 1")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      val row = sql("SELECT val, price FROM upd_heavy_set_mv_comp_upd WHERE id = 1").collect().head
      row.getInt(0) shouldBe 16
      row.getDecimal(1).compareTo(new java.math.BigDecimal("90.000")) shouldBe 0

      // (5) computed update on ALL rows (no WHERE clause)
      sql("UPDATE upd_heavy_set_comp_upd SET val = val + 100")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
    }
  }
}
