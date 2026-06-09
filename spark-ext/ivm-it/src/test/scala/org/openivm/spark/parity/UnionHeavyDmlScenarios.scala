package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Heavy carve-out of `UnionSpec.scala` §(1a) — the mv_all_orders UNION ALL
  * incremental maintenance walk across INSERTs and DELETEs on both branches
  * (~3m47).  Lives in its own forked JVM so the rest of the parity suite is
  * not blocked by this monster test.
  *
  * Table / MV names are prefixed `union_heavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
abstract class UnionHeavyDmlScenarios extends IvmParitySpecBase("union-heavy-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ============================================================================
  // (1a) Basic UNION ALL over two tables — INSERT/DELETE batched (union.test:7-119)
  //      Sub-section without insert of fully-identical rows: this works under
  //      the openivm-spark SIMPLE_PROJECTION MERGE.
  // ============================================================================
  describe("(1a) Basic UNION ALL (unique rows only): INSERT/DELETE batched") {
    it("incrementally maintains the union across INSERTs and DELETEs on both branches") {
      sql("CREATE TABLE IF NOT EXISTS union_heavy_u_us(id INT, product STRING, amount INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS union_heavy_u_eu(id INT, product STRING, amount INT) USING DELTA")
      sql("INSERT INTO union_heavy_u_us VALUES (1, 'widget', 100), (2, 'gadget', 200)")
      sql("INSERT INTO union_heavy_u_eu VALUES (3, 'widget', 150), (4, 'gizmo', 300)")

      sql(
        "CREATE MATERIALIZED VIEW union_heavy_mv_all_orders AS " +
          "SELECT id, product, amount FROM union_heavy_u_us " +
          "UNION ALL " +
          "SELECT id, product, amount FROM union_heavy_u_eu"
      )

      // Initial state
      val expected =
        "SELECT id, product, amount FROM union_heavy_u_us UNION ALL SELECT id, product, amount FROM union_heavy_u_eu"
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // Multi-source unions are commonly demoted to FullRefresh by hasRealDelta.
      val rt = mvRefreshType("union_heavy_mv_all_orders")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      // Insert into both sides, single refresh — all rows distinct
      sql("INSERT INTO union_heavy_u_us VALUES (5, 'bolt', 50), (6, 'nut', 25)")
      sql("INSERT INTO union_heavy_u_eu VALUES (7, 'screw', 75)")
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // Delete from one side
      sql("DELETE FROM union_heavy_u_us WHERE id = 1")
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // Delete from both sides + insert a distinct new row (8, 'nail', 10) once
      sql("DELETE FROM union_heavy_u_us WHERE id = 2")
      sql("DELETE FROM union_heavy_u_eu WHERE id = 3")
      sql("INSERT INTO union_heavy_u_eu VALUES (8, 'nail', 10)")
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // No-op refresh
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)
    }
  }
}
