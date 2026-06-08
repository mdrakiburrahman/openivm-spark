package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy carve-out of `FullOuterJoinSpec.scala` §(3) — the cat_sales FULL
  * OUTER JOIN aggregate via Zhang & Larson MERGE walk through matched /
  * unmatched transitions and cross-group transfer (~4m23).  Lives in its own
  * forked JVM so the rest of the parity suite is not blocked by this monster
  * test.
  *
  * Table / MV names are prefixed `foj_heavy_merge_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
abstract class FullOuterJoinHeavyMergeTransitionsScenarios
    extends IvmParitySpecBase("full-outer-join-heavy-merge-transitions") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ============================================================================
  // (3) FULL OUTER JOIN aggregate (Zhang & Larson MERGE)
  //     openivm/test/sql/full_outer_join.test L393–L585
  //
  //     openivm sets `openivm_full_outer_merge = true` (default) for this path.
  //     openivm-spark does not expose a per-session toggle, but the default
  //     behaviour is equivalent — correctness comes from bidirectional EXCEPT ALL.
  // ============================================================================

  describe("(3) FULL OUTER JOIN aggregate (Zhang & Larson MERGE): items ⟗ sales") {
    it("MERGE-style refresh handles matched/unmatched transitions and cross-group transfer") {
      sql("CREATE TABLE IF NOT EXISTS foj_heavy_merge_items(id INT, category STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS foj_heavy_merge_sales(id INT, item_id INT, amount INT) USING DELTA")
      sql("INSERT INTO foj_heavy_merge_items VALUES (1, 'A'), (2, 'B'), (3, 'C')")
      sql("INSERT INTO foj_heavy_merge_sales VALUES (1, 1, 100), (2, 1, 200), (3, 5, 300)")

      sql(
        "CREATE MATERIALIZED VIEW foj_heavy_merge_cat_sales AS " +
          "SELECT i.category, SUM(s.amount) AS total, COUNT(s.amount) AS cnt " +
          "FROM foj_heavy_merge_items i FULL OUTER JOIN foj_heavy_merge_sales s ON i.id = s.item_id " +
          "GROUP BY i.category"
      )

      val viewBody =
        "SELECT i.category, SUM(s.amount) AS total, COUNT(s.amount) AS cnt " +
          "FROM foj_heavy_merge_items i FULL OUTER JOIN foj_heavy_merge_sales s ON i.id = s.item_id " +
          "GROUP BY i.category"

      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: insert sale matching B (unmatched-left → matched)
      sql("INSERT INTO foj_heavy_merge_sales VALUES (4, 2, 150)")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: insert unmatched-right sale (NULL group grows)
      sql("INSERT INTO foj_heavy_merge_sales VALUES (5, 99, 400)")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: delete all sales for A (matched → unmatched-left, aggregates → NULL)
      sql("DELETE FROM foj_heavy_merge_sales WHERE item_id = 1")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: delete unmatched-right sale (NULL group shrinks)
      sql("DELETE FROM foj_heavy_merge_sales WHERE id = 3")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: cross-group transfer (add item matching a previously unmatched-right sale)
      sql("INSERT INTO foj_heavy_merge_items VALUES (99, 'X')")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)

      // MERGE: batch mixed DML on both sides
      sql("INSERT INTO foj_heavy_merge_items VALUES (5, 'D')")
      sql("INSERT INTO foj_heavy_merge_sales VALUES (6, 3, 250), (7, 88, 500)")
      sql("DELETE FROM foj_heavy_merge_sales WHERE id = 4")
      sql("DELETE FROM foj_heavy_merge_items WHERE id = 2")
      refreshMv("foj_heavy_merge_cat_sales")
      assertMvCorrect("foj_heavy_merge_cat_sales", viewBody)
    }
  }
}
