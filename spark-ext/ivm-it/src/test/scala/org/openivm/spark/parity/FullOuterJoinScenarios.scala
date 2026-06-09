package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** P6f — 1:1 ScalaTest port of `openivm/test/sql/full_outer_join.test`.
  *
  * Mirrors the openivm sqllogic file section-by-section, adapted to Spark 3.5
  * + Delta.  Per `.temp/openivm/CLAUDE.md` ("Join Delta Rule"):
  *
  *   - FULL OUTER projection views use bidirectional key-based partial
  *     recompute in openivm.
  *   - FULL OUTER aggregate views use Zhang & Larson-style MERGE by default
  *     (`openivm_full_outer_merge=true`) with group-recompute fallback.
  *
  * == What this spec verifies (end-to-end through the assembled extension) ==
  *
  *   - Basic FULL OUTER JOIN projection with matched / unmatched-left /
  *     unmatched-right rows across many incremental DML rounds.
  *   - FULL OUTER JOIN aggregate with GROUP BY (group-recompute path).
  *   - FULL OUTER JOIN aggregate via Zhang & Larson incremental MERGE.
  *   - Compound GROUP BY with `COALESCE(l.key, r.key)` and a non-join
  *     attribute column.
  *   - Stress cases: empty initial state, NULL join keys, insert+delete
  *     cancellation, all 8 {ins,del}×{left,right}×{matched,unmatched}
  *     transitions in one refresh, large conflicting batches, simultaneous
  *     same-key inserts on both sides, multi-aggregate (SUM+COUNT+AVG).
  *   - Regression: FULL OUTER hidden right-key flow through a chained
  *     projection/filter stack (depth-2 MV-over-MV).
  *
  * == DuckDB → Spark translation notes ==
  *
  *   - `pragma refresh('mv')` → `REFRESH MATERIALIZED VIEW mv`.
  *   - `IS NOT DISTINCT FROM` is supported natively in Spark 3.5.
  *   - `FULL OUTER JOIN` and `COALESCE` are supported natively.
  *   - String concat `||` → `concat()` (not used here — openivm test has none).
  *   - `openivm_views` system table / `pragma_table_info` introspections are
  *     openivm-internal and intentionally NOT ported.
  *
  * == Classification & verification ==
  *
  * Per the user's brief and CLAUDE.md, this spec does **not** assert specific
  * `RefreshTypeCode` values — openivm may classify a given shape as
  * `AggregateGroup`, `GroupRecompute`, `SimpleProjection`, or `FullRefresh`
  * depending on the demotion logic in `RefreshMaterializedViewCommand`.
  * Correctness is verified strictly via bidirectional `EXCEPT ALL` between
  * the MV (projected to user-visible columns) and a fresh re-evaluation of
  * the view body, per CLAUDE.md ("Every IVM refresh in a test MUST be
  * cross-checked with EXCEPT ALL in both directions").
  *
  * Batched DML stress (CLAUDE.md: "Stress tests must batch many conflicting
  * DML ops before a single refresh") is preserved in tests (7)–(10).
  */
abstract class FullOuterJoinScenarios extends IvmParitySpecBase("full-outer-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  /** Refresh a chain of MVs in dependency order (upstream first). */
  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(refreshMv)

  // ============================================================================
  // (1) Basic FULL OUTER JOIN projection
  //     openivm/test/sql/full_outer_join.test L26–L269
  //     Extracted to [[FullOuterJoinHeavyOuterSidesSpec]] (~4m44 wall) so it
  //     runs in its own forked JVM and does not bottleneck the rest of this
  //     spec.
  // ============================================================================

  // ============================================================================
  // (2) FULL OUTER JOIN aggregate with GROUP BY (group-recompute mode)
  //     openivm/test/sql/full_outer_join.test L271–L391
  // ============================================================================

  describe("(2) FULL OUTER JOIN aggregate (group-recompute path): departments ⟗ staff") {
    it("SUM + COUNT aggregate updates across matched/unmatched transitions and batches") {
      sql("CREATE TABLE IF NOT EXISTS departments(id INT, dept STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS staff(id INT, dept_id INT, salary INT) USING DELTA")
      sql("INSERT INTO departments VALUES (1, 'Eng'), (2, 'Sales'), (3, 'HR')")
      sql("INSERT INTO staff VALUES (1, 1, 100), (2, 1, 200), (3, 4, 300)")

      sql(
        "CREATE MATERIALIZED VIEW dept_stats AS " +
          "SELECT d.dept, SUM(s.salary) AS total_sal, COUNT(s.salary) AS cnt " +
          "FROM departments d FULL OUTER JOIN staff s ON d.id = s.dept_id " +
          "GROUP BY d.dept"
      )

      val viewBody =
        "SELECT d.dept, SUM(s.salary) AS total_sal, COUNT(s.salary) AS cnt " +
          "FROM departments d FULL OUTER JOIN staff s ON d.id = s.dept_id " +
          "GROUP BY d.dept"

      assertMvCorrect("dept_stats", viewBody)

      // Insert staff into Sales (was unmatched-left, now matched)
      sql("INSERT INTO staff VALUES (4, 2, 150)")
      refreshMv("dept_stats")
      assertMvCorrect("dept_stats", viewBody)

      // Delete all staff from Eng (matched → unmatched-left)
      sql("DELETE FROM staff WHERE dept_id = 1")
      refreshMv("dept_stats")
      assertMvCorrect("dept_stats", viewBody)

      // Batch DML on both sides
      sql("INSERT INTO departments VALUES (4, 'Legal')")
      sql("INSERT INTO staff VALUES (5, 3, 250), (6, 99, 400)")
      sql("DELETE FROM staff WHERE id = 3")
      refreshMv("dept_stats")
      assertMvCorrect("dept_stats", viewBody)
    }
  }

  // ============================================================================
  // (3) FULL OUTER JOIN aggregate (Zhang & Larson MERGE)
  //     openivm/test/sql/full_outer_join.test L393–L585
  //
  //     openivm sets `openivm_full_outer_merge = true` (default) for this path.
  //     openivm-spark does not expose a per-session toggle, but the default
  //     behaviour is equivalent — correctness comes from bidirectional EXCEPT ALL.
  //
  //     Extracted to [[FullOuterJoinHeavyMergeTransitionsSpec]] (~4m23 wall) so
  //     it runs in its own forked JVM and does not bottleneck the rest of this
  //     spec.
  // ============================================================================

  // ============================================================================
  // (4) FULL OUTER MERGE — compound GROUP BY with a non-join attribute column
  //     openivm/test/sql/full_outer_join.test L587–L635
  // ============================================================================

  describe("(4) FULL OUTER JOIN MERGE — compound GROUP BY with non-join attribute") {
    it("handles UPDATE on a left-side attribute column under a compound GROUP BY") {
      sql("CREATE TABLE IF NOT EXISTS foj_attr_l(id INT, grp INT, attr INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS foj_attr_r(id INT, lid INT, amount INT) USING DELTA")
      sql("INSERT INTO foj_attr_l VALUES (1, 10, 50), (2, 10, 60), (3, 20, 70)")
      sql("INSERT INTO foj_attr_r VALUES (1, 1, 100), (2, 4, 400)")

      sql(
        "CREATE MATERIALIZED VIEW foj_attr_mv AS " +
          "SELECT COALESCE(l.grp, r.lid) AS g, COALESCE(l.id, r.lid) AS k, l.attr, SUM(r.amount) AS total " +
          "FROM foj_attr_l l FULL OUTER JOIN foj_attr_r r ON l.id = r.lid " +
          "GROUP BY COALESCE(l.grp, r.lid), COALESCE(l.id, r.lid), l.attr"
      )

      val viewBody =
        "SELECT COALESCE(l.grp, r.lid) AS g, COALESCE(l.id, r.lid) AS k, l.attr, SUM(r.amount) AS total " +
          "FROM foj_attr_l l FULL OUTER JOIN foj_attr_r r ON l.id = r.lid " +
          "GROUP BY COALESCE(l.grp, r.lid), COALESCE(l.id, r.lid), l.attr"

      assertMvCorrect("foj_attr_mv", viewBody)

      // Update a left-side attribute; insert a matching right-side row.
      sql("UPDATE foj_attr_l SET attr = 55 WHERE id = 1")
      sql("INSERT INTO foj_attr_r VALUES (3, 2, 200)")
      refreshMv("foj_attr_mv")
      assertMvCorrect("foj_attr_mv", viewBody)
    }
  }

  // ============================================================================
  // (5) STRESS: both tables empty initially
  //     openivm/test/sql/full_outer_join.test L641–L709
  // ============================================================================

  describe("(5) STRESS — both tables empty initially") {
    it("correctly populates the FULL OUTER aggregate from zero-row sources upward") {
      sql("CREATE TABLE IF NOT EXISTS empty_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS empty_r(id INT, lid INT, val INT) USING DELTA")

      sql(
        "CREATE MATERIALIZED VIEW mv_empty AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM empty_l l FULL OUTER JOIN empty_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM empty_l l FULL OUTER JOIN empty_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      assertMvCorrect("mv_empty", viewBody)

      // Insert into right only (all unmatched-right → NULL group)
      sql("INSERT INTO empty_r VALUES (1, 10, 100), (2, 20, 200)")
      refreshMv("mv_empty")
      assertMvCorrect("mv_empty", viewBody)

      // Insert into left, some keys matching right
      sql("INSERT INTO empty_l VALUES (10, 'X'), (30, 'Y')")
      refreshMv("mv_empty")
      assertMvCorrect("mv_empty", viewBody)
    }
  }

  // ============================================================================
  // (6) STRESS: NULL join keys (never match in equijoin)
  //     openivm/test/sql/full_outer_join.test L711–L765
  // ============================================================================

  describe("(6) STRESS — NULL join keys (never match in equijoin)") {
    it("NULL-keyed rows remain unmatched on both sides through inserts") {
      sql("CREATE TABLE IF NOT EXISTS nullk_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS nullk_r(id INT, lid INT, val INT) USING DELTA")
      sql("INSERT INTO nullk_l VALUES (1, 'A'), (NULL, 'NULLKEY')")
      sql("INSERT INTO nullk_r VALUES (1, 1, 50), (2, NULL, 75)")

      sql(
        "CREATE MATERIALIZED VIEW mv_nullkeys AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM nullk_l l FULL OUTER JOIN nullk_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM nullk_l l FULL OUTER JOIN nullk_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      assertMvCorrect("mv_nullkeys", viewBody)

      // Insert another NULL-keyed row on each side
      sql("INSERT INTO nullk_l VALUES (NULL, 'NULLKEY2')")
      sql("INSERT INTO nullk_r VALUES (3, NULL, 125)")
      refreshMv("mv_nullkeys")
      assertMvCorrect("mv_nullkeys", viewBody)
    }
  }

  // ============================================================================
  // (7) STRESS: insert then delete same row before refresh (cancels out)
  //     openivm/test/sql/full_outer_join.test L767–L839
  // ============================================================================

  describe("(7) STRESS — insert+delete same row before refresh (delta cancels out)") {
    it("net-zero deltas on either side are correctly consolidated to no-op") {
      sql("CREATE TABLE IF NOT EXISTS cancel_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS cancel_r(id INT, lid INT, val INT) USING DELTA")
      sql("INSERT INTO cancel_l VALUES (1, 'A'), (2, 'B')")
      sql("INSERT INTO cancel_r VALUES (1, 1, 100)")

      sql(
        "CREATE MATERIALIZED VIEW mv_cancel AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM cancel_l l FULL OUTER JOIN cancel_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM cancel_l l FULL OUTER JOIN cancel_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      // Insert and delete same right-side row before refresh — should be a no-op
      sql("INSERT INTO cancel_r VALUES (2, 2, 500)")
      sql("DELETE FROM cancel_r WHERE id = 2")
      refreshMv("mv_cancel")
      assertMvCorrect("mv_cancel", viewBody)

      // Insert and delete on left side too — cancel out
      sql("INSERT INTO cancel_l VALUES (5, 'E')")
      sql("DELETE FROM cancel_l WHERE id = 5")
      refreshMv("mv_cancel")
      assertMvCorrect("mv_cancel", viewBody)
    }
  }

  // ============================================================================
  // (8) STRESS: all 8 transitions in a single refresh
  //     {insert,delete} × {left,right} × {matched,unmatched}
  //     openivm/test/sql/full_outer_join.test L841–L913
  // ============================================================================

  describe("(8) STRESS — all 8 {ins,del}×{left,right}×{matched,unmatched} transitions in one refresh") {
    it("delta consolidation correctly handles every transition class in a single REFRESH") {
      sql("CREATE TABLE IF NOT EXISTS trans_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS trans_r(id INT, lid INT, val INT) USING DELTA")
      sql("INSERT INTO trans_l VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')")
      sql("INSERT INTO trans_r VALUES (1, 1, 10), (2, 2, 20), (3, 50, 30), (4, 60, 40)")

      sql(
        "CREATE MATERIALIZED VIEW mv_trans AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM trans_l l FULL OUTER JOIN trans_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM trans_l l FULL OUTER JOIN trans_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      // 1. INSERT LEFT MATCHED:    insert left id=50, matches right lid=50
      // 2. INSERT LEFT UNMATCHED:  insert left id=99, no right match
      // 3. DELETE LEFT MATCHED:    delete left id=1, right lid=1 becomes unmatched
      // 4. DELETE LEFT UNMATCHED:  delete left id=3, was unmatched-left
      // 5. INSERT RIGHT MATCHED:   insert right lid=2, adds to B
      // 6. INSERT RIGHT UNMATCHED: insert right lid=200, no left match
      // 7. DELETE RIGHT MATCHED:   delete right lid=2 (id=2), B loses its match
      // 8. DELETE RIGHT UNMATCHED: delete right lid=60 (id=4), shrinks NULL group
      sql("INSERT INTO trans_l VALUES (50, 'E')")
      sql("INSERT INTO trans_l VALUES (99, 'Z')")
      sql("DELETE FROM trans_l WHERE id = 1")
      sql("DELETE FROM trans_l WHERE id = 3")
      sql("INSERT INTO trans_r VALUES (5, 2, 25)")
      sql("INSERT INTO trans_r VALUES (6, 200, 60)")
      sql("DELETE FROM trans_r WHERE id = 2")
      sql("DELETE FROM trans_r WHERE id = 4")
      refreshMv("mv_trans")
      assertMvCorrect("mv_trans", viewBody)
    }
  }

  // ============================================================================
  // (9) STRESS: large batch with many conflicting INSERT+DELETE
  //     openivm/test/sql/full_outer_join.test L915–L1020
  // ============================================================================

  describe("(9) STRESS — large batch with many conflicting INSERT+DELETE on both sides") {
    it("two consecutive heavily-mixed batches both reconcile correctly") {
      sql("CREATE TABLE IF NOT EXISTS batch_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS batch_r(id INT, lid INT, val INT) USING DELTA")
      sql("INSERT INTO batch_l VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E')")
      sql(
        "INSERT INTO batch_r VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 6, 40), (5, 7, 50)"
      )

      sql(
        "CREATE MATERIALIZED VIEW mv_batch AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM batch_l l FULL OUTER JOIN batch_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM batch_l l FULL OUTER JOIN batch_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      // First batch: many conflicting ops
      sql("DELETE FROM batch_r WHERE lid = 1")
      sql("INSERT INTO batch_r VALUES (6, 3, 60), (7, 4, 70)")
      sql("DELETE FROM batch_l WHERE id = 2")
      sql("INSERT INTO batch_l VALUES (6, 'F')")
      sql("INSERT INTO batch_r VALUES (8, 99, 80), (9, 100, 90)")
      sql("INSERT INTO batch_r VALUES (10, 5, 999)")
      sql("DELETE FROM batch_r WHERE id = 10")
      sql("DELETE FROM batch_r WHERE id = 5")
      sql("INSERT INTO batch_r VALUES (11, 7, 500)")
      refreshMv("mv_batch")
      assertMvCorrect("mv_batch", viewBody)

      // Second batch
      sql("INSERT INTO batch_l VALUES (7, 'G'), (100, 'H')")
      sql("DELETE FROM batch_l WHERE id = 3")
      sql("INSERT INTO batch_r VALUES (12, 1, 15), (13, 1, 25)")
      sql("DELETE FROM batch_r WHERE lid = 4")
      sql("DELETE FROM batch_l WHERE id = 6")
      sql("INSERT INTO batch_r VALUES (14, 5, 55)")
      refreshMv("mv_batch")
      assertMvCorrect("mv_batch", viewBody)
    }
  }

  // ============================================================================
  // (10) STRESS: simultaneous same-key inserts (and deletes) in both deltas
  //      openivm/test/sql/full_outer_join.test L1022–L1094
  // ============================================================================

  describe("(10) STRESS — simultaneous same-key inserts/deletes in both deltas") {
    it("matching-key inserts on both sides combine; matching-key deletes also cancel correctly") {
      sql("CREATE TABLE IF NOT EXISTS simul_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS simul_r(id INT, lid INT, val INT) USING DELTA")
      sql("INSERT INTO simul_l VALUES (1, 'A')")
      sql("INSERT INTO simul_r VALUES (1, 1, 50)")

      sql(
        "CREATE MATERIALIZED VIEW mv_simul AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM simul_l l FULL OUTER JOIN simul_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt " +
          "FROM simul_l l FULL OUTER JOIN simul_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      // Both sides insert with the same join key (id=10/lid=10) simultaneously
      sql("INSERT INTO simul_l VALUES (10, 'NEW')")
      sql("INSERT INTO simul_r VALUES (2, 10, 200), (3, 10, 300)")
      refreshMv("mv_simul")
      assertMvCorrect("mv_simul", viewBody)

      // Both sides delete with the same join key simultaneously
      sql("DELETE FROM simul_l WHERE id = 10")
      sql("DELETE FROM simul_r WHERE lid = 10")
      refreshMv("mv_simul")
      assertMvCorrect("mv_simul", viewBody)
    }
  }

  // ============================================================================
  // (11) Multiple aggregates: SUM + COUNT + AVG
  //      openivm/test/sql/full_outer_join.test L1096–L1155
  // ============================================================================

  describe("(11) Multiple aggregates (SUM + COUNT + AVG) over FULL OUTER JOIN") {
    it("SUM/COUNT/AVG all remain consistent under matched/unmatched transitions") {
      sql("CREATE TABLE IF NOT EXISTS multi_l(id INT, grp STRING) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS multi_r(id INT, lid INT, val INT) USING DELTA")
      sql("INSERT INTO multi_l VALUES (1, 'A'), (2, 'B'), (3, 'C')")
      sql("INSERT INTO multi_r VALUES (1, 1, 100), (2, 1, 200), (3, 5, 300)")

      sql(
        "CREATE MATERIALIZED VIEW mv_multi_agg AS " +
          "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt, AVG(r.val) AS avg_val " +
          "FROM multi_l l FULL OUTER JOIN multi_r r ON l.id = r.lid " +
          "GROUP BY l.grp"
      )

      val viewBody =
        "SELECT l.grp, SUM(r.val) AS total, COUNT(r.val) AS cnt, AVG(r.val) AS avg_val " +
          "FROM multi_l l FULL OUTER JOIN multi_r r ON l.id = r.lid " +
          "GROUP BY l.grp"

      assertMvCorrect("mv_multi_agg", viewBody)

      // Batched: insert matched + insert unmatched-right + delete matched left-side + add new left
      sql("INSERT INTO multi_r VALUES (4, 2, 150)")
      sql("INSERT INTO multi_r VALUES (5, 99, 400)")
      sql("DELETE FROM multi_r WHERE lid = 1")
      sql("INSERT INTO multi_l VALUES (5, 'D')")
      refreshMv("mv_multi_agg")
      assertMvCorrect("mv_multi_agg", viewBody)
    }
  }

  // ============================================================================
  // (12) Regression: FULL OUTER hidden right-key through projection/filter stack
  //      openivm/test/sql/full_outer_join.test L1157–L1317
  //
  //      Depth-2 MV-over-MV chain: two SIMPLE_PROJECTION views feed a third
  //      FULL OUTER JOIN view with a CASE-based status column.  Per
  //      ChainedSpec's notes, openivm-spark currently runs cascades manually by
  //      issuing per-MV `REFRESH MATERIALIZED VIEW` in dependency order.
  //
  //      The openivm test uses `WITH a AS (SELECT * FROM mv_chain_foj_left)`.
  //      We project the upstream MVs explicitly by their user-visible column
  //      names so that any hidden `openivm_*` columns physically present in
  //      the Delta-backed MV table do not leak into the FULL OUTER JOIN.
  // ============================================================================

  describe("(12) Regression — FULL OUTER hidden right key through projection/filter stack") {
    it("depth-2 chain with FULL OUTER JOIN + CASE/COALESCE/filter stays bag-equal after refresh") {
      sql(
        "CREATE TABLE IF NOT EXISTS chain_foj_left_src(id INT, totaln INT, districts INT) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS chain_foj_right_src(id INT, total_orders INT, order_avg DOUBLE) USING DELTA"
      )
      sql("INSERT INTO chain_foj_left_src VALUES (1, 20, 2), (2, 7, 1), (4, 11, 3)")
      sql("INSERT INTO chain_foj_right_src VALUES (1, 12, 2.5), (3, 5, 1.5), (4, 20, 0.5)")

      sql(
        "CREATE MATERIALIZED VIEW mv_chain_foj_left AS " +
          "SELECT id AS C_W_ID, totaln, districts FROM chain_foj_left_src WHERE totaln > 0"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_chain_foj_right AS " +
          "SELECT id AS w_id, total_orders, order_avg FROM chain_foj_right_src"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_chain_foj_cross AS " +
          "WITH a AS (SELECT C_W_ID, totaln, districts FROM mv_chain_foj_left), " +
          "     b AS (SELECT w_id, total_orders, order_avg FROM mv_chain_foj_right), " +
          "     combined AS (" +
          "       SELECT COALESCE(a.C_W_ID, b.w_id) AS w_id, " +
          "              a.totaln AS cust_totaln, " +
          "              a.districts AS cust_districts, " +
          "              b.total_orders AS order_lines, " +
          "              b.order_avg AS order_avg " +
          "       FROM a FULL OUTER JOIN b ON a.C_W_ID = b.w_id" +
          "     ) " +
          "SELECT w_id, cust_totaln, cust_districts, order_lines, order_avg, " +
          "       CASE WHEN cust_totaln IS NULL THEN 'order-only' " +
          "            WHEN order_lines IS NULL THEN 'cust-only' " +
          "            WHEN cust_totaln > 0 AND order_avg > 1 THEN 'active' " +
          "            ELSE 'dormant' END AS status " +
          "FROM combined " +
          "WHERE cust_totaln IS NOT NULL OR order_lines IS NOT NULL"
      )

      val viewBody =
        "WITH a AS (SELECT id AS C_W_ID, totaln, districts FROM chain_foj_left_src WHERE totaln > 0), " +
          "     b AS (SELECT id AS w_id, total_orders, order_avg FROM chain_foj_right_src), " +
          "     combined AS (" +
          "       SELECT COALESCE(a.C_W_ID, b.w_id) AS w_id, " +
          "              a.totaln AS cust_totaln, " +
          "              a.districts AS cust_districts, " +
          "              b.total_orders AS order_lines, " +
          "              b.order_avg AS order_avg " +
          "       FROM a FULL OUTER JOIN b ON a.C_W_ID = b.w_id" +
          "     ) " +
          "SELECT w_id, cust_totaln, cust_districts, order_lines, order_avg, " +
          "       CASE WHEN cust_totaln IS NULL THEN 'order-only' " +
          "            WHEN order_lines IS NULL THEN 'cust-only' " +
          "            WHEN cust_totaln > 0 AND order_avg > 1 THEN 'active' " +
          "            ELSE 'dormant' END AS status " +
          "FROM combined " +
          "WHERE cust_totaln IS NOT NULL OR order_lines IS NOT NULL"

      assertMvCorrect("mv_chain_foj_cross", viewBody)

      // Now exercise refresh with mixed DML on both base tables.
      sql("INSERT INTO chain_foj_right_src VALUES (2, 14, 3.0)")
      sql("DELETE FROM chain_foj_left_src WHERE id = 1")
      sql("INSERT INTO chain_foj_left_src VALUES (3, 9, 2)")

      refreshChain("mv_chain_foj_left", "mv_chain_foj_right", "mv_chain_foj_cross")
      assertMvCorrect("mv_chain_foj_cross", viewBody)
    }
  }
}
