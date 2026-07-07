package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** 1:1 ScalaTest port of openivm's `test/sql/semi_anti_join.test`.
  *
  * This spec lives alongside `SemiAntiSpec.scala` which exercises a
  * curated, parallel set of 11 SEMI/ANTI shapes against the same FULL_REFRESH
  * fallback path.  Where `SemiAntiSpec` is hand-crafted from first principles,
  * this spec is a structural mirror of the openivm-side `.test` file: every
  * `CREATE MATERIALIZED VIEW`, every batched DML run, and every `EXCEPT ALL`
  * cross-check in the openivm source has a corresponding `describe(...) {
  * it(...) { ... } }` block here, using the exact data the openivm test uses.
  *
  * == Why FULL_REFRESH everywhere on the current pin? ==
  *
  * openivm's aux-state SEMI/ANTI path (`SEMI_ANTI_RECOMPUTE = 9` in the
  * `RefreshType` enum) is implemented for the DuckDB runtime in
  * `openivm/src/upsert/refresh_compiler_aux.cpp::CompileSemiAntiRecompute`.
  * It requires three pieces that openivm-spark does not (yet) emit on the
  * current `OPENIVM_COMMIT` pin:
  *
  *   1. `RefreshMetadata::SemiAntiAuxMeta` populated at CREATE time and
  *      persisted to the Spark MV catalog (the bridge runs openivm in
  *      `compile_only=true`; aux-meta is not threaded through).
  *   2. A per-MV `<view>_aux(left_cols…, _match_count, _left_count)` Delta
  *      table boot-strapped from the initial snapshot.
  *   3. DuckDB-specific physical SQL in the refresh program
  *      (`generate_series`, `rowid`, `MERGE INTO _aux USING <delta>`) rewritten
  *      for Delta MERGE / `LATERAL VIEW explode(sequence(...))`.
  *
  * Because none of the above is wired through `SparkRefreshRewriter` yet,
  * `CreateMaterializedViewCommand` demotes every SEMI/ANTI/EXISTS/NOT IN view
  * to `RefreshTypeCode.FullRefresh` (3) — the refresh handler then
  * `INSERT OVERWRITE`s the MV from the original user SQL, which is always
  * correct by construction.  See `SemiAntiSpec.scala`'s scaladoc for the full
  * write-up.  This spec deliberately does **not** assert specific
  * `RefreshType` codes; it only asserts that the MV
  * is bag-equivalent to the re-evaluated view body after each refresh.
  *
  * == DuckDB → Spark dialect translation ==
  *
  *   - DuckDB bare `SEMI JOIN` / `ANTI JOIN` → Spark `LEFT SEMI JOIN` /
  *     `LEFT ANTI JOIN` (Spark only accepts the explicit `LEFT` prefix; the
  *     openivm CREATE-side bridge strips it back off before forwarding to
  *     DuckDB via `OpenIvmCompiler.normalizeSparkSqlForDuckdb`).
  *   - DuckDB `rowid` pseudo-column does not exist in Spark/Delta.  The single
  *     openivm test that uses it (`DELETE FROM saj_l WHERE rowid IN (...)`)
  *     is rewritten as DELETE+re-INSERT to remove exactly one duplicate row
  *     of `(1, 10)` — semantically equivalent for SEMI/ANTI which is
  *     bag-aware via match-count, not by `rowid` identity.
  *   - DuckDB `BOOLEAN` literals (`true`/`false`) and `IS NOT DISTINCT FROM`
  *     are accepted by Spark 3.4+ verbatim.
  *   - DuckDB `VARCHAR` → Spark `STRING`.
  *   - All tables `USING DELTA` so UPDATE / DELETE work natively.
  *
  * == Skipped openivm-internal queries ==
  *
  *   The openivm source has two probe queries that inspect
  *   `openivm_views.semi_anti_aux_meta_json` to assert which views were
  *   classified as aux-state SEMI/ANTI.  Those rows live inside the embedded
  *   DuckDB metadata DB on the openivm side and are not exposed to Spark; the
  *   Spark-side classification is FULL_REFRESH for all 11 views anyway, so
  *   the assertions are vacuous on this pin and are omitted.
  */
abstract class SemiAntiJoinScenarios extends IvmParitySpecBase("semi-anti-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (1) SEMI JOIN with non-equi predicate `abs(l.x - r.y) <= 2` ──────────
  // Mirrors openivm/test/sql/semi_anti_join.test lines 9-100.
  // Initial verify, then a batch of mixed DML (right INSERT/DELETE, left
  // INSERT, plus removing one of two duplicate `(1, 10)` rows), then a second
  // round that deletes the right-row whose insertion in the previous batch
  // had been paired with a new left tuple — exercising the
  // double-counting-of-`current S` + `ΔS` aux-state edge case.

  describe("(1) SEMI JOIN with non-equi predicate — batched DML across L+R") {
    it("refresh tracks per-left-tuple match count under abs(l.x - r.y) <= 2") {
      sql("CREATE TABLE IF NOT EXISTS saj_l_1(id INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_r_1(rid INT, y INT) USING DELTA")
      sql("INSERT INTO saj_l_1 VALUES (1, 10), (1, 10), (2, 20), (3, 30), (4, 40)")
      sql("INSERT INTO saj_r_1 VALUES (100, 8), (101, 100), (102, 41)")

      val viewBody =
        "SELECT l.id, l.x FROM saj_l_1 l " +
          "LEFT SEMI JOIN saj_r_1 r ON abs(l.x - r.y) <= 2"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_semi_1 AS $viewBody")
      assertMvCorrect("mv_saj_semi_1", viewBody)

      // First batch of conflicting DML:
      //   - right insert makes id=2 visible (via y=21) and id=3 reachable (via y=32)
      //   - right delete removes the only match for id=4 (y=41 → bye)
      //   - left insert adds a second id=3 row
      //   - left delete removes one duplicate id=1 row (Spark/Delta has no
      //     `rowid`; emulate the openivm `DELETE … rowid IN (… LIMIT 1)` by
      //     deleting both copies then re-inserting one — semantically
      //     equivalent for a SEMI/ANTI bag-comparison test).
      sql("INSERT INTO saj_r_1 VALUES (103, 21), (104, 32)")
      sql("DELETE FROM saj_r_1 WHERE rid = 102")
      sql("INSERT INTO saj_l_1 VALUES (3, 30)")
      sql("DELETE FROM saj_l_1 WHERE id = 1 AND x = 10")
      sql("INSERT INTO saj_l_1 VALUES (1, 10)")
      refreshMv("mv_saj_semi_1")
      assertMvCorrect("mv_saj_semi_1", viewBody)

      // Second round: delete the right-row whose insertion in the previous
      // batch was paired with a new left tuple (3, 30).  Aux-state
      // implementations must not double-count `current S` + `ΔS` during init.
      sql("DELETE FROM saj_r_1 WHERE rid = 104")
      refreshMv("mv_saj_semi_1")
      assertMvCorrect("mv_saj_semi_1", viewBody)
    }
  }

  // ── (2) ANTI JOIN under GROUP BY ─────────────────────────────────────────
  // Mirrors openivm lines 102-159.  Aggregate over the ANTI-join survival
  // set with batched DML (right INSERT, right DELETE, left UPDATE, left
  // INSERTs).  ANTI JOIN under GROUP BY currently uses FULL_REFRESH rather
  // than aux-state SEMI/ANTI in openivm too.

  describe("(2) ANTI JOIN under GROUP BY — batched DML including UPDATE") {
    it("aggregate over ANTI-join survival set tracks INSERTs/DELETEs/UPDATEs") {
      sql("CREATE TABLE IF NOT EXISTS saj_ag_l_2(id INT, grp INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_ag_r_2(rid INT, y INT) USING DELTA")
      sql(
        "INSERT INTO saj_ag_l_2 VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50)"
      )
      sql("INSERT INTO saj_ag_r_2 VALUES (1, 10), (2, 41)")

      val viewBody =
        "SELECT l.grp, COUNT(*) AS cnt, SUM(l.x) AS total " +
          "FROM saj_ag_l_2 l " +
          "LEFT ANTI JOIN saj_ag_r_2 r ON abs(l.x - r.y) <= 1 " +
          "GROUP BY l.grp"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_ag_anti_2 AS $viewBody")
      assertMvCorrect("mv_saj_ag_anti_2", viewBody)

      sql("INSERT INTO saj_ag_r_2 VALUES (3, 20), (4, 50)")
      sql("DELETE FROM saj_ag_r_2 WHERE rid = 2")
      sql("UPDATE saj_ag_l_2 SET x = 11 WHERE id = 3")
      sql("INSERT INTO saj_ag_l_2 VALUES (6, 3, 60), (7, 3, 20)")
      refreshMv("mv_saj_ag_anti_2")
      assertMvCorrect("mv_saj_ag_anti_2", viewBody)
    }
  }

  // ── (3) ANTI JOIN after an inner-join chain (3 base tables) ──────────────
  // Mirrors openivm lines 161-225.  The left input of ANTI is a join subplan,
  // not a single base table, so the simple aux-state extractor cannot apply.

  describe("(3) ANTI JOIN after JOIN chain — left input is a subplan, not a base table") {
    it("ANTI JOIN over a 3-table join chain handles batched DML across all 3 tables") {
      sql(
        "CREATE TABLE IF NOT EXISTS saj_cj_orders_3(oid INT, cust_id INT, amount INT) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS saj_cj_customers_3(cid INT, region STRING) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS saj_cj_blocked_3(bid INT, region STRING, min_amount INT) USING DELTA"
      )
      sql(
        "INSERT INTO saj_cj_orders_3 VALUES (1, 10, 40), (2, 10, 120), (3, 20, 80), (4, 30, 15)"
      )
      sql("INSERT INTO saj_cj_customers_3 VALUES (10, 'north'), (20, 'south'), (30, 'west')")
      sql("INSERT INTO saj_cj_blocked_3 VALUES (1, 'north', 100), (2, 'west', 10)")

      val viewBody =
        "SELECT o.oid, c.region, o.amount " +
          "FROM saj_cj_orders_3 o JOIN saj_cj_customers_3 c ON o.cust_id = c.cid " +
          "LEFT ANTI JOIN saj_cj_blocked_3 b " +
          "ON c.region = b.region AND o.amount >= b.min_amount"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_chain_anti_3 AS $viewBody")
      assertMvCorrect("mv_saj_chain_anti_3", viewBody)

      sql("INSERT INTO saj_cj_blocked_3 VALUES (3, 'south', 50)")
      sql("DELETE FROM saj_cj_blocked_3 WHERE bid = 2")
      sql("UPDATE saj_cj_orders_3 SET amount = 105 WHERE oid = 1")
      sql("INSERT INTO saj_cj_orders_3 VALUES (5, 30, 20)")
      refreshMv("mv_saj_chain_anti_3")
      assertMvCorrect("mv_saj_chain_anti_3", viewBody)
    }
  }

  // ── (4) SEMI / ANTI with subquery on the RHS ─────────────────────────────
  // Mirrors openivm lines 227-301.  A correlated-filter subquery on the
  // right (`SELECT y FROM sq_r WHERE active`) is bridged with both SEMI and
  // ANTI; batched DML includes an UPDATE that flips the filter.

  describe("(4) SEMI / ANTI with subquery RHS — UPDATE on RHS flips the filter") {
    it("subquery-filtered RHS; UPDATEs to the filter propagate correctly") {
      sql("CREATE TABLE IF NOT EXISTS saj_sq_l_4(id INT, x INT) USING DELTA")
      sql(
        "CREATE TABLE IF NOT EXISTS saj_sq_r_4(rid INT, y INT, active BOOLEAN) USING DELTA"
      )
      sql("INSERT INTO saj_sq_l_4 VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
      sql("INSERT INTO saj_sq_r_4 VALUES (1, 10, true), (2, 20, false), (3, 99, true)")

      val semiBody =
        "SELECT l.id, l.x FROM saj_sq_l_4 l " +
          "LEFT SEMI JOIN (SELECT y FROM saj_sq_r_4 WHERE active) r ON l.x = r.y"
      val antiBody =
        "SELECT l.id, l.x FROM saj_sq_l_4 l " +
          "LEFT ANTI JOIN (SELECT y FROM saj_sq_r_4 WHERE active) r ON l.x = r.y"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_subq_semi_4 AS $semiBody")
      sql(s"CREATE MATERIALIZED VIEW mv_saj_subq_anti_4 AS $antiBody")
      assertMvCorrect("mv_saj_subq_semi_4", semiBody)
      assertMvCorrect("mv_saj_subq_anti_4", antiBody)

      sql("UPDATE saj_sq_r_4 SET active = true WHERE rid = 2")
      sql("INSERT INTO saj_sq_r_4 VALUES (4, 40, true)")
      sql("DELETE FROM saj_sq_l_4 WHERE id = 1")
      sql("INSERT INTO saj_sq_l_4 VALUES (5, 50)")
      refreshMv("mv_saj_subq_semi_4")
      refreshMv("mv_saj_subq_anti_4")
      assertMvCorrect("mv_saj_subq_semi_4", semiBody)
      assertMvCorrect("mv_saj_subq_anti_4", antiBody)
    }
  }

  // ── (5) NOT IN with CAST expressions in output AND predicate ─────────────
  // Mirrors openivm lines 303-353.  Keeps expression-valued aux columns; the
  // openivm RHS contains no NULLs, so standard NOT-IN-NULL gotcha doesn't
  // apply.

  describe("(5) NOT IN with CAST(x AS BIGINT) in output and predicate") {
    it("computed BIGINT cast columns are preserved across refresh") {
      sql("CREATE TABLE IF NOT EXISTS saj_inc_l_5(id INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_inc_r_5(rid INT, y BIGINT) USING DELTA")
      sql("INSERT INTO saj_inc_l_5 VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
      sql("INSERT INTO saj_inc_r_5 VALUES (1, 20), (2, 99)")

      val viewBody =
        "SELECT CAST(x AS BIGINT) AS item_id " +
          "FROM saj_inc_l_5 " +
          "WHERE CAST(x AS BIGINT) NOT IN (SELECT CAST(y AS BIGINT) FROM saj_inc_r_5)"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_not_in_cast_5 AS $viewBody")
      assertMvCorrect("mv_saj_not_in_cast_5", viewBody)

      sql("INSERT INTO saj_inc_l_5 VALUES (5, 50), (6, 20)")
      sql("DELETE FROM saj_inc_r_5 WHERE rid = 1")
      sql("INSERT INTO saj_inc_r_5 VALUES (3, 30), (4, 50)")
      refreshMv("mv_saj_not_in_cast_5")
      assertMvCorrect("mv_saj_not_in_cast_5", viewBody)
    }
  }

  // ── (6) EXISTS / NOT EXISTS with correlated predicate + outer filter ─────
  // Mirrors openivm lines 355-470.  EXISTS / NOT EXISTS lower to SEMI / ANTI;
  // the outer `l.keep` filter is preserved (and tested in both pre- and
  // post-EXISTS positions to exercise predicate ordering).

  describe("(6) EXISTS / NOT EXISTS with correlated predicate + outer filter") {
    it("EXISTS/NOT EXISTS lower to SEMI/ANTI; outer l.keep filter is preserved") {
      sql(
        "CREATE TABLE IF NOT EXISTS saj_ex_l_6(id INT, x INT, keep BOOLEAN) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS saj_ex_r_6(rid INT, y INT, active BOOLEAN) USING DELTA"
      )
      sql(
        "INSERT INTO saj_ex_l_6 VALUES " +
          "(1, 10, true), (1, 10, true), (2, 20, true), (3, 30, false), (4, 40, true)"
      )
      sql(
        "INSERT INTO saj_ex_r_6 VALUES (10, 8, true), (11, 20, false), (12, 99, true)"
      )

      val semiBody =
        "SELECT l.id, l.x FROM saj_ex_l_6 l " +
          "WHERE l.keep AND EXISTS (" +
          "SELECT 1 FROM saj_ex_r_6 r WHERE r.active AND abs(l.x - r.y) <= 2)"
      val antiBody =
        "SELECT l.id, l.x FROM saj_ex_l_6 l " +
          "WHERE NOT EXISTS (" +
          "SELECT 1 FROM saj_ex_r_6 r WHERE r.active AND abs(l.x - r.y) <= 2) " +
          "AND l.keep"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_exists_semi_6 AS $semiBody")
      sql(s"CREATE MATERIALIZED VIEW mv_saj_exists_anti_6 AS $antiBody")
      assertMvCorrect("mv_saj_exists_semi_6", semiBody)
      assertMvCorrect("mv_saj_exists_anti_6", antiBody)

      sql("UPDATE saj_ex_r_6 SET active = true WHERE rid = 11")
      sql("INSERT INTO saj_ex_r_6 VALUES (13, 41, true), (14, 30, true)")
      sql("DELETE FROM saj_ex_r_6 WHERE rid = 10")
      sql("UPDATE saj_ex_l_6 SET keep = true WHERE id = 3")
      sql("INSERT INTO saj_ex_l_6 VALUES (5, 50, true), (6, 40, true)")
      refreshMv("mv_saj_exists_semi_6")
      refreshMv("mv_saj_exists_anti_6")
      assertMvCorrect("mv_saj_exists_semi_6", semiBody)
      assertMvCorrect("mv_saj_exists_anti_6", antiBody)
    }
  }

  // ── (7) EXISTS combined with GROUP BY (aggregate) and UNION ALL ──────────
  // Mirrors openivm lines 472-615.  Aggregates and UNION ALL are outside the
  // simple aux-state shape; openivm itself takes the fallback path for both
  // views.  The skipped openivm-internal probe (`SELECT … FROM openivm_views
  // WHERE … semi_anti_aux_meta_json IS NOT NULL` → expects 0) is not relevant
  // here because the Spark side already takes the FULL_REFRESH path.

  describe("(7) EXISTS combined with GROUP BY and UNION ALL — non-aux-state fallback paths") {
    it("aggregate over NOT EXISTS and UNION ALL of EXISTS+NOT EXISTS stay correct") {
      sql(
        "CREATE TABLE IF NOT EXISTS saj_co_l_7(id INT, grp INT, x INT) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS saj_co_r_7(rid INT, y INT, active BOOLEAN) USING DELTA"
      )
      sql(
        "INSERT INTO saj_co_l_7 VALUES " +
          "(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50)"
      )
      sql(
        "INSERT INTO saj_co_r_7 VALUES (10, 10, true), (11, 19, true), (12, 40, false)"
      )

      val groupBody =
        "SELECT l.grp, COUNT(*) AS cnt, SUM(l.x) AS total " +
          "FROM saj_co_l_7 l " +
          "WHERE NOT EXISTS (" +
          "SELECT 1 FROM saj_co_r_7 r WHERE r.active AND abs(l.x - r.y) <= 1) " +
          "GROUP BY l.grp"
      val unionBody =
        "SELECT l.id, l.x FROM saj_co_l_7 l " +
          "WHERE EXISTS (" +
          "SELECT 1 FROM saj_co_r_7 r WHERE r.active AND abs(l.x - r.y) <= 1) " +
          "UNION ALL " +
          "SELECT l.id, l.x FROM saj_co_l_7 l " +
          "WHERE NOT EXISTS (" +
          "SELECT 1 FROM saj_co_r_7 r WHERE r.active AND abs(l.x - r.y) <= 1) " +
          "AND l.grp = 3"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_exists_group_7 AS $groupBody")
      sql(s"CREATE MATERIALIZED VIEW mv_saj_exists_union_7 AS $unionBody")
      assertMvCorrect("mv_saj_exists_group_7", groupBody)
      assertMvCorrect("mv_saj_exists_union_7", unionBody)

      sql("UPDATE saj_co_r_7 SET active = true WHERE rid = 12")
      sql("INSERT INTO saj_co_r_7 VALUES (13, 31, true), (14, 50, true)")
      sql("DELETE FROM saj_co_r_7 WHERE rid = 10")
      sql("UPDATE saj_co_l_7 SET x = 18 WHERE id = 3")
      sql("INSERT INTO saj_co_l_7 VALUES (6, 3, 60), (7, 1, 11)")
      refreshMv("mv_saj_exists_group_7")
      refreshMv("mv_saj_exists_union_7")
      assertMvCorrect("mv_saj_exists_group_7", groupBody)
      assertMvCorrect("mv_saj_exists_union_7", unionBody)
    }
  }

  // ── (8) ANTI JOIN basic — initial verify + second-round batched DML ──────
  // Mirrors openivm lines 617-648 (initial verify) and lines 953-983
  // (second-round batched DML).  Combined into a single test case here for
  // locality; verifies bag-equivalence at both checkpoints.

  describe("(8) ANTI JOIN basic — initial verify + second-round batched DML") {
    it("ANTI survives multi-round mixed DML on both sides") {
      sql("CREATE TABLE IF NOT EXISTS saj_aaj_l_8(id INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_aaj_r_8(rid INT, y INT) USING DELTA")
      sql(
        "INSERT INTO saj_aaj_l_8 VALUES (1, 10), (2, 20), (3, 30), (3, 30), (4, 40)"
      )
      sql("INSERT INTO saj_aaj_r_8 VALUES (200, 8), (201, 41)")

      val viewBody =
        "SELECT l.id, l.x FROM saj_aaj_l_8 l " +
          "LEFT ANTI JOIN saj_aaj_r_8 r ON abs(l.x - r.y) <= 2"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_anti_8 AS $viewBody")
      assertMvCorrect("mv_saj_anti_8", viewBody)

      // Second-round batched DML: two right INSERTs that should make id=2
      // and id=3 disappear from ANTI; one right DELETE; one left INSERT
      // adding a new row; one left DELETE removing id=1.
      sql("INSERT INTO saj_aaj_r_8 VALUES (202, 20), (203, 29)")
      sql("DELETE FROM saj_aaj_r_8 WHERE rid = 201")
      sql("INSERT INTO saj_aaj_l_8 VALUES (5, 50)")
      sql("DELETE FROM saj_aaj_l_8 WHERE id = 1")
      refreshMv("mv_saj_anti_8")
      assertMvCorrect("mv_saj_anti_8", viewBody)
    }
  }

  // ── (9) Duplicate RHS matches — SEMI / ANTI bag semantics ────────────────
  // Mirrors openivm lines 650-755.  Two right rows match the same `y=10`;
  // SEMI must keep id=1 until BOTH right matches are deleted, then drop it
  // exactly once.  ANTI must mirror: id=1 stays out until both right matches
  // are gone, then reappears.

  describe("(9) Duplicate RHS matches — SEMI keeps row until last RHS match deleted") {
    it("partial RHS retraction does not drop the SEMI row; full retraction does") {
      sql("CREATE TABLE IF NOT EXISTS saj_dup_l_9(id INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_dup_r_9(rid INT, y INT) USING DELTA")
      sql("INSERT INTO saj_dup_l_9 VALUES (1, 10), (2, 20), (3, 30)")
      sql("INSERT INTO saj_dup_r_9 VALUES (10, 10), (11, 10), (20, 20)")

      val semiBody =
        "SELECT l.id, l.x FROM saj_dup_l_9 l LEFT SEMI JOIN saj_dup_r_9 r ON l.x = r.y"
      val antiBody =
        "SELECT l.id, l.x FROM saj_dup_l_9 l LEFT ANTI JOIN saj_dup_r_9 r ON l.x = r.y"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_dup_semi_9 AS $semiBody")
      sql(s"CREATE MATERIALIZED VIEW mv_saj_dup_anti_9 AS $antiBody")

      // Remove one of two duplicate matches for x=10 — SEMI/ANTI must not flip.
      sql("DELETE FROM saj_dup_r_9 WHERE rid = 10")
      refreshMv("mv_saj_dup_semi_9")
      refreshMv("mv_saj_dup_anti_9")
      assertMvCorrect("mv_saj_dup_semi_9", semiBody)
      assertMvCorrect("mv_saj_dup_anti_9", antiBody)

      // Remove the last match — SEMI drops id=1, ANTI gains id=1.
      sql("DELETE FROM saj_dup_r_9 WHERE rid = 11")
      refreshMv("mv_saj_dup_semi_9")
      refreshMv("mv_saj_dup_anti_9")
      assertMvCorrect("mv_saj_dup_semi_9", semiBody)
      assertMvCorrect("mv_saj_dup_anti_9", antiBody)
    }
  }

  // ── (10) UPDATEs (as delete+insert) + NULL-safe `IS NOT DISTINCT FROM` ───
  // Mirrors openivm lines 757-830 (round 1) and lines 907-952 (round 2).
  // UPDATE on the left moves a left identity; UPDATE on the right moves a
  // right match; INSERTs introduce NULLs that must match NULLs in `y` via
  // the NULL-safe `IS NOT DISTINCT FROM`.  Two refresh rounds.

  describe("(10) UPDATEs as delete+insert; NULL-safe predicate IS NOT DISTINCT FROM") {
    it("UPDATE / DELETE / INSERT across L and R; two refresh rounds; NULLs match NULLs") {
      sql("CREATE TABLE IF NOT EXISTS saj_upd_l_10(id INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_upd_r_10(rid INT, y INT) USING DELTA")
      sql(
        "INSERT INTO saj_upd_l_10 VALUES " +
          "(1, 10), (2, CAST(NULL AS INT)), (3, 30), (4, 40)"
      )
      sql(
        "INSERT INTO saj_upd_r_10 VALUES " +
          "(100, 10), (101, CAST(NULL AS INT)), (102, 99)"
      )

      val semiBody =
        "SELECT l.id, l.x FROM saj_upd_l_10 l " +
          "LEFT SEMI JOIN saj_upd_r_10 r ON l.x IS NOT DISTINCT FROM r.y"
      val antiBody =
        "SELECT l.id, l.x FROM saj_upd_l_10 l " +
          "LEFT ANTI JOIN saj_upd_r_10 r ON l.x IS NOT DISTINCT FROM r.y"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_upd_semi_10 AS $semiBody")
      sql(s"CREATE MATERIALIZED VIEW mv_saj_upd_anti_10 AS $antiBody")

      // Round 1: left-identity UPDATE, right-match UPDATE, right DELETE
      // (removing the NULL match), insert two duplicate NULL left rows.
      sql("UPDATE saj_upd_l_10 SET x = 99 WHERE id = 3")
      sql("UPDATE saj_upd_r_10 SET y = 40 WHERE rid = 102")
      sql("DELETE FROM saj_upd_r_10 WHERE rid = 101")
      sql(
        "INSERT INTO saj_upd_l_10 VALUES (5, CAST(NULL AS INT)), (5, CAST(NULL AS INT))"
      )
      refreshMv("mv_saj_upd_semi_10")
      refreshMv("mv_saj_upd_anti_10")
      assertMvCorrect("mv_saj_upd_semi_10", semiBody)
      assertMvCorrect("mv_saj_upd_anti_10", antiBody)

      // Round 2: delete the two NULL left rows, insert a NULL on the right
      // (restores the NULL↔NULL match), insert a single NULL left row.
      sql("DELETE FROM saj_upd_l_10 WHERE id = 5")
      sql("INSERT INTO saj_upd_r_10 VALUES (103, CAST(NULL AS INT))")
      sql("INSERT INTO saj_upd_l_10 VALUES (6, CAST(NULL AS INT))")
      refreshMv("mv_saj_upd_semi_10")
      refreshMv("mv_saj_upd_anti_10")
      assertMvCorrect("mv_saj_upd_semi_10", semiBody)
      assertMvCorrect("mv_saj_upd_anti_10", antiBody)
    }
  }

  // ── (11) Stacked FILTER above SEMI / ANTI ────────────────────────────────
  // Mirrors openivm lines 832-905.  An outer `WHERE l.id >= 2` is stacked
  // above the SEMI / ANTI; aux state tracks ALL left tuples but refresh only
  // materializes the ones passing the outer filter.  Includes an
  // identity-changing UPDATE on the left.

  describe("(11) Stacked FILTER above SEMI / ANTI with identity-changing UPDATE") {
    it("outer WHERE l.id >= 2 is preserved through refresh under L+R DML") {
      sql("CREATE TABLE IF NOT EXISTS saj_stk_l_11(id INT, x INT) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS saj_stk_r_11(rid INT, y INT) USING DELTA")
      sql(
        "INSERT INTO saj_stk_l_11 VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"
      )
      sql("INSERT INTO saj_stk_r_11 VALUES (1, 10), (2, 21), (3, 70)")

      val semiBody =
        "SELECT l.id, l.x FROM saj_stk_l_11 l " +
          "LEFT SEMI JOIN saj_stk_r_11 r ON abs(l.x - r.y) <= 1 WHERE l.id >= 2"
      val antiBody =
        "SELECT l.id, l.x FROM saj_stk_l_11 l " +
          "LEFT ANTI JOIN saj_stk_r_11 r ON abs(l.x - r.y) <= 1 WHERE l.id >= 2"
      sql(s"CREATE MATERIALIZED VIEW mv_saj_stack_semi_11 AS $semiBody")
      sql(s"CREATE MATERIALIZED VIEW mv_saj_stack_anti_11 AS $antiBody")
      assertMvCorrect("mv_saj_stack_semi_11", semiBody)
      assertMvCorrect("mv_saj_stack_anti_11", antiBody)

      sql("INSERT INTO saj_stk_r_11 VALUES (4, 40), (5, 51)")
      sql("UPDATE saj_stk_l_11 SET id = 6 WHERE id = 1")
      sql("DELETE FROM saj_stk_r_11 WHERE rid = 2")
      sql("INSERT INTO saj_stk_l_11 VALUES (7, 70)")
      refreshMv("mv_saj_stack_semi_11")
      refreshMv("mv_saj_stack_anti_11")
      assertMvCorrect("mv_saj_stack_semi_11", semiBody)
      assertMvCorrect("mv_saj_stack_anti_11", antiBody)
    }
  }
}
