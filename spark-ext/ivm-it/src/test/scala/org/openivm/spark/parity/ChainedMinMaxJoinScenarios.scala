package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Repro for `.research/OPENIVM-BUG.md`: chained refresh from an upstream
  * AGGREGATE_GROUP MV (MIN/MAX over a conditional-NULL projection) into a
  * downstream SIMPLE_PROJECTION MV that JOINs the upstream MV against a SCD2
  * dim on `upstream.placed BETWEEN dim.eff AND dim.endts`.
  *
  * Mirrors §8.1 of the bug report exactly:
  *
  * {{{
  *   CREATE MATERIALIZED VIEW cmmj_upstream AS
  *     SELECT k, MIN(p) AS placed, MAX(r) AS removed
  *     FROM cmmj_events GROUP BY k;
  *
  *   CREATE MATERIALIZED VIEW cmmj_downstream AS
  *     SELECT u.k, u.placed, u.removed
  *     FROM cmmj_upstream u
  *     JOIN cmmj_dim d
  *       ON u.k = d.k
  *      AND u.placed BETWEEN d.eff AND d.endts;
  * }}}
  *
  * Determinism harness: `local[1]` + `shuffle.partitions=1` +
  * `adaptive.enabled=false` so any partition-iteration-order race the bug
  * report mentions cannot mask the failure.
  */
abstract class ChainedMinMaxJoinScenarios extends IvmParitySpecBase("chained-min-max-join") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def refreshChain(mvs: String*): Unit =
    mvs.foreach(m => sql(s"REFRESH MATERIALIZED VIEW $m").collect())

  // ──────────────────────────────────────────────────────────────────────────
  // (1) Update-miss: batch 1 ACTV (p, NULL) then batch 2 CNCL (NULL, r) for
  //     the same key — silver.watches MIN/MAX merges them into (p, r); the
  //     downstream SCD2 BETWEEN join must DELETE the stale (p, NULL) row
  //     from cmmj_downstream AND INSERT (p, r) under the same dim version.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(1) Update-miss: chained MIN/MAX → JOIN BETWEEN SCD2 dim") {

    it("downstream stays consistent when an upstream row mutates only its non-key columns") {
      sql("CREATE TABLE IF NOT EXISTS cmmj_events(k INT, p TIMESTAMP, r TIMESTAMP) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS cmmj_dim(k INT, eff TIMESTAMP, endts TIMESTAMP) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW cmmj_upstream AS " +
          "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k"
      )
      sql(
        "CREATE MATERIALIZED VIEW cmmj_downstream AS " +
          "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM cmmj_upstream u " +
          "JOIN cmmj_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 1: insert ACTV (p, NULL) and the dim version that spans
      // the entire range we care about. Refresh both MVs.
      sql(
        "INSERT INTO cmmj_events VALUES " +
          "(1, TIMESTAMP '2016-01-01 00:00:00', NULL)"
      )
      sql(
        "INSERT INTO cmmj_dim VALUES " +
          "(1, TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00')"
      )
      refreshChain("cmmj_upstream", "cmmj_downstream")

      assertMvCorrect(
        "cmmj_upstream",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k"
      )
      assertMvCorrect(
        "cmmj_downstream",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k) u " +
          "JOIN cmmj_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 2: insert CNCL (NULL, r) for the SAME key. The upstream MV
      // MIN/MAX rolls (1, '2016-01-01', NULL) into (1, '2016-01-01', '2017-01-01').
      // The downstream SCD2 BETWEEN match on placed=2016-01-01 is unchanged
      // (still falls inside the [2000,2030] window) but the row's `removed`
      // projection column changes from NULL to '2017-01-01'.
      sql(
        "INSERT INTO cmmj_events VALUES " +
          "(1, NULL, TIMESTAMP '2017-01-01 00:00:00')"
      )
      refreshChain("cmmj_upstream", "cmmj_downstream")

      assertMvCorrect(
        "cmmj_upstream",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k"
      )
      assertMvCorrect(
        "cmmj_downstream",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events GROUP BY k) u " +
          "JOIN cmmj_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) Insert-miss: brand-new upstream group in batch 2. No prior row to
  //     mutate — the downstream should pick up the brand-new joined row.
  //     Bug report §3 calls this "pure-insert miss" on customer 6e228a…
  // ──────────────────────────────────────────────────────────────────────────

  describe("(2) Insert-miss: brand-new upstream key in batch 2") {

    it("downstream picks up a brand-new key whose first event is an ACTV") {
      sql("CREATE TABLE IF NOT EXISTS cmmj_events2(k INT, p TIMESTAMP, r TIMESTAMP) USING DELTA")
      sql("CREATE TABLE IF NOT EXISTS cmmj_dim2(k INT, eff TIMESTAMP, endts TIMESTAMP) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW cmmj_upstream2 AS " +
          "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k"
      )
      sql(
        "CREATE MATERIALIZED VIEW cmmj_downstream2 AS " +
          "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM cmmj_upstream2 u " +
          "JOIN cmmj_dim2 d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 1: ACTV for k=1 + dim versions covering both k=1 and k=2.
      sql(
        "INSERT INTO cmmj_events2 VALUES " +
          "(1, TIMESTAMP '2016-01-01 00:00:00', NULL)"
      )
      sql(
        "INSERT INTO cmmj_dim2 VALUES " +
          "(1, TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00')," +
          "(2, TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00')"
      )
      refreshChain("cmmj_upstream2", "cmmj_downstream2")
      assertMvCorrect(
        "cmmj_downstream2",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k) u " +
          "JOIN cmmj_dim2 d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 2: brand-new key 2 with a single ACTV. Insert miss check:
      // the new group must propagate through both MVs.
      sql(
        "INSERT INTO cmmj_events2 VALUES " +
          "(2, TIMESTAMP '2017-07-08 00:00:00', NULL)"
      )
      refreshChain("cmmj_upstream2", "cmmj_downstream2")
      assertMvCorrect(
        "cmmj_upstream2",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k"
      )
      assertMvCorrect(
        "cmmj_downstream2",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj_events2 GROUP BY k) u " +
          "JOIN cmmj_dim2 d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (3) TPC-DI fact_watches shape (3-way JOIN, dim is itself an SCD2 MV that
  //     mutates in the same batch). Mirrors the actual failing CI shape:
  //
  //     events_hist (DML)
  //        └── upstream (AGGREGATE_GROUP, MIN/MAX over conditional-NULL)
  //                                           │
  //     customers_raw (DML)                   │
  //        └── dim (WINDOW_PARTITION SCD2)    │
  //                                           ▼
  //                              downstream (SIMPLE_PROJECTION, 2-way JOIN
  //                                          with BETWEEN on dim.eff/end)
  //
  //   Both the upstream MV (silver.watches-equivalent) AND the SCD2 dim MV
  //   (dim_customer-equivalent) emit a cascade-delta in batch 2, exercising
  //   the multi-delta IVM rule on the downstream. Two scenarios:
  //
  //     (a) Update: existing customer's watch UPDATE (ACTV→CNCL) +
  //         dim_customer SCD2 update for the same customer.
  //     (b) Insert: brand-new customer (no prior dim row) with first ACTV.
  // ──────────────────────────────────────────────────────────────────────────

  describe("(3) Multi-delta cascade: upstream AGGREGATE_GROUP + SCD2 dim MV") {

    it("downstream insert-miss / update-miss when both upstream and dim emit cascade-deltas") {
      // ── Source tables (DML targets). `cmmj3_events` mirrors silver.watches's
      // source (watches_history); `cmmj3_customers_raw` mirrors silver.customers.
      sql(
        "CREATE TABLE IF NOT EXISTS cmmj3_events(k INT, p TIMESTAMP, r TIMESTAMP) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS cmmj3_customers_raw(k INT, name STRING, eff TIMESTAMP) USING DELTA"
      )

      // ── Upstream MV: AGGREGATE_GROUP with MIN/MAX (silver.watches shape).
      sql(
        "CREATE MATERIALIZED VIEW cmmj3_upstream AS " +
          "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj3_events GROUP BY k"
      )

      // ── SCD2 dim MV (dim_customer-equivalent). WINDOW_PARTITION with
      // ROW_NUMBER (synthetic surrogate key) + LEAD on `eff` to derive
      // `end` from the next version's effective_timestamp.  Matches the
      // TPC-DI dim_customer compile path (WINDOW_PARTITION RefreshType=5).
      sql(
        "CREATE MATERIALIZED VIEW cmmj3_dim AS " +
          "SELECT k, name, eff, " +
          "COALESCE(LEAD(eff) OVER (PARTITION BY k ORDER BY eff) - INTERVAL 1 SECOND, " +
          "         TIMESTAMP '9999-12-31 00:00:00') AS endts " +
          "FROM cmmj3_customers_raw"
      )

      // ── Downstream MV: SIMPLE_PROJECTION 2-way JOIN with SCD2 BETWEEN.
      sql(
        "CREATE MATERIALIZED VIEW cmmj3_downstream AS " +
          "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM cmmj3_upstream u " +
          "JOIN cmmj3_dim d ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 1: customer A's first ACTV + initial customer registration.
      // Layout:
      //   customer k=1 ("alice"): registered 2000-01-01; ACTV watch on 2016-03-22
      //   (Both will get a batch-2 mutation to reproduce the failing CI case.)
      sql(
        "INSERT INTO cmmj3_events VALUES " +
          "(1, TIMESTAMP '2016-03-22 00:00:00', NULL)"
      )
      sql(
        "INSERT INTO cmmj3_customers_raw VALUES " +
          "(1, 'alice', TIMESTAMP '2000-01-01 00:00:00')"
      )
      refreshChain("cmmj3_upstream", "cmmj3_dim", "cmmj3_downstream")

      assertMvCorrect(
        "cmmj3_downstream",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj3_events GROUP BY k) u " +
          "JOIN (SELECT k, name, eff, " +
          "       COALESCE(LEAD(eff) OVER (PARTITION BY k ORDER BY eff) - INTERVAL 1 SECOND, " +
          "                TIMESTAMP '9999-12-31 00:00:00') AS endts " +
          "      FROM cmmj3_customers_raw) d " +
          "ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )

      // ── Batch 2: mutate BOTH upstream and dim_customer at the same time.
      //
      // (a) UPDATE: customer 1's ACTV gets a CNCL on 2017-07-08 → upstream
      //     row updates from (placed=2016-03-22, removed=NULL) to
      //     (placed=2016-03-22, removed=2017-07-08).
      // (b) UPDATE: customer 1 ALSO gets an SCD2 name change effective
      //     2018-01-01 (still after the watch placement, so dim BETWEEN
      //     match is unchanged for the placed=2016-03-22 row, but the
      //     dim MV emits a cascade-delta).
      // (c) INSERT: brand-new customer k=2 ("bob") with first ACTV on
      //     2017-07-08 — pure insert miss case (6e228a in the CI diff).
      sql(
        "INSERT INTO cmmj3_events VALUES " +
          "(1, NULL, TIMESTAMP '2017-07-08 00:00:00'), " +
          "(2, TIMESTAMP '2017-07-08 00:00:00', NULL)"
      )
      sql(
        "INSERT INTO cmmj3_customers_raw VALUES " +
          "(1, 'alice-renamed', TIMESTAMP '2018-01-01 00:00:00'), " +
          "(2, 'bob', TIMESTAMP '2017-01-01 00:00:00')"
      )
      refreshChain("cmmj3_upstream", "cmmj3_dim", "cmmj3_downstream")

      assertMvCorrect(
        "cmmj3_upstream",
        "SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj3_events GROUP BY k"
      )
      assertMvCorrect(
        "cmmj3_dim",
        "SELECT k, name, eff, " +
          "COALESCE(LEAD(eff) OVER (PARTITION BY k ORDER BY eff) - INTERVAL 1 SECOND, " +
          "         TIMESTAMP '9999-12-31 00:00:00') AS endts " +
          "FROM cmmj3_customers_raw"
      )
      assertMvCorrect(
        "cmmj3_downstream",
        "SELECT u.k AS k, u.placed AS placed, u.removed AS removed " +
          "FROM (SELECT k, MIN(p) AS placed, MAX(r) AS removed FROM cmmj3_events GROUP BY k) u " +
          "JOIN (SELECT k, name, eff, " +
          "       COALESCE(LEAD(eff) OVER (PARTITION BY k ORDER BY eff) - INTERVAL 1 SECOND, " +
          "                TIMESTAMP '9999-12-31 00:00:00') AS endts " +
          "      FROM cmmj3_customers_raw) d " +
          "ON u.k = d.k AND u.placed BETWEEN d.eff AND d.endts"
      )
    }
  }
}
