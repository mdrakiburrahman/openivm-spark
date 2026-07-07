package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Port of `openivm/test/sql/ducklake_chained.test`.
  *
  * Depth limit (per PLAN §12: "MV-over-MV cascade depth > 2" is explicitly
  * out of scope):
  *   - Depth-2 chains (base → mv1 → mv2) are ported here.
  *   - The openivm `cross_schema_l2 AS SELECT id, amount FROM
  *     dl.silver.openivm_data_cross_schema_l1` shape reads the upstream MV's
  *     internal data table. spark-ext uses a different naming convention
  *     (`<mv_name>` IS the data table for incremental MVs; only HAVING views
  *     split into `<mv>` + `<mv>__ivm_data`), so we read the MV directly.
  *
  * Other DuckLake-specific translations / N/A skips:
  *   - openivm cascade modes (`SET openivm_cascade_refresh = 'downstream' /
  *     'upstream'`) have no analogue in spark-ext: REFRESH is single-MV by
  *     design (per PLAN §11.5 — topo-sort cascade is left to a separate
  *     phase). The corresponding openivm scenarios are documented but not
  *     ported.
  *   - Multi-catalog ATTACH names (`chain_a`, `chain_b`) collapse to flat
  *     table names in the default Spark+Delta catalog (`chain_a_pipeline_src`,
  *     `chain_b_pipeline_l1`, ...).
  *
  * Downstream-refresh caveat:
  *   `IvmDmlInterceptorRule.bypass` is set to `true` during REFRESH so that
  *   the openivm-emitted MERGE program does not re-enter the interceptor.
  *   This means writes to mv1's Delta table during REFRESH do NOT generate
  *   staging entries for mv2. The depth-2 incremental refresh test below
  *   therefore exercises only what is observable end-to-end through the
  *   chain — see the test's inline comment.
  *
  * Source: `.temp/openivm/test/sql/ducklake_chained.test`.
  */
abstract class DucklakeChainedScenarios extends IvmParitySpecBase("ducklake-chained") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (DCh-1) Initial depth-2 chain — CTAS reads correct upstream state ────
  // openivm: ducklake_chained.test "2-level chain: dl.sales → dl.mv_region → dl.mv_top"
  // (the initial-state and post-CREATE checks)

  describe("(DCh-1) Initial depth-2 chain — CTAS at CREATE time") {
    it("mv2 created over mv1 reflects mv1's current data") {
      sql("CREATE TABLE IF NOT EXISTS dl_ch1_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO dl_ch1_sales VALUES " +
          "(1, 'east', 100), (2, 'west', 200), (3, 'east', 150), (4, 'south', 50)"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_ch1_mv_region AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch1_sales GROUP BY region"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_ch1_mv_top AS " +
          "SELECT region, total, cnt FROM dl_ch1_mv_region WHERE total > 100"
      )

      // Both MVs must be correct at this point — direct CTAS read.
      assertMvCorrect(
        "dl_ch1_mv_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch1_sales GROUP BY region"
      )
      assertMvCorrect(
        "dl_ch1_mv_top",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_ch1_sales GROUP BY region HAVING SUM(amount) > 100"
      )
    }
  }

  // ── (DCh-2) Refresh mv1 alone — mv1 picks up base-table DML ──────────────
  // openivm: ducklake_chained.test "Insert: boost south above threshold"
  //
  // mv2 incremental refresh would require staging deltas on mv1, which are NOT
  // emitted today (bypass-during-refresh, documented above). So this scenario
  // verifies only mv1's incremental correctness; mv2 is verified by a fresh
  // re-creation that performs a new CTAS over current mv1.

  describe("(DCh-2) Depth-2 chain — incremental REFRESH of mv1 keeps mv1 correct") {
    it("after INSERT into base, REFRESH mv_region — mv_region matches grouped SUM") {
      sql("CREATE TABLE IF NOT EXISTS dl_ch2_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO dl_ch2_sales VALUES " +
          "(1, 'east', 100), (2, 'west', 200), (3, 'east', 150), (4, 'south', 50)"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_ch2_mv_region AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch2_sales GROUP BY region"
      )

      // Boost south above threshold (openivm comment)
      sql("INSERT INTO dl_ch2_sales VALUES (5, 'south', 200)")
      refreshMv("dl_ch2_mv_region")
      assertMvCorrect(
        "dl_ch2_mv_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch2_sales GROUP BY region"
      )

      // Delete: drop east below threshold (openivm comment)
      sql("DELETE FROM dl_ch2_sales WHERE id = 1")
      refreshMv("dl_ch2_mv_region")
      assertMvCorrect(
        "dl_ch2_mv_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch2_sales GROUP BY region"
      )

      // Stress: batched insert + delete + insert before single refresh
      sql("INSERT INTO dl_ch2_sales VALUES (6, 'north', 500)")
      sql("DELETE FROM dl_ch2_sales WHERE id = 4")
      sql("INSERT INTO dl_ch2_sales VALUES (7, 'west', 100)")
      refreshMv("dl_ch2_mv_region")
      assertMvCorrect(
        "dl_ch2_mv_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch2_sales GROUP BY region"
      )
    }
  }

  // ── (DCh-3) Depth-2 chain — mv2 reads current mv1 at CREATE time ─────────
  // For each base-table edit + mv1 refresh, mv2 is re-created (DROP+CREATE) so
  // we can verify the depth-2 dependency without relying on downstream
  // staging propagation. This is the workaround for the bypass limitation
  // documented in the class-level scaladoc.

  describe("(DCh-3) Depth-2 chain — mv2 = SELECT…FROM mv1 reflects each mv1 refresh") {
    it("each DROP+CREATE of mv2 picks up mv1's most recent state") {
      sql("CREATE TABLE IF NOT EXISTS dl_ch3_sales(id INT, region STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO dl_ch3_sales VALUES " +
          "(1, 'east', 100), (2, 'west', 200), (3, 'east', 150), (4, 'south', 50)"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_ch3_mv_region AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM dl_ch3_sales GROUP BY region"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_ch3_mv_top AS " +
          "SELECT region, total, cnt FROM dl_ch3_mv_region WHERE total > 100"
      )
      assertMvCorrect(
        "dl_ch3_mv_top",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_ch3_sales GROUP BY region HAVING SUM(amount) > 100"
      )

      // Three rounds of base DML → mv1 refresh → mv2 re-CREATE → bidirectional check.
      val rounds = Seq(
        "INSERT INTO dl_ch3_sales VALUES (5, 'south', 200)",
        "DELETE FROM dl_ch3_sales WHERE id = 1",
        "INSERT INTO dl_ch3_sales VALUES (6, 'north', 500); DELETE FROM dl_ch3_sales WHERE id = 4"
      )
      rounds.foreach { sqlBatch =>
        sqlBatch.split(";\\s*").foreach(stmt => sql(stmt.trim))
        refreshMv("dl_ch3_mv_region")
        // Re-CREATE mv2 so its CTAS sees current mv1 — verifies the chain
        // depth-2 relationship without relying on downstream staging.
        sql("DROP MATERIALIZED VIEW dl_ch3_mv_top")
        sql(
          "CREATE MATERIALIZED VIEW dl_ch3_mv_top AS " +
            "SELECT region, total, cnt FROM dl_ch3_mv_region WHERE total > 100"
        )
        assertMvCorrect(
          "dl_ch3_mv_top",
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
            "FROM dl_ch3_sales GROUP BY region HAVING SUM(amount) > 100"
        )
      }
    }
  }

  // ── (DCh-4) Multi-namespace pipeline — chain_a → chain_b → chain_a ───────
  // openivm: ducklake_chained.test "Cross-catalog DuckLake pipeline with non-dl catalog names"
  // Translation: distinct prefixed table names in the default Spark catalog
  // play the role of two separate DuckLake catalogs.

  describe("(DCh-4) Two-namespace depth-2 chain — verifies CTAS through mv1 to mv2") {
    it("pipeline_l2 chains correctly through pipeline_l1") {
      sql("CREATE TABLE IF NOT EXISTS chain_a_pipeline_src(id INT, amount INT) USING DELTA")
      sql("INSERT INTO chain_a_pipeline_src VALUES (1, 10), (2, 20)")
      sql(
        "CREATE MATERIALIZED VIEW chain_b_pipeline_l1 AS " +
          "SELECT id, amount FROM chain_a_pipeline_src"
      )
      sql(
        "CREATE MATERIALIZED VIEW chain_a_pipeline_l2 AS " +
          "SELECT id, amount, amount * 2 AS doubled FROM chain_b_pipeline_l1"
      )

      // Initial state — direct CTAS reads
      assertMvCorrect(
        "chain_b_pipeline_l1",
        "SELECT id, amount FROM chain_a_pipeline_src"
      )
      assertMvCorrect(
        "chain_a_pipeline_l2",
        "SELECT id, amount, amount * 2 AS doubled FROM chain_a_pipeline_src"
      )

      // After a new INSERT + mv1 refresh + mv2 re-CREATE, the chain stays
      // consistent. (See DCh-3 for the rationale for re-CREATE-ing mv2.)
      sql("INSERT INTO chain_a_pipeline_src VALUES (3, 30)")
      refreshMv("chain_b_pipeline_l1")
      sql("DROP MATERIALIZED VIEW chain_a_pipeline_l2")
      sql(
        "CREATE MATERIALIZED VIEW chain_a_pipeline_l2 AS " +
          "SELECT id, amount, amount * 2 AS doubled FROM chain_b_pipeline_l1"
      )
      assertMvCorrect(
        "chain_b_pipeline_l1",
        "SELECT id, amount FROM chain_a_pipeline_src"
      )
      assertMvCorrect(
        "chain_a_pipeline_l2",
        "SELECT id, amount, amount * 2 AS doubled FROM chain_a_pipeline_src"
      )
    }
  }

  // ── (DCh-5) Projection chain — catalog_chain_base → l1 → l2 ──────────────
  // openivm: ducklake_chained.test "Projection chains use the upstream MV's
  // materialized data table directly."

  describe("(DCh-5) Projection-only depth-2 chain") {
    it("catalog_chain_l2 (projection over catalog_chain_l1) tracks base via CTAS re-creation") {
      sql("CREATE TABLE IF NOT EXISTS dl_catalog_chain_base(id INT, amount INT) USING DELTA")
      sql("INSERT INTO dl_catalog_chain_base VALUES (1, 10), (2, 20)")
      sql(
        "CREATE MATERIALIZED VIEW dl_catalog_chain_l1 AS " +
          "SELECT id, amount FROM dl_catalog_chain_base"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_catalog_chain_l2 AS " +
          "SELECT id, amount FROM dl_catalog_chain_l1"
      )
      assertMvCorrect(
        "dl_catalog_chain_l2",
        "SELECT id, amount FROM dl_catalog_chain_base"
      )

      sql("INSERT INTO dl_catalog_chain_base VALUES (3, 30)")
      refreshMv("dl_catalog_chain_l1")
      sql("DROP MATERIALIZED VIEW dl_catalog_chain_l2")
      sql(
        "CREATE MATERIALIZED VIEW dl_catalog_chain_l2 AS " +
          "SELECT id, amount FROM dl_catalog_chain_l1"
      )
      assertMvCorrect("dl_catalog_chain_l2", "SELECT id, amount FROM dl_catalog_chain_base")
    }
  }

  // ── (DCh-6) Cascade-mode tests are N/A in spark-ext ──────────────────────
  // The openivm sections "Cascade mode: downstream" and "Cascade mode: upstream"
  // exercise `SET openivm_cascade_refresh = 'downstream' / 'upstream'`. spark-ext
  // has no equivalent: REFRESH is single-MV (one MV per REFRESH transaction)
  // per PLAN §12. The user must explicitly REFRESH each MV in topological
  // order. This is documented here rather than tested.

  describe("(DCh-6) Cascade-mode N/A in spark-ext — documented limitation") {
    it("there is no openivm_cascade_refresh equivalent; REFRESH is per-MV") {
      // Pure documentation test: assert that the cascade-mode flag is not
      // honored as a Spark conf — confirming the N/A status.
      val confKeys =
        spark.conf.getAll.keys.filter(k =>
          k.toLowerCase.contains("cascade") || k.toLowerCase.contains("openivm.refresh.mode")
        )
      confKeys shouldBe empty
    }
  }
}
