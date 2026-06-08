package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Heavy-test isolation spin-off of [[DucklakeWindowSpec]] section (5).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `dlwin_heavy_chain_*` so two
  * parallel JVMs (this one and the host `DucklakeWindowSpec`) cannot collide
  * on Delta paths even if their warehouse directories ever overlap.
  */
abstract class DucklakeWindowHeavyChainScenarios extends IvmParitySpecBase("ducklake-window-heavy-chain") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (5) LAG + ROW_NUMBER over LEFT JOIN of two MVs ────────────────────────

  describe("(5) Window over LEFT JOIN of two upstream MVs (CRM-like + tax-like)") {
    it("upstream refreshes propagate to a downstream window MV; out-of-order refresh recovers correctly") {
      sql(
        "CREATE TABLE IF NOT EXISTS dlwin_heavy_chain_accounts_crm_src(" +
          "ca_id INT, action_ts TIMESTAMP, action_type STRING, " +
          "nat_tax_id INT, lcl_tax_id INT, balance INT) USING DELTA"
      )
      sql(
        "CREATE TABLE IF NOT EXISTS dlwin_heavy_chain_accounts_tax_src(" +
          "tax_id INT, tax_rate DECIMAL(6, 4)) USING DELTA"
      )
      sql(
        "CREATE MATERIALIZED VIEW dlwin_heavy_chain_accounts_crm AS " +
          "SELECT ca_id, action_ts, action_type, nat_tax_id, lcl_tax_id, balance " +
          "FROM dlwin_heavy_chain_accounts_crm_src"
      )
      sql(
        "CREATE MATERIALIZED VIEW dlwin_heavy_chain_accounts_tax AS " +
          "SELECT tax_id, tax_rate FROM dlwin_heavy_chain_accounts_tax_src"
      )

      val downstreamSql =
        "SELECT c.ca_id AS account_id, " +
          "c.action_ts AS start_timestamp, " +
          "COALESCE(" +
          "  LAG(c.action_ts) OVER (PARTITION BY c.ca_id ORDER BY c.action_ts DESC) " +
          "    - INTERVAL 1 MILLISECOND, " +
          "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp, " +
          "CASE WHEN ROW_NUMBER() OVER (PARTITION BY c.ca_id ORDER BY c.action_ts DESC) = 1 " +
          "  THEN TRUE ELSE FALSE END AS is_current, " +
          "c.action_type, c.balance, " +
          "ntx.tax_rate AS nat_tax_rate, ltx.tax_rate AS lcl_tax_rate " +
          "FROM dlwin_heavy_chain_accounts_crm c " +
          "LEFT JOIN dlwin_heavy_chain_accounts_tax ntx ON c.nat_tax_id = ntx.tax_id " +
          "LEFT JOIN dlwin_heavy_chain_accounts_tax ltx ON c.lcl_tax_id = ltx.tax_id " +
          "WHERE c.ca_id IS NOT NULL"
      sql(s"CREATE MATERIALIZED VIEW dlwin_heavy_chain_mv_accounts_like AS $downstreamSql")

      sql("INSERT INTO dlwin_heavy_chain_accounts_tax_src VALUES (1, 0.0500), (2, 0.0750)")
      sql(
        "INSERT INTO dlwin_heavy_chain_accounts_crm_src VALUES " +
          "(10, TIMESTAMP '2026-01-01 09:00:00', 'NEW', 1, 2, 100), " +
          "(20, TIMESTAMP '2026-01-01 10:00:00', 'NEW', 2, 1, 200)"
      )
      refreshMv("dlwin_heavy_chain_accounts_tax")
      refreshMv("dlwin_heavy_chain_accounts_crm")
      refreshMv("dlwin_heavy_chain_mv_accounts_like")
      assertMvCorrect("dlwin_heavy_chain_mv_accounts_like", downstreamSql)

      // Add CRM changes only; refresh CRM then downstream.
      sql(
        "INSERT INTO dlwin_heavy_chain_accounts_crm_src VALUES " +
          "(30, TIMESTAMP '2026-01-02 09:00:00', 'NEW', 1, 2, 300), " +
          "(10, TIMESTAMP '2026-01-02 10:00:00', 'UPDATE', 2, 1, 150)"
      )
      refreshMv("dlwin_heavy_chain_accounts_crm")
      refreshMv("dlwin_heavy_chain_mv_accounts_like")
      assertMvCorrect("dlwin_heavy_chain_mv_accounts_like", downstreamSql)
    }
  }
}
