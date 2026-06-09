package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** P6d — Port of `openivm/test/sql/ducklake_filter.test`.
  *
  * Translation:
  *   - DuckLake catalog `dl` → Spark/Delta default catalog. The openivm test
  *     uses `dl.<table>` (a DuckLake-attached schema) for both base tables and
  *     materialized views; we mirror the namespace with `dl_<table>` Delta
  *     tables in the default Spark catalog because spark-ext does not model
  *     DuckLake-specific catalog types.
  *   - `ATTACH … (TYPE ducklake)` → no-op (Delta is the only target).
  *   - `PRAGMA refresh('v')` → `REFRESH MATERIALIZED VIEW v`.
  *   - DuckLake-specific pragmas (e.g. `enable_ducklake_lineage`) have no
  *     Delta equivalent and are intentionally omitted (per PLAN §9: the
  *     Delta-equivalent invariant — snapshot read, OCC, no missed deltas — is
  *     verified by the bidirectional EXCEPT ALL checks that follow each
  *     REFRESH).
  *
  * Each scenario uses bidirectional `EXCEPT ALL` between the MV (with hidden
  * `openivm_*` columns stripped) and a freshly-evaluated view body.
  *
  * Source: `.temp/openivm/test/sql/ducklake_filter.test`.
  */
abstract class DucklakeFilterScenarios extends IvmParitySpecBase("ducklake-filter") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── Test 1: MV with WHERE clause, INSERT matching/not-matching filter ─────
  // openivm: ducklake_filter.test §1 ("Test 1")

  describe("(F1) MV with WHERE clause — INSERT matching / not-matching") {
    it("incremental refresh propagates only rows passing the predicate") {
      sql("CREATE TABLE IF NOT EXISTS dl_sensors(id INT, reading INT, status STRING) USING DELTA")
      sql("INSERT INTO dl_sensors VALUES (1, 80, 'ok'), (2, 150, 'warn'), (3, 45, 'ok')")
      sql(
        "CREATE MATERIALIZED VIEW dl_high_readings AS " +
          "SELECT id, reading, status FROM dl_sensors WHERE reading > 100"
      )

      // INSERT matching row
      sql("INSERT INTO dl_sensors VALUES (4, 200, 'critical')")
      refreshMv("dl_high_readings")
      assertMvCorrect(
        "dl_high_readings",
        "SELECT id, reading, status FROM dl_sensors WHERE reading > 100"
      )

      // INSERT non-matching row (reading <= 100) — MV unchanged, filtered out
      sql("INSERT INTO dl_sensors VALUES (5, 30, 'ok')")
      refreshMv("dl_high_readings")
      assertMvCorrect(
        "dl_high_readings",
        "SELECT id, reading, status FROM dl_sensors WHERE reading > 100"
      )
    }
  }

  // ── Test 2: Filtered aggregate (WHERE + GROUP BY) ─────────────────────────
  // openivm: ducklake_filter.test §2 ("Test 2")

  describe("(F2) Filtered aggregate — WHERE amount >= 100 + GROUP BY region") {
    it("incremental refresh applies WHERE before aggregation") {
      sql("CREATE TABLE IF NOT EXISTS dl_orders(id INT, region STRING, amount INT) USING DELTA")
      sql("INSERT INTO dl_orders VALUES (1, 'US', 500), (2, 'EU', 100), (3, 'US', 300), (4, 'EU', 50)")
      sql(
        "CREATE MATERIALIZED VIEW dl_big_orders_by_region AS " +
          "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_orders WHERE amount >= 100 GROUP BY region"
      )

      // Initial state
      assertMvCorrect(
        "dl_big_orders_by_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_orders WHERE amount >= 100 GROUP BY region"
      )

      // Insert: one matching, one not
      sql("INSERT INTO dl_orders VALUES (5, 'EU', 200), (6, 'US', 10)")
      refreshMv("dl_big_orders_by_region")
      assertMvCorrect(
        "dl_big_orders_by_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt " +
          "FROM dl_orders WHERE amount >= 100 GROUP BY region"
      )
    }
  }

  // ── Test 3: Stress — batch INSERT + DELETE before single refresh ─────────
  // openivm: ducklake_filter.test §3 ("Test 3")

  describe("(F3) Stress — batched conflicting DML on filter MV") {
    it("single REFRESH reconciles all batched inserts and deletes") {
      sql("CREATE TABLE IF NOT EXISTS dl_events(id INT, score INT, category STRING) USING DELTA")
      sql(
        "INSERT INTO dl_events VALUES (1, 90, 'A'), (2, 40, 'B'), (3, 70, 'A'), (4, 110, 'B'), (5, 55, 'A')"
      )
      sql(
        "CREATE MATERIALIZED VIEW dl_hot_events AS " +
          "SELECT id, score, category FROM dl_events WHERE score > 50"
      )

      // Cross-check after initial load
      assertMvCorrect(
        "dl_hot_events",
        "SELECT id, score, category FROM dl_events WHERE score > 50"
      )

      // Batch: INSERTs that match filter, INSERTs that do not match, DELETEs in
      // and out of MV.
      sql(
        "INSERT INTO dl_events VALUES (6, 200, 'A'), (7, 10, 'B'), (8, 75, 'A'), (9, 5, 'B'), (10, 300, 'C')"
      )
      sql("DELETE FROM dl_events WHERE id = 1")
      sql("DELETE FROM dl_events WHERE id = 2")
      sql("DELETE FROM dl_events WHERE id = 4")
      sql("INSERT INTO dl_events VALUES (11, 60, 'B'), (12, 999, 'C')")
      sql("DELETE FROM dl_events WHERE id = 5")

      // Single refresh after all batched operations
      refreshMv("dl_hot_events")
      assertMvCorrect(
        "dl_hot_events",
        "SELECT id, score, category FROM dl_events WHERE score > 50"
      )
    }
  }
}
