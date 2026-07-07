package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Port of `openivm/test/sql/ducklake_distinct.test`.
  *
  * Translation:
  *   - DuckLake catalog `dl.<table>` → Delta tables in the default Spark
  *     catalog (named `dl_<table>` to preserve traceability).
  *   - `ATTACH … (TYPE ducklake)` → no-op.
  *   - `PRAGMA refresh('v')` → `REFRESH MATERIALIZED VIEW v`.
  *   - DuckLake snapshot semantics map onto Delta snapshot reads: the Delta
  *     equivalent of "DuckLake reads inserted/deleted rows between snapshots"
  *     is the staging delta produced by the DML interceptor + the MV's
  *     Delta version advance recorded in `MvCatalog`. Correctness is verified
  *     end-to-end via bidirectional `EXCEPT ALL`.
  *
  * Source: `.temp/openivm/test/sql/ducklake_distinct.test`.
  */
abstract class DucklakeDistinctScenarios extends IvmParitySpecBase("ducklake-distinct") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── Basic DISTINCT ────────────────────────────────────────────────────────
  // openivm: ducklake_distinct.test "Basic DISTINCT"

  describe("(DD1) Basic DISTINCT — duplicate inserts leave MV unchanged") {
    it("incremental refresh dedupes inserts and reflects new values") {
      sql("CREATE TABLE IF NOT EXISTS dl_tags(id INT, tag STRING) USING DELTA")
      sql("INSERT INTO dl_tags VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green')")
      sql("CREATE MATERIALIZED VIEW dl_mv_tags AS SELECT DISTINCT tag FROM dl_tags")

      // Initial state
      assertMvCorrect("dl_mv_tags", "SELECT DISTINCT tag FROM dl_tags")

      // Insert duplicate — should not change the user-visible set
      sql("INSERT INTO dl_tags VALUES (5, 'red')")
      refreshMv("dl_mv_tags")
      assertMvCorrect("dl_mv_tags", "SELECT DISTINCT tag FROM dl_tags")

      // Insert new distinct value
      sql("INSERT INTO dl_tags VALUES (6, 'yellow')")
      refreshMv("dl_mv_tags")
      assertMvCorrect("dl_mv_tags", "SELECT DISTINCT tag FROM dl_tags")
    }
  }

  // ── Stress: batch INSERT + DELETE ────────────────────────────────────────
  // openivm: ducklake_distinct.test "Stress: batch INSERT + DELETE"

  describe("(DD2) Stress — batched DELETEs + INSERTs before single refresh") {
    it("DELETEs that empty a distinct group remove it; re-introductions reappear") {
      sql("CREATE TABLE IF NOT EXISTS dl_tags2(id INT, tag STRING) USING DELTA")
      sql(
        "INSERT INTO dl_tags2 VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green'), (5, 'red'), (6, 'yellow')"
      )
      sql("CREATE MATERIALIZED VIEW dl_mv_tags2 AS SELECT DISTINCT tag FROM dl_tags2")

      // Delete all 'red' entries, insert 'purple', add another 'blue' — all in
      // one batch before a single refresh
      sql("DELETE FROM dl_tags2 WHERE tag = 'red'")
      sql("INSERT INTO dl_tags2 VALUES (7, 'purple')")
      sql("INSERT INTO dl_tags2 VALUES (8, 'blue')")
      refreshMv("dl_mv_tags2")

      assertMvCorrect("dl_mv_tags2", "SELECT DISTINCT tag FROM dl_tags2")
    }
  }
}
