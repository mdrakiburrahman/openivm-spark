package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Parity tests for RefreshType 8 — DISTINCT_INCREMENTAL, Shape 1.
  *
  * `SELECT DISTINCT region FROM sales` — single-column DISTINCT.
  *
  * Split out of the original `DistinctSpec` so this shape runs in its own
  * forked JVM and contributes ≤ 10 `it(...)` cases per file (parallelism
  * budget for `ivmIt`).
  */
abstract class DistinctSingleColumnScenarios extends IvmParitySpecBase("distinct-single-column") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ══════════════════════════════════════════════════════════════════════════
  // Shape 1 — SELECT DISTINCT region FROM sales  (single column)
  // ══════════════════════════════════════════════════════════════════════════

  describe("Shape-1: SELECT DISTINCT region (single-column)") {

    val baseTable = "dsc_sales_d1"
    val mvName    = "dsc_mv_d1"
    val body      = s"SELECT DISTINCT region FROM $baseTable"

    it("(1a) initial INSERT — MV has one row per unique region") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(region STRING, amount INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES ('east', 100), ('west', 200), ('east', 50)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(1b) INSERT duplicate — MV unchanged; occurrence count incremented internally") {
      sql(s"INSERT INTO $baseTable VALUES ('east', 99)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1c) DELETE one duplicate — MV unchanged; occurrence count decremented") {
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND amount = 99")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1d) DELETE the last copy of a region — MV row disappears") {
      sql(s"DELETE FROM $baseTable WHERE region = 'west'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1e) INSERT a new unique region — MV gains a new row") {
      sql(s"INSERT INTO $baseTable VALUES ('north', 300)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1f) batched DML (5 INSERTs + 3 DELETEs) → single REFRESH → MV ≡ live SELECT DISTINCT") {
      // 2 new values, 2 duplicates, 1 existing;  delete 2 that still have other copies + 1 that becomes last
      sql(s"INSERT INTO $baseTable VALUES ('south', 400)")
      sql(s"INSERT INTO $baseTable VALUES ('south', 401)")
      sql(s"INSERT INTO $baseTable VALUES ('east', 10)")
      sql(s"INSERT INTO $baseTable VALUES ('midwest', 50)")
      sql(s"INSERT INTO $baseTable VALUES ('east', 11)")
      sql(s"DELETE FROM $baseTable WHERE region = 'south' AND amount = 401")
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND amount = 10")
      sql(s"DELETE FROM $baseTable WHERE region = 'north'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1g) NULL deduplication — multiple NULL-region rows fold to one MV row") {
      sql(s"INSERT INTO $baseTable VALUES (NULL, 1), (NULL, 2), (NULL, 3)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
      val nullCount = spark.table(mvName).where("region IS NULL").count()
      withClue("DISTINCT must fold all NULL regions to exactly one MV row: ") {
        nullCount shouldBe 1L
      }
    }
  }
}
