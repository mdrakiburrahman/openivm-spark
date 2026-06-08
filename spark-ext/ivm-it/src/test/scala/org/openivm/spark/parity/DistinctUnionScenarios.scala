package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Parity tests for RefreshType 8 — DISTINCT_INCREMENTAL, Shape 3.
  *
  * `SELECT region FROM sales UNION DISTINCT SELECT region FROM promos` —
  * UNION DISTINCT across two sources.
  *
  * Split out of the original `DistinctSpec` so this shape runs in its own
  * forked JVM and contributes ≤ 10 `it(...)` cases per file.
  */
abstract class DistinctUnionScenarios extends IvmParitySpecBase("distinct-union") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ══════════════════════════════════════════════════════════════════════════
  // Shape 3 — UNION DISTINCT across two tables
  // ══════════════════════════════════════════════════════════════════════════

  describe("Shape-3: SELECT region … UNION DISTINCT SELECT region … (two sources)") {

    val salesTable = "du_sales_d3"
    val promoTable = "du_promos_d3"
    val mvName     = "du_mv_d3"
    val body       = s"SELECT region FROM $salesTable UNION DISTINCT SELECT region FROM $promoTable"

    it("(3a) initial INSERT into both tables — MV has all unique regions from both") {
      sql(s"CREATE TABLE IF NOT EXISTS $salesTable(region STRING, amount INT) USING DELTA")
      sql(s"CREATE TABLE IF NOT EXISTS $promoTable(region STRING, discount INT) USING DELTA")
      sql(s"INSERT INTO $salesTable VALUES ('east', 100), ('west', 200)")
      sql(s"INSERT INTO $promoTable VALUES ('west', 10), ('south', 5)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(3b) INSERT region into sales that already appears in promos — MV unchanged") {
      sql(s"INSERT INTO $salesTable VALUES ('south', 50)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(3c) DELETE the only sales row for a region still present in promos — MV unchanged") {
      sql(s"DELETE FROM $salesTable WHERE region = 'south'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(3d) DELETE region from promos when no sales row remains — MV row disappears") {
      sql(s"DELETE FROM $promoTable WHERE region = 'south'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(3e) INSERT new region only into promos — MV gains a new row") {
      sql(s"INSERT INTO $promoTable VALUES ('midwest', 8)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(3f) batched DML across both sources → single REFRESH → MV ≡ live UNION DISTINCT") {
      sql(s"INSERT INTO $salesTable VALUES ('north', 300), ('east', 99)")
      sql(s"INSERT INTO $promoTable VALUES ('north', 3), ('pacific', 7)")
      sql(s"DELETE FROM $salesTable WHERE region = 'east' AND amount = 99")
      sql(s"DELETE FROM $promoTable WHERE region = 'midwest'")
      sql(s"INSERT INTO $salesTable VALUES ('plains', 400)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(3g) NULL in one source — NULL folds across both sources to one MV row") {
      sql(s"INSERT INTO $salesTable VALUES (NULL, 1)")
      sql(s"INSERT INTO $promoTable VALUES (NULL, 2)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
      val nullCount = spark.table(mvName).where("region IS NULL").count()
      withClue("NULL region must appear exactly once in MV (UNION DISTINCT): ") {
        nullCount shouldBe 1L
      }
    }
  }
}
