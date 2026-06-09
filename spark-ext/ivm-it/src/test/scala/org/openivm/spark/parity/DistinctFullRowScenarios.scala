package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Parity tests for RefreshType 8 — DISTINCT_INCREMENTAL, Shape 4.
  *
  * `SELECT DISTINCT * FROM sales` — full-row deduplication.
  *
  * Split out of the original `DistinctSpec` so this shape runs in its own
  * forked JVM and contributes ≤ 10 `it(...)` cases per file.
  */
abstract class DistinctFullRowScenarios extends IvmParitySpecBase("distinct-full-row") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ══════════════════════════════════════════════════════════════════════════
  // Shape 4 — SELECT DISTINCT * FROM sales  (full-row)
  // ══════════════════════════════════════════════════════════════════════════

  describe("Shape-4: SELECT DISTINCT * (full-row deduplication)") {

    val baseTable = "dfr_sales_d4"
    val mvName    = "dfr_mv_d4"
    val body      = s"SELECT DISTINCT * FROM $baseTable"

    it("(4a) initial INSERT with exact duplicate rows — MV has one row per unique combination") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(region STRING, product STRING) USING DELTA")
      // Three rows: two copies of ('east','widget'), one of ('west','gadget')
      sql(s"INSERT INTO $baseTable VALUES ('east', 'widget'), ('west', 'gadget'), ('east', 'widget')")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
      // Deduplication: ('east','widget') must appear exactly once
      spark
        .table(mvName)
        .where("region = 'east' AND product = 'widget'")
        .count() shouldBe 1L
    }

    it("(4b) INSERT more exact duplicate rows — MV unchanged; occurrence count incremented") {
      sql(s"INSERT INTO $baseTable VALUES ('east', 'widget'), ('east', 'widget')")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(4c) DELETE ALL copies of a row — MV row disappears") {
      // ('east','widget') was inserted 4 times total; delete all of them
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND product = 'widget'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(4d) INSERT a new unique row — MV gains a new row") {
      sql(s"INSERT INTO $baseTable VALUES ('north', 'doohickey')")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(4e) batched DML → single REFRESH → MV ≡ live SELECT DISTINCT *") {
      // Add new unique pairs and duplicates, then remove some entirely
      sql(s"INSERT INTO $baseTable VALUES ('south', 'gizmo')")
      sql(s"INSERT INTO $baseTable VALUES ('south', 'gizmo')")
      sql(s"INSERT INTO $baseTable VALUES ('east', 'gadget'), ('east', 'gadget')")
      sql(s"INSERT INTO $baseTable VALUES ('midwest', 'thingamajig')")
      // Remove 'west','gadget' (previously the only copy → MV row disappears)
      sql(s"DELETE FROM $baseTable WHERE region = 'west' AND product = 'gadget'")
      // Remove all copies of 'south','gizmo' (both inserted above → MV row disappears)
      sql(s"DELETE FROM $baseTable WHERE region = 'south' AND product = 'gizmo'")
      // Remove 'north','doohickey' from 4d
      sql(s"DELETE FROM $baseTable WHERE region = 'north' AND product = 'doohickey'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(4f) NULL full-row — (NULL, NULL) row folds to exactly one MV row") {
      sql(s"INSERT INTO $baseTable VALUES (NULL, NULL), (NULL, NULL)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
      val nullCount = spark.table(mvName).where("region IS NULL AND product IS NULL").count()
      withClue("Full-row (NULL,NULL) must appear exactly once in MV: ") {
        nullCount shouldBe 1L
      }
    }
  }
}
