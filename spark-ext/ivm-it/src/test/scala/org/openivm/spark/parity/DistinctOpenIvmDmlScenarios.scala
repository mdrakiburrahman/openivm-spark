package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** 1:1 ScalaTest port of openivm `test/sql/distinct.test` — DML / expression
  * sections (4), (5) and (9).
  *
  * Split out of the original `DistinctOpenIvmSpec` so each file contributes
  * ≤ 10 `it(...)` cases per forked JVM. Covers:
  *
  *   4.  DISTINCT over all-NULL rows        (distinct.test:157‒240)
  *   5.  DISTINCT over collapsing ABS(val)  (distinct.test:242‒350)
  *   9.  DISTINCT + ORDER BY + UPDATE       (distinct.test:526‒593)
  */
abstract class DistinctOpenIvmDmlScenarios extends IvmParitySpecBase("distinct-open-ivm-dml") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ══════════════════════════════════════════════════════════════════════════
  // (4) DISTINCT with all-NULL rows
  //     openivm distinct.test:157‒240  (dist_all_null / mv_dist_all_null)
  // ══════════════════════════════════════════════════════════════════════════

  describe("(4) DISTINCT over rows that include NULLs (dist_all_null / mv_dist_all_null)") {

    val baseTable = "oid_dist_all_null"
    val mvName    = "oid_mv_dist_all_null"
    val body      = s"SELECT DISTINCT a, b FROM $baseTable"

    it("(4a) (NULL,NULL), (NULL,NULL), (1,NULL), (NULL,'x') folds to 3 distinct rows") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(a INT, b STRING) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES (NULL, NULL), (NULL, NULL), (1, NULL), (NULL, 'x')")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(4b) DELETE both copies of (NULL,NULL) — row disappears from MV") {
      sql(s"DELETE FROM $baseTable WHERE a IS NULL AND b IS NULL")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(4c) INSERT (NULL,NULL) back — row reappears in MV") {
      sql(s"INSERT INTO $baseTable VALUES (NULL, NULL)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // (5) DISTINCT with an expression that collapses values: ABS(val)
  //     openivm distinct.test:242‒350  (dist_collapse / mv_dist_collapse)
  // ══════════════════════════════════════════════════════════════════════════

  describe("(5) DISTINCT ABS(val) — collapsing expression (dist_collapse / mv_dist_collapse)") {

    val baseTable = "oid_dist_collapse"
    val mvName    = "oid_mv_dist_collapse"
    val body      = s"SELECT DISTINCT ABS(val) AS abs_val FROM $baseTable"

    it("(5a) {-5,5,-10,10,0,0} folds to 3 distinct abs values {0,5,10}") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(val INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES (-5), (5), (-10), (10), (0), (0)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(5b) DELETE val=-5 — ABS(5) still produced by val=5, MV unchanged") {
      sql(s"DELETE FROM $baseTable WHERE val = -5")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(5c) DELETE val=5 — ABS(5) no longer produced, MV loses the row") {
      sql(s"DELETE FROM $baseTable WHERE val = 5")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(5d) INSERT (-5),(-5) — ABS(5) reappears (two duplicate sources)") {
      sql(s"INSERT INTO $baseTable VALUES (-5), (-5)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // (9) DISTINCT + ORDER BY + WHERE, with a batch of conflicting DML
  //     including UPDATE before a single REFRESH.
  //     openivm distinct.test:526‒593  (dist_order / mv_dist_order)
  // ══════════════════════════════════════════════════════════════════════════

  describe("(9) DISTINCT + ORDER BY + UPDATE batch (dist_order / mv_dist_order)") {

    val baseTable = "oid_dist_order"
    val mvName    = "oid_mv_dist_order"
    // Spark IVM stores data without ORDER BY in the MV body. The recomputed
    // expected SQL drops ORDER BY since EXCEPT ALL is order-insensitive.
    val createBody =
      s"SELECT DISTINCT w_id, d_id FROM $baseTable WHERE local = 1 ORDER BY w_id"
    val checkBody =
      s"SELECT DISTINCT w_id, d_id FROM $baseTable WHERE local = 1"

    it("(9a) initial INSERT — MV matches DISTINCT w_id,d_id WHERE local=1") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(w_id INT, d_id INT, local INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES (1, 1, 1), (1, 2, 0), (2, 1, 1), (1, 1, 1), (3, 2, 1)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $createBody")
      assertMvCorrect(mvName, checkBody)
    }

    it("(9b) batched INSERT + DELETE + INSERT + UPDATE → single REFRESH") {
      sql(s"INSERT INTO $baseTable VALUES (4, 3, 1), (5, 1, 1), (5, 1, 1)")
      sql(s"DELETE FROM $baseTable WHERE w_id = 1 AND d_id = 1")
      sql(s"INSERT INTO $baseTable VALUES (1, 1, 1)")
      sql(s"UPDATE $baseTable SET local = 0 WHERE w_id = 2")
      refreshMv(mvName)
      assertMvCorrect(mvName, checkBody)
    }
  }
}
