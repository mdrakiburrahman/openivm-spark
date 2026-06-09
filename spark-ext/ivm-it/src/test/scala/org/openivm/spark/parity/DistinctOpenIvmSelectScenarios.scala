package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** 1:1 ScalaTest port of openivm `test/sql/distinct.test` — SELECT-shape
  * sections (1) and (2).
  *
  * Split out of the original `DistinctOpenIvmSpec` so each file contributes
  * ≤ 10 `it(...)` cases per forked JVM. Covers:
  *
  *   1.  Basic DISTINCT — single column     (distinct.test:11‒79, plus the
  *       batched-DML continuation from section 3 at distinct.test:131‒155)
  *   2.  DISTINCT, multiple columns         (distinct.test:82‒129)
  */
abstract class DistinctOpenIvmSelectScenarios extends IvmParitySpecBase("distinct-open-ivm-select") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ══════════════════════════════════════════════════════════════════════════
  // (1) Basic DISTINCT — single column
  //     openivm distinct.test:11‒79  (colors / distinct_colors)
  //     Continued in section (3) for batched mixed DML.
  // ══════════════════════════════════════════════════════════════════════════

  describe("(1) Basic DISTINCT — single column (colors / distinct_colors)") {

    val baseTable = "ois_colors"
    val mvName    = "ois_distinct_colors"
    val body      = s"SELECT DISTINCT color FROM $baseTable"

    it("(1a) initial INSERT + CREATE MATERIALIZED VIEW") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(id INT, color STRING) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green')")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(1b) INSERT duplicates ('red','blue') — DISTINCT result unchanged") {
      sql(s"INSERT INTO $baseTable VALUES (5, 'red'), (6, 'blue')")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1c) INSERT new value 'yellow' — MV gains a row") {
      sql(s"INSERT INTO $baseTable VALUES (7, 'yellow')")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1d) DELETE all rows of 'green' — MV loses that row") {
      sql(s"DELETE FROM $baseTable WHERE color = 'green'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(3a) batched INSERT + DELETE + INSERT — single REFRESH (distinct.test:131‒155)") {
      sql(s"INSERT INTO $baseTable VALUES (10, 'purple'), (11, 'purple'), (12, 'orange')")
      sql(s"DELETE FROM $baseTable WHERE color = 'yellow'")
      sql(s"INSERT INTO $baseTable VALUES (13, 'yellow')")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // (2) DISTINCT with multiple columns
  //     openivm distinct.test:82‒129  (pairs / distinct_pairs)
  // ══════════════════════════════════════════════════════════════════════════

  describe("(2) DISTINCT, multiple columns (pairs / distinct_pairs)") {

    val baseTable = "ois_pairs"
    val mvName    = "ois_distinct_pairs"
    val body      = s"SELECT DISTINCT a, b FROM $baseTable"

    it("(2a) initial INSERT + CREATE MATERIALIZED VIEW") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(a STRING, b INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES ('x', 1), ('x', 2), ('y', 1), ('x', 1)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(2b) INSERT duplicate pair — MV unchanged") {
      sql(s"INSERT INTO $baseTable VALUES ('x', 1), ('y', 1)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(2c) DELETE one copy of (x,1) — pair still present, MV unchanged") {
      sql(s"DELETE FROM $baseTable WHERE a = 'x' AND b = 1")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }
  }
}
