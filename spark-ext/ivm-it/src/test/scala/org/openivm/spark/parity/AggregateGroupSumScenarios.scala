package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateGroupSpec.scala` so that each chunk of ~8 tests
  * runs in its own forked JVM (see `spark-ext/project/Settings.scala`). All
  * table and MV names are prefixed with `aggrpsum_` to guarantee that
  * parallel specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateGroupSumScenarios extends IvmParitySpecBase("aggregate-group-sum") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  describe("(1a) GROUP BY k, SUM(x) — INSERT only") {
    it("incremental refresh yields correct sums after INSERT") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_sum_1a(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_sum_1a VALUES ('a', 10), ('b', 20)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_sum_1a AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1a GROUP BY k"
      )
      sql("INSERT INTO aggrpsum_ag_sum_1a VALUES ('a', 5), ('c', 30)")
      refreshMv("aggrpsum_mv_ag_sum_1a")
      assertMvCorrect("aggrpsum_mv_ag_sum_1a", "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1a GROUP BY k")
    }
  }

  describe("(1b) GROUP BY k, SUM(x) — DELETE") {
    it("incremental refresh drops deleted group from MV") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_sum_1b(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_sum_1b VALUES ('a', 10), ('b', 20), ('c', 5)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_sum_1b AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1b GROUP BY k"
      )
      sql("DELETE FROM aggrpsum_ag_sum_1b WHERE k = 'b'")
      refreshMv("aggrpsum_mv_ag_sum_1b")
      assertMvCorrect("aggrpsum_mv_ag_sum_1b", "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1b GROUP BY k")
    }
  }

  describe("(1c) GROUP BY k, SUM(x) — UPDATE") {
    it("incremental refresh recomputes sum for updated group") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_sum_1c(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_sum_1c VALUES ('a', 10), ('b', 20)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_sum_1c AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1c GROUP BY k"
      )
      sql("UPDATE aggrpsum_ag_sum_1c SET x = 99 WHERE k = 'b'")
      refreshMv("aggrpsum_mv_ag_sum_1c")
      assertMvCorrect("aggrpsum_mv_ag_sum_1c", "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1c GROUP BY k")
    }
  }

  describe("(1d) GROUP BY k, SUM(x) — batched mixed DML") {
    it("incremental refresh reconciles INSERT + DELETE + UPDATE in one pass") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_sum_1d(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_sum_1d VALUES ('a', 10), ('b', 20), ('c', 5)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_sum_1d AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1d GROUP BY k"
      )
      sql("INSERT INTO aggrpsum_ag_sum_1d VALUES ('d', 50)")
      sql("DELETE FROM aggrpsum_ag_sum_1d WHERE k = 'c'")
      sql("UPDATE aggrpsum_ag_sum_1d SET x = 100 WHERE k = 'a'")
      refreshMv("aggrpsum_mv_ag_sum_1d")
      assertMvCorrect("aggrpsum_mv_ag_sum_1d", "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1d GROUP BY k")
    }
  }

  describe("(1e) GROUP BY k, SUM(x) — source INSERT OVERWRITE routes to FULL_REFRESH") {
    // CDF-only: `replaceBatch` is detected from the CDF commit verdicts, so the
    // REPLACE→FULL_REFRESH routing (and the count-monoid arity regression it
    // guards) is a CDF-mode path. Intercept-mode REPLACE detection via staging
    // is a separate, pre-existing gap. SF10 runs in CDF mode.
    itCdf("count-monoid MV survives a replaced source without an arity mismatch") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_sum_1e(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_sum_1e VALUES ('a', 10), ('b', 20)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_sum_1e AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1e GROUP BY k"
      )
      refreshMv("aggrpsum_mv_ag_sum_1e")
      sql("INSERT OVERWRITE TABLE aggrpsum_ag_sum_1e VALUES ('a', 1), ('c', 7), ('c', 3)")
      refreshMv("aggrpsum_mv_ag_sum_1e")
      assertMvCorrect("aggrpsum_mv_ag_sum_1e", "SELECT k, SUM(x) AS total FROM aggrpsum_ag_sum_1e GROUP BY k")
    }
  }

  describe("(8a) GROUP BY k1, k2, SUM(x) — composite key INSERT") {
    it("composite group key is tracked correctly on INSERT") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_comp_8a(k1 STRING, k2 INT, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_comp_8a VALUES ('a', 1, 10), ('a', 2, 20), ('b', 1, 30)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_comp_8a AS " +
          "SELECT k1, k2, SUM(x) AS total FROM aggrpsum_ag_comp_8a GROUP BY k1, k2"
      )
      sql("INSERT INTO aggrpsum_ag_comp_8a VALUES ('a', 1, 5), ('b', 2, 15), ('c', 3, 25)")
      refreshMv("aggrpsum_mv_ag_comp_8a")
      assertMvCorrect(
        "aggrpsum_mv_ag_comp_8a",
        "SELECT k1, k2, SUM(x) AS total FROM aggrpsum_ag_comp_8a GROUP BY k1, k2"
      )
    }
  }

  describe("(8b) GROUP BY k1, k2, SUM(x) — composite key mixed DML") {
    it("composite group key handles mixed DML correctly") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_comp_8b(k1 STRING, k2 INT, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_comp_8b VALUES ('a', 1, 10), ('a', 2, 20), ('b', 1, 30), ('b', 2, 40)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_comp_8b AS " +
          "SELECT k1, k2, SUM(x) AS total FROM aggrpsum_ag_comp_8b GROUP BY k1, k2"
      )
      sql("DELETE FROM aggrpsum_ag_comp_8b WHERE k1 = 'b' AND k2 = 2")
      sql("UPDATE aggrpsum_ag_comp_8b SET x = 99 WHERE k1 = 'a' AND k2 = 1")
      sql("INSERT INTO aggrpsum_ag_comp_8b VALUES ('c', 1, 50)")
      refreshMv("aggrpsum_mv_ag_comp_8b")
      assertMvCorrect(
        "aggrpsum_mv_ag_comp_8b",
        "SELECT k1, k2, SUM(x) AS total FROM aggrpsum_ag_comp_8b GROUP BY k1, k2"
      )
    }
  }

  describe("(10) Empty initial table — INSERT after CREATE") {
    it("MV correctly populated when base table starts empty") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_empty_10(k STRING, x INT) USING DELTA")
      // No rows yet — create MV on an empty table
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_empty_10 AS " +
          "SELECT k, SUM(x) AS total, COUNT(*) AS cnt FROM aggrpsum_ag_empty_10 GROUP BY k"
      )
      // Now insert data and refresh
      sql("INSERT INTO aggrpsum_ag_empty_10 VALUES ('a', 10), ('b', 20), ('a', 5)")
      refreshMv("aggrpsum_mv_ag_empty_10")
      assertMvCorrect(
        "aggrpsum_mv_ag_empty_10",
        "SELECT k, SUM(x) AS total, COUNT(*) AS cnt FROM aggrpsum_ag_empty_10 GROUP BY k"
      )
    }
  }

  describe("(11) INSERT then DELETE of the same row — net-zero change") {
    it("MV equals base table when inserted rows are subsequently deleted") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsum_ag_netzer_11(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsum_ag_netzer_11 VALUES ('a', 10), ('b', 20)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsum_mv_ag_netzer_11 AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsum_ag_netzer_11 GROUP BY k"
      )
      val userCols = Seq("k", "total")
      val beforeMv = spark.table("aggrpsum_mv_ag_netzer_11").select(userCols.head, userCols.tail: _*).collect().toSet
      // Insert a row and immediately delete it — net effect is zero
      sql("INSERT INTO aggrpsum_ag_netzer_11 VALUES ('c', 99)")
      sql("DELETE FROM aggrpsum_ag_netzer_11 WHERE k = 'c'")
      refreshMv("aggrpsum_mv_ag_netzer_11")
      assertMvCorrect("aggrpsum_mv_ag_netzer_11", "SELECT k, SUM(x) AS total FROM aggrpsum_ag_netzer_11 GROUP BY k")
      // Also confirm the MV snapshot equals what we saw before (user cols only)
      val afterMv = spark.table("aggrpsum_mv_ag_netzer_11").select(userCols.head, userCols.tail: _*).collect().toSet
      afterMv shouldBe beforeMv
    }
  }

}
