package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateGroupSpec.scala` so that each chunk of ~8 tests
  * runs in its own forked JVM (see `spark-ext/project/Settings.scala`). All
  * table and MV names are prefixed with `aggrpsc_` to guarantee that
  * parallel specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateGroupStddevCompositeScenarios extends IvmParitySpecBase("aggregate-group-stddev-composite") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  describe("(5a) GROUP BY k, STDDEV(x) — INSERT") {
    it("sample STDDEV is maintained via hidden SUM/COUNT/SUM_SQ decomposition") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_std_5a(k STRING, x DOUBLE) USING DELTA")
      sql(
        "INSERT INTO aggrpsc_ag_std_5a VALUES ('a', 2.0), ('a', 4.0), ('a', 4.0), ('a', 4.0), ('a', 5.0), ('a', 5.0), ('a', 7.0), ('a', 9.0), ('b', 1.0), ('b', 3.0)"
      )
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_std_5a AS " +
          "SELECT k, STDDEV(x) AS sd FROM aggrpsc_ag_std_5a GROUP BY k"
      )
      sql("INSERT INTO aggrpsc_ag_std_5a VALUES ('a', 6.0), ('c', 2.0), ('c', 4.0)")
      refreshMv("aggrpsc_mv_ag_std_5a")
      assertMvCorrect("aggrpsc_mv_ag_std_5a", "SELECT k, STDDEV(x) AS sd FROM aggrpsc_ag_std_5a GROUP BY k")
    }
  }

  describe("(5b) GROUP BY k, STDDEV(x) — DELETE") {
    it("sample STDDEV is correctly updated after DELETE") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_std_5b(k STRING, x DOUBLE) USING DELTA")
      sql(
        "INSERT INTO aggrpsc_ag_std_5b VALUES ('a', 1.0), ('a', 2.0), ('a', 3.0), ('a', 4.0), ('b', 10.0), ('b', 20.0)"
      )
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_std_5b AS " +
          "SELECT k, STDDEV(x) AS sd FROM aggrpsc_ag_std_5b GROUP BY k"
      )
      sql("DELETE FROM aggrpsc_ag_std_5b WHERE k = 'a' AND x = 1.0")
      refreshMv("aggrpsc_mv_ag_std_5b")
      assertMvCorrect("aggrpsc_mv_ag_std_5b", "SELECT k, STDDEV(x) AS sd FROM aggrpsc_ag_std_5b GROUP BY k")
    }
  }

  describe("(6) GROUP BY k, STDDEV_POP(x)") {
    it("population STDDEV is maintained incrementally") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_stdp_6(k STRING, x DOUBLE) USING DELTA")
      sql(
        "INSERT INTO aggrpsc_ag_stdp_6 VALUES ('a', 0.0), ('a', 4.0), ('b', 2.0), ('b', 8.0), ('c', 1.0), ('c', 3.0)"
      )
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_stdp_6 AS " +
          "SELECT k, STDDEV_POP(x) AS sdp FROM aggrpsc_ag_stdp_6 GROUP BY k"
      )
      sql("INSERT INTO aggrpsc_ag_stdp_6 VALUES ('d', 4.0), ('d', 8.0)")
      sql("DELETE FROM aggrpsc_ag_stdp_6 WHERE k = 'c' AND x = 3.0")
      refreshMv("aggrpsc_mv_ag_stdp_6")
      assertMvCorrect("aggrpsc_mv_ag_stdp_6", "SELECT k, STDDEV_POP(x) AS sdp FROM aggrpsc_ag_stdp_6 GROUP BY k")
    }
  }

  describe("(7a) GROUP BY k, VARIANCE(x)") {
    it("sample VARIANCE is maintained incrementally") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_var_7a(k STRING, x DOUBLE) USING DELTA")
      sql(
        "INSERT INTO aggrpsc_ag_var_7a VALUES ('a', 2.0), ('a', 4.0), ('a', 6.0), ('b', 1.0), ('b', 5.0), ('b', 9.0)"
      )
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_var_7a AS " +
          "SELECT k, VARIANCE(x) AS vr FROM aggrpsc_ag_var_7a GROUP BY k"
      )
      sql("INSERT INTO aggrpsc_ag_var_7a VALUES ('a', 8.0), ('c', 3.0), ('c', 7.0)")
      refreshMv("aggrpsc_mv_ag_var_7a")
      assertMvCorrect("aggrpsc_mv_ag_var_7a", "SELECT k, VARIANCE(x) AS vr FROM aggrpsc_ag_var_7a GROUP BY k")
    }
  }

  describe("(7b) GROUP BY k, VAR_POP(x)") {
    it("population VARIANCE is maintained incrementally") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_varp_7b(k STRING, x DOUBLE) USING DELTA")
      sql("INSERT INTO aggrpsc_ag_varp_7b VALUES ('a', 2.0), ('a', 4.0), ('a', 6.0), ('b', 1.0), ('b', 3.0)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_varp_7b AS " +
          "SELECT k, VAR_POP(x) AS vrp FROM aggrpsc_ag_varp_7b GROUP BY k"
      )
      sql("INSERT INTO aggrpsc_ag_varp_7b VALUES ('b', 5.0), ('c', 10.0)")
      sql("DELETE FROM aggrpsc_ag_varp_7b WHERE k = 'a' AND x = 2.0")
      refreshMv("aggrpsc_mv_ag_varp_7b")
      assertMvCorrect("aggrpsc_mv_ag_varp_7b", "SELECT k, VAR_POP(x) AS vrp FROM aggrpsc_ag_varp_7b GROUP BY k")
    }
  }

  describe("(9a) GROUP BY k, SUM(x) + COUNT(*) + AVG(x) — multiple aggregates INSERT") {
    it("multiple aggregates in one view are all maintained correctly") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_multi_9a(k STRING, x DOUBLE) USING DELTA")
      sql("INSERT INTO aggrpsc_ag_multi_9a VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0), ('b', 15.0)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_multi_9a AS " +
          "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x " +
          "FROM aggrpsc_ag_multi_9a GROUP BY k"
      )
      sql("INSERT INTO aggrpsc_ag_multi_9a VALUES ('a', 30.0), ('c', 7.0)")
      refreshMv("aggrpsc_mv_ag_multi_9a")
      assertMvCorrect(
        "aggrpsc_mv_ag_multi_9a",
        "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x FROM aggrpsc_ag_multi_9a GROUP BY k"
      )
    }
  }

  describe("(9b) GROUP BY k, SUM(x) + COUNT(*) + AVG(x) — mixed DML") {
    // Note: all operations use count_delta ≠ 0 per group (see note in test 4c).
    // The UPDATE pattern for AVG is avoided; instead 'a' has one row removed.
    it("all aggregates remain consistent after batched mixed DML") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_multi_9b(k STRING, x DOUBLE) USING DELTA")
      sql("INSERT INTO aggrpsc_ag_multi_9b VALUES ('a', 10.0), ('a', 20.0), ('b', 5.0), ('c', 100.0)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_multi_9b AS " +
          "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x " +
          "FROM aggrpsc_ag_multi_9b GROUP BY k"
      )
      sql("DELETE FROM aggrpsc_ag_multi_9b WHERE k = 'c'")
      sql("INSERT INTO aggrpsc_ag_multi_9b VALUES ('b', 25.0), ('d', 50.0)")
      // Remove one 'a' row (count_delta('a') = -1 ≠ 0) so AVG refreshes correctly.
      sql("DELETE FROM aggrpsc_ag_multi_9b WHERE k = 'a' AND x = 10.0")
      refreshMv("aggrpsc_mv_ag_multi_9b")
      assertMvCorrect(
        "aggrpsc_mv_ag_multi_9b",
        "SELECT k, SUM(x) AS total, COUNT(*) AS cnt, AVG(x) AS avg_x FROM aggrpsc_ag_multi_9b GROUP BY k"
      )
    }
  }

  describe("(13) NULL aggregate input — SUM/AVG handle NULLs per SQL semantics") {
    it("SUM ignores NULLs; a group of all-NULLs returns NULL sum") {
      sql("CREATE TABLE IF NOT EXISTS aggrpsc_ag_nullv_13(k STRING, x INT) USING DELTA")
      sql("INSERT INTO aggrpsc_ag_nullv_13 VALUES ('a', 10), ('a', NULL), ('b', NULL)")
      sql(
        "CREATE MATERIALIZED VIEW aggrpsc_mv_ag_nullv_13 AS " +
          "SELECT k, SUM(x) AS total FROM aggrpsc_ag_nullv_13 GROUP BY k"
      )
      // Add another NULL for 'b' — sum of group 'b' is still NULL (all inputs NULL)
      sql("INSERT INTO aggrpsc_ag_nullv_13 VALUES ('b', NULL), ('c', 5)")
      refreshMv("aggrpsc_mv_ag_nullv_13")
      assertMvCorrect("aggrpsc_mv_ag_nullv_13", "SELECT k, SUM(x) AS total FROM aggrpsc_ag_nullv_13 GROUP BY k")
    }
  }

}
