package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Slice of `ParserSpec` covering the aggregate edge cases: empty base
  * table, NULL / negative / VARCHAR aggregates, and multi-column GROUP BY.
  *
  * Forked-JVM isolation: own SparkSession + warehouse dir per spec.
  */
abstract class ParserAggregateScenarios extends IvmParitySpecBase("parser-aggregate") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ──────────────────────────────────────────────────────────────────────────
  // (7) Empty base table, then INSERT (parser.test Test 18): MV starts empty
  //     and grows as data appears.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(7) Empty base table") {

    it("MV over an empty table is empty; INSERTs flow through to refresh") {
      sql("CREATE TABLE IF NOT EXISTS empty_p7(id INT, grp STRING, val INT) USING DELTA")
      sql(
        "CREATE MATERIALIZED VIEW mv_empty_p7 AS SELECT grp, min(val) AS min_val FROM empty_p7 GROUP BY grp"
      )
      spark.table("mv_empty_p7").count() shouldBe 0L

      sql("INSERT INTO empty_p7 VALUES (1, 'a', 42)")
      refreshMv("mv_empty_p7")
      assertMvCorrect(
        "mv_empty_p7",
        "SELECT grp, min(val) AS min_val FROM empty_p7 GROUP BY grp"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (8) NULL / negative / VARCHAR aggregates (parser.test Tests 19-22).
  // ──────────────────────────────────────────────────────────────────────────
  describe("(8) NULL, negative and VARCHAR aggregates") {

    it("MIN/MAX/COUNT ignore NULLs in the aggregated column") {
      sql("CREATE TABLE IF NOT EXISTS nulls_p8(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO nulls_p8 VALUES (1,'a',10),(2,'a',NULL),(3,'b',NULL)")
      sql(
        "CREATE MATERIALIZED VIEW mv_nulls_p8 AS " +
          "SELECT grp, min(val) AS min_val, max(val) AS max_val, count(val) AS cnt " +
          "FROM nulls_p8 GROUP BY grp"
      )
      assertMvCorrect(
        "mv_nulls_p8",
        "SELECT grp, min(val) AS min_val, max(val) AS max_val, count(val) AS cnt " +
          "FROM nulls_p8 GROUP BY grp"
      )

      sql("INSERT INTO nulls_p8 VALUES (4, 'b', 7)")
      refreshMv("mv_nulls_p8")
      assertMvCorrect(
        "mv_nulls_p8",
        "SELECT grp, min(val) AS min_val, max(val) AS max_val, count(val) AS cnt " +
          "FROM nulls_p8 GROUP BY grp"
      )
    }

    it("negative numbers and DELETEs reset MIN to recovered value") {
      sql("CREATE TABLE IF NOT EXISTS neg_p8(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO neg_p8 VALUES (1,'x',-100),(2,'x',50),(3,'x',-200)")
      sql(
        "CREATE MATERIALIZED VIEW mv_neg_p8 AS " +
          "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total " +
          "FROM neg_p8 GROUP BY grp"
      )
      assertMvCorrect(
        "mv_neg_p8",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total FROM neg_p8 GROUP BY grp"
      )

      sql("INSERT INTO neg_p8 VALUES (4,'x',-300)")
      refreshMv("mv_neg_p8")
      assertMvCorrect(
        "mv_neg_p8",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total FROM neg_p8 GROUP BY grp"
      )

      // Delete the most-negative row and confirm MIN recovers via openivm's
      // GROUP_RECOMPUTE / FULL_REFRESH path.
      sql("DELETE FROM neg_p8 WHERE id = 4")
      refreshMv("mv_neg_p8")
      assertMvCorrect(
        "mv_neg_p8",
        "SELECT grp, min(val) AS lo, max(val) AS hi, sum(val) AS total FROM neg_p8 GROUP BY grp"
      )
    }

    it("VARCHAR MIN/MAX returns alphabetical ordering and recovers on DELETE") {
      sql("CREATE TABLE IF NOT EXISTS words_p8(id INT, cat STRING, word STRING) USING DELTA")
      sql(
        "INSERT INTO words_p8 VALUES (1,'fruit','banana'),(2,'fruit','apple'),(3,'fruit','cherry')"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_words_p8 AS " +
          "SELECT cat, min(word) AS first_word, max(word) AS last_word FROM words_p8 GROUP BY cat"
      )
      assertMvCorrect(
        "mv_words_p8",
        "SELECT cat, min(word) AS first_word, max(word) AS last_word FROM words_p8 GROUP BY cat"
      )

      sql("DELETE FROM words_p8 WHERE id = 2") // remove 'apple'
      refreshMv("mv_words_p8")
      assertMvCorrect(
        "mv_words_p8",
        "SELECT cat, min(word) AS first_word, max(word) AS last_word FROM words_p8 GROUP BY cat"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (9) Multi-key GROUP BY (parser.test Test 23).
  // ──────────────────────────────────────────────────────────────────────────
  describe("(9) Multi-column GROUP BY") {

    it("MV grouped by (dept, team) tracks INSERTs correctly") {
      sql(
        "CREATE TABLE IF NOT EXISTS multi_p9(id INT, dept STRING, team STRING, val INT) USING DELTA"
      )
      sql(
        "INSERT INTO multi_p9 VALUES " +
          "(1,'eng','be',10),(2,'eng','be',20),(3,'eng','fe',5),(4,'sales','us',100)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_multi_p9 AS " +
          "SELECT dept, team, min(val) AS lo, max(val) AS hi FROM multi_p9 GROUP BY dept, team"
      )
      assertMvCorrect(
        "mv_multi_p9",
        "SELECT dept, team, min(val) AS lo, max(val) AS hi FROM multi_p9 GROUP BY dept, team"
      )

      sql("INSERT INTO multi_p9 VALUES (5,'eng','be',1)")
      refreshMv("mv_multi_p9")
      assertMvCorrect(
        "mv_multi_p9",
        "SELECT dept, team, min(val) AS lo, max(val) AS hi FROM multi_p9 GROUP BY dept, team"
      )
    }
  }
}
