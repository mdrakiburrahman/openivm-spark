package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

/** Split-off from `AggregateSpec.scala` so that each chunk of ~10 tests runs
  * in its own forked JVM (see `spark-ext/project/Settings.scala`). All table
  * and MV names are prefixed with `aggsum_` to guarantee that parallel
  * specs cannot collide on a Delta warehouse path.
  */
abstract class AggregateSumScenarios extends IvmParitySpecBase("aggregate-sum") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  // ── (basic aggregate aggsum_sales) extracted to AggregateSumHeavyDmlSpec
  //    (~5m wall) so it runs in its own forked JVM and does not bottleneck
  //    the rest of this spec.

  // openivm test/sql/aggregate.test §NULL group keys
  describe("NULL values as GROUP BY keys — aggsum_nullable_grp") {
    it("NULL is treated as a distinct group; INSERT and DELETE update it correctly") {
      sql("CREATE TABLE aggsum_nullable_grp (grp STRING, val INT) USING DELTA")
      sql("INSERT INTO aggsum_nullable_grp VALUES ('a', 10), (NULL, 20), ('a', 30), (NULL, 40)")
      val viewBody = "SELECT grp, SUM(val) AS total FROM aggsum_nullable_grp GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_null_grp AS $viewBody")
      assertMvCorrect("aggsum_mv_null_grp", viewBody)

      sql("INSERT INTO aggsum_nullable_grp VALUES (NULL, 5), ('b', 100)")
      refreshMv("aggsum_mv_null_grp")
      assertMvCorrect("aggsum_mv_null_grp", viewBody)

      sql("DELETE FROM aggsum_nullable_grp WHERE grp IS NULL")
      refreshMv("aggsum_mv_null_grp")
      assertMvCorrect("aggsum_mv_null_grp", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §DECIMAL / NUMERIC columns in SUM
  describe("DECIMAL / NUMERIC columns in SUM — aggsum_decimals(category, amount)") {
    it("SUM over DECIMAL(10,2) is maintained correctly across INSERTs") {
      sql("CREATE TABLE aggsum_decimals (category STRING, amount DECIMAL(10,2)) USING DELTA")
      sql("INSERT INTO aggsum_decimals VALUES ('x', 10.50), ('x', 20.75), ('y', 100.00)")
      val viewBody = "SELECT category, SUM(amount) AS total FROM aggsum_decimals GROUP BY category"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_dec AS $viewBody")
      assertMvCorrect("aggsum_mv_dec", viewBody)

      sql("INSERT INTO aggsum_decimals VALUES ('x', 0.01), ('y', 99.99)")
      refreshMv("aggsum_mv_dec")
      assertMvCorrect("aggsum_mv_dec", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §BIGINT columns
  describe("BIGINT columns — aggsum_big_nums(grp, val BIGINT)") {
    it("SUM over near-max BIGINT values is maintained correctly") {
      sql("CREATE TABLE aggsum_big_nums (grp INT, val BIGINT) USING DELTA")
      sql("INSERT INTO aggsum_big_nums VALUES (1, 9223372036854775000), (1, 100)")
      val viewBody = "SELECT grp, SUM(val) AS total FROM aggsum_big_nums GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_big AS $viewBody")
      assertMvCorrect("aggsum_mv_big", viewBody)

      sql("INSERT INTO aggsum_big_nums VALUES (1, 500)")
      refreshMv("aggsum_mv_big")
      assertMvCorrect("aggsum_mv_big", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §DATE columns as GROUP BY keys
  describe("DATE columns as GROUP BY keys — aggsum_daily_sales(sale_date, amount)") {
    it("DATE-keyed SUM is maintained across INSERTs") {
      sql("CREATE TABLE aggsum_daily_sales (sale_date DATE, amount INT) USING DELTA")
      sql(
        "INSERT INTO aggsum_daily_sales VALUES " +
          "(DATE'2024-01-01', 100), (DATE'2024-01-01', 200), (DATE'2024-01-02', 300)"
      )
      val viewBody =
        "SELECT sale_date, SUM(amount) AS total FROM aggsum_daily_sales GROUP BY sale_date"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_daily AS $viewBody")
      assertMvCorrect("aggsum_mv_daily", viewBody)

      sql("INSERT INTO aggsum_daily_sales VALUES (DATE'2024-01-02', 50), (DATE'2024-01-03', 400)")
      refreshMv("aggsum_mv_daily")
      assertMvCorrect("aggsum_mv_daily", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Empty table → CREATE MV → INSERT → refresh
  describe("Empty table → CREATE MV → INSERT → refresh — aggsum_empty_base") {
    it("MV created on empty base table is populated after first INSERT + REFRESH") {
      sql("CREATE TABLE aggsum_empty_base (id INT, val INT) USING DELTA")
      val viewBody = "SELECT id, SUM(val) AS total FROM aggsum_empty_base GROUP BY id"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_empty AS $viewBody")
      assertMvCorrect("aggsum_mv_empty", viewBody)

      sql("INSERT INTO aggsum_empty_base VALUES (1, 100), (2, 200)")
      refreshMv("aggsum_mv_empty")
      assertMvCorrect("aggsum_mv_empty", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §Large batch INSERT (1000 rows)
  describe("Large batch INSERT (1000 rows) — aggsum_batch_tbl") {
    it("INSERT of 1000 generated rows is consolidated in a single refresh") {
      sql("CREATE TABLE aggsum_batch_tbl (grp INT, val INT) USING DELTA")
      val viewBody =
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM aggsum_batch_tbl GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_batch AS $viewBody")

      // Spark's `range(0, 1000)` TVF translation done via DataFrame insertInto
      // because the SQL-form `INSERT INTO … FROM range(…)` can be flaky on
      // Spark 3.5 depending on the catalog implementation.
      spark
        .range(0, 1000)
        .selectExpr("CAST(id % 10 AS INT) AS grp", "CAST(id AS INT) AS val")
        .write
        .mode("append")
        .insertInto("aggsum_batch_tbl")
      refreshMv("aggsum_mv_batch")
      assertMvCorrect("aggsum_mv_batch", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §DELETE all rows → MV should be empty
  describe("DELETE all rows → MV should be empty — aggsum_del_all") {
    it("After DELETE-all, the MV becomes empty") {
      sql("CREATE TABLE aggsum_del_all (grp STRING, val INT) USING DELTA")
      sql("INSERT INTO aggsum_del_all VALUES ('a', 1), ('b', 2)")
      val viewBody = "SELECT grp, SUM(val) AS total FROM aggsum_del_all GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_del_all AS $viewBody")
      assertMvCorrect("aggsum_mv_del_all", viewBody)

      sql("DELETE FROM aggsum_del_all")
      refreshMv("aggsum_mv_del_all")
      assertMvCorrect("aggsum_mv_del_all", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §UPDATE that changes GROUP BY key (rows move between groups)
  describe("UPDATE moves rows between groups — aggsum_agg_grp_move") {
    it("MV reflects the new groupings after rows are UPDATEd to a different group") {
      sql("CREATE TABLE aggsum_agg_grp_move (grp STRING, val INT) USING DELTA")
      sql("INSERT INTO aggsum_agg_grp_move VALUES ('x', 10), ('x', 20), ('y', 30)")
      val viewBody = "SELECT grp, SUM(val) AS total FROM aggsum_agg_grp_move GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_agg_grp_move AS $viewBody")
      refreshMv("aggsum_mv_agg_grp_move")
      assertMvCorrect("aggsum_mv_agg_grp_move", viewBody)

      // Move val=20 row from x to y
      sql("UPDATE aggsum_agg_grp_move SET grp = 'y' WHERE val = 20")
      refreshMv("aggsum_mv_agg_grp_move")
      assertMvCorrect("aggsum_mv_agg_grp_move", viewBody)

      // Move val=30 row from y to brand-new group z
      sql("UPDATE aggsum_agg_grp_move SET grp = 'z' WHERE val = 30")
      refreshMv("aggsum_mv_agg_grp_move")
      assertMvCorrect("aggsum_mv_agg_grp_move", viewBody)
    }
  }

  // openivm test/sql/aggregate.test §GROUP disappears entirely
  describe("GROUP disappears entirely — aggsum_agg_vanish") {
    it("Deleting the only row in a group removes that group from the MV") {
      sql("CREATE TABLE aggsum_agg_vanish (grp STRING, val INT) USING DELTA")
      sql("INSERT INTO aggsum_agg_vanish VALUES ('p', 10), ('q', 20), ('r', 30)")
      val viewBody = "SELECT grp, SUM(val) AS total FROM aggsum_agg_vanish GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aggsum_mv_agg_vanish AS $viewBody")
      refreshMv("aggsum_mv_agg_vanish")
      assertMvCorrect("aggsum_mv_agg_vanish", viewBody)

      sql("DELETE FROM aggsum_agg_vanish WHERE grp = 'q'")
      refreshMv("aggsum_mv_agg_vanish")
      assertMvCorrect("aggsum_mv_agg_vanish", viewBody)
    }
  }

}
