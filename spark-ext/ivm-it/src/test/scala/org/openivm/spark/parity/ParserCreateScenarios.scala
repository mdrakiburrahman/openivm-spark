package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.commands.{
  CreateMaterializedViewCommand,
  DropMaterializedViewCommand,
  RefreshMaterializedViewCommand
}
import org.openivm.spark.common.MvCatalog

/** Slice of `ParserSpec` covering CREATE / REFRESH / DROP wiring, MIN/MAX
  * with GROUP BY, whitespace + mixed-case tolerance, and simple
  * projection / projection-with-filter MVs.
  *
  * Each slice runs in its own forked JVM (see `Settings.parallelForkSettings`),
  * which is why every slice carries its own SparkSession, warehouse dir and
  * helper bundle — they cannot share a base trait without serialising the
  * JVMs.
  */
abstract class ParserCreateScenarios extends IvmParitySpecBase("parser-create") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  protected def mvExists(name: String): Boolean =
    MvCatalog.lookup(spark, TableIdentifier(name)).isDefined

  // ──────────────────────────────────────────────────────────────────────────
  // (1) The injected IvmParser drives every CREATE/REFRESH/DROP MV statement
  //     end-to-end through Spark — proves parser injection actually fires.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(1) IvmParser is wired into SparkSession via OpenIvmSparkExtensions") {

    it("CREATE MATERIALIZED VIEW reaches CreateMaterializedViewCommand and executes") {
      sql(
        "CREATE TABLE IF NOT EXISTS sales_p1(id INT, region STRING, amount INT) USING DELTA"
      )
      sql("INSERT INTO sales_p1 VALUES (1,'east',100), (2,'west',200), (3,'east',150)")

      // Inspect the parsed plan AND execute it.
      val plan = spark.sessionState.sqlParser.parsePlan(
        "CREATE MATERIALIZED VIEW mv_sales_p1 AS " +
          "SELECT region, sum(amount) AS sum_amount, count(amount) AS count_amount " +
          "FROM sales_p1 GROUP BY region"
      )
      plan shouldBe a[CreateMaterializedViewCommand]

      sql(
        "CREATE MATERIALIZED VIEW mv_sales_p1 AS " +
          "SELECT region, sum(amount) AS sum_amount, count(amount) AS count_amount " +
          "FROM sales_p1 GROUP BY region"
      )

      // MV is queryable and bag-equivalent to the equivalent base query.
      assertMvCorrect(
        "mv_sales_p1",
        "SELECT region, sum(amount) AS sum_amount, count(amount) AS count_amount " +
          "FROM sales_p1 GROUP BY region"
      )

      // System (MV catalog) table populated.
      mvExists("mv_sales_p1") shouldBe true
    }

    it("REFRESH MATERIALIZED VIEW reaches RefreshMaterializedViewCommand and applies deltas") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "REFRESH MATERIALIZED VIEW mv_sales_p1"
      )
      plan shouldBe a[RefreshMaterializedViewCommand]

      sql("INSERT INTO sales_p1 VALUES (4, 'east', 50)")
      refreshMv("mv_sales_p1")
      assertMvCorrect(
        "mv_sales_p1",
        "SELECT region, sum(amount) AS sum_amount, count(amount) AS count_amount " +
          "FROM sales_p1 GROUP BY region"
      )
    }

    it("DROP MATERIALIZED VIEW reaches DropMaterializedViewCommand and removes catalog row") {
      val plan = spark.sessionState.sqlParser.parsePlan(
        "DROP MATERIALIZED VIEW mv_sales_p1"
      )
      plan shouldBe a[DropMaterializedViewCommand]

      sql("DROP MATERIALIZED VIEW mv_sales_p1")
      mvExists("mv_sales_p1") shouldBe false
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) MIN/MAX with GROUP BY (parser.test Test 2): execution → INSERT →
  //     REFRESH → DELETE → REFRESH all produce correct results.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(2) MIN/MAX with GROUP BY — end-to-end") {

    it("creates the MV, refreshes after INSERT and DELETE, and remains bag-equivalent") {
      sql("CREATE TABLE IF NOT EXISTS scores_p2(id INT, team STRING, score INT) USING DELTA")
      sql(
        "INSERT INTO scores_p2 VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',5)"
      )
      sql(
        "CREATE MATERIALIZED VIEW mv_scores_p2 AS " +
          "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )
      assertMvCorrect(
        "mv_scores_p2",
        "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )

      sql("INSERT INTO scores_p2 VALUES (5,'a',1)")
      refreshMv("mv_scores_p2")
      assertMvCorrect(
        "mv_scores_p2",
        "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )

      // Delete the current `a` min — group_recompute or refresh must recover next min.
      sql("DELETE FROM scores_p2 WHERE id = 5")
      refreshMv("mv_scores_p2")
      assertMvCorrect(
        "mv_scores_p2",
        "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (3) Whitespace / mixed case (parser.test Tests 6, 7, 24, 25): the IvmParser's
  //     ANTLR grammar must accept arbitrary whitespace and lower/mixed casing.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(3) Whitespace and mixed-case forgiveness") {

    it("accepts extra spaces around keywords and identifiers") {
      sql("CREATE TABLE IF NOT EXISTS t_ws(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO t_ws VALUES (1,'a',5)")
      sql(
        "CREATE  MATERIALIZED  VIEW  mv_t_ws  AS  " +
          "SELECT  grp, sum(val) AS sum_val  FROM  t_ws  GROUP  BY  grp"
      )
      assertMvCorrect(
        "mv_t_ws",
        "SELECT grp, sum(val) AS sum_val FROM t_ws GROUP BY grp"
      )
    }

    it("accepts mixed-case keywords: 'Create Materialized View … Select …'") {
      sql("CREATE TABLE IF NOT EXISTS t_mc(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO t_mc VALUES (1,'a',5),(2,'a',10)")
      sql(
        "Create Materialized View mv_t_mc AS " +
          "Select grp, Min(val) AS min_val, Max(val) AS max_val, Sum(val) AS sum_val " +
          "From t_mc Group By grp"
      )
      assertMvCorrect(
        "mv_t_mc",
        "SELECT grp, min(val) AS min_val, max(val) AS max_val, sum(val) AS sum_val " +
          "FROM t_mc GROUP BY grp"
      )
    }

    it("accepts tabs and ragged spacing inside the SELECT projection") {
      sql("CREATE TABLE IF NOT EXISTS t_tabs(id INT, grp STRING, val INT) USING DELTA")
      sql("INSERT INTO t_tabs VALUES (1,'z',99)")
      sql(
        "CREATE   MATERIALIZED    VIEW    mv_t_tabs   AS   " +
          "SELECT   grp  ,   min( val )  AS   min_val   FROM   t_tabs   GROUP   BY   grp"
      )
      assertMvCorrect(
        "mv_t_tabs",
        "SELECT grp, min(val) AS min_val FROM t_tabs GROUP BY grp"
      )
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (4) Projection / filter only (parser.test Tests 8-9): SIMPLE_PROJECTION
  //     classification end-to-end.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(4) Simple projection and projection + filter") {

    it("a pure projection MV reflects subsequent INSERTs after refresh") {
      sql("CREATE TABLE IF NOT EXISTS t_proj(id INT, name STRING) USING DELTA")
      sql("INSERT INTO t_proj VALUES (1,'alice'),(2,'bob')")
      sql("CREATE MATERIALIZED VIEW mv_t_proj AS SELECT id, name FROM t_proj")
      assertMvCorrect("mv_t_proj", "SELECT id, name FROM t_proj")

      sql("INSERT INTO t_proj VALUES (3,'carol')")
      refreshMv("mv_t_proj")
      assertMvCorrect("mv_t_proj", "SELECT id, name FROM t_proj")
    }

    it("a projection-with-filter MV obeys the WHERE clause both initially and after DML") {
      sql("CREATE TABLE IF NOT EXISTS t_filt(id INT, status STRING, val INT) USING DELTA")
      sql("INSERT INTO t_filt VALUES (1,'active',10),(2,'inactive',20),(3,'active',30)")
      sql(
        "CREATE MATERIALIZED VIEW mv_t_filt AS SELECT id, val FROM t_filt WHERE status = 'active'"
      )
      assertMvCorrect("mv_t_filt", "SELECT id, val FROM t_filt WHERE status = 'active'")

      sql("INSERT INTO t_filt VALUES (4,'inactive',999),(5,'active',5)")
      refreshMv("mv_t_filt")
      assertMvCorrect("mv_t_filt", "SELECT id, val FROM t_filt WHERE status = 'active'")
    }
  }

  describe("(5) Case-sensitive output aliases") {

    it("preserves quoted mixed-case aliases through initial load and refresh") {
      sql("CREATE TABLE IF NOT EXISTS t_case_alias(region STRING, amount INT) USING DELTA")
      sql("INSERT INTO t_case_alias VALUES ('east',10),('east',20),('west',5)")
      val expected =
        "SELECT region AS `RegionName`, sum(amount) AS `MetricValue_Daily` " +
          "FROM t_case_alias GROUP BY region"

      sql(s"CREATE MATERIALIZED VIEW mv_t_case_alias AS $expected")
      spark.table("mv_t_case_alias").schema.fieldNames.take(2) shouldBe
        Array("RegionName", "MetricValue_Daily")
      assertMvCorrect("mv_t_case_alias", expected)

      sql("INSERT INTO t_case_alias VALUES ('east',7)")
      refreshMv("mv_t_case_alias")
      spark.table("mv_t_case_alias").schema.fieldNames.take(2) shouldBe
        Array("RegionName", "MetricValue_Daily")
      assertMvCorrect("mv_t_case_alias", expected)
    }
  }
}
