package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.commands.{
  CreateMaterializedViewCommand,
  DropMaterializedViewCommand,
  RefreshMaterializedViewCommand
}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Slice of `ParserSpec` covering CREATE / REFRESH / DROP wiring, MIN/MAX
  * with GROUP BY, whitespace + mixed-case tolerance, and simple
  * projection / projection-with-filter MVs.
  *
  * Each slice runs in its own forked JVM (see `Settings.parallelForkSettings`),
  * which is why every slice carries its own SparkSession, warehouse dir and
  * helper bundle — they cannot share a base trait without serialising the
  * JVMs.
  */
class ParserCreateSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-parser-create-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-ParserCreateSpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
      deleteDir(new File(warehouseDir))
    } finally {
      super.afterAll()
    }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  private def assertMvCorrect(mvName: String, expectedSql: String): Unit = {
    val expected: DataFrame = spark.sql(expectedSql)
    val userCols            = expected.columns.toSeq
    val mv                  = spark.table(mvName).select(userCols.head, userCols.tail: _*)
    withClue(s"$mvName EXCEPT ALL <expected>: ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName: ") {
      expected.exceptAll(mv).count() shouldBe 0L
    }
  }

  private def mvExists(name: String): Boolean =
    MvCatalog.lookup(spark, TableIdentifier(name)).isDefined

  // ──────────────────────────────────────────────────────────────────────────
  // (1) The injected IvmParser drives every CREATE/REFRESH/DROP MV statement
  //     end-to-end through Spark — proves parser injection actually fires.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(1) IvmParser is wired into SparkSession via OpenIvmSparkExtensions") {

    it("CREATE MATERIALIZED VIEW reaches CreateMaterializedViewCommand and executes") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS sales_p1(id INT, region STRING, amount INT) USING DELTA"
      )
      spark.sql("INSERT INTO sales_p1 VALUES (1,'east',100), (2,'west',200), (3,'east',150)")

      // Inspect the parsed plan AND execute it.
      val plan = spark.sessionState.sqlParser.parsePlan(
        "CREATE MATERIALIZED VIEW mv_sales_p1 AS " +
          "SELECT region, sum(amount) AS sum_amount, count(amount) AS count_amount " +
          "FROM sales_p1 GROUP BY region"
      )
      plan shouldBe a[CreateMaterializedViewCommand]

      spark.sql(
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

      spark.sql("INSERT INTO sales_p1 VALUES (4, 'east', 50)")
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

      spark.sql("DROP MATERIALIZED VIEW mv_sales_p1")
      mvExists("mv_sales_p1") shouldBe false
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // (2) MIN/MAX with GROUP BY (parser.test Test 2): execution → INSERT →
  //     REFRESH → DELETE → REFRESH all produce correct results.
  // ──────────────────────────────────────────────────────────────────────────
  describe("(2) MIN/MAX with GROUP BY — end-to-end") {

    it("creates the MV, refreshes after INSERT and DELETE, and remains bag-equivalent") {
      spark.sql("CREATE TABLE IF NOT EXISTS scores_p2(id INT, team STRING, score INT) USING DELTA")
      spark.sql(
        "INSERT INTO scores_p2 VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',5)"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_scores_p2 AS " +
          "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )
      assertMvCorrect(
        "mv_scores_p2",
        "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )

      spark.sql("INSERT INTO scores_p2 VALUES (5,'a',1)")
      refreshMv("mv_scores_p2")
      assertMvCorrect(
        "mv_scores_p2",
        "SELECT team, min(score) AS min_score, max(score) AS max_score FROM scores_p2 GROUP BY team"
      )

      // Delete the current `a` min — group_recompute or refresh must recover next min.
      spark.sql("DELETE FROM scores_p2 WHERE id = 5")
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
      spark.sql("CREATE TABLE IF NOT EXISTS t_ws(id INT, grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO t_ws VALUES (1,'a',5)")
      spark.sql(
        "CREATE  MATERIALIZED  VIEW  mv_t_ws  AS  " +
          "SELECT  grp, sum(val) AS sum_val  FROM  t_ws  GROUP  BY  grp"
      )
      assertMvCorrect(
        "mv_t_ws",
        "SELECT grp, sum(val) AS sum_val FROM t_ws GROUP BY grp"
      )
    }

    it("accepts mixed-case keywords: 'Create Materialized View … Select …'") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_mc(id INT, grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO t_mc VALUES (1,'a',5),(2,'a',10)")
      spark.sql(
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
      spark.sql("CREATE TABLE IF NOT EXISTS t_tabs(id INT, grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO t_tabs VALUES (1,'z',99)")
      spark.sql(
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
      spark.sql("CREATE TABLE IF NOT EXISTS t_proj(id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO t_proj VALUES (1,'alice'),(2,'bob')")
      spark.sql("CREATE MATERIALIZED VIEW mv_t_proj AS SELECT id, name FROM t_proj")
      assertMvCorrect("mv_t_proj", "SELECT id, name FROM t_proj")

      spark.sql("INSERT INTO t_proj VALUES (3,'carol')")
      refreshMv("mv_t_proj")
      assertMvCorrect("mv_t_proj", "SELECT id, name FROM t_proj")
    }

    it("a projection-with-filter MV obeys the WHERE clause both initially and after DML") {
      spark.sql("CREATE TABLE IF NOT EXISTS t_filt(id INT, status STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO t_filt VALUES (1,'active',10),(2,'inactive',20),(3,'active',30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_t_filt AS SELECT id, val FROM t_filt WHERE status = 'active'"
      )
      assertMvCorrect("mv_t_filt", "SELECT id, val FROM t_filt WHERE status = 'active'")

      spark.sql("INSERT INTO t_filt VALUES (4,'inactive',999),(5,'active',5)")
      refreshMv("mv_t_filt")
      assertMvCorrect("mv_t_filt", "SELECT id, val FROM t_filt WHERE status = 'active'")
    }
  }
}
