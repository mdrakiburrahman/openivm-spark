package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6f — first half of the 1:1 ScalaTest port of openivm `test/sql/cte.test`.
  *
  * Covers sections §1–§8 of openivm/cte (basic aggregate, two- and three-table
  * joins, scalar SUM+COUNT, DISTINCT, HAVING, self-join, and the AVG-via-CTE
  * decomposition regression). See [[CteOpenIvmDmlSpec]] for §9 onwards.
  */
class CteOpenIvmSelectSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-cte-openivm-select-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-CteOpenIvmSelectSpec")
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

  private def refreshMv(name: String): Unit =
    spark.sql(s"REFRESH MATERIALIZED VIEW $name").collect()

  // ─────────────────────────────────────────────────────────────────────────
  // (1) Basic: MV with aggregate (LPTS generates CTEs internally)
  //     openivm cte.test lines 10–45
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §1: basic MV with aggregate (cte_base / cte_agg)") {
    it("incremental refresh of SUM/GROUP-BY tracks INSERTs to the base table") {
      spark.sql("CREATE TABLE cte_base (grp STRING, val INT) USING DELTA")
      spark.sql("INSERT INTO cte_base VALUES ('a', 10), ('a', 20), ('b', 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW cte_agg AS SELECT grp, SUM(val) AS total FROM cte_base GROUP BY grp"
      )

      spark.sql("INSERT INTO cte_base VALUES ('a', 5), ('c', 100)")
      refreshMv("cte_agg")
      assertMvCorrect(
        "cte_agg",
        "SELECT grp, SUM(val) AS total FROM cte_base GROUP BY grp"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (2) Multi-table join (LPTS generates nested CTEs)
  //     openivm cte.test lines 47–89
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §2: two-table inner join (cte_users / cte_orders / cte_joined)") {
    it("incremental refresh of join handles delta rows on one side") {
      spark.sql("CREATE TABLE cte_users (id INT, name STRING) USING DELTA")
      spark.sql("CREATE TABLE cte_orders (user_id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO cte_users VALUES (1, 'Alice'), (2, 'Bob')")
      spark.sql("INSERT INTO cte_orders VALUES (1, 100), (2, 200)")
      spark.sql(
        "CREATE MATERIALIZED VIEW cte_joined AS " +
          "SELECT u.name, o.amount FROM cte_users u INNER JOIN cte_orders o ON u.id = o.user_id"
      )

      spark.sql("INSERT INTO cte_orders VALUES (1, 50)")
      refreshMv("cte_joined")
      assertMvCorrect(
        "cte_joined",
        "SELECT u.name, o.amount FROM cte_users u INNER JOIN cte_orders o ON u.id = o.user_id"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (3) 3-table join (deeply nested CTEs)
  //     openivm cte.test lines 91–167
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §3: three-table inner join (cte_t1 / cte_t2 / cte_t3 / cte_3way)") {
    it("incremental refresh of 3-way join tracks both INSERTs and DELETEs") {
      spark.sql("CREATE TABLE cte_t1 (id INT, val STRING) USING DELTA")
      spark.sql("CREATE TABLE cte_t2 (t1_id INT, t2_id INT) USING DELTA")
      spark.sql("CREATE TABLE cte_t3 (id INT, label STRING) USING DELTA")
      spark.sql("INSERT INTO cte_t1 VALUES (1, 'x'), (2, 'y')")
      spark.sql("INSERT INTO cte_t2 VALUES (1, 10), (2, 20)")
      spark.sql("INSERT INTO cte_t3 VALUES (10, 'alpha'), (20, 'beta')")
      spark.sql(
        "CREATE MATERIALIZED VIEW cte_3way AS " +
          "SELECT t1.val, t3.label " +
          "FROM cte_t1 t1 " +
          "INNER JOIN cte_t2 t2 ON t1.id = t2.t1_id " +
          "INNER JOIN cte_t3 t3 ON t2.t2_id = t3.id"
      )

      // Insert and verify
      spark.sql("INSERT INTO cte_t2 VALUES (1, 20)")
      refreshMv("cte_3way")
      assertMvCorrect(
        "cte_3way",
        "SELECT t1.val, t3.label FROM cte_t1 t1 " +
          "INNER JOIN cte_t2 t2 ON t1.id = t2.t1_id " +
          "INNER JOIN cte_t3 t3 ON t2.t2_id = t3.id"
      )

      // Delete and verify
      spark.sql("DELETE FROM cte_t2 WHERE t1_id = 1 AND t2_id = 10")
      refreshMv("cte_3way")
      assertMvCorrect(
        "cte_3way",
        "SELECT t1.val, t3.label FROM cte_t1 t1 " +
          "INNER JOIN cte_t2 t2 ON t1.id = t2.t1_id " +
          "INNER JOIN cte_t3 t3 ON t2.t2_id = t3.id"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (4) Scalar aggregate (single-row MV via CTE)
  //     openivm cte.test lines 169–191
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §4: scalar aggregate (cte_scores / cte_total)") {
    it("single-row SUM+COUNT MV refreshes correctly after INSERT") {
      spark.sql("CREATE TABLE cte_scores (val INT) USING DELTA")
      spark.sql("INSERT INTO cte_scores VALUES (10), (20), (30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW cte_total AS SELECT SUM(val) AS total, COUNT(*) AS cnt FROM cte_scores"
      )

      spark.sql("INSERT INTO cte_scores VALUES (40)")
      refreshMv("cte_total")
      assertMvCorrect(
        "cte_total",
        "SELECT SUM(val) AS total, COUNT(*) AS cnt FROM cte_scores"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (5) DISTINCT via CTE (plan rewrite adds COUNT)
  //     openivm cte.test lines 193–219
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §5: DISTINCT via CTE (cte_colors / cte_distinct)") {
    it("DISTINCT MV tracks INSERTs of new and duplicate values") {
      spark.sql("CREATE TABLE cte_colors (color STRING) USING DELTA")
      spark.sql("INSERT INTO cte_colors VALUES ('red'), ('blue'), ('red'), ('green')")
      spark.sql("CREATE MATERIALIZED VIEW cte_distinct AS SELECT DISTINCT color FROM cte_colors")

      spark.sql("INSERT INTO cte_colors VALUES ('red'), ('yellow')")
      refreshMv("cte_distinct")
      assertMvCorrect(
        "cte_distinct",
        "SELECT DISTINCT color FROM cte_colors"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (6) CTE with aggregate + HAVING
  //     openivm cte.test lines 221–263
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §6: aggregate + HAVING (cte_sales / cte_big)") {
    it("HAVING-filtered aggregate MV tracks INSERTs that flip the HAVING predicate") {
      spark.sql("CREATE TABLE cte_sales (region STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO cte_sales VALUES ('US', 100), ('US', 200), ('EU', 50), ('EU', 30)")
      spark.sql(
        "CREATE MATERIALIZED VIEW cte_big AS " +
          "SELECT region, SUM(amount) AS total FROM cte_sales GROUP BY region HAVING SUM(amount) > 100"
      )

      // First check: initial state is consistent.
      assertMvCorrect(
        "cte_big",
        "SELECT region, SUM(amount) AS total FROM cte_sales GROUP BY region HAVING SUM(amount) > 100"
      )

      // Insert flips EU above the threshold.
      spark.sql("INSERT INTO cte_sales VALUES ('EU', 200)")
      refreshMv("cte_big")
      assertMvCorrect(
        "cte_big",
        "SELECT region, SUM(amount) AS total FROM cte_sales GROUP BY region HAVING SUM(amount) > 100"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (7) CTE with self-join
  //     openivm cte.test lines 265–300
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §7: self-join (cte_emp / cte_reports)") {
    it("employee/manager self-join MV tracks new rows in the single source table") {
      spark.sql("CREATE TABLE cte_emp (id INT, name STRING, mgr_id INT) USING DELTA")
      spark.sql("INSERT INTO cte_emp VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Carol', 1)")
      spark.sql(
        "CREATE MATERIALIZED VIEW cte_reports AS " +
          "SELECT e.name AS employee, m.name AS manager " +
          "FROM cte_emp e INNER JOIN cte_emp m ON e.mgr_id = m.id"
      )

      spark.sql("INSERT INTO cte_emp VALUES (4, 'Dave', 2)")
      refreshMv("cte_reports")
      assertMvCorrect(
        "cte_reports",
        "SELECT e.name AS employee, m.name AS manager " +
          "FROM cte_emp e INNER JOIN cte_emp m ON e.mgr_id = m.id"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // (8) CTE-wrapped AVG decomposition (q0535 regression)
  //     openivm cte.test lines 302–354
  // ─────────────────────────────────────────────────────────────────────────

  describe("openivm/cte §8: CTE-wrapped AVG decomposes correctly (cte_avg_t / mv_cte_avg)") {
    it("AVG-via-CTE MV stays equal to native AVG after UPDATE+INSERT") {
      spark.sql("CREATE TABLE cte_avg_t (id INT, tier STRING, bal INT) USING DELTA")
      spark.sql(
        "INSERT INTO cte_avg_t VALUES " +
          "(1, 'A', 100), (2, 'A', 200), (3, 'B', 50), (4, 'B', 150), (5, 'C', 300)"
      )
      spark.sql(
        """CREATE MATERIALIZED VIEW mv_cte_avg AS
          |WITH tiers AS (SELECT id, tier, bal FROM cte_avg_t)
          |SELECT tier, COUNT(*) AS cnt, AVG(bal) AS avg_bal FROM tiers GROUP BY tier""".stripMargin
      )

      // Initial consistency
      assertMvCorrect(
        "mv_cte_avg",
        "SELECT tier, COUNT(*) AS cnt, AVG(bal) AS avg_bal FROM cte_avg_t GROUP BY tier"
      )

      // Update values so AVG changes, then insert another B row.
      spark.sql("UPDATE cte_avg_t SET bal = bal + 1000 WHERE tier = 'A'")
      spark.sql("INSERT INTO cte_avg_t VALUES (6, 'B', 250)")
      refreshMv("mv_cte_avg")
      assertMvCorrect(
        "mv_cte_avg",
        "SELECT tier, COUNT(*) AS cnt, AVG(bal) AS avg_bal FROM cte_avg_t GROUP BY tier"
      )
    }
  }
}
