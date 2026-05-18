package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Delete-heavy and DML-edge slice of the openivm `inner_join.test` parity
  * port: net-zero INSERT+DELETE batches, INNER-JOIN NULL-key exclusion,
  * self-join cascading delete, JOIN + filter on both sides, JOIN + DISTINCT
  * delete semantics, and constant-rowset JOIN with batched INSERT/DELETE/UPDATE.
  * See [[InnerJoinInsertSpec]] for the multi-table, CROSS-JOIN, and
  * unmatched-key scenarios.
  */
class InnerJoinDeleteSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-ij-delete-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-InnerJoinDeleteSpec")
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

  // ============================================================================
  // (4) mv_join_nz — INSERT + DELETE same row in one batch (net zero delta).
  //     Mirrors openivm lines 372–469.
  // ============================================================================

  describe("(4) mv_join_nz — INSERT+DELETE same row in one batch (net-zero delta)") {
    it("batched INSERT followed by DELETE of the same row before refresh produces no MV change") {
      spark.sql("CREATE TABLE IF NOT EXISTS jn_left_nz (id INT, val STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS jn_right_nz (id INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO jn_left_nz VALUES (1, 'a'), (2, 'b')")
      spark.sql("INSERT INTO jn_right_nz VALUES (1, 100), (2, 200)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_join_nz AS " +
          "SELECT l.val, r.amount FROM jn_left_nz l INNER JOIN jn_right_nz r ON l.id = r.id"
      )

      val viewBody =
        "SELECT l.val, r.amount FROM jn_left_nz l INNER JOIN jn_right_nz r ON l.id = r.id"

      refreshMv("mv_join_nz")
      assertMvCorrect("mv_join_nz", viewBody)

      // INSERT + DELETE same 999 row before refresh — net zero
      spark.sql("INSERT INTO jn_right_nz VALUES (1, 999)")
      spark.sql("DELETE FROM jn_right_nz WHERE id = 1 AND amount = 999")
      refreshMv("mv_join_nz")
      assertMvCorrect("mv_join_nz", viewBody)

      // Insert new row for id=3 (no left match) + delete id=2 from right — batched
      spark.sql("INSERT INTO jn_right_nz VALUES (3, 300)")
      spark.sql("DELETE FROM jn_right_nz WHERE id = 2")
      refreshMv("mv_join_nz")
      assertMvCorrect("mv_join_nz", viewBody)
    }
  }

  // ============================================================================
  // (5) mv_join_null — NULL join keys never match in INNER JOIN.
  //     Mirrors openivm lines 471–561.
  // ============================================================================

  describe("(5) mv_join_null — NULL join keys (INNER JOIN equality is NULL-rejecting)") {
    it("rows with NULL join keys are excluded from INNER JOIN under all DML mutations") {
      spark.sql("CREATE TABLE IF NOT EXISTS jn_left_null (id INT, name STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS jn_right_null (id INT, score INT) USING DELTA")
      spark.sql("INSERT INTO jn_left_null VALUES (1, 'a'), (NULL, 'b'), (3, 'c')")
      spark.sql("INSERT INTO jn_right_null VALUES (1, 10), (NULL, 20), (3, 30)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_join_null AS " +
          "SELECT l.name, r.score FROM jn_left_null l INNER JOIN jn_right_null r ON l.id = r.id"
      )

      val viewBody =
        "SELECT l.name, r.score FROM jn_left_null l INNER JOIN jn_right_null r ON l.id = r.id"

      refreshMv("mv_join_null")
      assertMvCorrect("mv_join_null", viewBody)

      // Insert another NULL on right — still no NULL matches in INNER JOIN
      spark.sql("INSERT INTO jn_right_null VALUES (NULL, 99)")
      refreshMv("mv_join_null")
      assertMvCorrect("mv_join_null", viewBody)

      // Delete NULL row from left — MV stays bag-equal (NULLs were never joined)
      spark.sql("DELETE FROM jn_left_null WHERE id IS NULL")
      refreshMv("mv_join_null")
      assertMvCorrect("mv_join_null", viewBody)
    }
  }

  // ============================================================================
  // (6) mv_reports — self-join (employee / manager).
  //     Mirrors openivm lines 563–652.
  // ============================================================================

  describe("(6) mv_reports — self-join: people aliased twice as employees and managers") {
    it("self-join cascades: deleting a manager removes all rows reporting to them") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS people (id INT, name STRING, manager_id INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO people VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Charlie', 1), (4, 'Diana', 2)"
      )

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_reports AS " +
          "SELECT e.name AS employee, m.name AS manager " +
          "FROM people e INNER JOIN people m ON e.manager_id = m.id"
      )

      val viewBody =
        "SELECT e.name AS employee, m.name AS manager " +
          "FROM people e INNER JOIN people m ON e.manager_id = m.id"

      assertMvCorrect("mv_reports", viewBody)

      // Insert new person who reports to Charlie
      spark.sql("INSERT INTO people VALUES (5, 'Eve', 3)")
      refreshMv("mv_reports")
      assertMvCorrect("mv_reports", viewBody)

      // Delete a manager — cascades to their reports disappearing
      spark.sql("DELETE FROM people WHERE id = 1")
      refreshMv("mv_reports")
      assertMvCorrect("mv_reports", viewBody)

      // Insert the manager back + a new report simultaneously
      spark.sql("INSERT INTO people VALUES (1, 'Alice', NULL), (6, 'Frank', 1)")
      refreshMv("mv_reports")
      assertMvCorrect("mv_reports", viewBody)
    }
  }

  // ============================================================================
  // (8) mv_expensive_sales — JOIN + FILTER. Mirrors openivm lines 783–852.
  // ============================================================================

  describe("(8) mv_expensive_sales — JOIN + WHERE filter on joined columns") {
    it("filter is preserved across batched DML on both sides") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS jf_products (id INT, name STRING, price INT) USING DELTA"
      )
      spark.sql("CREATE TABLE IF NOT EXISTS jf_sales (product_id INT, qty INT) USING DELTA")
      spark.sql(
        "INSERT INTO jf_products VALUES (1, 'Widget', 10), (2, 'Gadget', 50), (3, 'Doohickey', 5)"
      )
      spark.sql("INSERT INTO jf_sales VALUES (1, 100), (2, 20), (3, 200), (1, 50)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_expensive_sales AS " +
          "SELECT p.name, s.qty, p.price " +
          "FROM jf_products p INNER JOIN jf_sales s ON p.id = s.product_id " +
          "WHERE p.price > 8"
      )

      val viewBody =
        "SELECT p.name, s.qty, p.price " +
          "FROM jf_products p INNER JOIN jf_sales s ON p.id = s.product_id " +
          "WHERE p.price > 8"

      assertMvCorrect("mv_expensive_sales", viewBody)

      // Insert sale for cheap product (filtered out) + expensive product
      spark.sql("INSERT INTO jf_sales VALUES (3, 10), (2, 5)")
      refreshMv("mv_expensive_sales")
      assertMvCorrect("mv_expensive_sales", viewBody)

      // Delete from both sides simultaneously (batched)
      spark.sql("DELETE FROM jf_sales WHERE product_id = 1 AND qty = 100")
      spark.sql("DELETE FROM jf_products WHERE id = 3")
      refreshMv("mv_expensive_sales")
      assertMvCorrect("mv_expensive_sales", viewBody)
    }
  }

  // ============================================================================
  // (9) mv_unique_tags — JOIN + DISTINCT. Mirrors openivm lines 854–919.
  // ============================================================================

  describe("(9) mv_unique_tags — JOIN + DISTINCT collapses duplicates") {
    it("DISTINCT semantics preserved: duplicate INSERTs don't grow MV; full-key DELETE removes the row") {
      spark.sql("CREATE TABLE IF NOT EXISTS jd_tags (item_id INT, tag STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS jd_items (id INT, name STRING) USING DELTA")
      spark.sql("INSERT INTO jd_items VALUES (1, 'Foo'), (2, 'Bar')")
      spark.sql("INSERT INTO jd_tags VALUES (1, 'hot'), (1, 'new'), (2, 'hot'), (2, 'hot')")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_unique_tags AS " +
          "SELECT DISTINCT i.name, t.tag " +
          "FROM jd_items i INNER JOIN jd_tags t ON i.id = t.item_id"
      )

      val viewBody =
        "SELECT DISTINCT i.name, t.tag " +
          "FROM jd_items i INNER JOIN jd_tags t ON i.id = t.item_id"

      assertMvCorrect("mv_unique_tags", viewBody)

      // Insert duplicate tag (shouldn't change DISTINCT result)
      spark.sql("INSERT INTO jd_tags VALUES (1, 'hot')")
      refreshMv("mv_unique_tags")
      assertMvCorrect("mv_unique_tags", viewBody)

      // Delete all 'hot' tags for Bar — (Bar, hot) should disappear
      spark.sql("DELETE FROM jd_tags WHERE item_id = 2")
      refreshMv("mv_unique_tags")
      assertMvCorrect("mv_unique_tags", viewBody)
    }
  }

  // ============================================================================
  // (12) mv_const_unnest_join — constant-rowset under a join aggregate.
  //      Mirrors openivm lines 1011–1067.
  // ============================================================================

  describe("(12) mv_const_unnest_join — constant rowset JOINed against base table under aggregate") {
    it("batched INSERT+DELETE+UPDATE before a single refresh yields a correct aggregate") {
      spark.sql("CREATE TABLE IF NOT EXISTS unnest_stock (s_id INT, qty INT) USING DELTA")
      spark.sql("INSERT INTO unnest_stock VALUES (1, 5), (2, 55), (3, 120)")

      // DuckDB: (SELECT unnest([10, 50, 100]) AS threshold) t
      // Spark : (VALUES (10), (50), (100)) AS t(threshold)
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_const_unnest_join AS " +
          "SELECT t.threshold, COUNT(*) AS above_cnt " +
          "FROM (VALUES (10), (50), (100)) AS t(threshold) " +
          "JOIN unnest_stock s ON s.qty >= t.threshold " +
          "GROUP BY t.threshold"
      )

      val viewBody =
        "SELECT t.threshold, COUNT(*) AS above_cnt " +
          "FROM (VALUES (10), (50), (100)) AS t(threshold) " +
          "JOIN unnest_stock s ON s.qty >= t.threshold " +
          "GROUP BY t.threshold"

      assertMvCorrect("mv_const_unnest_join", viewBody)

      // Batched DML stress (INSERT + DELETE + UPDATE on overlapping rows) → one refresh
      spark.sql("INSERT INTO unnest_stock VALUES (4, 80)")
      spark.sql("DELETE FROM unnest_stock WHERE s_id = 1")
      spark.sql("UPDATE unnest_stock SET qty = 8 WHERE s_id = 3")
      refreshMv("mv_const_unnest_join")
      assertMvCorrect("mv_const_unnest_join", viewBody)
    }
  }
}
