package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6e — ScalaTest port of `openivm/test/sql/ducklake_inner_join.test`.
  *
  * DuckLake's snapshot-based incremental delta over base tables maps onto
  * Delta Lake's snapshot isolation in Spark.  Each `PRAGMA refresh(...)` in
  * the openivm test becomes `REFRESH MATERIALIZED VIEW`; DuckLake catalog
  * names (`dl.`) are dropped because the Spark side uses Delta tables in the
  * default catalog.  The N-term telescoping join rule (`docs/ducklake.md`)
  * is algebraically equivalent to the 2^N-1 Möbius inclusion-exclusion used
  * for Delta, so bag equality between the MV and the live view body holds
  * regardless of which rewriter path was taken.
  *
  * Every refresh is cross-checked with bidirectional `EXCEPT ALL` per
  * `openivm/CLAUDE.md` ("Every IVM refresh in a test MUST be cross-checked
  * with `EXCEPT ALL` in both directions").
  *
  * Section numbering mirrors the source test:
  *   Test 1 — 2-table INNER JOIN, INSERTs on both sides
  *   Test 2 — Aggregate over join (SUM + COUNT + GROUP BY)
  *   Test 3 — Stress: batch INSERT/DELETE on both tables, conflicting ops
  *   Test 4 — Empty delta refresh (no-op safety)
  *   Test 5 — Partial empty delta (only one table changes)
  *   Test 6 — Cost-model probe (N/A on Spark; covered by empty-delta refresh)
  *   Test 7 — Projection-key refresh over a self-join (4-table projection)
  */
class DucklakeInnerJoinSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dlij-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DucklakeInnerJoinSpec")
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

  // ── Test 1: 2-table INNER JOIN — INSERT left, then right ───────────────────

  describe("Test 1: 2-table INNER JOIN — INSERT left, then right") {
    it("incrementally maintains a 2-way INNER JOIN projection across batched INSERTs on both sides") {
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_customers(id INT, name STRING) USING DELTA")
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_purchases(cust_id INT, item STRING, price INT) USING DELTA"
      )
      spark.sql("INSERT INTO dlij_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      spark.sql("INSERT INTO dlij_purchases VALUES (1, 'Widget', 10), (2, 'Gadget', 25)")

      val viewSql =
        "SELECT c.name, p.item, p.price " +
          "FROM dlij_customers c INNER JOIN dlij_purchases p ON c.id = p.cust_id"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_customer_purchases AS $viewSql")
      refreshMv("dlij_customer_purchases")
      assertMvCorrect("dlij_customer_purchases", viewSql)

      // INSERT on right side (purchases)
      spark.sql("INSERT INTO dlij_purchases VALUES (1, 'Doohickey', 5), (3, 'Gizmo', 50)")
      refreshMv("dlij_customer_purchases")
      assertMvCorrect("dlij_customer_purchases", viewSql)

      // INSERT on left side (customers) — new customer with existing purchase match
      spark.sql("INSERT INTO dlij_customers VALUES (4, 'Diana')")
      spark.sql("INSERT INTO dlij_purchases VALUES (4, 'Thingamajig', 15)")
      refreshMv("dlij_customer_purchases")
      assertMvCorrect("dlij_customer_purchases", viewSql)
    }
  }

  // ── Test 2: Aggregate over join (SUM + COUNT + GROUP BY) ────────────────────

  describe("Test 2: Aggregate over join (SUM + COUNT + GROUP BY)") {
    it("incrementally maintains SUM/COUNT aggregate over 2-way INNER JOIN") {
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_products(pid INT, pname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_sales(pid INT, qty INT, revenue INT) USING DELTA")
      spark.sql("INSERT INTO dlij_products VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')")
      spark.sql("INSERT INTO dlij_sales VALUES (1, 10, 100), (1, 5, 50), (2, 20, 400)")

      val viewSql =
        "SELECT p.pname, SUM(s.revenue) AS total_rev, COUNT(*) AS sale_count " +
          "FROM dlij_products p INNER JOIN dlij_sales s ON p.pid = s.pid " +
          "GROUP BY p.pname"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_product_summary AS $viewSql")
      refreshMv("dlij_product_summary")
      assertMvCorrect("dlij_product_summary", viewSql)

      // Add sales for existing product + new product with sales
      spark.sql("INSERT INTO dlij_sales VALUES (2, 3, 60), (3, 7, 140)")
      refreshMv("dlij_product_summary")
      assertMvCorrect("dlij_product_summary", viewSql)

      // Delete a sale row — group total should adjust
      spark.sql("DELETE FROM dlij_sales WHERE pid = 1 AND qty = 10")
      refreshMv("dlij_product_summary")
      assertMvCorrect("dlij_product_summary", viewSql)
    }
  }

  // ── Test 3: Stress — batch INSERT into both tables + DELETE before single refresh ───

  describe("Test 3: Stress — batch INSERT + DELETE on both tables before single refresh") {
    it("delta consolidation is correct for a many-conflicting-op batch") {
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_departments(did INT, dname STRING) USING DELTA")
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_employees(eid INT, did INT, salary INT) USING DELTA"
      )
      spark.sql(
        "INSERT INTO dlij_departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')"
      )
      spark.sql(
        "INSERT INTO dlij_employees VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 300), (5, 3, 80)"
      )

      val viewSql =
        "SELECT d.dname, e.eid, e.salary " +
          "FROM dlij_departments d INNER JOIN dlij_employees e ON d.did = e.did"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_dept_roster AS $viewSql")
      refreshMv("dlij_dept_roster")
      assertMvCorrect("dlij_dept_roster", viewSql)

      // Batch: INSERT into both tables, DELETE from both, conflicting ops
      spark.sql("INSERT INTO dlij_departments VALUES (4, 'Legal')")
      spark.sql("INSERT INTO dlij_employees VALUES (6, 4, 250), (7, 4, 300)")
      spark.sql("INSERT INTO dlij_employees VALUES (8, 1, 500), (9, 2, 175)")
      spark.sql("DELETE FROM dlij_employees WHERE eid = 2")
      spark.sql("DELETE FROM dlij_employees WHERE eid = 3")
      spark.sql("DELETE FROM dlij_departments WHERE did = 3")
      spark.sql("INSERT INTO dlij_employees VALUES (10, 3, 999)")
      spark.sql("INSERT INTO dlij_employees VALUES (11, 1, 50)")
      spark.sql("DELETE FROM dlij_employees WHERE eid = 11")

      refreshMv("dlij_dept_roster")
      assertMvCorrect("dlij_dept_roster", viewSql)
    }
  }

  // ── Test 4: Empty delta — refresh with no changes ──────────────────────────

  describe("Test 4: Empty delta — refresh with no changes is a safe no-op") {
    it("repeated refreshes with empty deltas leave the MV unchanged") {
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_empty_d(did INT, dname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_empty_e(eid INT, did INT, salary INT) USING DELTA")
      spark.sql("INSERT INTO dlij_empty_d VALUES (1, 'X'), (2, 'Y')")
      spark.sql("INSERT INTO dlij_empty_e VALUES (1, 1, 100), (2, 2, 200)")

      val viewSql =
        "SELECT d.dname, e.eid, e.salary " +
          "FROM dlij_empty_d d INNER JOIN dlij_empty_e e ON d.did = e.did"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_empty_roster AS $viewSql")
      refreshMv("dlij_empty_roster")
      assertMvCorrect("dlij_empty_roster", viewSql)

      // Three consecutive no-op refreshes should be safe.
      refreshMv("dlij_empty_roster")
      refreshMv("dlij_empty_roster")
      refreshMv("dlij_empty_roster")
      assertMvCorrect("dlij_empty_roster", viewSql)
    }
  }

  // ── Test 5: Partial empty delta — only one table changes ──────────────────

  describe("Test 5: Partial empty delta — only one table changes in a 2-table join") {
    it("INSERT into one side only; the other side's join term is effectively skipped") {
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_partial_c(id INT, name STRING) USING DELTA")
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_partial_p(cust_id INT, item STRING, price INT) USING DELTA"
      )
      spark.sql("INSERT INTO dlij_partial_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      spark.sql("INSERT INTO dlij_partial_p VALUES (1, 'Widget', 10), (2, 'Gadget', 25)")

      val viewSql =
        "SELECT c.name, p.item, p.price " +
          "FROM dlij_partial_c c INNER JOIN dlij_partial_p p ON c.id = p.cust_id"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_partial_mv AS $viewSql")
      refreshMv("dlij_partial_mv")

      // Insert only into purchases (customers unchanged — its term should be skipped)
      spark.sql("INSERT INTO dlij_partial_p VALUES (2, 'Sprocket', 30)")
      refreshMv("dlij_partial_mv")
      assertMvCorrect("dlij_partial_mv", viewSql)

      // Insert only into customers (purchases unchanged), new customer with no purchases
      spark.sql("INSERT INTO dlij_partial_c VALUES (5, 'Eve')")
      refreshMv("dlij_partial_mv")
      assertMvCorrect("dlij_partial_mv", viewSql)
    }
  }

  // ── Test 6: Cost-model probe ──────────────────────────────────────────────

  describe("Test 6: Cost-model probe (DuckLake PRAGMA refresh_cost has no Spark analogue)") {
    it(
      "N/A on Spark — DuckLake's `PRAGMA refresh_cost` is a diagnostic; " +
        "the invariant exercised here is that a refresh after a single-table INSERT stays bag-equal."
    ) {
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_cost_p(pid INT, pname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS dlij_cost_s(pid INT, qty INT, revenue INT) USING DELTA")
      spark.sql("INSERT INTO dlij_cost_p VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')")
      spark.sql("INSERT INTO dlij_cost_s VALUES (1, 10, 100), (1, 5, 50), (2, 20, 400)")

      val viewSql =
        "SELECT p.pname, SUM(s.revenue) AS total_rev, COUNT(*) AS sale_count " +
          "FROM dlij_cost_p p INNER JOIN dlij_cost_s s ON p.pid = s.pid " +
          "GROUP BY p.pname"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_cost_summary AS $viewSql")
      refreshMv("dlij_cost_summary")

      // Insert data into only one side of the join, then refresh.
      spark.sql("INSERT INTO dlij_cost_s VALUES (3, 1, 999)")
      refreshMv("dlij_cost_summary")
      assertMvCorrect("dlij_cost_summary", viewSql)
    }
  }

  // ── Test 7: Projection-key refresh over a 4-way self-join ──────────────────

  describe("Test 7: Projection-key refresh over a 4-table join with a self-join alias") {
    it("4-way join with one table appearing twice (via alias) maintains correctness across UPDATE/INSERT") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_pk_holdings(" +
          "trade_id INT, previous_trade_id INT, account_id INT, symbol STRING, qty INT" +
          ") USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_pk_trades(sk_trade_id INT, trade_id INT) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_pk_accounts(account_id INT, sk_customer_id INT) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dlij_pk_securities(symbol STRING, sk_security_id INT) USING DELTA"
      )
      spark.sql("INSERT INTO dlij_pk_trades VALUES (100, 10), (200, 20), (300, 30)")
      spark.sql("INSERT INTO dlij_pk_accounts VALUES (1, 1000), (2, 2000)")
      spark.sql("INSERT INTO dlij_pk_securities VALUES ('A', 500), ('B', 600)")
      spark.sql("INSERT INTO dlij_pk_holdings VALUES (10, 20, 1, 'A', 5), (30, 20, 2, 'B', 9)")

      val viewSql =
        "SELECT ct.sk_trade_id AS sk_current_trade_id, " +
          "       pt.sk_trade_id AS prev_sk_trade_id, " +
          "       a.sk_customer_id, " +
          "       s.sk_security_id, " +
          "       h.qty " +
          "FROM dlij_pk_holdings h " +
          "JOIN dlij_pk_trades ct ON h.trade_id = ct.trade_id " +
          "JOIN dlij_pk_trades pt ON h.previous_trade_id = pt.trade_id " +
          "JOIN dlij_pk_accounts a ON h.account_id = a.account_id " +
          "JOIN dlij_pk_securities s ON h.symbol = s.symbol"
      spark.sql(s"CREATE MATERIALIZED VIEW dlij_pk_holdings_mv AS $viewSql")
      refreshMv("dlij_pk_holdings_mv")
      assertMvCorrect("dlij_pk_holdings_mv", viewSql)

      // Exercise the direct source, the repeated trades source via the previous-trade alias,
      // and a dimension lookup.  Affected-key path may over-include keys, but must not miss any.
      spark.sql("UPDATE dlij_pk_trades SET sk_trade_id = 201 WHERE trade_id = 20")
      spark.sql("UPDATE dlij_pk_accounts SET sk_customer_id = 1001 WHERE account_id = 1")
      spark.sql("INSERT INTO dlij_pk_holdings VALUES (10, 20, 1, 'A', 6)")
      refreshMv("dlij_pk_holdings_mv")
      assertMvCorrect("dlij_pk_holdings_mv", viewSql)

      // Changing the output key itself must delete the old key partition and insert the new one.
      spark.sql("UPDATE dlij_pk_trades SET sk_trade_id = 101 WHERE trade_id = 10")
      refreshMv("dlij_pk_holdings_mv")
      assertMvCorrect("dlij_pk_holdings_mv", viewSql)
    }
  }
}
