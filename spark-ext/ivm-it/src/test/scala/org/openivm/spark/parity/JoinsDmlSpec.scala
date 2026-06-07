package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** DML-focused slice of the [[JoinsSpec]] coverage (one-sided INSERTs/DELETEs,
  * batched mixed DML, key migration, FK-pruning star schema). See
  * JoinsInnerSpec / JoinsOuterSpec for the remaining cases.
  */
class JoinsDmlSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-joins-dml-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-JoinsDmlSpec")
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

  private def mvRefreshType(name: String): Int = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
    MvCatalog.lookup(spark, id).getOrElse(fail(s"MV $name not found in catalog")).refreshType
  }

  // ============================================================================
  // DML scenarios
  // ============================================================================

  describe("(9) INNER JOIN aggregate — INSERT on LEFT side that joins existing RIGHT rows adds to MV") {
    it("inserting a new left key that matches existing right rows yields the expected group") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml9_a(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml9_b(k INT, v INT) USING DELTA")
      // RIGHT side already has rows for k=5 with no matching LEFT row
      spark.sql("INSERT INTO j_dml9_a VALUES (1, 'A1')")
      spark.sql("INSERT INTO j_dml9_b VALUES (1, 10), (5, 50), (5, 60)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_dml9 AS " +
          "SELECT a.label, SUM(b.v) AS total " +
          "FROM j_dml9_a a JOIN j_dml9_b b ON a.k = b.k GROUP BY a.label"
      )

      // Initially, k=5 has no LEFT match — those right rows are not in the MV.
      // Now add LEFT k=5: the MV must gain a new group 'A5' with total 50+60=110.
      spark.sql("INSERT INTO j_dml9_a VALUES (5, 'A5')")
      refreshMv("mv_j_dml9")
      assertMvCorrect(
        "mv_j_dml9",
        "SELECT a.label, SUM(b.v) AS total " +
          "FROM j_dml9_a a JOIN j_dml9_b b ON a.k = b.k GROUP BY a.label"
      )
    }
  }

  describe("(10) INNER JOIN projection — INSERT on RIGHT that joins existing LEFT adds MV rows") {
    it("inserting matching right rows produces new join-projection rows after refresh") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml10_l(id INT, lname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml10_r(lid INT, val INT) USING DELTA")
      spark.sql("INSERT INTO j_dml10_l VALUES (1, 'Alice'), (2, 'Bob')")
      spark.sql("INSERT INTO j_dml10_r VALUES (1, 10)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_dml10 AS " +
          "SELECT l.lname, r.val FROM j_dml10_l l JOIN j_dml10_r r ON l.id = r.lid"
      )

      // INSERT on the right side referencing both existing left rows
      spark.sql("INSERT INTO j_dml10_r VALUES (1, 20), (2, 30), (2, 40)")
      refreshMv("mv_j_dml10")
      assertMvCorrect(
        "mv_j_dml10",
        "SELECT l.lname, r.val FROM j_dml10_l l JOIN j_dml10_r r ON l.id = r.lid"
      )
    }
  }

  describe("(11) LEFT JOIN aggregate — DELETE on LEFT side removes group from MV") {
    it("deleting a left row removes its entry from the aggregate") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml11_c(id INT, cname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml11_o(cid INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO j_dml11_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      spark.sql("INSERT INTO j_dml11_o VALUES (1, 100), (1, 200), (2, 50)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_dml11 AS " +
          "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_dml11_c c LEFT JOIN j_dml11_o o ON c.id = o.cid GROUP BY c.cname"
      )

      // Delete a customer who has matching orders — group 'Alice' disappears
      spark.sql("DELETE FROM j_dml11_c WHERE id = 1")
      refreshMv("mv_j_dml11")
      assertMvCorrect(
        "mv_j_dml11",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_dml11_c c LEFT JOIN j_dml11_o o ON c.id = o.cid GROUP BY c.cname"
      )

      // Delete a customer with no matching orders — group 'Charlie' disappears
      spark.sql("DELETE FROM j_dml11_c WHERE id = 3")
      refreshMv("mv_j_dml11")
      assertMvCorrect(
        "mv_j_dml11",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_dml11_c c LEFT JOIN j_dml11_o o ON c.id = o.cid GROUP BY c.cname"
      )
    }
  }

  describe("(12) INNER JOIN aggregate — DELETE on RIGHT side updates SUM (and may drop a group)") {
    it("deleting right rows reduces SUM in the matched group; deleting all matched rows drops the group") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml12_a(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml12_b(k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO j_dml12_a VALUES (1, 'A1'), (2, 'A2')")
      spark.sql("INSERT INTO j_dml12_b VALUES (1, 10), (1, 20), (2, 30)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_dml12 AS " +
          "SELECT a.label, SUM(b.v) AS total " +
          "FROM j_dml12_a a JOIN j_dml12_b b ON a.k = b.k GROUP BY a.label"
      )

      // Delete one of A1's right rows — A1 total goes from 30 to 20
      spark.sql("DELETE FROM j_dml12_b WHERE k = 1 AND v = 10")
      refreshMv("mv_j_dml12")
      assertMvCorrect(
        "mv_j_dml12",
        "SELECT a.label, SUM(b.v) AS total " +
          "FROM j_dml12_a a JOIN j_dml12_b b ON a.k = b.k GROUP BY a.label"
      )

      // Delete all of A2's right rows — INNER JOIN means A2 group disappears entirely
      spark.sql("DELETE FROM j_dml12_b WHERE k = 2")
      refreshMv("mv_j_dml12")
      assertMvCorrect(
        "mv_j_dml12",
        "SELECT a.label, SUM(b.v) AS total " +
          "FROM j_dml12_a a JOIN j_dml12_b b ON a.k = b.k GROUP BY a.label"
      )
    }
  }

  describe("(13) INNER JOIN aggregate — UPDATE join key migrates a row between groups") {
    it("changing a right row's join key reattaches it to a different left group") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml13_a(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml13_b(k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO j_dml13_a VALUES (1, 'A1'), (2, 'A2')")
      spark.sql("INSERT INTO j_dml13_b VALUES (1, 10), (1, 20), (2, 5)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_dml13 AS " +
          "SELECT a.label, SUM(b.v) AS total, COUNT(*) AS n " +
          "FROM j_dml13_a a JOIN j_dml13_b b ON a.k = b.k GROUP BY a.label"
      )

      // Migrate a b-row from k=1 to k=2: A1 loses 20, A2 gains 20.
      spark.sql("UPDATE j_dml13_b SET k = 2 WHERE k = 1 AND v = 20")
      refreshMv("mv_j_dml13")
      assertMvCorrect(
        "mv_j_dml13",
        "SELECT a.label, SUM(b.v) AS total, COUNT(*) AS n " +
          "FROM j_dml13_a a JOIN j_dml13_b b ON a.k = b.k GROUP BY a.label"
      )
    }
  }

  describe("(14) INNER JOIN aggregate — batched DML on BOTH sides → single REFRESH") {
    it("a single REFRESH after mixed INSERT+DELETE+UPDATE on both sides yields correct merged result") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml14_a(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_dml14_b(k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO j_dml14_a VALUES (1, 'A1'), (2, 'A2'), (3, 'A3')")
      spark.sql("INSERT INTO j_dml14_b VALUES (1, 10), (1, 20), (2, 30), (3, 40)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_dml14 AS " +
          "SELECT a.label, SUM(b.v) AS total, COUNT(*) AS n " +
          "FROM j_dml14_a a JOIN j_dml14_b b ON a.k = b.k GROUP BY a.label"
      )

      // Stress: lots of conflicting DML on both sides — one REFRESH after.
      spark.sql("INSERT INTO j_dml14_a VALUES (4, 'A4'), (5, 'A5')")       // new groups
      spark.sql("INSERT INTO j_dml14_b VALUES (4, 100), (5, 200), (1, 5)") // new + extra A1
      spark.sql("DELETE FROM j_dml14_a WHERE k = 3")                       // drop A3
      spark.sql("DELETE FROM j_dml14_b WHERE k = 2")                       // A2 group goes away
      spark.sql("UPDATE j_dml14_a SET label = 'A1x' WHERE k = 1")          // rename A1
      spark.sql("UPDATE j_dml14_b SET v = 999 WHERE k = 1 AND v = 20")     // change a value
      refreshMv("mv_j_dml14")

      assertMvCorrect(
        "mv_j_dml14",
        "SELECT a.label, SUM(b.v) AS total, COUNT(*) AS n " +
          "FROM j_dml14_a a JOIN j_dml14_b b ON a.k = b.k GROUP BY a.label"
      )
    }
  }

  describe("(15) 3-way FK-pruning star schema — inserts on the FACT (referencing) side") {
    it("INSERT-only fact side: SUM aggregate updates correctly across 3 tables") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dim_product_15(product_id INT, name STRING) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS dim_region_15(region_id INT, region_name STRING) USING DELTA"
      )
      spark.sql(
        "CREATE TABLE IF NOT EXISTS fact_sales_15(sale_id INT, product_id INT, region_id INT, amount INT) USING DELTA"
      )

      spark.sql("INSERT INTO dim_product_15 VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')")
      spark.sql("INSERT INTO dim_region_15 VALUES (10, 'North'), (20, 'South')")
      spark.sql("INSERT INTO fact_sales_15 VALUES (1, 1, 10, 100), (2, 2, 20, 200), (3, 1, 20, 150)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_fk_star AS " +
          "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales_15 f " +
          "JOIN dim_product_15 p ON f.product_id = p.product_id " +
          "JOIN dim_region_15 r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )

      val rt = mvRefreshType("mv_j_fk_star")
      Seq(
        RefreshTypeCode.AggregateGroup,
        RefreshTypeCode.GroupRecompute,
        RefreshTypeCode.FullRefresh
      ) should contain(rt)

      assertMvCorrect(
        "mv_j_fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales_15 f " +
          "JOIN dim_product_15 p ON f.product_id = p.product_id " +
          "JOIN dim_region_15 r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )

      // Insert only into the FACT side (referencing-side, FK pruning eligible)
      spark.sql("INSERT INTO fact_sales_15 VALUES (4, 2, 10, 80), (5, 3, 20, 25), (6, 1, 10, 5)")
      refreshMv("mv_j_fk_star")
      assertMvCorrect(
        "mv_j_fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales_15 f " +
          "JOIN dim_product_15 p ON f.product_id = p.product_id " +
          "JOIN dim_region_15 r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )

      // Now an INSERT into a dimension that no fact references — MV unchanged
      spark.sql("INSERT INTO dim_product_15 VALUES (4, 'Thingamajig')")
      refreshMv("mv_j_fk_star")
      assertMvCorrect(
        "mv_j_fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales_15 f " +
          "JOIN dim_product_15 p ON f.product_id = p.product_id " +
          "JOIN dim_region_15 r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )

      // Now insert a fact that references the new product — group 'Thingamajig' appears
      spark.sql("INSERT INTO fact_sales_15 VALUES (7, 4, 10, 300)")
      refreshMv("mv_j_fk_star")
      assertMvCorrect(
        "mv_j_fk_star",
        "SELECT p.name, r.region_name, SUM(f.amount) AS total, COUNT(*) AS cnt " +
          "FROM fact_sales_15 f " +
          "JOIN dim_product_15 p ON f.product_id = p.product_id " +
          "JOIN dim_region_15 r ON f.region_id = r.region_id " +
          "GROUP BY p.name, r.region_name"
      )
    }
  }
}
