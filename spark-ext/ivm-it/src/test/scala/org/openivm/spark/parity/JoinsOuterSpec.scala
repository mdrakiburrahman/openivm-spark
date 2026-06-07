package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** LEFT / RIGHT / FULL OUTER join slice of the [[JoinsSpec]] coverage.
  * See JoinsInnerSpec / JoinsDmlSpec for the remaining cases.
  */
class JoinsOuterSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-joins-outer-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-JoinsOuterSpec")
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
  // LEFT / RIGHT JOIN
  // ============================================================================

  describe("(4) 2-way LEFT JOIN projection: nullable right side") {
    it("MV preserves NULL right-side rows for unmatched left rows") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_left_proj_c(id INT, cname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_left_proj_o(cid INT, product STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO j_left_proj_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      spark.sql("INSERT INTO j_left_proj_o VALUES (1, 'Widget', 100), (1, 'Gadget', 200)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_left_proj AS " +
          "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )

      val rt = mvRefreshType("mv_j_left_proj")
      // Either incremental projection (2) or full-refresh fallback (3).
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      assertMvCorrect(
        "mv_j_left_proj",
        "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )

      // Right-side insert — Bob's NULL row should be replaced with a matched row
      spark.sql("INSERT INTO j_left_proj_o VALUES (2, 'Bolt', 50)")
      refreshMv("mv_j_left_proj")
      assertMvCorrect(
        "mv_j_left_proj",
        "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )

      // Left-side insert — new customer with no orders, NULL-extended row appears
      spark.sql("INSERT INTO j_left_proj_c VALUES (4, 'Dave')")
      refreshMv("mv_j_left_proj")
      assertMvCorrect(
        "mv_j_left_proj",
        "SELECT c.cname, o.product, o.amount " +
          "FROM j_left_proj_c c LEFT JOIN j_left_proj_o o ON c.id = o.cid"
      )
    }
  }

  describe("(5) 2-way LEFT JOIN aggregate: COUNT preserves left rows with NULL right") {
    it("COUNT(o.amount) on the right is 0 for unmatched-left rows; refreshes incrementally") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_left_agg_c(id INT, cname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_left_agg_o(cid INT, amount INT) USING DELTA")
      spark.sql("INSERT INTO j_left_agg_c VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      spark.sql("INSERT INTO j_left_agg_o VALUES (1, 100), (1, 200)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_left_agg AS " +
          "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )

      val rt = mvRefreshType("mv_j_left_agg")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      assertMvCorrect(
        "mv_j_left_agg",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )

      // Right-side insert — Bob's count goes from 0 to 1
      spark.sql("INSERT INTO j_left_agg_o VALUES (2, 50)")
      refreshMv("mv_j_left_agg")
      assertMvCorrect(
        "mv_j_left_agg",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )

      // Left-side insert — new customer with no orders (COUNT = 0)
      spark.sql("INSERT INTO j_left_agg_c VALUES (4, 'Dave')")
      refreshMv("mv_j_left_agg")
      assertMvCorrect(
        "mv_j_left_agg",
        "SELECT c.cname, COUNT(o.amount) AS n " +
          "FROM j_left_agg_c c LEFT JOIN j_left_agg_o o ON c.id = o.cid " +
          "GROUP BY c.cname"
      )
    }
  }

  describe("(6) 2-way RIGHT JOIN projection: nullable left side") {
    it("preserves NULL left-side rows for unmatched right rows") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_right_proj_l(id INT, lname STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_right_proj_r(lid INT, rval INT) USING DELTA")
      spark.sql("INSERT INTO j_right_proj_l VALUES (1, 'Alice'), (2, 'Bob')")
      spark.sql("INSERT INTO j_right_proj_r VALUES (1, 10), (1, 20), (3, 99)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_right_proj AS " +
          "SELECT l.lname, r.rval " +
          "FROM j_right_proj_l l RIGHT JOIN j_right_proj_r r ON l.id = r.lid"
      )

      // Accept either incremental SimpleProjection (2) or FullRefresh (3).
      val rt = mvRefreshType("mv_j_right_proj")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      assertMvCorrect(
        "mv_j_right_proj",
        "SELECT l.lname, r.rval " +
          "FROM j_right_proj_l l RIGHT JOIN j_right_proj_r r ON l.id = r.lid"
      )

      // Insert on the right with a matching key (replaces unmatched-right semantics)
      spark.sql("INSERT INTO j_right_proj_r VALUES (2, 30)")
      // Insert on the left with no matching right (no MV change expected — right join)
      spark.sql("INSERT INTO j_right_proj_l VALUES (4, 'Dave')")
      // Insert on the right with no matching left (unmatched-right NULL row appears)
      spark.sql("INSERT INTO j_right_proj_r VALUES (5, 77)")
      refreshMv("mv_j_right_proj")

      assertMvCorrect(
        "mv_j_right_proj",
        "SELECT l.lname, r.rval " +
          "FROM j_right_proj_l l RIGHT JOIN j_right_proj_r r ON l.id = r.lid"
      )
    }
  }

  // ============================================================================
  // FULL OUTER JOIN
  // ============================================================================

  describe("(7) 2-way FULL OUTER JOIN projection") {
    it("preserves unmatched rows from both sides") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_fo_proj_e(id INT, ename STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_fo_proj_p(id INT, emp_id INT, title STRING) USING DELTA")
      spark.sql("INSERT INTO j_fo_proj_e VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
      spark.sql(
        "INSERT INTO j_fo_proj_p VALUES (10, 1, 'Alpha'), (20, 1, 'Beta'), (30, 4, 'Gamma')"
      )

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_fo_proj AS " +
          "SELECT e.ename, p.title " +
          "FROM j_fo_proj_e e FULL OUTER JOIN j_fo_proj_p p ON e.id = p.emp_id"
      )

      // Acceptable: SimpleProjection (2) or FullRefresh (3).
      val rt = mvRefreshType("mv_j_fo_proj")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      assertMvCorrect(
        "mv_j_fo_proj",
        "SELECT e.ename, p.title " +
          "FROM j_fo_proj_e e FULL OUTER JOIN j_fo_proj_p p ON e.id = p.emp_id"
      )

      // Insert on right: Bob (unmatched-left) → matched
      spark.sql("INSERT INTO j_fo_proj_p VALUES (40, 2, 'Delta')")
      // Insert on right with no match (new unmatched-right row)
      spark.sql("INSERT INTO j_fo_proj_p VALUES (50, 99, 'Epsilon')")
      // Insert on left with no match (new unmatched-left row)
      spark.sql("INSERT INTO j_fo_proj_e VALUES (5, 'Eve')")
      refreshMv("mv_j_fo_proj")

      assertMvCorrect(
        "mv_j_fo_proj",
        "SELECT e.ename, p.title " +
          "FROM j_fo_proj_e e FULL OUTER JOIN j_fo_proj_p p ON e.id = p.emp_id"
      )
    }
  }

  describe("(8) 2-way FULL OUTER JOIN aggregate (Zhang & Larson MERGE)") {
    it("aggregate over FULL OUTER refreshes incrementally") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_fo_agg_e(id INT, ename STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_fo_agg_p(id INT, emp_id INT, score INT) USING DELTA")
      spark.sql("INSERT INTO j_fo_agg_e VALUES (1, 'Alice'), (2, 'Bob')")
      spark.sql("INSERT INTO j_fo_agg_p VALUES (10, 1, 100), (20, 1, 50), (30, 4, 200)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_fo_agg AS " +
          "SELECT COALESCE(e.ename, '<orphan>') AS who, " +
          "       SUM(p.score) AS total, COUNT(p.score) AS n " +
          "FROM j_fo_agg_e e FULL OUTER JOIN j_fo_agg_p p ON e.id = p.emp_id " +
          "GROUP BY COALESCE(e.ename, '<orphan>')"
      )

      val rt = mvRefreshType("mv_j_fo_agg")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      assertMvCorrect(
        "mv_j_fo_agg",
        "SELECT COALESCE(e.ename, '<orphan>') AS who, " +
          "       SUM(p.score) AS total, COUNT(p.score) AS n " +
          "FROM j_fo_agg_e e FULL OUTER JOIN j_fo_agg_p p ON e.id = p.emp_id " +
          "GROUP BY COALESCE(e.ename, '<orphan>')"
      )

      // Insert on right: Bob (unmatched-left) → matched; new orphan
      spark.sql("INSERT INTO j_fo_agg_p VALUES (40, 2, 75), (50, 99, 1)")
      // Insert on left: a new employee with no projects (NULL right)
      spark.sql("INSERT INTO j_fo_agg_e VALUES (3, 'Carol')")
      refreshMv("mv_j_fo_agg")

      assertMvCorrect(
        "mv_j_fo_agg",
        "SELECT COALESCE(e.ename, '<orphan>') AS who, " +
          "       SUM(p.score) AS total, COUNT(p.score) AS n " +
          "FROM j_fo_agg_e e FULL OUTER JOIN j_fo_agg_p p ON e.id = p.emp_id " +
          "GROUP BY COALESCE(e.ename, '<orphan>')"
      )
    }
  }
}
