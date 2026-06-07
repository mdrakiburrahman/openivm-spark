package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Inner-join slice of the [[JoinsSpec]] coverage. See JoinsOuterSpec /
  * JoinsDmlSpec for the remaining cases. Each split owns its SparkSession,
  * warehouse directory and table-name prefix so it can run in its own forked JVM.
  */
class JoinsInnerSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-joins-inner-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-JoinsInnerSpec")
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
  // INNER JOIN
  // ============================================================================

  describe("(1) 2-way INNER JOIN projection: SELECT * FROM a JOIN b ON a.k = b.k") {
    it("incrementally maintains the join projection across INSERTs on both sides") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_inner_proj_a(k INT, v_a STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_inner_proj_b(k INT, v_b INT) USING DELTA")
      spark.sql("INSERT INTO j_inner_proj_a VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
      spark.sql("INSERT INTO j_inner_proj_b VALUES (1, 10), (1, 20), (2, 30)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_inner_proj AS " +
          "SELECT a.k, a.v_a, b.v_b FROM j_inner_proj_a a JOIN j_inner_proj_b b ON a.k = b.k"
      )

      val rt = mvRefreshType("mv_j_inner_proj")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      // Verify the initial snapshot is correct
      assertMvCorrect(
        "mv_j_inner_proj",
        "SELECT a.k, a.v_a, b.v_b FROM j_inner_proj_a a JOIN j_inner_proj_b b ON a.k = b.k"
      )

      // INSERT on both sides
      spark.sql("INSERT INTO j_inner_proj_a VALUES (4, 'Dave')")
      spark.sql("INSERT INTO j_inner_proj_b VALUES (3, 99), (4, 7)")
      refreshMv("mv_j_inner_proj")
      assertMvCorrect(
        "mv_j_inner_proj",
        "SELECT a.k, a.v_a, b.v_b FROM j_inner_proj_a a JOIN j_inner_proj_b b ON a.k = b.k"
      )
    }
  }

  describe("(2) 2-way INNER JOIN aggregate: SELECT a.k, SUM(b.v) FROM a JOIN b ON a.k=b.k GROUP BY a.k") {
    it("incrementally maintains a joined aggregate via affected-keys recompute") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_inner_agg_a(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_inner_agg_b(k INT, v INT) USING DELTA")
      spark.sql("INSERT INTO j_inner_agg_a VALUES (1, 'A'), (2, 'B'), (3, 'C')")
      spark.sql("INSERT INTO j_inner_agg_b VALUES (1, 10), (1, 20), (2, 30)")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_inner_agg AS " +
          "SELECT a.k, SUM(b.v) AS total " +
          "FROM j_inner_agg_a a JOIN j_inner_agg_b b ON a.k = b.k GROUP BY a.k"
      )

      val rt = mvRefreshType("mv_j_inner_agg")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      // INSERT on both sides at once
      spark.sql("INSERT INTO j_inner_agg_a VALUES (4, 'D')")
      spark.sql("INSERT INTO j_inner_agg_b VALUES (2, 1), (4, 100), (4, 5)")
      refreshMv("mv_j_inner_agg")
      assertMvCorrect(
        "mv_j_inner_agg",
        "SELECT a.k, SUM(b.v) AS total " +
          "FROM j_inner_agg_a a JOIN j_inner_agg_b b ON a.k = b.k GROUP BY a.k"
      )
    }
  }

  describe("(3) 3-way INNER JOIN aggregate: a JOIN b ON a.k=b.k JOIN c ON b.j=c.j (7 Möbius terms)") {
    it("incrementally maintains the 3-way joined aggregate (exercises 7 Möbius terms)") {
      spark.sql("CREATE TABLE IF NOT EXISTS j_three_a(k INT, label STRING) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_three_b(k INT, j INT, v INT) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS j_three_c(j INT, descr STRING) USING DELTA")

      spark.sql("INSERT INTO j_three_a VALUES (1, 'A1'), (2, 'A2'), (3, 'A3')")
      spark.sql("INSERT INTO j_three_b VALUES (1, 10, 100), (1, 20, 200), (2, 10, 50)")
      spark.sql("INSERT INTO j_three_c VALUES (10, 'X'), (20, 'Y')")

      spark.sql(
        "CREATE MATERIALIZED VIEW mv_j_three AS " +
          "SELECT a.k, c.descr, SUM(b.v) AS total " +
          "FROM j_three_a a " +
          "JOIN j_three_b b ON a.k = b.k " +
          "JOIN j_three_c c ON b.j = c.j " +
          "GROUP BY a.k, c.descr"
      )

      val rt = mvRefreshType("mv_j_three")
      Seq(RefreshTypeCode.AggregateGroup, RefreshTypeCode.GroupRecompute) should contain(rt)

      // Exercise all three sides — Möbius will mix 7 (+/-) terms
      spark.sql("INSERT INTO j_three_a VALUES (4, 'A4')")
      spark.sql("INSERT INTO j_three_b VALUES (3, 20, 7), (4, 10, 11)")
      spark.sql("INSERT INTO j_three_c VALUES (30, 'Z')")
      refreshMv("mv_j_three")
      assertMvCorrect(
        "mv_j_three",
        "SELECT a.k, c.descr, SUM(b.v) AS total " +
          "FROM j_three_a a " +
          "JOIN j_three_b b ON a.k = b.k " +
          "JOIN j_three_c c ON b.j = c.j " +
          "GROUP BY a.k, c.descr"
      )

      // Add c-rows after the fact: previously unmatched fact rows now match
      spark.sql("INSERT INTO j_three_c VALUES (10, 'X2')") // a new c row duplicates the join key 10
      refreshMv("mv_j_three")
      assertMvCorrect(
        "mv_j_three",
        "SELECT a.k, c.descr, SUM(b.v) AS total " +
          "FROM j_three_a a " +
          "JOIN j_three_b b ON a.k = b.k " +
          "JOIN j_three_c c ON b.j = c.j " +
          "GROUP BY a.k, c.descr"
      )
    }
  }
}
