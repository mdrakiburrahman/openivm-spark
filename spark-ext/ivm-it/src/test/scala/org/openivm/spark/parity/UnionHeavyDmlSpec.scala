package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `UnionSpec.scala` §(1a) — the mv_all_orders UNION ALL
  * incremental maintenance walk across INSERTs and DELETEs on both branches
  * (~3m47).  Lives in its own forked JVM so the rest of the parity suite is
  * not blocked by this monster test.
  *
  * Table / MV names are prefixed `union_heavy_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
class UnionHeavyDmlSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-union-heavy-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-UnionHeavyDmlSpec")
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
    withClue(s"$mvName EXCEPT ALL <expected> (MV has extra rows): ") {
      mv.exceptAll(expected).count() shouldBe 0L
    }
    withClue(s"<expected> EXCEPT ALL $mvName (MV missing rows): ") {
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
  // (1a) Basic UNION ALL over two tables — INSERT/DELETE batched (union.test:7-119)
  //      Sub-section without insert of fully-identical rows: this works under
  //      the openivm-spark SIMPLE_PROJECTION MERGE.
  // ============================================================================
  describe("(1a) Basic UNION ALL (unique rows only): INSERT/DELETE batched") {
    it("incrementally maintains the union across INSERTs and DELETEs on both branches") {
      spark.sql("CREATE TABLE IF NOT EXISTS union_heavy_u_us(id INT, product STRING, amount INT) USING DELTA")
      spark.sql("CREATE TABLE IF NOT EXISTS union_heavy_u_eu(id INT, product STRING, amount INT) USING DELTA")
      spark.sql("INSERT INTO union_heavy_u_us VALUES (1, 'widget', 100), (2, 'gadget', 200)")
      spark.sql("INSERT INTO union_heavy_u_eu VALUES (3, 'widget', 150), (4, 'gizmo', 300)")

      spark.sql(
        "CREATE MATERIALIZED VIEW union_heavy_mv_all_orders AS " +
          "SELECT id, product, amount FROM union_heavy_u_us " +
          "UNION ALL " +
          "SELECT id, product, amount FROM union_heavy_u_eu"
      )

      // Initial state
      val expected =
        "SELECT id, product, amount FROM union_heavy_u_us UNION ALL SELECT id, product, amount FROM union_heavy_u_eu"
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // Multi-source unions are commonly demoted to FullRefresh by hasRealDelta.
      val rt = mvRefreshType("union_heavy_mv_all_orders")
      Seq(RefreshTypeCode.SimpleProjection, RefreshTypeCode.FullRefresh) should contain(rt)

      // Insert into both sides, single refresh — all rows distinct
      spark.sql("INSERT INTO union_heavy_u_us VALUES (5, 'bolt', 50), (6, 'nut', 25)")
      spark.sql("INSERT INTO union_heavy_u_eu VALUES (7, 'screw', 75)")
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // Delete from one side
      spark.sql("DELETE FROM union_heavy_u_us WHERE id = 1")
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // Delete from both sides + insert a distinct new row (8, 'nail', 10) once
      spark.sql("DELETE FROM union_heavy_u_us WHERE id = 2")
      spark.sql("DELETE FROM union_heavy_u_eu WHERE id = 3")
      spark.sql("INSERT INTO union_heavy_u_eu VALUES (8, 'nail', 10)")
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)

      // No-op refresh
      refreshMv("union_heavy_mv_all_orders")
      assertMvCorrect("union_heavy_mv_all_orders", expected)
    }
  }
}
