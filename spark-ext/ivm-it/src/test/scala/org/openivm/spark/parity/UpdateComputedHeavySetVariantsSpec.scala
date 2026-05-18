package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy carve-out of `UpdateComputedSpec.scala` §(1)-(5) — the mv_comp_upd
  * SET-variant walk through col±constant, col*constant, multi-col SET, and
  * no-WHERE-update over a SIMPLE_PROJECTION view (~4m01).  Lives in its own
  * forked JVM so the rest of the parity suite is not blocked by this monster
  * test.
  *
  * Table / MV names are prefixed `upd_heavy_set_` to guarantee no Delta-path
  * collision with the host spec or any other parallel forked JVM.
  */
class UpdateComputedHeavySetVariantsSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-upd-heavy-set-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-UpdateComputedHeavySetVariantsSpec")
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

  // ============================================================================
  // (1)-(5) Computed UPDATEs walked through on a SIMPLE_PROJECTION view
  //     mirroring update_computed.test:7-149
  //     Spark's DECIMAL default precision differs from DuckDB's, so we declare
  //     `price DECIMAL(10,3)` to match the test's printed value `90.000`.
  // ============================================================================
  describe("(1)-(5) Computed UPDATE expressions on a SIMPLE_PROJECTION view") {
    it("col±constant, col*constant, multi-col SET, no-WHERE-update all propagate after REFRESH") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS upd_heavy_set_comp_upd(" +
          "  id INT, val INT, price DECIMAL(10,3), label STRING" +
          ") USING DELTA"
      )
      spark.sql(
        "INSERT INTO upd_heavy_set_comp_upd VALUES " +
          "(1, 10, 100.0, 'alpha'), (2, 20, 200.0, 'beta'), (3, 30, 300.0, 'gamma')"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW upd_heavy_set_mv_comp_upd AS SELECT id, val, price, label FROM upd_heavy_set_comp_upd"
      )
      val expected = "SELECT id, val, price, label FROM upd_heavy_set_comp_upd"
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)

      // (1) col + constant
      spark.sql("UPDATE upd_heavy_set_comp_upd SET val = val + 5 WHERE id = 1")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      spark.sql("SELECT val FROM upd_heavy_set_mv_comp_upd WHERE id = 1").collect().head.getInt(0) shouldBe 15

      // (2) col - constant
      spark.sql("UPDATE upd_heavy_set_comp_upd SET val = val - 3 WHERE id = 2")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      spark.sql("SELECT val FROM upd_heavy_set_mv_comp_upd WHERE id = 2").collect().head.getInt(0) shouldBe 17

      // (3) col * constant
      spark.sql("UPDATE upd_heavy_set_comp_upd SET val = val * 2 WHERE id = 3")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      spark.sql("SELECT val FROM upd_heavy_set_mv_comp_upd WHERE id = 3").collect().head.getInt(0) shouldBe 60

      // (4) multi-column computed UPDATE
      spark.sql("UPDATE upd_heavy_set_comp_upd SET val = val + 1, price = price - 10.0 WHERE id = 1")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
      val row = spark.sql("SELECT val, price FROM upd_heavy_set_mv_comp_upd WHERE id = 1").collect().head
      row.getInt(0) shouldBe 16
      row.getDecimal(1).compareTo(new java.math.BigDecimal("90.000")) shouldBe 0

      // (5) computed update on ALL rows (no WHERE clause)
      spark.sql("UPDATE upd_heavy_set_comp_upd SET val = val + 100")
      refreshMv("upd_heavy_set_mv_comp_upd")
      assertMvCorrect("upd_heavy_set_mv_comp_upd", expected)
    }
  }
}
