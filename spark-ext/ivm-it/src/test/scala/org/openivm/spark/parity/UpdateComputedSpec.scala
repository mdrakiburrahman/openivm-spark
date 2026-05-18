package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6a — Port of `openivm/test/sql/update_computed.test`.
  *
  * Exercises UPDATEs whose SET clause uses computed expressions referencing
  * the column being updated (`val = val + 5`, `price = price - 10.0`,
  * `val = val * 2`, …) and asserts they propagate correctly through both
  * SIMPLE_PROJECTION (full table image) and predicate-filtered views.
  *
  * The openivm test exercises:
  *
  *   1. `col + constant`
  *   2. `col - constant`
  *   3. `col * constant`
  *   4. Multi-column computed UPDATE in a single statement
  *      (`SET val = val + 1, price = price - 10.0`)
  *   5. Computed UPDATE on every row (no WHERE clause)
  *   6. Mandatory stress: computed UPDATE + constant UPDATE + DELETE + INSERT
  *      batched into a single REFRESH
  *   7. Computed UPDATE that moves a row across a filter boundary (in or out
  *      of the predicate)
  *
  * Per CLAUDE.md, every refresh assertion uses bidirectional `EXCEPT ALL`;
  * the stress test in shape (6) batches conflicting DML.
  *
  * Source: `.temp/openivm/test/sql/update_computed.test`.
  */
class UpdateComputedSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-upd-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-UpdateComputedSpec")
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
  //
  //     Extracted to [[UpdateComputedHeavySetVariantsSpec]] (~4m01 wall) so it
  //     runs in its own forked JVM and does not bottleneck the rest of this
  //     spec.
  // ============================================================================

  // ============================================================================
  // (6) Mandatory stress test (update_computed.test:151-181)
  //     INSERT + computed UPDATE + constant UPDATE + DELETE batched → one REFRESH
  // ============================================================================
  describe("(6) Stress: computed UPDATE + constant UPDATE + DELETE + INSERT → single REFRESH") {
    it("MV is bag-equal to view body after a single refresh covering all four DML kinds") {
      spark.sql(
        "CREATE TABLE IF NOT EXISTS comp_upd_stress(" +
          "  id INT, val INT, price DECIMAL(10,3), label STRING" +
          ") USING DELTA"
      )
      spark.sql(
        "INSERT INTO comp_upd_stress VALUES " +
          "(1, 10, 100.0, 'alpha'), (2, 20, 200.0, 'beta'), (3, 30, 300.0, 'gamma')"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_comp_upd_stress AS " +
          "SELECT id, val, price, label FROM comp_upd_stress"
      )
      val expected = "SELECT id, val, price, label FROM comp_upd_stress"

      // Conflicting batch — exercises delta consolidation
      spark.sql("INSERT INTO comp_upd_stress VALUES (4, 40, 400.0, 'delta')")
      spark.sql("UPDATE comp_upd_stress SET val = val - 50 WHERE id = 1") // computed UPDATE
      spark.sql("UPDATE comp_upd_stress SET label = 'BETA' WHERE id = 2") // constant UPDATE
      spark.sql("DELETE FROM comp_upd_stress WHERE id = 3")
      refreshMv("mv_comp_upd_stress")
      assertMvCorrect("mv_comp_upd_stress", expected)
    }
  }

  // ============================================================================
  // (7) Computed UPDATE moves rows across a filter boundary
  //     (update_computed.test:183-238)
  // ============================================================================
  describe("(7) Computed UPDATE crosses the filter boundary in both directions") {
    it("row entering the predicate appears in MV; row leaving the predicate disappears") {
      spark.sql("CREATE TABLE IF NOT EXISTS filt_upd(id INT, val INT) USING DELTA")
      spark.sql("INSERT INTO filt_upd VALUES (1, 10), (2, 50), (3, 100)")
      spark.sql("CREATE MATERIALIZED VIEW mv_filt_upd AS SELECT id, val FROM filt_upd WHERE val > 40")
      val expected = "SELECT id, val FROM filt_upd WHERE val > 40"

      // Initial: ids {2,3} (val > 40)
      assertMvCorrect("mv_filt_upd", expected)

      // (7a) UPDATE id=1: val = 10 + 35 = 45 → enters MV
      spark.sql("UPDATE filt_upd SET val = val + 35 WHERE id = 1")
      refreshMv("mv_filt_upd")
      assertMvCorrect("mv_filt_upd", expected)

      // (7b) UPDATE id=2: val = 50 - 20 = 30 → leaves MV
      spark.sql("UPDATE filt_upd SET val = val - 20 WHERE id = 2")
      refreshMv("mv_filt_upd")
      assertMvCorrect("mv_filt_upd", expected)
    }
  }
}
