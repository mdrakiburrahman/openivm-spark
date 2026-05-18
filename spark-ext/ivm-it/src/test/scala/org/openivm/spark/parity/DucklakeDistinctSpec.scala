package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** P6d — Port of `openivm/test/sql/ducklake_distinct.test`.
  *
  * Translation:
  *   - DuckLake catalog `dl.<table>` → Delta tables in the default Spark
  *     catalog (named `dl_<table>` to preserve traceability).
  *   - `ATTACH … (TYPE ducklake)` → no-op.
  *   - `PRAGMA refresh('v')` → `REFRESH MATERIALIZED VIEW v`.
  *   - DuckLake snapshot semantics map onto Delta snapshot reads: the Delta
  *     equivalent of "DuckLake reads inserted/deleted rows between snapshots"
  *     is the staging delta produced by the DML interceptor + the MV's
  *     Delta version advance recorded in `MvCatalog`. Correctness is verified
  *     end-to-end via bidirectional `EXCEPT ALL`.
  *
  * Source: `.temp/openivm/test/sql/ducklake_distinct.test`.
  */
class DucklakeDistinctSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-dl-dist-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DucklakeDistinctSpec")
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

  // ── Basic DISTINCT ────────────────────────────────────────────────────────
  // openivm: ducklake_distinct.test "Basic DISTINCT"

  describe("(DD1) Basic DISTINCT — duplicate inserts leave MV unchanged") {
    it("incremental refresh dedupes inserts and reflects new values") {
      spark.sql("CREATE TABLE IF NOT EXISTS dl_tags(id INT, tag STRING) USING DELTA")
      spark.sql("INSERT INTO dl_tags VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green')")
      spark.sql("CREATE MATERIALIZED VIEW dl_mv_tags AS SELECT DISTINCT tag FROM dl_tags")

      // Initial state
      assertMvCorrect("dl_mv_tags", "SELECT DISTINCT tag FROM dl_tags")

      // Insert duplicate — should not change the user-visible set
      spark.sql("INSERT INTO dl_tags VALUES (5, 'red')")
      refreshMv("dl_mv_tags")
      assertMvCorrect("dl_mv_tags", "SELECT DISTINCT tag FROM dl_tags")

      // Insert new distinct value
      spark.sql("INSERT INTO dl_tags VALUES (6, 'yellow')")
      refreshMv("dl_mv_tags")
      assertMvCorrect("dl_mv_tags", "SELECT DISTINCT tag FROM dl_tags")
    }
  }

  // ── Stress: batch INSERT + DELETE ────────────────────────────────────────
  // openivm: ducklake_distinct.test "Stress: batch INSERT + DELETE"

  describe("(DD2) Stress — batched DELETEs + INSERTs before single refresh") {
    it("DELETEs that empty a distinct group remove it; re-introductions reappear") {
      spark.sql("CREATE TABLE IF NOT EXISTS dl_tags2(id INT, tag STRING) USING DELTA")
      spark.sql(
        "INSERT INTO dl_tags2 VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green'), (5, 'red'), (6, 'yellow')"
      )
      spark.sql("CREATE MATERIALIZED VIEW dl_mv_tags2 AS SELECT DISTINCT tag FROM dl_tags2")

      // Delete all 'red' entries, insert 'purple', add another 'blue' — all in
      // one batch before a single refresh
      spark.sql("DELETE FROM dl_tags2 WHERE tag = 'red'")
      spark.sql("INSERT INTO dl_tags2 VALUES (7, 'purple')")
      spark.sql("INSERT INTO dl_tags2 VALUES (8, 'blue')")
      refreshMv("dl_mv_tags2")

      assertMvCorrect("dl_mv_tags2", "SELECT DISTINCT tag FROM dl_tags2")
    }
  }
}
