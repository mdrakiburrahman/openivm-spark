package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Parity tests for RefreshType 8 — DISTINCT_INCREMENTAL, Shape 2.
  *
  * `SELECT DISTINCT region, product FROM sales` — multi-column DISTINCT.
  *
  * Split out of the original `DistinctSpec` so this shape runs in its own
  * forked JVM and contributes ≤ 10 `it(...)` cases per file.
  */
class DistinctMultiColumnSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-distinct-multi-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DistinctMultiColumnSpec")
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

  private def sql(q: String): Unit = spark.sql(q).collect()

  private def refreshMv(name: String): Unit = sql(s"REFRESH MATERIALIZED VIEW $name")

  // ══════════════════════════════════════════════════════════════════════════
  // Shape 2 — SELECT DISTINCT region, product FROM sales  (multi-column)
  // ══════════════════════════════════════════════════════════════════════════

  describe("Shape-2: SELECT DISTINCT region, product (multi-column)") {

    val baseTable = "dmc_sales_d2"
    val mvName    = "dmc_mv_d2"
    val body      = s"SELECT DISTINCT region, product FROM $baseTable"

    it("(2a) initial INSERT — MV has one row per unique (region, product) pair") {
      sql(
        s"CREATE TABLE IF NOT EXISTS $baseTable(region STRING, product STRING, amount INT) USING DELTA"
      )
      sql(s"INSERT INTO $baseTable VALUES ('east', 'widget', 100), ('east', 'gadget', 200), ('west', 'widget', 50)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(2b) INSERT duplicate pair — MV unchanged") {
      sql(s"INSERT INTO $baseTable VALUES ('east', 'widget', 150)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(2c) DELETE one duplicate of a pair — MV unchanged") {
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND product = 'widget' AND amount = 150")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(2d) DELETE the last copy of a pair — MV row disappears") {
      sql(s"DELETE FROM $baseTable WHERE region = 'west' AND product = 'widget'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(2e) INSERT new (region, product) pair — MV gains a new row") {
      sql(s"INSERT INTO $baseTable VALUES ('north', 'doohickey', 300)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(2f) batched DML → single REFRESH → MV ≡ live SELECT DISTINCT") {
      sql(s"INSERT INTO $baseTable VALUES ('south', 'widget', 1)")
      sql(s"INSERT INTO $baseTable VALUES ('south', 'widget', 2)")
      sql(s"INSERT INTO $baseTable VALUES ('east', 'gadget', 99)")
      sql(s"INSERT INTO $baseTable VALUES ('midwest', 'gizmo', 10)")
      sql(s"INSERT INTO $baseTable VALUES ('east', 'gizmo', 11)")
      sql(s"DELETE FROM $baseTable WHERE region = 'south' AND product = 'widget' AND amount = 2")
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND product = 'gadget' AND amount = 99")
      sql(s"DELETE FROM $baseTable WHERE region = 'north'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(2g) NULL deduplication — (NULL, NULL) folds to one MV row") {
      sql(s"INSERT INTO $baseTable VALUES (NULL, NULL, 1), (NULL, NULL, 2)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
      val nullCount = spark.table(mvName).where("region IS NULL AND product IS NULL").count()
      withClue("(NULL,NULL) pair must appear exactly once in MV: ") {
        nullCount shouldBe 1L
      }
    }
  }
}
