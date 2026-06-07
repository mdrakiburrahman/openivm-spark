package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Parity tests for RefreshType 8 — DISTINCT_INCREMENTAL, Shape 1.
  *
  * `SELECT DISTINCT region FROM sales` — single-column DISTINCT.
  *
  * Split out of the original `DistinctSpec` so this shape runs in its own
  * forked JVM and contributes ≤ 10 `it(...)` cases per file (parallelism
  * budget for `ivmIt`).
  */
class DistinctSingleColumnSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-distinct-single-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-DistinctSingleColumnSpec")
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
  // Shape 1 — SELECT DISTINCT region FROM sales  (single column)
  // ══════════════════════════════════════════════════════════════════════════

  describe("Shape-1: SELECT DISTINCT region (single-column)") {

    val baseTable = "dsc_sales_d1"
    val mvName    = "dsc_mv_d1"
    val body      = s"SELECT DISTINCT region FROM $baseTable"

    it("(1a) initial INSERT — MV has one row per unique region") {
      sql(s"CREATE TABLE IF NOT EXISTS $baseTable(region STRING, amount INT) USING DELTA")
      sql(s"INSERT INTO $baseTable VALUES ('east', 100), ('west', 200), ('east', 50)")
      sql(s"CREATE MATERIALIZED VIEW $mvName AS $body")
      assertMvCorrect(mvName, body)
    }

    it("(1b) INSERT duplicate — MV unchanged; occurrence count incremented internally") {
      sql(s"INSERT INTO $baseTable VALUES ('east', 99)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1c) DELETE one duplicate — MV unchanged; occurrence count decremented") {
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND amount = 99")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1d) DELETE the last copy of a region — MV row disappears") {
      sql(s"DELETE FROM $baseTable WHERE region = 'west'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1e) INSERT a new unique region — MV gains a new row") {
      sql(s"INSERT INTO $baseTable VALUES ('north', 300)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1f) batched DML (5 INSERTs + 3 DELETEs) → single REFRESH → MV ≡ live SELECT DISTINCT") {
      // 2 new values, 2 duplicates, 1 existing;  delete 2 that still have other copies + 1 that becomes last
      sql(s"INSERT INTO $baseTable VALUES ('south', 400)")
      sql(s"INSERT INTO $baseTable VALUES ('south', 401)")
      sql(s"INSERT INTO $baseTable VALUES ('east', 10)")
      sql(s"INSERT INTO $baseTable VALUES ('midwest', 50)")
      sql(s"INSERT INTO $baseTable VALUES ('east', 11)")
      sql(s"DELETE FROM $baseTable WHERE region = 'south' AND amount = 401")
      sql(s"DELETE FROM $baseTable WHERE region = 'east' AND amount = 10")
      sql(s"DELETE FROM $baseTable WHERE region = 'north'")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
    }

    it("(1g) NULL deduplication — multiple NULL-region rows fold to one MV row") {
      sql(s"INSERT INTO $baseTable VALUES (NULL, 1), (NULL, 2), (NULL, 3)")
      refreshMv(mvName)
      assertMvCorrect(mvName, body)
      val nullCount = spark.table(mvName).where("region IS NULL").count()
      withClue("DISTINCT must fold all NULL regions to exactly one MV row: ") {
        nullCount shouldBe 1L
      }
    }
  }
}
