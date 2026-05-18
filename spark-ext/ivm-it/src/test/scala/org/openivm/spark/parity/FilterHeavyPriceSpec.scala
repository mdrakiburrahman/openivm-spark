package org.openivm.spark.parity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openivm.spark.common.{MvCatalog, StagingCatalog}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

/** Heavy-test isolation spin-off of [[FilterSpec]] section (1).
  *
  * Extracted into its own spec class so that `Test/testGrouping` runs it in a
  * dedicated forked JVM (per `Settings.parallelForkSettings`), shrinking the
  * crowded host spec's wall-clock.
  *
  * Table / MV names inside this spec are prefixed `fhp_*` so two parallel JVMs
  * (this one and the host `FilterSpec`) cannot collide on Delta paths.
  */
class FilterHeavyPriceSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-filt-heavy-price-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-FilterHeavyPriceSpec")
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

  describe("(1) WHERE price < 20: INSERT/DELETE/boundary/mixed batch") {
    it("incrementally maintains the cheap_products MV across all DML shapes") {
      spark.sql("CREATE TABLE IF NOT EXISTS fhp_products(id INT, name STRING, price INT, category STRING) USING DELTA")
      spark.sql(
        "INSERT INTO fhp_products VALUES " +
          "(1, 'Widget', 10, 'A'), (2, 'Gadget', 25, 'B'), (3, 'Doohickey', 5, 'A')"
      )
      spark.sql(
        "CREATE MATERIALIZED VIEW mv_fhp_cheap_products AS " +
          "SELECT id, name, price FROM fhp_products WHERE price < 20"
      )

      // Initial state: ids {1,3} present
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Insert matching filter
      spark.sql("INSERT INTO fhp_products VALUES (4, 'Thingamajig', 8, 'A')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Insert NOT matching filter (price=50 >= 20)
      spark.sql("INSERT INTO fhp_products VALUES (5, 'Expensive', 50, 'B')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Delete row that was in the view
      spark.sql("DELETE FROM fhp_products WHERE id = 1")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // No-op refresh (no DML since last refresh)
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Insert at boundary: price=19 matches (strict <), price=20 fails
      spark.sql("INSERT INTO fhp_products VALUES (6, 'Boundary', 19, 'C')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      spark.sql("INSERT INTO fhp_products VALUES (7, 'TooExpensive', 20, 'C')")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Mixed INSERT + DELETE in one batch
      spark.sql("INSERT INTO fhp_products VALUES (8, 'Cheap', 1, 'A')")
      spark.sql("DELETE FROM fhp_products WHERE id = 3")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")

      // Delete row NOT in MV (id=5 is filtered out) — MV unchanged
      spark.sql("DELETE FROM fhp_products WHERE id = 5")
      refreshMv("mv_fhp_cheap_products")
      assertMvCorrect("mv_fhp_cheap_products", "SELECT id, name, price FROM fhp_products WHERE price < 20")
    }
  }
}
