package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID

class StagingCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-staging-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StagingCatalogSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteDir(new File(warehouseDir))
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def ts(epochMs: Long): Timestamp = new Timestamp(epochMs)

  private def delta(
      baseTable: String,
      opType: String,
      path: String,
      epochMs: Long,
      consumedBy: Seq[String] = Seq.empty
  ): StagingDelta =
    StagingDelta(baseTable, opType, path, ts(epochMs), consumedBy)

  // ---------------------------------------------------------------------------
  // Test 1 (catalog-level): ensureTables is idempotent
  // ---------------------------------------------------------------------------
  describe("StagingCatalog.ensureTables") {
    it("is idempotent — calling twice does not throw") {
      StagingCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 8: record + collectFor returns only unconsumed deltas in txn_ts order
  // ---------------------------------------------------------------------------
  describe("StagingCatalog.record + collectFor") {
    it("returns only unconsumed deltas for the given sources, ordered by txn_ts") {
      StagingCatalog.ensureTables(spark)

      val d1 = delta("orders", "INSERT", s"$warehouseDir/stg/orders/ins/1", 1000L)
      val d2 = delta("orders", "DELETE", s"$warehouseDir/stg/orders/del/2", 2000L)
      val d3 = delta("products", "INSERT", s"$warehouseDir/stg/products/ins/3", 500L)
      // d4 already consumed by mv_a
      val d4 = delta("orders", "INSERT", s"$warehouseDir/stg/orders/ins/4", 800L, Seq("mv_a"))

      Seq(d1, d2, d3, d4).foreach(StagingCatalog.record(spark, _))

      // mv_a asking for orders — should see d1, d2 (not d4, already consumed; not d3, wrong table)
      val result = StagingCatalog.collectFor(spark, "mv_a", Seq("orders"))
      result.map(_.stagingPath) shouldBe Seq(d1.stagingPath, d2.stagingPath)

      // txn_ts ordering: d1=1000, d2=2000 → ascending
      result.map(_.txnTs.getTime) shouldBe Seq(1000L, 2000L)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 9: markConsumed updates consumed_by idempotently
  // ---------------------------------------------------------------------------
  describe("StagingCatalog.markConsumed") {
    it("appends viewName to consumed_by and is idempotent on repeated calls") {
      StagingCatalog.ensureTables(spark)

      val d = delta("orders", "INSERT", s"$warehouseDir/stg/orders/ins/mc", 3000L)
      StagingCatalog.record(spark, d)

      StagingCatalog.markConsumed(spark, "mv_x", Seq(d.stagingPath))
      StagingCatalog.markConsumed(spark, "mv_x", Seq(d.stagingPath)) // idempotent

      // mv_x should now have consumed it
      val afterFirst = StagingCatalog.collectFor(spark, "mv_x", Seq("orders"))
      afterFirst.filter(_.stagingPath == d.stagingPath) shouldBe empty

      // Verify consumed_by has exactly one "mv_x" entry (no duplicates)
      import org.apache.spark.sql.functions.col
      val rows = spark.read
        .format("delta")
        .load(s"$warehouseDir/_ivm/_meta/staging")
        .where(col("staging_path") === d.stagingPath)
        .collect()
      rows should have size 1
      val consumedBy = rows.head.getSeq[String](rows.head.fieldIndex("consumed_by"))
      consumedBy.count(_ == "mv_x") shouldBe 1
    }
  }

  // ---------------------------------------------------------------------------
  // Test 10: pruneFullyConsumed deletes fully-consumed rows, leaves others
  // ---------------------------------------------------------------------------
  describe("StagingCatalog.pruneFullyConsumed") {
    it("deletes rows consumed by all dependent MVs; leaves partially-consumed rows") {
      StagingCatalog.ensureTables(spark)

      // Two MVs depend on "orders": mv_p and mv_q
      val dFull    = delta("orders", "INSERT", s"$warehouseDir/stg/orders/ins/full", 5000L, Seq("mv_p", "mv_q"))
      val dPartial = delta("orders", "INSERT", s"$warehouseDir/stg/orders/ins/partial", 6000L, Seq("mv_p"))
      val dNone    = delta("orders", "INSERT", s"$warehouseDir/stg/orders/ins/none", 7000L)

      Seq(dFull, dPartial, dNone).foreach(StagingCatalog.record(spark, _))

      // Mark dFull as consumed by both
      StagingCatalog.markConsumed(spark, "mv_p", Seq(dFull.stagingPath, dPartial.stagingPath))
      StagingCatalog.markConsumed(spark, "mv_q", Seq(dFull.stagingPath))

      StagingCatalog.pruneFullyConsumed(spark, Map("orders" -> Seq("mv_p", "mv_q")))

      import org.apache.spark.sql.functions.col
      val remaining = spark.read
        .format("delta")
        .load(s"$warehouseDir/_ivm/_meta/staging")
        .where(col("base_table") === "orders")
        .collect()
        .map(_.getAs[String]("staging_path"))
        .toSet

      // dFull is fully consumed → pruned
      remaining should not contain dFull.stagingPath
      // dPartial only consumed by mv_p → kept
      remaining should contain(dPartial.stagingPath)
      // dNone consumed by nobody → kept
      remaining should contain(dNone.stagingPath)
    }
  }
}
