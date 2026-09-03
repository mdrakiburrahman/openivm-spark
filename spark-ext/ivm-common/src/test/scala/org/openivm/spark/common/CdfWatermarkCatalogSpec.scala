package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.UUID

class CdfWatermarkCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _
  private val warehouseDir = {
    val dir = new File(s"target/test-warehouse-cdf-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-CdfWatermarkCatalogSpec")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally {
      OpenIvmRocksDBRegistry.closeAll()
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def sampleMvMeta(name: String, sources: Seq[String]): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(name),
      querySql = s"SELECT count(*) FROM ${sources.head}",
      refreshType = 0,
      refreshTypeName = "SIMPLE_PROJECTION",
      lastVersion = 0L,
      sourceTables = sources,
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        sources.map(_ -> StructType(Seq(StructField("id", LongType)))).toMap
      ),
      location = s"$warehouseDir/$name",
      createdAt = new Timestamp(1700000000000L),
      properties = Map.empty
    )

  describe("CdfWatermarkCatalog sharding") {
    it("stores independent view watermarks without creating the shared index DB") {
      CdfWatermarkCatalog.putAll(spark, "mv_a", Map("orders" -> 3L, "lineitem" -> 7L))
      CdfWatermarkCatalog.put(spark, "mv_b", "orders", 11L)

      CdfWatermarkCatalog.get(spark, "mv_a", "orders") shouldBe Some(3L)
      CdfWatermarkCatalog.get(spark, "mv_a", "lineitem") shouldBe Some(7L)
      CdfWatermarkCatalog.get(spark, "mv_b", "orders") shouldBe Some(11L)
      Files.exists(Paths.get(OpenIvmStatePaths.indexDbPath(spark), "CURRENT")) shouldBe false
    }

    it("removes one base-table watermark from every registered dependent MV shard") {
      // removeForBaseTable now uses MvCatalog.viewsForSource (reverse dependency
      // index) instead of scanning every per-MV path. MVs must be registered in
      // the catalog for cleanup to reach their shards.
      val mvCMeta = sampleMvMeta("mv_c", Seq("orders", "customer"))
      val mvDMeta = sampleMvMeta("mv_d", Seq("orders"))
      MvCatalog.upsert(spark, mvCMeta)
      MvCatalog.upsert(spark, mvDMeta)

      CdfWatermarkCatalog.putAll(spark, "mv_c", Map("orders" -> 13L, "customer" -> 17L))
      CdfWatermarkCatalog.put(spark, "mv_d", "orders", 19L)

      CdfWatermarkCatalog.removeForBaseTable(spark, "orders")

      CdfWatermarkCatalog.get(spark, "mv_c", "orders") shouldBe None
      CdfWatermarkCatalog.get(spark, "mv_d", "orders") shouldBe None
      // Non-target source watermark is preserved
      CdfWatermarkCatalog.get(spark, "mv_c", "customer") shouldBe Some(17L)

      MvCatalog.remove(spark, mvCMeta.name)
      MvCatalog.remove(spark, mvDMeta.name)
    }

    it("does not open DBs of MVs unrelated to the base table") {
      // Register mv_x (depends on "orders") and mv_y (depends on "lineitem" only).
      // Cleaning up "orders" must not open mv_y's shard.
      val mvXMeta = sampleMvMeta("mv_x_unrelated", Seq("orders"))
      val mvYMeta = sampleMvMeta("mv_y_unrelated", Seq("lineitem"))
      MvCatalog.upsert(spark, mvXMeta)
      MvCatalog.upsert(spark, mvYMeta)

      CdfWatermarkCatalog.put(spark, "mv_x_unrelated", "orders", 5L)
      CdfWatermarkCatalog.put(spark, "mv_y_unrelated", "lineitem", 7L)

      // Force mv_y_unrelated's per-MV DB to exist on disk so an open-all scan
      // would find it, then verify it is untouched after the cleanup.
      CdfWatermarkCatalog.get(spark, "mv_y_unrelated", "lineitem") shouldBe Some(7L)

      CdfWatermarkCatalog.removeForBaseTable(spark, "orders")

      // orders watermark on mv_x is gone; lineitem watermark on mv_y is untouched
      CdfWatermarkCatalog.get(spark, "mv_x_unrelated", "orders") shouldBe None
      CdfWatermarkCatalog.get(spark, "mv_y_unrelated", "lineitem") shouldBe Some(7L)

      MvCatalog.remove(spark, mvXMeta.name)
      MvCatalog.remove(spark, mvYMeta.name)
    }

    it("lazily migrates a legacy shared watermark into the owning MV shard") {
      val legacy = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.indexDbPath(spark),
        IndexDbColumnFamilies.All
      )
      val key = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8("mv_legacy"), RocksDBCodec.utf8("orders")))
      legacy.withBatch { batch =>
        OpenIvmRocksDBBatchOps.put(
          legacy,
          batch,
          IndexDbColumnFamilies.CdfWatermarks,
          key,
          RocksDBCodec.encodeLongBE(23L)
        )
      }

      CdfWatermarkCatalog.get(spark, "mv_legacy", "orders") shouldBe Some(23L)
      val shard = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        OpenIvmStatePaths.perMvDbPath(spark, "mv_legacy"),
        OpenIvmStatePaths.PerMvColumnFamilies
      )
      shard
        .get(IndexDbColumnFamilies.CdfWatermarks, RocksDBCodec.utf8("orders"))
        .map(RocksDBCodec.decodeLongBE) shouldBe Some(23L)
    }
  }
}
