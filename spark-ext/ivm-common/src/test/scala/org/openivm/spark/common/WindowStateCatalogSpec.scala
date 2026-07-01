package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class WindowStateCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-window-state-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-WindowStateCatalogSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    WindowStateCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  describe("WindowStateCatalog.ensureTables") {
    it("is idempotent") {
      WindowStateCatalog.ensureTables(spark)
      WindowStateCatalog.ensureTables(spark)
    }
  }

  describe("WindowStateCatalog put/get") {
    it("round-trips an opaque UTF-8 state payload") {
      WindowStateCatalog.put(spark, "default.mv_ws_put", "part=2026-07-01", """{"running":42,"max":"k9"}""")

      WindowStateCatalog.get(spark, "default.mv_ws_put", "part=2026-07-01") shouldBe Some(
        """{"running":42,"max":"k9"}"""
      )
    }

    it("returns None for a missing key") {
      WindowStateCatalog.get(spark, "default.mv_ws_missing", "missing") shouldBe None
    }
  }

  describe("WindowStateCatalog putAll/getMany") {
    it("round-trips multiple partition states for one view") {
      val entries = Seq("p1" -> """{"v":1}""", "p2" -> """{"v":2}""", "p3" -> """{"v":3}""")

      WindowStateCatalog.putAll(spark, "default.mv_ws_many", entries)

      WindowStateCatalog.getMany(spark, "default.mv_ws_many", Seq("p3", "missing", "p1")) shouldBe Map(
        "p1" -> """{"v":1}""",
        "p3" -> """{"v":3}"""
      )
    }
  }

  describe("WindowStateCatalog scanForView") {
    it("returns all payloads with decoded partition keys for the requested view only") {
      WindowStateCatalog.putAll(
        spark,
        "default.mv_ws_scan",
        Seq("region=us|day=1" -> "state-us-1", "region=eu|day=1" -> "state-eu-1")
      )
      WindowStateCatalog.put(spark, "default.mv_ws_scan_other", "region=us|day=1", "other-state")

      WindowStateCatalog.scanForView(spark, "default.mv_ws_scan").toMap shouldBe Map(
        "region=us|day=1" -> "state-us-1",
        "region=eu|day=1" -> "state-eu-1"
      )
    }

    it("keeps similarly prefixed view names isolated") {
      WindowStateCatalog.put(spark, "default.mv_ws_prefix", "p", "short")
      WindowStateCatalog.put(spark, "default.mv_ws_prefix_extra", "p", "long")

      WindowStateCatalog.scanForView(spark, "default.mv_ws_prefix").toMap shouldBe Map("p" -> "short")
      WindowStateCatalog.get(spark, "default.mv_ws_prefix_extra", "p") shouldBe Some("long")
    }
  }

  describe("WindowStateCatalog removeAll") {
    it("clears only the requested view") {
      WindowStateCatalog.putAll(spark, "default.mv_ws_remove", Seq("p1" -> "s1", "p2" -> "s2"))
      WindowStateCatalog.put(spark, "default.mv_ws_remove_other", "p1", "other")

      WindowStateCatalog.removeAll(spark, "default.mv_ws_remove")

      WindowStateCatalog.scanForView(spark, "default.mv_ws_remove") shouldBe Seq.empty
      WindowStateCatalog.get(spark, "default.mv_ws_remove_other", "p1") shouldBe Some("other")
    }
  }
}
