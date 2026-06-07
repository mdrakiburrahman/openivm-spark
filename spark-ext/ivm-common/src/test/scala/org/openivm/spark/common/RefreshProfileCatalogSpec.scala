package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.UUID

class RefreshProfileCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-refresh-profile-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-RefreshProfileCatalogSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
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

  private def ts(epochMs: Long): Timestamp = new Timestamp(epochMs)

  private def row(
      refreshId: String,
      viewName: String,
      epochMs: Long,
      stepOrder: Int,
      stepName: String,
      durationMs: Long,
      detail: String
  ): RefreshProfileRow =
    RefreshProfileRow(refreshId, viewName, ts(epochMs), stepOrder, stepName, durationMs, detail)

  private def assertRowRoundTrips(actual: RefreshProfileRow, expected: RefreshProfileRow): Unit = {
    actual.refreshId shouldBe expected.refreshId
    actual.viewName shouldBe expected.viewName
    actual.stepOrder shouldBe expected.stepOrder
    actual.stepName shouldBe expected.stepName
    actual.durationMs shouldBe expected.durationMs
    actual.detail shouldBe expected.detail
    actual.profileTimestamp.getTime shouldBe expected.profileTimestamp.getTime
  }

  describe("RefreshProfileCatalog.ensureTables") {
    it("is idempotent — calling twice does not throw, no rows added") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.ensureTables(spark)

      RefreshProfileCatalog.scanAll(spark) shouldBe Seq.empty
    }
  }

  describe("RefreshProfileCatalog.scanAll") {
    it("on empty catalog returns Seq.empty") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      RefreshProfileCatalog.scanAll(spark) shouldBe Seq.empty
    }

    it("orders multiple refreshes by profileTimestamp, refreshId, stepOrder") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val refreshes = Seq(
        ("view_a_older", "default.view_a", 1700000001000L),
        ("view_b_middle", "default.view_b", 1700000002000L),
        ("view_c_newer", "default.view_c", 1700000003000L)
      )
      val rows = refreshes.flatMap { case (refreshId, viewName, epochMs) =>
        Seq(
          row(refreshId, viewName, epochMs, 2, "stmt", 30L, "step=2"),
          row(refreshId, viewName, epochMs, 1, "compile", 20L, "step=1"),
          row(refreshId, viewName, epochMs, 0, "acquire_locks", 10L, "step=0")
        )
      }

      RefreshProfileCatalog.record(spark, rows)

      val out      = RefreshProfileCatalog.scanAll(spark)
      val expected = rows.sortBy(r => (r.profileTimestamp.getTime, r.refreshId, r.stepOrder))
      out.map(r => (r.profileTimestamp.getTime, r.refreshId, r.stepOrder)) shouldBe expected.map(r =>
        (r.profileTimestamp.getTime, r.refreshId, r.stepOrder)
      )
    }
  }

  describe("RefreshProfileCatalog.record + scanAll") {
    it("round-trips a single refresh field-for-field") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val rows = Seq(
        row("view_a_12345", "default.view_a", 1700000010000L, 0, "acquire_locks", 3L, "locks=2"),
        row("view_a_12345", "default.view_a", 1700000010001L, 1, "compile", 11L, "key1=v1;key2=v2"),
        row("view_a_12345", "default.view_a", 1700000010002L, 2, "stmt", 17L, "statement=1"),
        row("view_a_12345", "default.view_a", 1700000010003L, 3, "stmt", 23L, ""),
        row("view_a_12345", "default.view_a", 1700000010004L, 4, "total_refresh", 54L, "rows=10")
      )

      RefreshProfileCatalog.record(spark, rows)

      val out = RefreshProfileCatalog.scanAll(spark)
      out should have size 5
      out.zip(rows).foreach { case (actual, expected) => assertRowRoundTrips(actual, expected) }
    }

    it("round-trips detail KV strings with semicolons exactly") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val detail = "refresh_type=INCR_SP;adaptive_recompute=false;sql_bytes=1234"
      val in     = row("view_detail_12345", "default.view_detail", 1700000020000L, 0, "compile", 42L, detail)
      RefreshProfileCatalog.record(spark, Seq(in))

      val out = RefreshProfileCatalog.scanAll(spark)
      out should have size 1
      out.head.detail shouldBe detail
      assertRowRoundTrips(out.head, in)
    }

    it("round-trips an empty detail string as empty, not null") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val in = row("view_empty_12345", "default.view_empty", 1700000030000L, 0, "stmt", 9L, "")
      RefreshProfileCatalog.record(spark, Seq(in))

      val out = RefreshProfileCatalog.scanAll(spark)
      out should have size 1
      out.head.detail shouldBe ""
      out.head.detail should not be null
      assertRowRoundTrips(out.head, in)
    }

    it("round-trips a long 4096-character detail string") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val detail = "x" * 4096
      val in     = row("view_long_12345", "default.view_long", 1700000040000L, 0, "stmt", 99L, detail)
      RefreshProfileCatalog.record(spark, Seq(in))

      val out = RefreshProfileCatalog.scanAll(spark)
      out should have size 1
      out.head.detail.length shouldBe 4096
      out.head.detail shouldBe detail
      assertRowRoundTrips(out.head, in)
    }

    it("round-trips a negative durationMs sentinel without corrupting the sign bit") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val in = row("view_negative_12345", "default.view_negative", 1700000050000L, 0, "stmt", -1L, "sentinel")
      RefreshProfileCatalog.record(spark, Seq(in))

      val out = RefreshProfileCatalog.scanAll(spark)
      out should have size 1
      out.head.durationMs shouldBe -1L
      assertRowRoundTrips(out.head, in)
    }

    it("round-trips step orders greater than 100 without truncation") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val rows = Seq(
        row("view_step_12345", "default.view_step", 1700000060000L, 0, "step_0", 1L, "order=0"),
        row("view_step_12345", "default.view_step", 1700000060000L, 1000, "step_1000", 2L, "order=1000"),
        row(
          "view_step_12345",
          "default.view_step",
          1700000060000L,
          Int.MaxValue,
          "step_int_max",
          3L,
          "order=int_max"
        )
      )

      RefreshProfileCatalog.record(spark, rows)

      val out = RefreshProfileCatalog.scanAll(spark)
      out.map(_.stepOrder) shouldBe Seq(0, 1000, Int.MaxValue)
      out.zip(rows).foreach { case (actual, expected) => assertRowRoundTrips(actual, expected) }
    }
  }

  describe("RefreshProfileCatalog.removeAll") {
    it("wipes all rows") {
      RefreshProfileCatalog.ensureTables(spark)
      RefreshProfileCatalog.removeAll(spark)

      val rows = (0 until 10).map { i =>
        val refreshId = if (i < 5) "view_wipe_a_12345" else "view_wipe_b_12345"
        row(refreshId, s"default.view_wipe_${i % 2}", 1700000070000L + i, i % 5, "stmt", i.toLong, s"i=$i")
      }
      RefreshProfileCatalog.record(spark, rows)
      RefreshProfileCatalog.scanAll(spark) should have size 10

      RefreshProfileCatalog.removeAll(spark)

      RefreshProfileCatalog.scanAll(spark) shouldBe Seq.empty
    }
  }
}
