package org.openivm.spark.common.rocksdb

import java.io.File

import org.openivm.spark.common.{
  CdfChangeBatch,
  CdfChangePropagation,
  CdfWatermarkCatalog,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  MvProjectionSource,
  RefreshProfileCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class RankGroupPublicCdfProgressSpec extends IvmParitySpecBase("rank-group-public-cdf") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey        -> "true",
    FeatureGate.StateSyncAsyncEnabledKey -> "false",
    FeatureGate.StateSyncUriKey          -> new File(warehouseDir, "state-sync-remote").toURI.toString
  )

  private def metadata(name: String): MvMetadata =
    MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name)).getOrElse(fail(s"Missing $name"))

  private def key(meta: MvMetadata): String = meta.name.database.fold(meta.name.table)(db => s"$db.${meta.name.table}")

  private def refreshOutcome(name: String, pending: Int): String = {
    val before = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
    refreshMv(name)
    val rows =
      RefreshProfileCatalog.scanAll(spark).filter(row => row.stepName == "total_refresh" && !before(row.refreshId))
    rows.size shouldBe 1
    rows.head.detail should endWith(s"pending_deltas=$pending")
    rows.head.detail.split(";")(1).stripPrefix("outcome=")
  }

  private def rank(source: String, order: String = "score DESC, id ASC"): String =
    "SELECT part, id, score, rn FROM (" +
      s"SELECT part, id, score, ROW_NUMBER() OVER (PARTITION BY part ORDER BY $order) AS rn FROM $source" +
      ") ranked WHERE rn <= 2"

  private final class Fixture(suffix: String, testGroup: Boolean, order: String = "score DESC, id ASC") {
    val prefix      = s"rgpc_$suffix"
    val base        = s"${prefix}_base"
    val producer    = s"${prefix}_producer"
    val ranked      = s"${prefix}_ranked"
    val grouped     = s"${prefix}_grouped"
    val producerSql = s"SELECT DISTINCT part, id, score FROM $base"
    val groupSql    = s"SELECT part, COUNT(*) AS n FROM $producer GROUP BY part"
    sql(s"CREATE TABLE $base(part INT, id INT, score INT, payload INT) USING DELTA")
    sql(s"INSERT INTO $base VALUES (1, 1, 90, 10), (1, 2, 90, 20), (1, 3, 80, 30), (NULL, 4, NULL, 40)")
    sql(s"CREATE MATERIALIZED VIEW $producer AS $producerSql")
    sql(s"CREATE MATERIALIZED VIEW $ranked AS ${rank(producer, order)}")
    metadata(ranked).refreshType shouldBe RefreshTypeCode.WindowPartition
    if (testGroup) {
      sql(s"CREATE MATERIALIZED VIEW $grouped AS $groupSql")
      metadata(grouped).refreshType shouldBe RefreshTypeCode.AggregateGroup
    }
    val consumers: Seq[String]             = if (testGroup) Seq(ranked, grouped) else Seq(ranked)
    def source(name: String): String       = metadata(name).sourceTables.head
    def offset(name: String): Option[Long] = CdfWatermarkCatalog.get(spark, key(metadata(name)), source(name))
    def pending(name: String): Seq[CdfChangeBatch] = {
      val meta = metadata(name)
      new CdfChangePropagation()
        .collectChanges(spark, key(meta), meta.sourceTables, meta.changeWatermarks)
        .collect { case cdf: CdfChangeBatch => cdf }
    }
    def check(): Unit = {
      assertMvCorrect(producer, producerSql)
      assertMvCorrect(ranked, rank(s"($producerSql) AS expected_rows", order))
      if (testGroup) assertMvCorrect(grouped, s"SELECT part, COUNT(*) AS n FROM ($producerSql) GROUP BY part")
    }
    def duplicate(): Unit = sql(s"INSERT INTO $base VALUES (1, 1, 90, 11), (NULL, 4, NULL, 41)")
    def feed(from: Long, to: Long): org.apache.spark.sql.DataFrame =
      MvProjectionSource.read(
        spark,
        source(ranked),
        Map(
          "readChangeFeed"  -> "true",
          "startingVersion" -> from.toString,
          "endingVersion"   -> to.toString
        )
      )
    def assertSignedZero(from: Long, to: Long): Unit = {
      val cdf   = feed(from, to)
      val plus  = cdf.where("_change_type IN ('insert', 'update_postimage')").select("part", "id", "score")
      val minus = cdf.where("_change_type IN ('delete', 'update_preimage')").select("part", "id", "score")
      plus.isEmpty shouldBe false
      plus.exceptAll(minus).isEmpty shouldBe true
      minus.exceptAll(plus).isEmpty shouldBe true
    }
  }

  it("compares matched inclusive refreshes and restores offsets from the durable archive") {
    for (enabled <- Seq(false, true)) {
      spark.conf.set(FeatureGate.PublicCdfRankGroupNoWriteEnabledKey, enabled.toString)
      val f        = new Fixture(if (enabled) "proof" else "baseline", testGroup = true)
      val versions = f.consumers.map(mvDataVersion)
      f.consumers.foreach(name => f.offset(name) shouldBe None)
      f.duplicate()
      refreshOutcome(f.producer, 1) shouldBe "incremental_executed"
      mvDataVersion(f.producer) shouldBe 1L
      f.consumers.foreach { name =>
        f.pending(name) shouldBe Seq(CdfChangeBatch(f.source(name), 0L, 1L))
        refreshOutcome(name, 1) shouldBe (if (enabled) "public_cdf_no_write" else "incremental_executed")
      }
      f.assertSignedZero(1L, 1L)
      val raw = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1L)
        .option("endingVersion", 1L)
        .load(mvDataLocation(f.producer))
      val countColumn = "openivm_distinct_count"
      raw.where("_change_type = 'update_preimage'").select(countColumn).collect().map(_.getLong(0)).toSeq shouldBe Seq(
        1L,
        1L
      )
      raw.where("_change_type = 'update_postimage'").select(countColumn).collect().map(_.getLong(0)).toSeq shouldBe Seq(
        2L,
        2L
      )
      if (enabled) {
        f.consumers.map(mvDataVersion) shouldBe versions
        OpenIvmRocksDBRegistry.closeAll()
        val local = new File(warehouseDir, "_openivm")
        deleteDir(local)
        local.exists() shouldBe false
        OpenIvmStateSync.resetForTesting()
      }
      f.consumers.foreach { name =>
        f.offset(name) shouldBe Some(1L)
        f.pending(name) shouldBe empty
        val before = mvDataVersion(name)
        refreshOutcome(name, 0) shouldBe "no_pending_deltas"
        mvDataVersion(name) shouldBe before
      }
      f.check()

      if (enabled) {
        // 2→1 cancels, including a NULL duplicate. Then a mixed 1→0, 0→1, 1→1 key move must apply.
        sql(s"DELETE FROM ${f.base} WHERE payload IN (11, 41)")
        refreshMv(f.producer)
        val duplicateRemoval = mvDataVersion(f.producer)
        f.assertSignedZero(2L, duplicateRemoval)
        f.consumers.zip(versions).foreach { case (name, version) =>
          refreshOutcome(name, 1) shouldBe "public_cdf_no_write"
          f.offset(name) shouldBe Some(duplicateRemoval)
          mvDataVersion(name) shouldBe version
        }
        sql(s"DELETE FROM ${f.base} WHERE payload = 20")
        sql(s"UPDATE ${f.base} SET id = 5, score = 95 WHERE payload = 30")
        sql(s"INSERT INTO ${f.base} VALUES (1, 6, 100, 60), (NULL, 4, NULL, 42)")
        refreshMv(f.producer)
        val mixed = mvDataVersion(f.producer)
        mixed should be > duplicateRemoval
        f.consumers.zip(versions).foreach { case (name, version) =>
          refreshOutcome(name, 1) shouldBe "incremental_executed"
          f.offset(name) shouldBe Some(mixed)
          mvDataVersion(name) should be > version
        }
        f.check()
      }
    }
  }

  it("declines uncertain ties and a missing CDF baseline") {
    spark.conf.set(FeatureGate.PublicCdfRankGroupNoWriteEnabledKey, "true")
    val f = new Fixture("ties", testGroup = false, order = "score DESC")
    f.duplicate()
    refreshMv(f.producer)
    refreshOutcome(f.ranked, 1) shouldBe "incremental_executed"
    f.offset(f.ranked) shouldBe Some(1L)

    val exact           = new Fixture("unknown", testGroup = false)
    val initial         = metadata(exact.ranked)
    val withoutBaseline = initial.properties.filterNot { case (k, _) => k.startsWith(MvMetadata.WatermarkKeyPrefix) }
    MvCatalog.upsert(spark, initial.copy(properties = withoutBaseline))
    exact.duplicate()
    refreshMv(exact.producer)
    refreshOutcome(exact.ranked, 1) shouldBe "full_refresh_executed"
    exact.offset(exact.ranked) shouldBe Some(1L)
    exact.check()
  }
}
