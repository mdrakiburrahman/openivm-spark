package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum, when}
import org.openivm.spark.common.{
  CdfChangeBatch,
  CdfChangePropagation,
  CdfWatermarkCatalog,
  ChangeWatermark,
  FeatureGate,
  MvCatalog,
  MvProjectionSource,
  RefreshProfileCatalog,
  RefreshSqlLogAsyncFlusher,
  RefreshSqlLogCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class DistinctPerInputCdfProgressSpec extends IvmParitySpecBase("distinct-per-input-cdf-progress") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey -> "true",
    FeatureGate.QueryLogEnabledKey -> "true",
    FeatureGate.RuntimeEmptyDeltaSkipEnabledKey -> "true"
  )

  private def assertBag(actual: DataFrame, expected: DataFrame): Unit = {
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  it("advances only the DISTINCT input with physical CDF when its public signed bag cancels") {
    val changed = "dpic_changed"
    val stable = "dpic_stable"
    val consumer = "dpic_consumer"
    for (name <- Seq(changed, stable)) {
      sql(s"CREATE TABLE ${name}_base(k INT, payload INT) USING DELTA")
      sql(s"INSERT INTO ${name}_base VALUES (1, 10), (2, 20)")
      sql(s"CREATE MATERIALIZED VIEW $name AS SELECT DISTINCT k FROM ${name}_base")
      mvDataVersion(name) shouldBe 0L
    }
    val query = s"SELECT c.k, COUNT(*) AS n FROM $changed c JOIN $stable s ON c.k = s.k GROUP BY c.k"
    sql(s"CREATE MATERIALIZED VIEW $consumer AS $query")
    def meta(name: String) = MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name)).get
    meta(changed).refreshType shouldBe RefreshTypeCode.DistinctIncremental
    meta(consumer).refreshType should not be RefreshTypeCode.FullRefresh
    val links = meta(consumer).sourceTables.map(source => source.split("\\.").last -> source).toMap
    links.keySet shouldBe Set(changed, stable)
    val baseline = meta(consumer).changeWatermarks
    baseline shouldBe links.values.map(_ -> ChangeWatermark.DeltaVersion(0L)).toMap
    val view = meta(consumer).name
    val viewKey = view.database.fold(view.table)(db => s"$db.${view.table}")
    def offsets: Map[String, Long] = CdfWatermarkCatalog.getAll(spark, viewKey, links.values.toSeq)
    def pending = new CdfChangePropagation().collectChanges(spark, viewKey, links.values.toSeq, baseline)
    offsets shouldBe empty
    pending shouldBe empty

    val path = mvDataLocation(changed)
    def snapshot(version: Long): DataFrame = spark.read.format("delta").option("versionAsOf", version).load(path)
    def feedOptions(end: Long): Map[String, String] =
      Map("readChangeFeed" -> "true", "startingVersion" -> "1", "endingVersion" -> end.toString)
    def publicNet(end: Long): DataFrame =
      MvProjectionSource.read(spark, links(changed), feedOptions(end))
        .groupBy("k")
        .agg(sum(when(col("_change_type").isin("insert", "update_postimage"), 1L).otherwise(-1L)).as("net"))
        .where("net <> 0")

    sql(s"INSERT INTO ${changed}_base VALUES (1, 11)")
    refreshMv(changed)
    mvDataVersion(changed) shouldBe 1L
    mvDataVersion(stable) shouldBe 0L
    val before = snapshot(0L)
    val after = snapshot(1L)
    val hidden = before.columns.filter(_.startsWith("openivm_")).toSeq
    hidden should not be empty
    val (oldCount, newCount) = (before.where("k = 1"), after.where("k = 1"))
    val increased = hidden.filter(name => newCount.selectExpr(s"CAST(`$name` AS BIGINT)").head().getLong(0) ==
      oldCount.selectExpr(s"CAST(`$name` AS BIGINT)").head().getLong(0) + 1L)
    increased.size shouldBe 1
    assertBag(after.select("k"), before.select("k"))
    val raw = spark.read.format("delta").options(feedOptions(1L)).load(path)
    raw.select("_commit_version", "_change_type").collect().map(row => (row.getLong(0), row.getString(1))).toSeq
      .sorted shouldBe Seq(1L -> "update_postimage", 1L -> "update_preimage")
    val physicalCols = before.columns.toSeq
    assertBag(raw.where("_change_type = 'update_preimage'").select(physicalCols.head, physicalCols.tail: _*), oldCount)
    assertBag(raw.where("_change_type = 'update_postimage'").select(physicalCols.head, physicalCols.tail: _*), newCount)
    publicNet(1L).isEmpty shouldBe true
    pending shouldBe Seq(CdfChangeBatch(links(changed), 0L, 1L))

    def refreshEvidence(): (String, Seq[String]) = {
      val oldIds = RefreshProfileCatalog.scanAll(spark).map(_.refreshId).toSet
      refreshMv(consumer)
      RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
      val totals = RefreshProfileCatalog.scanAll(spark)
        .filter(row => row.stepName == "total_refresh" && !oldIds(row.refreshId))
      totals.size shouldBe 1
      val total = totals.head
      val registrations = RefreshSqlLogCatalog.scanAll(spark)
        .filter(row => row.refreshId == total.refreshId && row.category == "register_source_delta").map(_.sqlText)
      (total.detail, registrations)
    }
    val (result, registrations) = refreshEvidence()
    result should endWith ("pending_deltas=1")
    result should not include "outcome=full_refresh_executed"
    registrations.size shouldBe 2
    registrations.filter(_.contains("table_changes(")) should have size 1
    registrations.count(_.contains(s"table_changes('${links(changed)}', 1, 1)")) shouldBe 1
    registrations.count(text => text.contains(s"openivm_delta_$stable") && text.contains("WHERE 1=0")) shouldBe 1
    assertMvCorrect(consumer, query)
    OpenIvmRocksDBRegistry.closeAll()
    offsets shouldBe Map(links(changed) -> 1L)
    pending shouldBe empty
    val consumerVersion = mvDataVersion(consumer)
    val (replay, replayRegistrations) = refreshEvidence()
    replay should include ("outcome=no_pending_deltas;pending_deltas=0")
    replayRegistrations shouldBe empty
    mvDataVersion(consumer) shouldBe consumerVersion
    offsets shouldBe Map(links(changed) -> 1L)

    // A later public insert makes an unbounded or widened read observably different.
    sql(s"INSERT INTO ${changed}_base VALUES (3, 30)")
    refreshMv(changed)
    mvDataVersion(changed) shouldBe 2L
    mvDataVersion(stable) shouldBe 0L
    publicNet(1L).isEmpty shouldBe true
    assertBag(publicNet(2L), sql("SELECT 3 AS k, 1L AS net"))
    pending shouldBe Seq(CdfChangeBatch(links(changed), 1L, 2L))
    offsets shouldBe Map(links(changed) -> 1L)
  }
}
