package org.openivm.spark.common.rocksdb

import java.io.File

import org.apache.spark.sql.DataFrame
import org.openivm.spark.common.{
  CdfChangeBatch,
  CdfChangePropagation,
  CdfWatermarkCatalog,
  ChangeWatermark,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  RefreshProfileCatalog,
  RefreshSqlLogAsyncFlusher,
  RefreshSqlLogCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class QueuedPhysicalCdfProgressSpec extends IvmParitySpecBase("queued-physical-cdf-progress") with CdfMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.ProfileRefreshKey        -> "true",
    FeatureGate.QueryLogEnabledKey       -> "true",
    FeatureGate.StateSyncUriKey          -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false"
  )

  private val propagation = new CdfChangePropagation

  private def metadata(name: String): MvMetadata =
    MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(name)).getOrElse(fail(s"Missing $name"))

  private def assertBag(actual: DataFrame, expected: DataFrame): Unit = {
    actual.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(actual).count() shouldBe 0L
  }

  private def cdf(path: String, end: Long): DataFrame =
    spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1L)
      .option("endingVersion", end)
      .load(path)

  private def viewKey(meta: MvMetadata): String =
    meta.name.database.fold(meta.name.table)(db => s"$db.${meta.name.table}")

  private def profile(name: String): (String, Seq[String]) = {
    val totals = RefreshProfileCatalog.scanAll(spark).filter(_.stepName == "total_refresh").map(_.refreshId).toSet
    refreshMv(name)
    RefreshSqlLogAsyncFlusher.awaitQuiescence(30000L) shouldBe true
    val newTotals = RefreshProfileCatalog
      .scanAll(spark)
      .filter(row => row.stepName == "total_refresh" && !totals.contains(row.refreshId))
    newTotals.size shouldBe 1
    val total = newTotals.head
    val registrations = RefreshSqlLogCatalog
      .scanAll(spark)
      .filter(row => row.refreshId == total.refreshId && row.category == "register_source_delta")
      .map(_.sqlText)
    (total.detail, registrations)
  }

  Seq("rocksdb", "delta").foreach { backend =>
    it(s"consumes queued physical CDF without losing progress on $backend") {
      spark.conf.set(FeatureGate.CatalogBackendKey, backend)
      val prefix = s"qpc_$backend"
      val targets = Seq("left" -> 3, "right" -> 3, "instance" -> 8).map { case (suffix, n) =>
        val base = s"${prefix}_${suffix}_base"
        val up   = s"${prefix}_${suffix}_agg"
        sql(s"CREATE TABLE $base(k INT, v INT) USING DELTA")
        sql(s"INSERT INTO $base SELECT CAST(id AS INT), CAST(id + 10 AS INT) FROM range($n)")
        sql(s"INSERT INTO $base VALUES (0, 10), (0, NULL)")
        sql(s"CREATE MATERIALIZED VIEW $up AS SELECT k, SUM(v) AS total FROM $base GROUP BY k")
        mvDataVersion(up) shouldBe 0L
        (up, n)
      }
      val Seq(left, right, instance) = targets.map(_._1)
      val databaseConsumer           = s"${prefix}_database_txn"
      val instanceConsumer           = s"${prefix}_instance_txn"
      sql(
        s"CREATE MATERIALIZED VIEW $databaseConsumer AS SELECT l.k, SUM(l.total + r.total) AS total " +
          s"FROM $left l JOIN $right r ON l.k = r.k GROUP BY l.k"
      )
      sql(s"CREATE MATERIALIZED VIEW $instanceConsumer AS SELECT k, SUM(total) AS total FROM $instance GROUP BY k")
      val consumers = Seq(databaseConsumer -> Seq(left, right), instanceConsumer -> Seq(instance)).map {
        case (name, sources) =>
          val meta = metadata(name)
          meta.refreshType should not be RefreshTypeCode.FullRefresh
          val links = meta.sourceTables
          links.map(_.split("\\.").last).toSet shouldBe sources.toSet
          meta.changeWatermarks shouldBe links.map(_ -> ChangeWatermark.DeltaVersion(0L)).toMap
          links.foreach(source => CdfWatermarkCatalog.get(spark, viewKey(meta), source) shouldBe None)
          (name, meta, links, mvDataVersion(name))
      }

      targets.foreach { case (up, n) =>
        withClue(s"$up: ") {
          val path     = mvDataLocation(up)
          val physical = s"delta.`$path`"
          val v0       = spark.read.format("delta").option("versionAsOf", 0L).load(path)
          val cols     = v0.columns.toSeq
          cols.exists(_.startsWith("openivm_")) shouldBe true
          v0.count() shouldBe n.toLong

          sql(s"DELETE FROM $physical")
          mvDataVersion(up) shouldBe 1L
          spark.read.format("delta").option("versionAsOf", 1L).load(path).isEmpty shouldBe true
          v0.write.format("delta").mode("append").save(path)
          mvDataVersion(up) shouldBe 2L
          assertBag(spark.read.format("delta").load(path), v0)

          // Execute the bounded read only after v2 exists: v2 cannot cancel v1 from outside the range.
          val bounded = cdf(path, 1L)
          bounded
            .select("_commit_version", "_change_type")
            .distinct()
            .collect()
            .map(row => (row.getLong(0), row.getString(1)))
            .toSeq shouldBe Seq(1L -> "delete")
          assertBag(bounded.select(cols.head, cols.tail: _*), v0)
          val queued = cdf(path, 2L)
          queued
            .select("_commit_version", "_change_type")
            .distinct()
            .collect()
            .map(row => (row.getLong(0), row.getString(1)))
            .toSet shouldBe Set(1L -> "delete", 2L -> "insert")
          val deleted  = queued.where("_change_type = 'delete'").select(cols.head, cols.tail: _*)
          val inserted = queued.where("_change_type = 'insert'").select(cols.head, cols.tail: _*)
          assertBag(inserted, deleted)

          val source    = consumers.flatMap(_._3).find(_.split("\\.").last == up).get
          val schema    = spark.table(source).schema
          val deltaView = s"openivm_delta_$up"
          val rawView   = s"_openivm_cdf_raw_$up"
          try {
            propagation.registerSourceDeltaView(spark, source, schema, Seq(CdfChangeBatch(source, 0L, 1L)))
            val registered = spark.table(deltaView)
            registered.where("openivm_multiplicity <> -1 OR openivm_multiplicity IS NULL").isEmpty shouldBe true
            val names = schema.fieldNames.toSeq
            assertBag(registered.select(names.head, names.tail: _*), v0.select(names.head, names.tail: _*))
          } finally {
            spark.catalog.dropTempView(deltaView)
            spark.catalog.dropTempView(rawView)
          }
        }
      }

      consumers.foreach { case (name, meta, links, originalVersion) =>
        withClue(s"$name queued refresh: ") {
          propagation.collectChanges(spark, viewKey(meta), links, meta.changeWatermarks).toSet shouldBe
            links.map(CdfChangeBatch(_, 0L, 2L)).toSet
          val (end, registrations) = profile(name)
          end should include("outcome=incremental_executed")
          end should endWith(s"pending_deltas=${links.size}")
          registrations.size shouldBe links.size
          links.foreach { source =>
            registrations.count(sqlText =>
              sqlText.contains("startingVersion=1 endingVersion=2") &&
                sqlText.contains(s"table_changes('$source', 1, 2)")
            ) shouldBe 1
          }
          assertBag(
            spark.read.format("delta").load(meta.location),
            spark.read.format("delta").option("versionAsOf", originalVersion).load(meta.location)
          )
          assertMvCorrect(name, meta.querySql)
        }
      }

      // Reopen from the published archive instead of trusting an in-memory offset or open DB.
      OpenIvmRocksDBRegistry.closeAll()
      spark.catalog.clearCache()
      if (backend == "rocksdb") {
        val local = new File(warehouseDir, "_openivm")
        deleteDir(local)
        local.exists() shouldBe false
        OpenIvmStateSync.resetForTesting()
      }
      consumers.foreach { case (name, meta, links, _) =>
        withClue(s"$name replay: ") {
          links.foreach(source => CdfWatermarkCatalog.get(spark, viewKey(meta), source) shouldBe Some(2L))
          propagation.collectChanges(spark, viewKey(meta), links, meta.changeWatermarks) shouldBe empty
          val beforeReplay         = mvDataVersion(name)
          val (end, registrations) = profile(name)
          end should include("outcome=no_pending_deltas")
          end should endWith("pending_deltas=0")
          registrations shouldBe empty
          mvDataVersion(name) shouldBe beforeReplay
          links.foreach(source => CdfWatermarkCatalog.get(spark, viewKey(meta), source) shouldBe Some(2L))
        }
      }
    }
  }
}
