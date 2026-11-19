package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, OpenIvmStateSync, RocksDBCodec}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp
import java.util.UUID
import scala.jdk.CollectionConverters._

class RefreshSqlLogSnapshotSpec extends AnyFunSpec with Matchers {
  private def withSpark(state: Path, snapshot: Path)(body: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("RefreshSqlLogSnapshotSpec")
      .config("spark.sql.warehouse.dir", state.toString)
      .config(FeatureGate.StatePathKey, state.toString)
      .config(FeatureGate.StateSyncUriKey, snapshot.toUri.toString)
      .getOrCreate()
    try body(spark)
    finally {
      try OpenIvmRocksDBRegistry.close(RefreshSqlLogCatalog.dbPath(spark))
      finally spark.stop()
    }
  }

  private def roundTrip(write: SparkSession => Unit)(verify: SparkSession => Unit): Unit = {
    val root = Files
      .createDirectories(Paths.get("target", s"test-sql-log-snapshot-${UUID.randomUUID()}"))
      .toAbsolutePath
    val snapshot = root.resolve("snapshot")
    try {
      withSpark(root.resolve("writer"), snapshot) { spark =>
        write(spark)
        OpenIvmStateSync.backupNow(spark)
      }
      val restored = root.resolve("reader")
      Files.exists(restored) shouldBe false
      withSpark(restored, snapshot)(verify)
    } finally {
      val files = Files.walk(root)
      try files.iterator().asScala.toVector.reverse.foreach(Files.delete)
      finally files.close()
    }
  }

  private val execution = RefreshSqlLogRow(
    "snapshot-refresh",
    "default.snapshot_mv",
    Timestamp.valueOf("2026-01-02 03:04:05.123456"),
    7,
    0,
    "refresh",
    "rewritten_stmt",
    "merge",
    41L,
    "MERGE INTO snapshot_mv USING snapshot_delta ON snapshot_mv.id = snapshot_delta.id WHEN MATCHED THEN DELETE"
  )

  it("restores execution, explain and retry rows recorded at the same timestamp and statement order") {
    val explain = execution.copy(category = "explain_formatted", durationMs = -1L, sqlText = "== Physical Plan ==")
    val retry   = execution.copy(attemptIdx = 1, durationMs = 53L)
    val rows    = Seq(execution, explain, retry)

    roundTrip(spark => RefreshSqlLogCatalog.record(spark, rows)) { spark =>
      RefreshSqlLogCatalog.scanAll(spark) should contain theSameElementsAs rows
    }
  }

  it("restores and reads legacy four-component keys in the catalog and export replay index") {
    val legacy          = execution.copy(refreshId = "legacy-refresh")
    val modern          = execution.copy(attemptIdx = 1)
    val appId           = "legacy-application"
    val requestId       = "legacy-request"
    val invocationOrder = 2
    val recordOrder     = 3

    roundTrip { spark =>
      val instant = legacy.profileTimestamp.toInstant
      val micros  = instant.getEpochSecond * 1000000L + instant.getNano / 1000L
      val legacyKey = RocksDBCodec.compositeKey(
        Seq(
          RocksDBCodec.encodeLongBE(micros),
          RocksDBCodec.utf8(legacy.refreshId),
          RocksDBCodec.encodeLongBE(legacy.stmtOrder.toLong),
          RocksDBCodec.encodeLongBE(legacy.attemptIdx.toLong)
        )
      )
      val legacyValue = RocksDBCodec.compositeKey(
        Seq(
          RocksDBCodec.utf8(legacy.viewName),
          RocksDBCodec.utf8(legacy.mode),
          RocksDBCodec.utf8(legacy.category),
          RocksDBCodec.utf8(legacy.stmtKind),
          RocksDBCodec.encodeLongBE(legacy.durationMs),
          RocksDBCodec.utf8(legacy.sqlText)
        )
      )
      val exportKey = RocksDBCodec.compositeKey(
        Seq(
          RocksDBCodec.utf8(appId),
          RocksDBCodec.utf8(requestId),
          RocksDBCodec.encodeLongBE(invocationOrder.toLong),
          RocksDBCodec.encodeLongBE(recordOrder.toLong)
        )
      )
      val db = OpenIvmRocksDBRegistry.getOrOpen(
        spark,
        RefreshSqlLogCatalog.dbPath(spark),
        RefreshSqlLogCatalog.ColumnFamilies
      )
      db.withBatch { batch =>
        OpenIvmRocksDBBatchOps.put(db, batch, RefreshSqlLogCatalog.CfName, legacyKey, legacyValue)
        OpenIvmRocksDBBatchOps.put(
          db,
          batch,
          RefreshSqlLogCatalog.ExportCfName,
          exportKey,
          RocksDBCodec.compositeKey(Seq(legacyKey, legacyValue))
        )
      }
      RefreshSqlLogCatalog.record(spark, Seq(modern))
    } { spark =>
      RefreshSqlLogCatalog.scanAll(spark) should contain theSameElementsAs Seq(legacy, modern)
      RefreshSqlLogCatalog.readExport(spark, appId, requestId, 10, 100000L) shouldBe
        Vector(RefreshSqlLogCatalog.ExportRow(invocationOrder, recordOrder, legacy))
      RefreshSqlLogCatalog.record(spark, Seq(legacy))
      RefreshSqlLogCatalog.scanAll(spark) should contain theSameElementsAs Seq(legacy, modern)
    }
  }
}
