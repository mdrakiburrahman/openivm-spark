package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

import java.nio.file.Paths

object WindowStateCatalog {

  private val Cf: String                          = IndexDbColumnFamilies.WindowState
  private[common] val ColumnFamilies: Seq[String] = IndexDbColumnFamilies.All

  private def warehouseDir(spark: SparkSession): String =
    RocksDBCodec.requireLocalPath(spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/"))

  private def indexDbPath(spark: SparkSession): String =
    Paths.get(warehouseDir(spark), "_openivm", "index", "rocksdb").toString

  private def openIndexDb(spark: SparkSession): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, indexDbPath(spark), ColumnFamilies)

  private def key(view: String, partitionKey: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(view), RocksDBCodec.utf8(partitionKey)))

  private def viewPrefix(view: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(view), Array.emptyByteArray))

  def ensureTables(spark: SparkSession): Unit = {
    openIndexDb(spark)
    ()
  }

  def put(spark: SparkSession, view: String, partitionKey: String, statePayload: String): Unit = {
    val db = openIndexDb(spark)
    db.withBatch { batch =>
      OpenIvmRocksDBBatchOps.put(db, batch, Cf, key(view, partitionKey), RocksDBCodec.utf8(statePayload))
    }
    ()
  }

  def putAll(spark: SparkSession, view: String, entries: Seq[(String, String)]): Unit = {
    if (entries.isEmpty) return
    val db = openIndexDb(spark)
    db.withBatch { batch =>
      entries.foreach { case (partitionKey, statePayload) =>
        OpenIvmRocksDBBatchOps.put(db, batch, Cf, key(view, partitionKey), RocksDBCodec.utf8(statePayload))
      }
    }
    ()
  }

  def get(spark: SparkSession, view: String, partitionKey: String): Option[String] =
    openIndexDb(spark).get(Cf, key(view, partitionKey)).map(RocksDBCodec.fromUtf8)

  def getMany(spark: SparkSession, view: String, partitionKeys: Seq[String]): Map[String, String] = {
    val db = openIndexDb(spark)
    partitionKeys
      .flatMap(partitionKey => db.get(Cf, key(view, partitionKey)).map(RocksDBCodec.fromUtf8).map(partitionKey -> _))
      .toMap
  }

  def scanForView(spark: SparkSession, view: String): Seq[(String, String)] = {
    val iterator = openIndexDb(spark).prefixScan(Cf, viewPrefix(view))
    try {
      iterator.flatMap { case (k, v) =>
        val parts = RocksDBCodec.splitComposite(k, 2)
        if (parts.length == 2) {
          Iterator.single(RocksDBCodec.fromUtf8(parts(1)) -> RocksDBCodec.fromUtf8(v))
        } else {
          Iterator.empty
        }
      }.toVector
    } finally {
      iterator match {
        case closeable: AutoCloseable =>
          try closeable.close()
          catch { case _: Throwable => () }
        case _ => ()
      }
    }
  }

  def removeAll(spark: SparkSession, view: String): Unit = {
    val db      = openIndexDb(spark)
    val victims = scanForView(spark, view).map { case (partitionKey, _) => key(view, partitionKey) }
    if (victims.nonEmpty) {
      db.withBatch { batch =>
        victims.foreach(k => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, k))
      }
    }
    ()
  }
}
