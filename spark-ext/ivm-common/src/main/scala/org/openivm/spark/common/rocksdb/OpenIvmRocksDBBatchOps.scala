package org.openivm.spark.common.rocksdb

import org.rocksdb.WriteBatch

private[common] object OpenIvmRocksDBBatchOps {
  def put(
      db: OpenIvmRocksDB,
      batch: WriteBatch,
      columnFamily: String,
      key: Array[Byte],
      value: Array[Byte]
  ): Unit =
    db.put(batch, columnFamily, key, value)

  def delete(
      db: OpenIvmRocksDB,
      batch: WriteBatch,
      columnFamily: String,
      key: Array[Byte]
  ): Unit =
    db.delete(batch, columnFamily, key)

  def deleteRange(
      db: OpenIvmRocksDB,
      batch: WriteBatch,
      columnFamily: String,
      startKey: Array[Byte],
      endKey: Array[Byte]
  ): Unit =
    db.deleteRange(batch, columnFamily, startKey, endKey)
}
