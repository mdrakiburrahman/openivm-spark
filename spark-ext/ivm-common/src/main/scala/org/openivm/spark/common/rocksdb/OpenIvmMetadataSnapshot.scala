package org.openivm.spark.common.rocksdb

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit
import scala.util.control.ControlThrowable

/** Bounded, immutable observations of already-open, application-owned metadata.
  * No handle, iterator, or native resource crosses this API's boundary.
  */
object OpenIvmMetadataSnapshot {
  private val mapper = new ObjectMapper()

  private[rocksdb] val ColumnFamilies =
    Vector("meta", "properties", "cdf_watermarks", "consumed", "dependent_mvs", "staging")

  private[rocksdb] final case class Entry(keyBase64: String, valueBase64: String)
  private[rocksdb] final case class Captured(
      version: Long,
      columnFamilies: Map[String, Vector[Entry]],
      entryCount: Int,
      byteCount: Long
  )
  private[rocksdb] final case class Unavailable(reason: String) extends ControlThrowable

  private[rocksdb] final class Budget(maxEntries: Int, maxBytes: Long, timeoutMs: Long) {
    require(maxEntries > 0 && maxEntries <= 100000, "maxEntries must be between 1 and 100000")
    require(maxBytes > 0L && maxBytes <= 16L * 1024 * 1024, "maxBytes must be between 1 and 16777216")
    require(timeoutMs > 0L && timeoutMs <= 5000L, "timeoutMs must be between 1 and 5000")

    private val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    var entryCount: Int  = 0
    var byteCount: Long  = 0L

    def remainingNanos: Long = math.max(0L, deadline - System.nanoTime())

    def checkTime(): Unit =
      if (remainingNanos == 0L) throw Unavailable("timeout")

    def addEntry(bytes: Long): Unit = {
      checkTime()
      if (entryCount >= maxEntries || bytes > maxBytes - byteCount)
        throw Unavailable("limit_exceeded")
      entryCount += 1
      byteCount += bytes
    }
  }

  /** Py4J-friendly default: at most 8192 records / 8 MiB of raw key+value bytes,
    * with a one-second lock/scan budget. Native reads already in flight are not
    * interrupted. An unavailable result never contains partial metadata.
    */
  def captureIfOpenJson(spark: SparkSession, dbPath: String): String =
    captureIfOpenJson(spark, dbPath, 8192, 8L * 1024 * 1024, 1000L)

  def captureIfOpenJson(
      spark: SparkSession,
      dbPath: String,
      maxEntries: Int,
      maxBytes: Long,
      timeoutMs: Long
  ): String = {
    val budget                  = new Budget(maxEntries, maxBytes, timeoutMs)
    val (canonicalPath, result) = OpenIvmRocksDBRegistry.captureMetadataIfOpen(spark, dbPath, budget)
    val root                    = mapper.createObjectNode()
    root.put("schema_id", "openivm.metadata-snapshot")
    root.put("schema_version", 1)
    root.put("path", canonicalPath)
    root.put("available", result.isRight)
    result match {
      case Left(reason) => root.put("reason", reason)
      case Right(snapshot) =>
        root.put("version", snapshot.version)
        root.put("entry_count", snapshot.entryCount)
        root.put("byte_count", snapshot.byteCount)
        val families = root.putObject("column_families")
        val absent   = root.putArray("absent_column_families")
        ColumnFamilies.foreach { name =>
          snapshot.columnFamilies.get(name) match {
            case None => absent.add(name)
            case Some(entries) =>
              val rows = families.putArray(name)
              entries.foreach { entry =>
                val row = rows.addObject()
                row.put("key_base64", entry.keyBase64)
                row.put("value_base64", entry.valueBase64)
              }
          }
        }
    }
    mapper.writeValueAsString(root)
  }
}
