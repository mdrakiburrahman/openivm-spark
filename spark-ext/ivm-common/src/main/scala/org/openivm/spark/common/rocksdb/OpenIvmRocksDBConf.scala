package org.openivm.spark.common.rocksdb

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession

final case class OpenIvmRocksDBConf(
    minVersionsToRetain: Int,
    maintenanceIntervalMs: Long,
    compactionThresholdSstCount: Int,
    walEnabled: Boolean,
    changelogEnabled: Boolean,
    multiProcess: Boolean,
    lockTimeoutMs: Long
)

object OpenIvmRocksDBConf {
  val MinVersionsToRetainKey         = "spark.openivm.rocksdb.minVersionsToRetain"
  val MaintenanceIntervalKey         = "spark.openivm.rocksdb.maintenanceInterval"
  val CompactionThresholdSstCountKey = "spark.openivm.rocksdb.compactionThresholdSstCount"
  val WalEnabledKey                  = "spark.openivm.rocksdb.wal.enabled"
  val ChangelogEnabledKey            = "spark.openivm.rocksdb.changelog.enabled"

  /** Multi-process mode: when true, every catalog operation acquires a POSIX
    * file lock on `<dbPath>/openivm-jvm.lock`, opens RocksDB, performs the
    * operation, then closes RocksDB and releases the lock. This is required
    * when multiple JVMs (e.g. multiple Livy session drivers) share a single
    * openivm warehouse directory because RocksDB itself rejects concurrent
    * multi-process opens with `IOError: Resource temporarily unavailable`.
    * Default `false` — single-JVM deployments keep handles open for the
    * lifetime of the Spark application (registry-cached).
    */
  val MultiProcessKey = "spark.openivm.rocksdb.multiProcess"

  /** How long an external-lock acquisition waits before failing. Defaults to
    * 60s — plenty for the slowest catalog op (a full warehouse-index scan).
    */
  val LockTimeoutKey = "spark.openivm.rocksdb.lockTimeout"

  val default: OpenIvmRocksDBConf = OpenIvmRocksDBConf(
    minVersionsToRetain = 3,
    maintenanceIntervalMs = JavaUtils.timeStringAsMs("60s"),
    compactionThresholdSstCount = 32,
    walEnabled = true,
    changelogEnabled = false,
    multiProcess = false,
    lockTimeoutMs = JavaUtils.timeStringAsMs("60s")
  )

  def fromSpark(spark: SparkSession): OpenIvmRocksDBConf = {
    val sparkConf = spark.conf
    OpenIvmRocksDBConf(
      minVersionsToRetain = sparkConf.get(MinVersionsToRetainKey, default.minVersionsToRetain.toString).toInt,
      maintenanceIntervalMs = JavaUtils.timeStringAsMs(
        sparkConf.get(MaintenanceIntervalKey, s"${default.maintenanceIntervalMs}ms")
      ),
      compactionThresholdSstCount = sparkConf
        .get(CompactionThresholdSstCountKey, default.compactionThresholdSstCount.toString)
        .toInt,
      walEnabled = sparkConf.get(WalEnabledKey, default.walEnabled.toString).toBoolean,
      changelogEnabled = sparkConf.get(ChangelogEnabledKey, default.changelogEnabled.toString).toBoolean,
      multiProcess = sparkConf.get(MultiProcessKey, default.multiProcess.toString).toBoolean,
      lockTimeoutMs = JavaUtils.timeStringAsMs(sparkConf.get(LockTimeoutKey, s"${default.lockTimeoutMs}ms"))
    )
  }
}
