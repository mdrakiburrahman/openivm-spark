package org.openivm.spark.common.rocksdb

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession

final case class OpenIvmRocksDBConf(
    minVersionsToRetain: Int,
    maintenanceIntervalMs: Long,
    compactionThresholdSstCount: Int,
    walEnabled: Boolean,
    changelogEnabled: Boolean
)

object OpenIvmRocksDBConf {
  val MinVersionsToRetainKey         = "spark.openivm.rocksdb.minVersionsToRetain"
  val MaintenanceIntervalKey         = "spark.openivm.rocksdb.maintenanceInterval"
  val CompactionThresholdSstCountKey = "spark.openivm.rocksdb.compactionThresholdSstCount"
  val WalEnabledKey                  = "spark.openivm.rocksdb.wal.enabled"
  val ChangelogEnabledKey            = "spark.openivm.rocksdb.changelog.enabled"

  val default: OpenIvmRocksDBConf = OpenIvmRocksDBConf(
    minVersionsToRetain = 3,
    maintenanceIntervalMs = JavaUtils.timeStringAsMs("60s"),
    compactionThresholdSstCount = 32,
    walEnabled = true,
    changelogEnabled = false
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
      changelogEnabled = sparkConf.get(ChangelogEnabledKey, default.changelogEnabled.toString).toBoolean
    )
  }
}
