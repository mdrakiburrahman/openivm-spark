package org.openivm.spark.common

/** Column families retained in the legacy shared index RocksDB.
  *
  * Fresh deployments do not create this database. `cdf_watermarks` remains
  * declared solely for lazy migration; RocksDB discovers any other old column
  * families when opening an existing DB.
  */
private[common] object IndexDbColumnFamilies {

  /** Legacy-only; remove after all deployed shared watermarks have migrated. */
  val CdfWatermarks: String = "cdf_watermarks"

  val All: Seq[String] = Seq(CdfWatermarks)
}
