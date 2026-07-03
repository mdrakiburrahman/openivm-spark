package org.openivm.spark.common

/** Single source of truth for the column families hosted by the shared
  * `<warehouse>/_openivm/index/rocksdb` instance.
  *
  * The [[org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry]] enforces
  * a no-widening invariant on the requested CF set per DB path, so every
  * caller that opens this DB MUST declare the full union.  Keep this list
  * in sync with all `openIndexDb` call sites: [[MvCatalog]],
  * [[StagingCatalog]], and [[CdfWatermarkCatalog]].
  */
private[common] object IndexDbColumnFamilies {

  val MvIndex: String               = "mv_index"
  val SourceToMvs: String           = "source_to_mvs"
  val TableIndex: String            = "table_index"
  val CdfWatermarks: String         = "cdf_watermarks"
  val DroppedMvFingerprints: String = "dropped_mv_fingerprints"

  val All: Seq[String] = Seq(MvIndex, SourceToMvs, TableIndex, CdfWatermarks, DroppedMvFingerprints)
}
