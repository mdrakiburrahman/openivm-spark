package org.openivm.spark.common

sealed trait ChangeBatch {

  def baseTable: String

  def endWatermark: ChangeWatermark
}

final case class StagingChangeBatch(
    baseTable: String,
    deltas: Seq[StagingDelta],
    endWatermark: ChangeWatermark
) extends ChangeBatch

final case class CdfChangeBatch(
    baseTable: String,
    startVersionExclusive: Long,
    endVersionInclusive: Long
) extends ChangeBatch {
  override def endWatermark: ChangeWatermark = ChangeWatermark.DeltaVersion(endVersionInclusive)
}
