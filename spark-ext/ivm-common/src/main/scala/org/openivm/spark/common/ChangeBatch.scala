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

  /** `false` when no consumed version is known for the source, so its change feed cannot be
    * applied on top of the view's rows.
    */
  def hasBaseline: Boolean = startVersionExclusive != CdfChangeBatch.NoBaseline
}

object CdfChangeBatch {

  /** `startVersionExclusive` of a source whose consumed version is unknown. */
  val NoBaseline: Long = -1L
}

/** Exact immutable source-snapshot transition requested by ALTER MATERIALIZED
  * VIEW ... ADVANCE SOURCE VERSIONS.
  */
final case class SourceVersionChangeBatch(
    baseTable: String,
    startVersionInclusive: Long,
    endVersionInclusive: Long
) extends ChangeBatch {
  override def endWatermark: ChangeWatermark = ChangeWatermark.DeltaVersion(endVersionInclusive)
}
