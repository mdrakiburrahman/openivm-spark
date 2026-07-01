package org.openivm.spark.common

final case class RefreshCostEstimate(
    incrementalCost: Double,
    fullRecomputeCost: Double,
    recommendFullRefresh: Boolean,
    affectedFraction: Double,
    rationale: String
)

object RefreshCostModel {

  private val FullRefreshHysteresis = 1.10d

  private val SimpleProjectionInsertOnlyMultiplier = 1.0d
  private val SimpleProjectionGeneralMultiplier    = 2.0d
  private val SimpleAggregateMultiplier            = 2.0d
  private val AggregateGroupMultiplier             = 3.0d
  private val DistinctIncrementalMultiplier        = 3.0d
  private val SemiAntiRecomputeMultiplier          = 4.0d
  private val DefaultIncrementalMultiplier         = 4.0d
  private val PartitionRecomputeBaseMultiplier     = 1.5d
  // SF telemetry shows WINDOW_PARTITION reads the MV about five extra passes when most partitions are affected.
  private val PartitionRecomputeMvPassesAtFullAffected = 5.0d

  def estimate(
      refreshType: Int,
      facts: WorkloadFacts,
      mvRowCount: Long,
      sourceTables: Seq[String]
  ): RefreshCostEstimate = {
    val sources               = selectedSources(facts, sourceTables)
    val sourceRowCount        = sources.flatMap(source => tableStatsFor(facts, source).flatMap(_.rowCount)).sum
    val nonNegativeMvRowCount = math.max(0L, mvRowCount)
    val deltaRowCount         = sources.map(source => effectiveDeltaRows(facts, source)).sum
    val affectedFraction      = estimateAffectedFraction(deltaRowCount, nonNegativeMvRowCount, sourceRowCount)
    val affectedRows          = nonNegativeMvRowCount.toDouble * affectedFraction
    val fullRecomputeCost     = math.max(1.0d, sourceRowCount.toDouble + nonNegativeMvRowCount.toDouble)
    val incrementalCost = estimateIncrementalCost(refreshType, facts, deltaRowCount, affectedRows, affectedFraction)
    val recommendFullRefresh = shouldRecommendFull(refreshType, incrementalCost, fullRecomputeCost)
    val rationale = rationaleFor(
      refreshType,
      deltaRowCount,
      sourceRowCount,
      affectedFraction,
      incrementalCost,
      fullRecomputeCost,
      recommendFullRefresh
    )

    RefreshCostEstimate(
      incrementalCost = incrementalCost,
      fullRecomputeCost = fullRecomputeCost,
      recommendFullRefresh = recommendFullRefresh,
      affectedFraction = affectedFraction,
      rationale = rationale
    )
  }

  private def selectedSources(facts: WorkloadFacts, sourceTables: Seq[String]): Seq[String] = {
    val configuredSources = if (sourceTables.nonEmpty) sourceTables else facts.tableStats.keys.toSeq
    val deltaOnlySources  = facts.deltaStats.keys.filterNot(delta => configuredSources.exists(matchesTable(_, delta)))
    (configuredSources ++ deltaOnlySources).distinct
  }

  private def tableStatsFor(facts: WorkloadFacts, table: String): Option[WorkloadTableStats] =
    lookupTable(facts.tableStats, table)

  private def deltaStatsFor(facts: WorkloadFacts, table: String): Option[WorkloadDeltaStats] =
    lookupTable(facts.deltaStats, table)

  private def deltaShapeFor(facts: WorkloadFacts, table: String): Option[DeltaShape] =
    lookupTable(facts.deltaShape, table)

  private def lookupTable[A](values: Map[String, A], table: String): Option[A] =
    values.get(table).orElse(values.collectFirst { case (key, value) if matchesTable(key, table) => value })

  private def matchesTable(left: String, right: String): Boolean =
    left.equalsIgnoreCase(right) || shortTableName(left).equalsIgnoreCase(shortTableName(right))

  private def shortTableName(table: String): String = table.split('.').lastOption.getOrElse(table)

  private def effectiveDeltaRows(facts: WorkloadFacts, source: String): Long =
    deltaShapeFor(facts, source) match {
      case Some(DeltaShape.Unchanged) => 0L
      case _                          => deltaStatsFor(facts, source).flatMap(_.rowCount).getOrElse(0L).max(0L)
    }

  private def estimateAffectedFraction(deltaRows: Long, mvRows: Long, sourceRows: Long): Double =
    if (deltaRows <= 0L || mvRows <= 0L) 0.0d
    else {
      val rowExpansion       = if (sourceRows > 0L) mvRows.toDouble / sourceRows.toDouble else 1.0d
      val affectedOutputRows = deltaRows.toDouble * rowExpansion
      clamp01(affectedOutputRows / mvRows.toDouble)
    }

  private def estimateIncrementalCost(
      refreshType: Int,
      facts: WorkloadFacts,
      deltaRows: Long,
      affectedRows: Double,
      affectedFraction: Double
  ): Double = {
    val readWriteRows = deltaRows.toDouble + affectedRows
    refreshType match {
      case RefreshTypeCode.FullRefresh => readWriteRows
      case RefreshTypeCode.SimpleProjection if allChangedSourcesInsertOnly(facts) =>
        readWriteRows * SimpleProjectionInsertOnlyMultiplier
      case RefreshTypeCode.SimpleProjection =>
        readWriteRows * SimpleProjectionGeneralMultiplier
      case RefreshTypeCode.SimpleAggregate =>
        readWriteRows * SimpleAggregateMultiplier
      case RefreshTypeCode.AggregateGroup | RefreshTypeCode.AggregateHaving =>
        readWriteRows * AggregateGroupMultiplier
      case RefreshTypeCode.WindowPartition | RefreshTypeCode.GroupRecompute =>
        readWriteRows * partitionRecomputeMultiplier(affectedFraction)
      case RefreshTypeCode.DistinctIncremental =>
        readWriteRows * DistinctIncrementalMultiplier
      case RefreshTypeCode.SemiAntiRecompute =>
        readWriteRows * SemiAntiRecomputeMultiplier
      case _ =>
        readWriteRows * DefaultIncrementalMultiplier
    }
  }

  private def allChangedSourcesInsertOnly(facts: WorkloadFacts): Boolean = {
    val changedShapes = facts.deltaShape.values.filter(_ != DeltaShape.Unchanged).toSeq
    changedShapes.nonEmpty && changedShapes.forall(_ == DeltaShape.InsertOnly)
  }

  private def partitionRecomputeMultiplier(affectedFraction: Double): Double =
    PartitionRecomputeBaseMultiplier + PartitionRecomputeMvPassesAtFullAffected * affectedFraction

  private def shouldRecommendFull(refreshType: Int, incrementalCost: Double, fullRecomputeCost: Double): Boolean =
    refreshType == RefreshTypeCode.FullRefresh || incrementalCost > fullRecomputeCost * FullRefreshHysteresis

  private def rationaleFor(
      refreshType: Int,
      deltaRows: Long,
      sourceRows: Long,
      affectedFraction: Double,
      incrementalCost: Double,
      fullRecomputeCost: Double,
      recommendFullRefresh: Boolean
  ): String = {
    val decision = if (recommendFullRefresh) "full refresh" else "incremental refresh"
    f"$decision: refreshType=$refreshType deltaRows=$deltaRows sourceRows=$sourceRows " +
      f"affectedFraction=$affectedFraction%.6f incrementalCost=$incrementalCost%.2f fullRecomputeCost=$fullRecomputeCost%.2f"
  }

  private def clamp01(value: Double): Double = math.max(0.0d, math.min(1.0d, value))
}
