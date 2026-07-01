package org.openivm.spark.common

import org.apache.spark.sql.SparkSession

/** openivm-workload-aware Delta table layout, calibrated per [[RefreshTypeCode]]
  * (issue #13, Proposal A).
  *
  * Given the refresh type and the columns that type's refresh SQL actually probes
  * (rowid-equality key for SIMPLE_PROJECTION, GROUP BY key for the aggregate
  * families, window PARTITION BY key for WINDOW_PARTITION, recompute key for the
  * recompute families), this returns the `CLUSTER BY` columns and any extra
  * `TBLPROPERTIES` to attach to the MV data table at CREATE time.
  *
  * Every knob is default-OFF: with the pre-issue-#13 configuration [[resolve]]
  * returns [[Empty]] (no clustering, no extra properties) for every type EXCEPT
  * the legacy `windowClusterPrune` path, which is preserved for back-compat so
  * the CREATE DDL is byte-identical to the baseline.
  *
  * The lesson from W7.2 (see docs/todos/compile-facts-todos.md §1.5): inline
  * clustering fires + is correct but loses on wall-clock because the Delta
  * clustering write/commit maintenance eats the read-pruning saving. These layout
  * levers therefore only pay off paired with the out-of-band maintenance daemon
  * (Proposal B) and must be proven on SF10 wall before flipping default-ON.
  */
object MvLayoutPolicy {

  /** Resolved layout snapshot of the layout flags (decoupled from `SparkSession`
    * so [[resolve]] is a pure function and unit-testable without Spark).
    */
  final case class Config(
      simpleProjection: Boolean,
      aggregateGroup: Boolean,
      window: Boolean,
      recompute: Boolean,
      legacyWindowClusterPrune: Boolean,
      fullRefreshOptimizeWrite: Boolean,
      dataSkippingStatsColumns: Boolean,
      checkpointInterval: Option[Int]
  )

  /** The physical layout for one MV data table.
    *
    * @param clusterColumns      raw (unquoted) column names for `CLUSTER BY`; the
    *                            caller is responsible for identifier quoting.
    * @param extraTblProperties  ready-to-embed `'key' = 'value'` property strings,
    *                            appended to [[FeatureGate.buildMvDataTblProperties]].
    */
  final case class Layout(clusterColumns: Seq[String], extraTblProperties: Seq[String])

  val Empty: Layout = Layout(Nil, Nil)

  def fromSession(spark: SparkSession): Config = Config(
    simpleProjection = FeatureGate.layoutSimpleProjectionEnabled(spark),
    aggregateGroup = FeatureGate.layoutAggregateGroupEnabled(spark),
    window = FeatureGate.layoutWindowEnabled(spark),
    recompute = FeatureGate.layoutRecomputeEnabled(spark),
    legacyWindowClusterPrune = FeatureGate.windowClusterPruneEnabled(spark),
    fullRefreshOptimizeWrite = FeatureGate.layoutFullRefreshOptimizeWriteEnabled(spark),
    dataSkippingStatsColumns = FeatureGate.deltaDataSkippingStatsColumnsEnabled(spark),
    checkpointInterval = FeatureGate.deltaCheckpointInterval(spark)
  )

  /** Whether the per-type `CLUSTER BY` lever is enabled for `refreshType`. */
  private def clusteringEnabled(cfg: Config, refreshType: Int): Boolean = refreshType match {
    case RefreshTypeCode.SimpleProjection                                 => cfg.simpleProjection
    case RefreshTypeCode.AggregateGroup | RefreshTypeCode.AggregateHaving => cfg.aggregateGroup
    case RefreshTypeCode.WindowPartition                                  => cfg.window || cfg.legacyWindowClusterPrune
    case RefreshTypeCode.GroupRecompute | RefreshTypeCode.DistinctIncremental | RefreshTypeCode.SemiAntiRecompute =>
      cfg.recompute
    case _ => false
  }

  def resolve(cfg: Config, refreshType: Int, probeKeys: Seq[String]): Layout = {
    val keys = probeKeys.filter(_.nonEmpty).distinct
    // Delta liquid clustering supports at most 4 clustering columns.
    val cluster = if (clusteringEnabled(cfg, refreshType)) keys.take(4) else Nil

    val props = scala.collection.mutable.ArrayBuffer.empty[String]
    // Index data-skipping stats on exactly the probe key(s) so min/max file
    // pruning is cheap + targeted, vs Delta's default first-N-column stats.
    if (cfg.dataSkippingStatsColumns && keys.nonEmpty)
      props += s"'delta.dataSkippingStatsColumns' = '${keys.take(4).mkString(",")}'"
    cfg.checkpointInterval.foreach(iv => props += s"'delta.checkpointInterval' = '$iv'")
    // FULL_REFRESH / SIMPLE_AGGREGATE rewrite the whole table each batch; optimized
    // writes coalesce output to avoid a tiny-file explosion (DA.5).
    if (
      cfg.fullRefreshOptimizeWrite &&
      (refreshType == RefreshTypeCode.FullRefresh || refreshType == RefreshTypeCode.SimpleAggregate)
    )
      props += "'delta.autoOptimize.optimizeWrite' = 'true'"

    Layout(cluster, props.toSeq)
  }

  def resolve(spark: SparkSession, refreshType: Int, probeKeys: Seq[String]): Layout =
    resolve(fromSession(spark), refreshType, probeKeys)
}
