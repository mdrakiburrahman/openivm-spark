package org.openivm.spark.common

/** RefreshType enum values mirrored from openivm/src/include/core/openivm_constants.hpp.
  * Matches the int values emitted by `PRAGMA compile_refresh`.
  */
object RefreshTypeCode {
  val AggregateGroup   = 0
  val SimpleAggregate  = 1
  val SimpleProjection = 2
  val FullRefresh      = 3
  val AggregateHaving  = 4
  val WindowPartition  = 5
  val GroupRecompute   = 6
  // TopK (7) is a dead enum value — the classifier never assigns it. OpenIVM strips ORDER BY/LIMIT
  // at CREATE time; the inner query rides on AggregateGroup or SimpleProjection instead.
  val TopK                = 7
  val DistinctIncremental = 8
  val SemiAntiRecompute   = 9

  /** True for refresh types whose openivm-emitted refresh program writes an
    * `INSERT INTO openivm_delta_<view>` statement carrying a USABLE signed
    * multiset delta — i.e. a persisted view-delta that downstream MVs can
    * consume as their source delta for MV-over-MV cascade refresh.
    *
    * Today this is restricted to **AGGREGATE_GROUP and AGGREGATE_HAVING** with
    * `openivm_force_view_delta_cascade=true`, which adds the per-key
    * retract+add companion in openivm (`refresh_sql.cpp`). For these types
    * the view-delta carries the SIGNED multiset semantics downstream
    * COUNT(*)/DISTINCT cascade requires.
    *
    * SIMPLE_AGGREGATE (1) is intentionally EXCLUDED. The openivm compile-only
    * mode does not emit a snapshot companion (refresh_sql.cpp build_snapshot_companion)
    * unless the registered downstream metadata is non-empty — and openivm-spark
    * compiles each MV in an isolated DuckDB subprocess where that metadata is
    * always absent. The MV_VIEW_DELTA we'd persist would carry only the +1
    * add rows, missing the -1 retract for the pre-refresh scalar value (e.g.
    * the initial-CTAS NULL row). Downstream SIMPLE_PROJECTION cascade over a
    * SIMPLE_AGGREGATE upstream would therefore double-emit (initial NULL +
    * new value both present in the downstream MV). The right fix requires
    * also rewriting the snapshot pre/post-companion on the Spark side, which
    * is out of scope; for now SIMPLE_AGGREGATE upstreams force downstream
    * FullRefresh-demotion.
    *
    * The complement set — SIMPLE_AGGREGATE, WINDOW_PARTITION, GROUP_RECOMPUTE,
    * DISTINCT_INCREMENTAL, SEMI_ANTI_RECOMPUTE, TOP_K, FULL_REFRESH — does
    * NOT emit a cascade-usable view-delta. Downstream MVs over a
    * non-cascade-delta-capable upstream MUST be FullRefresh-demoted at CREATE
    * time.
    *
    * Matches the switch in `SparkRefreshRewriter.rewrite` (lines ~115-126)
    * combined with the Phase 2.1 demotion rule in
    * `CreateMaterializedViewCommand`.
    */
  def emitsCascadeViewDelta(rt: Int): Boolean = rt match {
    case AggregateGroup | AggregateHaving => true
    case _                                => false
  }
}
