package org.openivm.spark.common

/** RefreshType enum values mirrored from openivm/src/include/core/openivm_constants.hpp.
  * Matches the int values emitted by `openivm_compile_with_facts`.
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

  /** True for refresh types whose compiled SQL shape CAN write an
    * `INSERT INTO openivm_delta_<view>` statement carrying a USABLE signed
    * multiset delta — i.e. a persisted view-delta that downstream MVs can
    * consume as their source delta for MV-over-MV cascade refresh.
    *
    * This is a coarse refresh-type capability only. Persisted MVs should use
    * `MvMetadata.emitsCascadeViewDelta`, which further gates WINDOW_PARTITION /
    * GROUP_RECOMPUTE (and any future dynamic cases) on whether the specific
    * compiled SQL actually emitted a real view-delta.
    *
    * Enabled for:
    *   - **AGGREGATE_GROUP / AGGREGATE_HAVING** (with
    *     `force_view_delta_cascade=true`): openivm adds the per-key
    *     retract+add companions in `refresh_sql.cpp` so the view-delta carries
    *     SIGNED multiset semantics. Downstream COUNT(*)/DISTINCT cascade works.
    *   - **SIMPLE_PROJECTION**: openivm's `GenerateRefreshSQL` always
    *     prepends a `delta_query` block that INSERTs into
    *     `openivm_delta_<view>`. The Spark side rewrites this into a CTAS
    *     into the per-refresh view-delta path. Downstream SIMPLE_PROJECTION
    *     / AGGREGATE_GROUP cascade picks up the +1/-1 rows directly.
    *   - **WINDOW_PARTITION / GROUP_RECOMPUTE** (with
    *     `force_view_delta_cascade=true`, which is what openivm-spark always
    *     sets in its CompileFacts payload): openivm
    *     snapshots the affected pre-refresh rows plus the recomputed
    *     post-refresh rows before mutating `openivm_data_<view>`, then appends
    *     them as `-1/+1` rows into `openivm_delta_<view>`. The old
    *     `WINDOW_PARTITION / GROUP_RECOMPUTE do not cascade` explanation is now
    *     obsolete once `force_view_delta_cascade=true` is set in CompileFacts.
    *   - **SIMPLE_AGGREGATE**: Spark pins the pre-refresh Delta version and,
    *     after the scalar update succeeds, materializes the exact old/new
    *     snapshot pair as `-1/+1`. This does not depend on isolated DuckDB
    *     compilation knowing the downstream catalog.
    *
    * The remaining complement set — DISTINCT_INCREMENTAL,
    * SEMI_ANTI_RECOMPUTE, TOP_K, FULL_REFRESH — does NOT
    * emit a cascade-usable view-delta. Downstream MVs over a
    * non-cascade-delta-capable upstream MUST be FullRefresh-demoted at CREATE
    * time.
    *
    * Matches the switch in `SparkRefreshRewriter.rewrite` (lines ~115-126)
    * combined with the Phase 2.1 demotion rule in
    * `CreateMaterializedViewCommand`.
    */
  def emitsCascadeViewDelta(rt: Int): Boolean = rt match {
    case AggregateGroup | AggregateHaving | SimpleAggregate | SimpleProjection | WindowPartition | GroupRecompute =>
      true
    case _ => false
  }
}
