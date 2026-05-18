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
}
