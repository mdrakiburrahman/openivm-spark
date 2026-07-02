package org.openivm.spark.common

sealed abstract class RefreshDecisionRoute(val name: String)
object RefreshDecisionRoute {
  case object Incremental   extends RefreshDecisionRoute("INCREMENTAL")
  case object FullRecompute extends RefreshDecisionRoute("FULL_RECOMPUTE")
  case object Skip          extends RefreshDecisionRoute("SKIP")
}

final case class RuntimeDeltaSize(rowsBySource: Map[String, Long]) {
  def totalRows: Long  = rowsBySource.values.sum
  def isEmpty: Boolean = rowsBySource.nonEmpty && totalRows == 0L

  def toJson: String = {
    import WorkloadFacts._

    val sources = rowsBySource.toSeq
      .sortBy(_._1)
      .map { case (source, rows) => s"${q(source)}:$rows" }
      .mkString("{", ",", "}")
    s"{${q("total_rows")}:$totalRows,${q("sources")}:$sources}"
  }
}

final case class RefreshDecision(
    route: RefreshDecisionRoute,
    reasons: Seq[String],
    costEstimate: RefreshCostEstimate,
    runtimeDeltaSize: Option[RuntimeDeltaSize]
) {

  def toJson: String = {
    import WorkloadFacts._

    val runtimeJson = runtimeDeltaSize.map(_.toJson).getOrElse("null")
    val fields = Seq(
      s"${q("schema_version")}:1",
      s"${q("route")}:${q(route.name)}",
      s"${q("reasons")}:${stringArray(reasons)}",
      s"${q("cost_estimate")}:${costEstimate.toJson}",
      s"${q("runtime_delta_size")}:$runtimeJson"
    )
    fields.mkString("{", ",", "}")
  }
}

object RefreshIntelligence {
  def decide(
      facts: WorkloadFacts,
      costEstimate: RefreshCostEstimate,
      runtimeDeltaSize: Option[RuntimeDeltaSize]
  ): RefreshDecision = {
    val factReasons = Seq(
      if (facts.tableStats.nonEmpty) "table_stats_present" else "table_stats_missing",
      if (facts.deltaStats.nonEmpty) "delta_stats_present" else "delta_stats_missing"
    )
    val runtimeReasons = runtimeDeltaSize.toSeq.flatMap { size =>
      Seq(
        Some("runtime_delta_size_present"),
        if (size.isEmpty) Some("runtime_delta_empty") else Some("runtime_delta_non_empty")
      ).flatten
    }
    val route =
      if (runtimeDeltaSize.exists(_.isEmpty)) RefreshDecisionRoute.Skip
      else if (costEstimate.fullRecomputeRecommended) RefreshDecisionRoute.FullRecompute
      else RefreshDecisionRoute.Incremental
    val routeReason = route match {
      case RefreshDecisionRoute.Skip          => "runtime_delta_empty_wins"
      case RefreshDecisionRoute.FullRecompute => "compile_cost_model_prefers_full_recompute"
      case RefreshDecisionRoute.Incremental   => "compile_cost_model_prefers_incremental"
    }
    RefreshDecision(
      route,
      factReasons ++ (runtimeReasons :+ routeReason) ++ costEstimate.reasons,
      costEstimate,
      runtimeDeltaSize
    )
  }
}
