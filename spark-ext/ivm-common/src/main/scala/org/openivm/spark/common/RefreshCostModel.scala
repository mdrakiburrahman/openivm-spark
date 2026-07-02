package org.openivm.spark.common

final case class RefreshCostEstimate(
    baseRows: Option[Long],
    deltaRows: Option[Long],
    deltaToBaseRatio: Option[Double],
    fullRecomputeRecommended: Boolean,
    reasons: Seq[String]
) {

  def hint: String = {
    val route = if (fullRecomputeRecommended) "FULL_RECOMPUTE" else "INCREMENTAL"
    val parts = Seq(
      Some(s"route=$route"),
      baseRows.map(v => s"base_rows=$v"),
      deltaRows.map(v => s"delta_rows=$v"),
      deltaToBaseRatio.map(v => f"delta_to_base_ratio=$v%.6f"),
      Some(s"reasons=${reasons.mkString("|")}")
    ).flatten
    parts.mkString(";")
  }

  def toJson: String = {
    import WorkloadFacts._

    val fields = Seq(
      baseRows.map(v => s"${q("base_rows")}:$v"),
      deltaRows.map(v => s"${q("delta_rows")}:$v"),
      deltaToBaseRatio.map(v => f"${q("delta_to_base_ratio")}:$v%.6f"),
      Some(s"${q("full_recompute_recommended")}:${bool(fullRecomputeRecommended)}"),
      Some(s"${q("hint")}:${q(hint)}"),
      Some(s"${q("reasons")}:${stringArray(reasons)}")
    ).flatten
    fields.mkString("{", ",", "}")
  }
}

object RefreshCostModel {
  val DefaultFullRecomputeDeltaRatio: Double = 0.5d

  def estimate(facts: WorkloadFacts): RefreshCostEstimate =
    estimate(facts, DefaultFullRecomputeDeltaRatio)

  def estimate(facts: WorkloadFacts, fullRecomputeDeltaRatio: Double): RefreshCostEstimate = {
    val baseRows  = sumKnownRows(facts.tableStats.values.flatMap(_.rowCount))
    val deltaRows = sumKnownRows(facts.deltaStats.values.flatMap(_.rowCount))
    val ratio = for {
      base  <- baseRows if base > 0L
      delta <- deltaRows
    } yield delta.toDouble / base.toDouble

    val recommendsFull = ratio.exists(_ >= fullRecomputeDeltaRatio)
    val reasons = Seq(
      if (baseRows.isDefined) Some("base_row_stats_present") else Some("base_row_stats_missing"),
      if (deltaRows.isDefined) Some("delta_row_stats_present") else Some("delta_row_stats_missing"),
      ratio.map { value =>
        if (value >= fullRecomputeDeltaRatio) "delta_ratio_above_full_recompute_threshold"
        else "delta_ratio_below_full_recompute_threshold"
      }
    ).flatten

    RefreshCostEstimate(
      baseRows = baseRows,
      deltaRows = deltaRows,
      deltaToBaseRatio = ratio,
      fullRecomputeRecommended = recommendsFull,
      reasons = reasons
    )
  }

  private def sumKnownRows(values: Iterable[Long]): Option[Long] = {
    val seq = values.toSeq
    if (seq.nonEmpty) Some(seq.sum) else None
  }
}
