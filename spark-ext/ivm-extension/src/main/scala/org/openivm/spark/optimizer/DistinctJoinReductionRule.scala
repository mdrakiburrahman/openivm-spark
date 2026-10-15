package org.openivm.spark.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.openivm.spark.common.FeatureGate

/** Avoid generating right-match multiplicity that an existing DISTINCT discards.
  * The DISTINCT/group-only aggregate remains authoritative for left duplicates.
  * This is deliberately not a rewrite of ordinary bag projections or aggregates.
  */
final class DistinctJoinReductionRule(session: SparkSession) extends Rule[LogicalPlan] {
  private def safe(e: Expression): Boolean =
    e.deterministic && !e.exists(_.isInstanceOf[SubqueryExpression])

  private def onlyGroupingKeys(aggregate: Aggregate): Boolean =
    aggregate.groupingExpressions.nonEmpty && aggregate.aggregateExpressions.forall { output =>
      val value = output match {
        case alias: Alias => alias.child
        case other        => other
      }
      value.foldable || aggregate.groupingExpressions.exists(_.semanticEquals(value))
    }

  private def reduce(child: LogicalPlan, required: AttributeSet): LogicalPlan = child match {
    case p: Project if p.projectList.forall(safe) =>
      // Keeping all project references is conservative when some outputs are unused.
      p.copy(child = reduce(p.child, p.references))
    case f: Filter if safe(f.condition) =>
      f.copy(child = reduce(f.child, required ++ f.condition.references))
    case a: SubqueryAlias => a.copy(child = reduce(a.child, required))
    case j @ Join(left, _, Inner, Some(condition), hint)
        if hint.leftHint.isEmpty && required.subsetOf(left.outputSet) && safe(condition) &&
          !j.children.exists(_.exists(p => !p.expressions.forall(safe))) =>
      j.copy(joinType = LeftSemi)
    case other => other
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!FeatureGate.enabled(session) || !plan.containsPattern(JOIN) || plan.isStreaming) plan
    else
      plan.transformUpWithPruning(_.containsPattern(JOIN)) {
        case d: Distinct if d.resolved =>
          d.copy(child = reduce(d.child, d.child.outputSet))
        case a: Aggregate if a.resolved && onlyGroupingKeys(a) && a.expressions.forall(safe) =>
          a.copy(child = reduce(a.child, a.references))
      }
}
