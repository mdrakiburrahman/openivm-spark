package org.openivm.spark.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, Exists, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{JOIN, PLAN_EXPRESSION}
import org.openivm.spark.common.FeatureGate

/** Avoid generating right-match multiplicity that DISTINCT or membership discards.
  * DISTINCT/group-only aggregates remain authoritative for left duplicates;
  * semi/anti joins are insensitive to duplicates on their right side.
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
      f.copy(child = reduce(f.child, (required ++ f.condition.references).intersect(f.child.outputSet)))
    case a: SubqueryAlias => a.copy(child = reduce(a.child, required))
    case j @ Join(left, _, Inner, Some(condition), hint)
        if hint.leftHint.isEmpty && required.subsetOf(left.outputSet) && safe(condition) &&
          !j.children.exists(_.exists(p => !p.expressions.forall(safe))) =>
      j.copy(joinType = LeftSemi)
    case other => other
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (
      !FeatureGate.enabled(session) ||
      (!plan.containsPattern(JOIN) && !plan.containsPattern(PLAN_EXPRESSION)) || plan.isStreaming
    ) plan
    else
      plan
        .transformUpWithPruning(_.containsPattern(JOIN)) {
          case d: Distinct if d.resolved =>
            d.copy(child = reduce(d.child, d.child.outputSet))
          case a: Aggregate if a.resolved && onlyGroupingKeys(a) && a.expressions.forall(safe) =>
            a.copy(child = reduce(a.child, a.references))
          case j @ Join(_, right, LeftSemi | LeftAnti, Some(condition), _) if j.resolved && safe(condition) =>
            // Direct EXISTS plans need no explicit DISTINCT wrapper. Only the
            // right-side correlation fields affect whether a match exists.
            j.copy(right = reduce(right, condition.references.intersect(right.outputSet)))
        }
        .transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          // Spark rewrites predicate subqueries after extension optimizer rules.
          // Keep the existence proof while it is still explicit in the plan.
          case e: Exists if e.resolved && e.deterministic =>
            e.withNewPlan(reduce(e.plan, e.plan.outputSet))
        }
}
