package org.openivm.spark.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Exists, Expression, InSubquery, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.openivm.spark.common.FeatureGate

/** Fold positive filter predicates whose source delta is provably empty.
  *
  * Typed empty delta relations expose a zero upper row bound. Folding before
  * Spark rewrites IN into semi joins lets its ordinary filter rules remove
  * unnecessary full-source scans. Global aggregates retain their one-row
  * bound, so COUNT/MAX over an empty input must not be treated as empty.
  * Projected/negated predicates retain Spark's configured NULL-IN-empty
  * behavior. This never probes row counts; the execution-time probe remains
  * opt-in.
  */
final class RuntimeDeltaSizeRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!FeatureGate.enabled(session) || plan.isStreaming) plan
    else {
      def fold(condition: Expression): Expression = condition match {
        case _: And | _: Or => condition.mapChildren(fold)
        case in: InSubquery if !in.query.plan.isStreaming && in.query.plan.maxRows.contains(0L) =>
          Literal(false)
        case exists: Exists if !exists.plan.isStreaming && exists.plan.maxRows.contains(0L) =>
          Literal(false)
        case _ => condition
      }
      plan.transformUp {
        case filter: Filter if filter.resolved =>
          val condition = fold(filter.condition)
          if (condition.fastEquals(filter.condition)) filter else filter.copy(condition = condition)
      }
    }
}
