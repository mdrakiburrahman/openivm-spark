package org.openivm.spark.parser

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedWith}

/** Generic Catalyst plan rewriter that recurses into CTE definitions
  * (`UnresolvedWith`, whose `cteRelations` live outside the ordinary
  * `children` traversed by `transformUp`/`mapChildren`) and into subquery
  * expression plans (`SubqueryExpression`), applying `rewriteCurrent` at each
  * node only after its children, CTEs, and subqueries have already been
  * rewritten.
  *
  * Plain `plan.transformUp { ... }` silently skips streaming relations that
  * are nested inside a CTE or a scalar/relational subquery, because those
  * plans are not reachable through `children`. This helper is shared between
  * [[StreamingQuerySql]] (source/watermark marker binding) and
  * `org.openivm.spark.streaming.StreamingTableDefinition` (symbolic
  * `startingVersion` resolution, see issue #63 / PR #64 follow-up) so both
  * walk CTEs/subqueries identically rather than maintaining divergent
  * traversals.
  */
private[spark] object CteAwarePlanRewriter {

  def rewrite(plan: LogicalPlan)(rewriteCurrent: LogicalPlan => LogicalPlan): LogicalPlan = {
    def go(current: LogicalPlan): LogicalPlan = {
      val withChildren = current.mapChildren(go)
      val withCtes = withChildren match {
        case unresolved: UnresolvedWith =>
          val rebound = SparkParserCompat.rebindCtes(unresolved, go)
          rebound.copyTagsFrom(unresolved)
          rebound
        case other => other
      }
      val withSubqueries = withCtes.transformExpressionsUp { case expression: SubqueryExpression =>
        expression.withNewPlan(go(expression.plan))
      }
      rewriteCurrent(withSubqueries)
    }
    go(plan)
  }
}
