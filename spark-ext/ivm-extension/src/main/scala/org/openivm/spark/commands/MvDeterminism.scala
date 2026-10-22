package org.openivm.spark.commands

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Nondeterministic, SubqueryExpression, WindowFunction}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.util.Try

/** Finds the expressions that make a view's rows depend on when or how often its query runs rather
  * than only on its source rows. Incremental maintenance evaluates such an expression only for the
  * rows, groups or partitions a refresh touches, so the view drifts away from a recompute.
  */
private[commands] object MvDeterminism {

  /** Spark treats these as deterministic within one query, but their value moves with the clock or
    * the session time zone between refreshes.
    */
  private val TimeDependentExpressions: Set[String] = Set(
    "CurrentTimestamp",
    "Now",
    "CurrentDate",
    "CurrentTime",
    "LocalTimestamp",
    "CurrentTimeZone",
    "CurrentBatchTimestamp"
  )

  private val BuiltInAggregatePackage = "org.apache.spark.sql.catalyst.expressions.aggregate."

  /** Random or time-dependent expressions of `plan`, subqueries included, in plan order.
    *
    * Window functions and Spark's built-in aggregates (`first`, `last`, `collect_list`, …) are not
    * reported: they depend only on row order, and maintenance recomputes each affected group or
    * partition from its source rows.
    *
    * @param plan analyzed query of the view
    */
  def varyingExpressions(plan: LogicalPlan): Seq[Expression] =
    plan
      .collectWithSubqueries { case node =>
        node.expressions.flatMap(_.collect { case e if introducesVariance(e) => e })
      }
      .flatten
      .distinct

  /** `name (sql)` rendering of `expression` for error messages.
    *
    * @param expression expression to describe
    */
  def describe(expression: Expression): String = {
    val sql = Try(expression.sql).getOrElse(expression.toString)
    s"${expression.prettyName} ($sql)"
  }

  private def introducesVariance(expression: Expression): Boolean =
    expression match {
      case _: SubqueryExpression                                                                          => false
      case _: WindowFunction                                                                              => false
      case aggregate: AggregateFunction if aggregate.getClass.getName.startsWith(BuiltInAggregatePackage) => false
      case _: Nondeterministic                                                                            => true
      case other if TimeDependentExpressions.contains(other.getClass.getSimpleName)                       => true
      case other => !other.deterministic && other.children.forall(_.deterministic)
    }
}
