package org.openivm.spark.commands

import org.apache.spark.sql.catalyst.analysis.TempResolvedColumn
import org.apache.spark.sql.catalyst.expressions.{And, Between, Expression, GreaterThanOrEqual, LessThanOrEqual}

private[commands] object SparkExpressionCompat {
  def normalizeHavingCondition(expression: Expression): Expression =
    expression.transformDown {
      case between: Between =>
        And(
          GreaterThanOrEqual(between.input, between.lower),
          LessThanOrEqual(between.input, between.upper)
        )
      case temp: TempResolvedColumn => temp.child
    }
}
