package org.openivm.spark.commands

import org.apache.spark.sql.catalyst.expressions.Expression

private[commands] object SparkExpressionCompat {
  def normalizeHavingCondition(expression: Expression): Expression = expression
}
