package org.openivm.spark.commands

import org.apache.spark.SparkThrowable

private[commands] object SparkCommandCompat {
  def errorCondition(error: SparkThrowable): Option[String] =
    Option(error.getErrorClass)
}
