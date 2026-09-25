package org.openivm.spark.commands

import org.apache.spark.sql.AnalysisException

private[commands] object SparkCommandCompat {
  def errorCondition(error: AnalysisException): Option[String] =
    Option(error.getCondition)
}
