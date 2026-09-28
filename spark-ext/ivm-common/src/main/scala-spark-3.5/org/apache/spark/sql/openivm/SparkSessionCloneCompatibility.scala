package org.apache.spark.sql.openivm

import org.apache.spark.sql.SparkSession

private[openivm] object SparkSessionCloneCompatibility {
  def cloneSession(spark: SparkSession): SparkSession = spark.cloneSession()
}
