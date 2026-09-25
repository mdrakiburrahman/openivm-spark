package org.apache.spark.sql.openivm

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset, SparkSession => ClassicSparkSession}

/** Narrow bridge to Spark 4.1's classic Dataset factory. */
object StreamingDatasetAccess {

  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame =
    Dataset.ofRows(spark.asInstanceOf[ClassicSparkSession], plan)
}
