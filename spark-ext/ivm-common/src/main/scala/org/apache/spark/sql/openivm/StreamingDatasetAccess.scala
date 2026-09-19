package org.apache.spark.sql.openivm

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Narrow bridge to Spark's package-private Dataset constructor.
  *
  * Streaming table commands must execute the plan supplied by the caller's
  * session. In particular, this deliberately does not clone or otherwise
  * replace that session.
  */
object StreamingDatasetAccess {

  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame =
    Dataset.ofRows(spark, plan)
}
