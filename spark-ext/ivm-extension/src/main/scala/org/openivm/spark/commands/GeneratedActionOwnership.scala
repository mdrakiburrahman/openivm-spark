package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession

private[commands] object GeneratedActionOwnership {
  val Property = "openivm.generated.action"

  // JobStart inherits ownership, not a classification: Delta may submit log/state jobs here too.
  def withBoundary[A](spark: SparkSession, owner: Option[String], boundary: String)(run: => A): A =
    owner match {
      case None => run
      case Some(value) =>
        val context = spark.sparkContext
        val previous = context.getLocalProperty(Property)
        context.setLocalProperty(Property, s"$value;boundary=$boundary")
        try run
        finally context.setLocalProperty(Property, previous)
    }
}
