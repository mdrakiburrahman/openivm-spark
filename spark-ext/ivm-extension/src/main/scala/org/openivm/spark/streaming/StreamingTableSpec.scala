package org.openivm.spark.streaming

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

final case class StreamingTableSpec(
    name: Seq[String],
    queryText: String,
    query: LogicalPlan,
    provider: Option[String] = None,
    location: Option[String] = None,
    partitionColumns: Seq[String] = Seq.empty,
    tableProperties: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty,
    ifNotExists: Boolean = false
)
