package org.openivm.spark.streaming

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation

private[streaming] object StreamingPlanCompatibility {

  def extract(plan: LogicalPlan): Option[CompatibleStreamingPlan] = plan match {
    case relation: StreamingRelation =>
      Some(CompatibleV1StreamingRelation(relation.dataSource, relation.sourceName, relation.output))
    case relation: StreamingDataSourceV2Relation =>
      Some(
        CompatibleStreamingDataSource(
          relation.stream,
          relation.output,
          relation.identifier.map { identifier =>
            relation.catalog.map(_.name()).toSeq ++ identifier.namespace().toSeq :+ identifier.name()
          },
          relation.identifier
        )
      )
    case LogicalRelation(relation: HadoopFsRelation, output, catalogTable, isStreaming) =>
      Some(CompatibleV1Relation(relation, output, catalogTable.map(_.identifier.nameParts), isStreaming))
    case _ => None
  }
}
