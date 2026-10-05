package org.openivm.spark.streaming

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming.runtime.StreamingRelation

import scala.jdk.CollectionConverters._

private[streaming] object StreamingPlanCompatibility {

  def extract(plan: LogicalPlan): Option[CompatibleStreamingPlan] = plan match {
    case relation: StreamingRelation =>
      Some(CompatibleV1StreamingRelation(relation.dataSource, relation.sourceName, relation.output))
    case relation: StreamingDataSourceV2Relation =>
      Some(
        CompatibleStreamingTable(
          relation.table,
          relation.output,
          relation.options.asCaseSensitiveMap().asScala.toSeq,
          relation.identifier,
          relation.catalog.map(_.name()),
          relation.table.name()
        )
      )
    case LogicalRelation(relation: HadoopFsRelation, output, catalogTable, isStreaming, stream) =>
      stream
        .map(value => CompatibleStreamingDataSource(value, output, catalogTable.map(_.identifier.nameParts), None))
        .orElse(
          Some(CompatibleV1Relation(relation, output, catalogTable.map(_.identifier.nameParts), isStreaming))
        )
    case _ => None
  }

  /** Rewrites a V1 `StreamingRelation`'s `DataSource.options` using `rewrite`,
    * returning `None` for any other plan node. `StreamingRelation` lives in a
    * version-specific package, so this reconstruction must stay alongside
    * [[extract]] rather than in shared, version-agnostic code.
    */
  def rewriteV1StreamingRelationOptions(plan: LogicalPlan)(
      rewrite: Map[String, String] => Map[String, String]
  ): Option[LogicalPlan] = plan match {
    case relation: StreamingRelation =>
      Some(relation.copy(dataSource = relation.dataSource.copy(options = rewrite(relation.dataSource.options))))
    case _ => None
  }
}
