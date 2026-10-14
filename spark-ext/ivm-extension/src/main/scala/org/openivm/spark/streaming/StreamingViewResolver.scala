package org.openivm.spark.streaming

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnresolvedWith, View}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.jdk.CollectionConverters._

/** Expands only explicit STREAM view boundaries, using Spark's stored-view resolution. */
private[streaming] object StreamingViewResolver {

  /** Resolve view namespaces and output casts before replacing their Delta leaves.
    *
    * @param spark caller's catalog and SQL configuration
    * @param plan parsed query, including relation-local reader options
    */
  def expand(spark: SparkSession, plan: LogicalPlan): LogicalPlan =
    expandPlan(spark, plan)

  private def expandPlan(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    val children = plan.mapChildren(child => expandPlan(spark, child))
    val withCtes = children match {
      case unresolved: UnresolvedWith =>
        val rebound = org.openivm.spark.parser.SparkParserCompat.rebindCtes(
          unresolved,
          child => expandPlan(spark, child)
        )
        rebound.copyTagsFrom(unresolved)
        rebound
      case other => other
    }
    val withSubqueries = withCtes.transformExpressionsUp { case expression: SubqueryExpression =>
      expression.withNewPlan(expandPlan(spark, expression.plan))
    }
    withSubqueries match {
      case relation: UnresolvedRelation if relation.isStreaming => resolveBoundary(spark, relation)
      case other                                                => other
    }
  }

  private def resolveBoundary(spark: SparkSession, relation: UnresolvedRelation): LogicalPlan = {
    val batch =
      try
        spark
          .sql(s"SELECT * FROM ${StreamingTableMetadata.quoteMultipart(relation.multipartIdentifier)}")
          .queryExecution
          .analyzed
      catch {
        // Missing physical tables retain the existing catalog/Fabric resolution path.
        case error: AnalysisException if Option(error.getMessage).exists(_.contains("[TABLE_OR_VIEW_NOT_FOUND]")) =>
          return relation
      }
    val containsView = batch.exists(_.isInstanceOf[View])
    val catalogView = spark.catalog
      .getTable(StreamingTableMetadata.quoteMultipart(relation.multipartIdentifier))
      .tableType
      .equalsIgnoreCase("VIEW")
    if (!containsView && !catalogView) return relation

    val name = relation.multipartIdentifier.mkString(".")
    if (batch.exists(_.expressions.exists(SubqueryExpression.hasSubquery)))
      StreamingTableErrors.invalid(s"STREAM view $name does not support subquery expressions")
    val options = relation.options.asCaseSensitiveMap().asScala.toMap
    batch.transformUp {
      case view: View => view.child
      case leaf: LogicalRelation =>
        leaf.relation match {
          case files: HadoopFsRelation =>
            files.location match {
              case delta: TahoeFileIndex =>
                delta match {
                  case log: TahoeLogFileIndex if log.versionToUse.nonEmpty =>
                    StreamingTableErrors.invalid(s"STREAM view $name does not support Delta time-travel leaves")
                  case _ =>
                }
                if (delta.path != delta.deltaLog.dataPath)
                  StreamingTableErrors.invalid(s"STREAM view $name does not support partial Delta path leaves")
                streamingLeaf(
                  spark,
                  leaf,
                  delta.deltaLog.dataPath.toString,
                  leaf.catalogTable.map(_.identifier.quotedString),
                  options,
                  name
                )
              case _ => unsupported(name, leaf)
            }
          case _ => unsupported(name, leaf)
        }
      case leaf: DataSourceV2Relation =>
        leaf.table match {
          case delta: DeltaTableV2 =>
            if (delta.timeTravelOpt.nonEmpty)
              StreamingTableErrors.invalid(s"STREAM view $name does not support Delta time-travel leaves")
            if (delta.path != delta.deltaLog.dataPath)
              StreamingTableErrors.invalid(s"STREAM view $name does not support partial Delta path leaves")
            val tableName = for {
              catalog    <- leaf.catalog
              identifier <- leaf.identifier
            } yield (Seq(catalog.name()) ++ identifier.namespace().toSeq :+ identifier.name())
              .map(part => s"`${part.replace("`", "``")}`")
              .mkString(".")
            streamingLeaf(
              spark,
              leaf,
              delta.deltaLog.dataPath.toString,
              tableName.orElse(delta.catalogTable.map(_.identifier.quotedString)),
              options,
              name
            )
          case _ => unsupported(name, leaf)
        }
      case leaf if leaf.children.isEmpty => unsupported(name, leaf)
    }
  }

  private def streamingLeaf(
      spark: SparkSession,
      batch: LogicalPlan,
      path: String,
      tableName: Option[String],
      options: Map[String, String],
      viewName: String
  ): LogicalPlan = {
    val reader = spark.readStream.format("delta").options(options)
    val stream = tableName.map(reader.table).getOrElse(reader.load(path)).queryExecution.analyzed
    if (
      stream.output.size != batch.output.size ||
      stream.output.zip(batch.output).exists { case (actual, expected) =>
        actual.name != expected.name || actual.dataType != expected.dataType
      }
    )
      StreamingTableErrors.invalid(
        s"STREAM view $viewName reader options change the underlying Delta schema; " +
          "the stored view schema must match the streaming source"
      )
    // Keep the resolved view's references, qualifiers, casts and branch occurrence order.
    Project(
      stream.output.zip(batch.output).map { case (actual, expected) =>
        Alias(actual, expected.name)(
          exprId = expected.exprId,
          qualifier = expected.qualifier,
          explicitMetadata = Some(expected.metadata)
        )
      },
      stream
    )
  }

  private def unsupported(viewName: String, leaf: LogicalPlan): Nothing =
    StreamingTableErrors.invalid(
      s"STREAM view $viewName requires physical Delta leaves; unsupported/non-Delta leaf ${leaf.nodeName}"
    )
}
