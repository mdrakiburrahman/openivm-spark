package org.apache.spark.sql.openivm

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.jdk.CollectionConverters._

/** Narrow bridge to Spark's package-private Dataset constructor.
  *
  * Streaming table commands must execute the plan supplied by the caller's
  * session. In particular, this deliberately does not clone or otherwise
  * replace that session.
  */
object StreamingDatasetAccess {

  private val TablesRoot = """(?i)(abfss://[^\s,;"']+?/Tables)(?:[/\s,;"']|$)""".r
  private val LakehouseRoot =
    """(?i)(abfss://[^\s,;"']+?/[^/\s,;"']+)/(?:Files|Tables)(?:[/\s,;"']|$)""".r

  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    try {
      val frame = Dataset.ofRows(spark, plan)
      frame.queryExecution.analyzed
      frame
    } catch {
      case original: AnalysisException if isMissingRelation(original) =>
        fabricLakehouseTablesRoot(spark) match {
          case Some(root) =>
            val rebound = plan.transformDown {
              case relation: UnresolvedRelation if relation.isStreaming && relation.multipartIdentifier.size == 2 =>
                val relative =
                  relation.multipartIdentifier
                    .map(segment => new Path(segment))
                    .reduce((left, right) => new Path(left, right))
                spark.readStream
                  .format("delta")
                  .options(relation.options.asCaseSensitiveMap().asScala.toMap)
                  .load(new Path(root, relative).toString)
                  .queryExecution
                  .logical
            }
            val reboundFrame = Dataset.ofRows(spark, rebound)
            reboundFrame.queryExecution.analyzed
            reboundFrame
          case None => throw original
        }
    }
  }

  private def isMissingRelation(error: AnalysisException): Boolean =
    Option(error.getMessage).exists(_.contains("[TABLE_OR_VIEW_NOT_FOUND]"))

  private def fabricLakehouseTablesRoot(spark: SparkSession): Option[Path] = {
    val explicit = Seq(
      spark.conf.getOption("spark.openivm.managedTablesRoot"),
      spark.conf.getOption("spark.sql.warehouse.dir")
    ).flatten
    val sqlConf    = spark.sessionState.conf.getAllConfs.values
    val hadoopConf = spark.sparkContext.hadoopConfiguration.iterator().asScala.map(_.getValue)
    val candidates = (explicit.iterator ++ sqlConf.iterator ++ hadoopConf).map(_.stripSuffix("/")).toSeq
    candidates.iterator
      .flatMap(candidate => TablesRoot.findFirstMatchIn(candidate).map(found => new Path(found.group(1))))
      .toSeq
      .headOption
      .orElse(
        candidates.iterator
          .flatMap(candidate =>
            LakehouseRoot.findFirstMatchIn(candidate).map(found => new Path(found.group(1) + "/Tables"))
          )
          .toSeq
          .headOption
      )
  }
}
