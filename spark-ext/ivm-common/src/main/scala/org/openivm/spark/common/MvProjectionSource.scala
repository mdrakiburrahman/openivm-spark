package org.openivm.spark.common

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/** Row-local public projections commute with signed changes to their backing
  * Delta table. HAVING/Top-K views are deliberately not marked with this contract.
  */
object MvProjectionSource {
  val CatalogProperty: String = "_ivm_public_projection_v1"

  def isProjection(view: View): Boolean =
    !view.isTempView && view.desc.properties.get(CatalogProperty).contains("true")

  def projection(plan: LogicalPlan): Option[View] = plan match {
    case alias: SubqueryAlias             => projection(alias.child)
    case view: View if isProjection(view) => Some(view)
    case _                                => None
  }

  def isProjection(spark: SparkSession, source: String): Boolean =
    try projection(spark.table(source).queryExecution.analyzed).nonEmpty
    catch { case _: AnalysisException => false }

  def project(raw: DataFrame, schema: StructType, metadataColumns: Seq[String] = Nil): DataFrame = {
    val columns = schema.fields.map { field =>
      val matches = raw.schema.fields.filter(_.name.equalsIgnoreCase(field.name))
      require(matches.length == 1, s"Public MV column '${field.name}' did not resolve uniquely on backing state")
      val physical = matches.head
      val value    = col(quote(physical.name))
      val typed    = if (physical.dataType == field.dataType) value else value.cast(field.dataType)
      typed.as(field.name, field.metadata)
    }
    raw.select((columns.toSeq ++ metadataColumns.map(name => col(quote(name)))): _*)
  }

  /** Delta's DataFrameReader cannot time-travel or read CDF through a Spark VIEW.
    * Resolve the same backing log, then reapply the persisted public schema.
    * This reads catalog/Delta metadata only; it never opens OpenIVM state.
    */
  def read(spark: SparkSession, source: String, options: Map[String, String]): DataFrame = {
    val logical = spark.table(source)
    projection(logical.queryExecution.analyzed) match {
      case Some(_) =>
        val log = DeltaTableVersion.deltaLogOption(spark, source).getOrElse {
          throw new IllegalStateException(s"Public MV projection '$source' has no committed Delta backing")
        }
        val raw = spark.read.format("delta").options(options).load(log.dataPath.toString)
        val metadata =
          if (options.get("readChangeFeed").exists(_.equalsIgnoreCase("true")))
            Seq("_change_type", "_commit_version", "_commit_timestamp")
          else Nil
        project(raw, logical.schema, metadata)
      case None =>
        spark.read.format("delta").options(options).table(source)
    }
  }

  private def quote(name: String): String = s"`${name.replace("`", "``")}`"
}
