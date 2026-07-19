package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.openivm.spark.common.RefreshTypeCode

/**
 * `EXPLAIN CREATE MATERIALIZED VIEW <name> [CLUSTER BY (...)] AS <query>` (#4).
 *
 * A dry-run eligibility verdict modelled on Databricks'
 * `EXPLAIN MATERIALIZED VIEW`. It compiles + classifies the view exactly as a
 * real CREATE would (via [[MvDryCompile]], which shares
 * [[MvCommandHelper.classifyEffectiveRefreshType]]) but materialises NOTHING —
 * no MV, no staging, no query-log rows. The single `explain` STRING column
 * holds one JSON object describing the incremental-maintenance verdict.
 *
 * The view's empty output schema is registered in [[DryRunMvRegistry]] so a
 * later `EXPLAIN`/`SHOW REFRESH SQL` for a downstream MV in the same session
 * (a dbt DAG fired in dependency order) can resolve this one as a source.
 */
case class ExplainCreateMaterializedViewCommand(
    name: TableIdentifier,
    queryText: String,
    clusterColumns: Seq[String] = Seq.empty
) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("explain", StringType, nullable = false)()
  )

  override def run(spark: SparkSession): Seq[Row] = {
    val result         = MvDryCompile.dryCompile(spark, name, queryText, clusterColumns)
    val classification = result.classification
    val eligible       = classification.refreshType != RefreshTypeCode.FullRefresh

    // Register the empty schema-only stand-in so downstream dry compiles resolve.
    DryRunMvRegistry.register(spark, name, result.outputSchema)

    val json =
      s"""{"view":${ExplainCreateMaterializedViewCommand.jsonStr(MvCommandHelper.metaName(name))},""" +
        s""""eligible":$eligible,""" +
        s""""refresh_type":${classification.refreshType},""" +
        s""""refresh_type_name":${ExplainCreateMaterializedViewCommand.jsonStr(classification.refreshTypeName)},""" +
        s""""reason":${ExplainCreateMaterializedViewCommand.jsonStr(classification.reason)},""" +
        s""""source_tables":${ExplainCreateMaterializedViewCommand.jsonStrArray(result.sourceTables)},""" +
        s""""emits_cascade_view_delta":${classification.emitsCascadeViewDelta}}"""

    Seq(Row(json))
  }
}

object ExplainCreateMaterializedViewCommand {

  /** Minimal JSON string encoder (escapes `"` `\` and control chars). Avoids a
    * JSON library dependency; mirrors the manual emitter in `WorkloadFacts`.
    */
  private[commands] def jsonStr(s: String): String = {
    val sb = new StringBuilder(s.length + 2)
    sb.append('"')
    s.foreach {
      case '"'                 => sb.append("\\\"")
      case '\\'                => sb.append("\\\\")
      case '\n'                => sb.append("\\n")
      case '\r'                => sb.append("\\r")
      case '\t'                => sb.append("\\t")
      case c if c.toInt < 0x20 => sb.append("\\u%04x".format(c.toInt))
      case c                   => sb.append(c)
    }
    sb.append('"')
    sb.toString
  }

  private[commands] def jsonStrArray(xs: Seq[String]): String =
    xs.map(jsonStr).mkString("[", ",", "]")
}
