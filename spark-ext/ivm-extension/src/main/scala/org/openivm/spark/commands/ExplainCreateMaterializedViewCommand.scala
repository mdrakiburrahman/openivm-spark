package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._
import org.openivm.spark.common.RefreshTypeCode

import java.util.UUID

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
    val operationId = s"preflight-${UUID.randomUUID()}"
    val viewName    = MvCommandHelper.metaName(name)
    val startedAt   = System.nanoTime()
    OpenIvmInsightEvents.operationStarted(
      spark,
      operationId,
      viewName,
      org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Preflight
    )
    try {
      val result         = MvDryCompile.dryCompile(spark, name, queryText, clusterColumns)
      val classification = result.classification
      val eligible       = classification.refreshType != RefreshTypeCode.FullRefresh

      OpenIvmInsightEvents.classification(
        spark = spark,
        operationId = operationId,
        materializedView = viewName,
        operation = org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Preflight,
        compileRefreshType = classification.compileRefreshTypeName,
        effectiveRefreshType = classification.refreshTypeName,
        reason = classification.reason,
        emitsCascadeViewDelta = classification.emitsCascadeViewDelta,
        upstreamSnapshotTrigger = classification.upstreamSnapshotTrigger
      )

      // Register the empty schema-only stand-in so downstream dry compiles resolve.
      DryRunMvRegistry.register(spark, name, result.outputSchema)

      val json =
        s"""{"view":${ExplainCreateMaterializedViewCommand.jsonStr(viewName)},""" +
          s""""eligible":$eligible,""" +
          s""""refresh_type":${classification.refreshType},""" +
          s""""refresh_type_name":${ExplainCreateMaterializedViewCommand.jsonStr(classification.refreshTypeName)},""" +
          s""""reason":${ExplainCreateMaterializedViewCommand.jsonStr(classification.reason)},""" +
          s""""source_tables":${ExplainCreateMaterializedViewCommand.jsonStrArray(result.sourceTables)},""" +
          classification.upstreamSnapshotTrigger.fold("")(detail =>
            s""""upstream_snapshot_trigger":${ExplainCreateMaterializedViewCommand.jsonStr(detail)},"""
          ) +
          s""""emits_cascade_view_delta":${classification.emitsCascadeViewDelta},""" +
          s""""graph_version":1,""" +
          s""""operation":"preflight",""" +
          s""""materialized_view":${ExplainCreateMaterializedViewCommand.jsonStr(viewName)},""" +
          s""""compile_refresh_type_name":${ExplainCreateMaterializedViewCommand
              .jsonStr(classification.compileRefreshTypeName)},""" +
          s""""direct_dependencies":${ExplainCreateMaterializedViewCommand.jsonStrArray(result.sourceTables)},""" +
          s""""cascade_capability":${ExplainCreateMaterializedViewCommand
              .jsonStr(if (classification.emitsCascadeViewDelta) "view_delta" else "snapshot_or_terminal")}}"""

      OpenIvmInsightEvents.operationFinished(
        spark,
        operationId,
        viewName,
        org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Preflight,
        outcome = "preflight_completed",
        durationMs = (System.nanoTime() - startedAt) / 1000000L,
        failed = false
      )
      Seq(Row(json))
    } catch {
      case error: Throwable =>
        OpenIvmInsightEvents.operationFinished(
          spark,
          operationId,
          viewName,
          org.openivm.spark.insights.OpenIvmInsightsContract.Operation.Preflight,
          outcome = "preflight_failed",
          durationMs = (System.nanoTime() - startedAt) / 1000000L,
          failed = true
        )
        throw error
    }
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
