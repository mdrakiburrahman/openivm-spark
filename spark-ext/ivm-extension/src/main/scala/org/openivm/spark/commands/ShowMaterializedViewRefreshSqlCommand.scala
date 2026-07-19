package org.openivm.spark.commands

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._

/**
 * `SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW <name> [CLUSTER BY (...)] AS
 * <query>` (#25).
 *
 * A dry run of the incrementally-rewritten refresh program: it compiles the
 * view through openivm and rewrites the emitted program to the exact
 * Spark-executable statements a REFRESH would run — without materialising the
 * MV, staging, or query log. The single `refresh_sql` STRING column holds the
 * whole program as copy-pasteable, semicolon-separated SQL.
 *
 * Like [[ExplainCreateMaterializedViewCommand]], the view's empty output schema
 * is registered in [[DryRunMvRegistry]] so downstream dry compiles in the same
 * session resolve it.
 */
case class ShowMaterializedViewRefreshSqlCommand(
    name: TableIdentifier,
    queryText: String,
    clusterColumns: Seq[String] = Seq.empty
) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("refresh_sql", StringType, nullable = false)()
  )

  override def run(spark: SparkSession): Seq[Row] = {
    val result = MvDryCompile.dryCompile(spark, name, queryText, clusterColumns)

    // Register the empty schema-only stand-in so downstream dry compiles resolve.
    DryRunMvRegistry.register(spark, name, result.outputSchema)

    val statements = result.rewrittenStatements.map(_.trim).filter(_.nonEmpty)
    val refreshSql =
      if (statements.isEmpty)
        "-- no refresh statements: openivm emitted an empty delta program"
      else
        statements.mkString(";\n") + ";"

    Seq(Row(refreshSql))
  }
}
