package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common._
import org.openivm.spark.compiler.{CompiledRefresh, CompileRequest, LptsSparkDialect, OpenIvmCompileException}

/**
 * Session-scoped "cold DAG" registry for dry-run materialized views.
 *
 * `EXPLAIN CREATE MATERIALIZED VIEW` and `SHOW REFRESH SQL FOR ...` compile a
 * view WITHOUT creating it. In a dbt DAG the models fire in dependency order
 * over one session, so a downstream MV's dry compile references upstream MVs
 * that were only explained (never materialized). To let the downstream query
 * resolve, each dry compile registers its user-facing output as an EMPTY,
 * schema-only managed table.
 *
 * A temp view will NOT work here: [[MvCommandHelper.collectSourceSchemas]] only
 * recognises catalog `LogicalRelation`s / `DataSourceV2Relation`s (temp views
 * are inlined during analysis and never surface as a named source), so openivm
 * would not see the upstream MV as a source table. An empty managed table does
 * surface, carries the right schema, and is naturally ephemeral (it lives only
 * in the session's throwaway catalog/warehouse — no data is written).
 */
object DryRunMvRegistry {

  /** Register `name`'s empty, schema-only output so downstream dry compiles in
    * the same session can resolve it as a source. Best-effort: a failure to
    * register just means a later dry compile over this MV reports the usual
    * "table or view not found" error.
    */
  def register(spark: SparkSession, name: TableIdentifier, schema: StructType): Unit = {
    if (schema.isEmpty) return
    val ident = MvCommandHelper.sqlIdent(name)
    try {
      name.database.foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS `${db.replace("`", "``")}`"))
      spark.sql(s"DROP TABLE IF EXISTS $ident")
      spark.sql(s"CREATE TABLE $ident (${schema.toDDL}) USING parquet")
    } catch {
      case _: Throwable => ()
    }
  }
}

/**
 * Side-effect-free "compile only" path shared by `EXPLAIN CREATE MATERIALIZED
 * VIEW` (#4) and `SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW` (#25).
 *
 * It resolves the source schemas, runs the openivm compile, applies the SAME
 * [[MvCommandHelper.classifyEffectiveRefreshType]] verdict the live CREATE
 * path uses (so EXPLAIN can never disagree with a real CREATE), and rewrites
 * the compiled program to the Spark-executable statements a REFRESH would run.
 *
 * Nothing is materialised: no MV Delta table, no staging, no query-log rows,
 * no execution. The only permitted side effect is [[DryRunMvRegistry]] cold-DAG
 * stub registration, which callers opt into explicitly.
 */
object MvDryCompile {

  /** The dry-compile result surfaced to the two introspection commands. */
  final case class DryCompileResult(
      name: TableIdentifier,
      classification: MvCommandHelper.EffectiveClassification,
      compiled: CompiledRefresh,
      sourceTables: Seq[String],
      rewrittenStatements: Seq[String],
      outputSchema: StructType
  )

  def dryCompile(
      spark: SparkSession,
      name: TableIdentifier,
      queryText: String,
      clusterColumns: Seq[String] = Seq.empty
  ): DryCompileResult = {
    import MvCommandHelper._
    // Clustering is a physical-layout hint only; it changes neither the refresh
    // classification nor the rewritten SQL, so it is intentionally unused here.
    val _ = clusterColumns

    val (qualNames, qualSchemas, compileSchemas, shortToQual) =
      collectSourceSchemas(spark, queryText)

    val analyzed     = spark.sql(queryText).queryExecution.analyzed
    val outputSchema = spark.sql(queryText).schema

    // Best-effort workload facts. Cold-DAG stub tables are plain parquet (not
    // Delta), so stats/constraint discovery may throw — fall back to defaults.
    val workloadFacts: WorkloadFacts =
      try {
        val constraintFacts = WorkloadFactsRegistry.forRefresh().discover(spark, qualNames)
        val statsFacts      = SparkDeltaStatsService.forRefresh().workloadFactsFor(spark, qualNames)
        statsFacts.copy(
          fkRelations = constraintFacts.fkRelations,
          uniqueKeys = constraintFacts.uniqueKeys,
          declareRelyFk = FeatureGate.declareRelyFkEnabled(spark)
        )
      } catch {
        case _: Throwable => WorkloadFacts()
      }

    val compiler = OpenIvmCompilers.forSession(spark)
    val compiled =
      try
        compiler.compile(
          CompileRequest(
            viewName = name.table,
            viewSql = queryText,
            sources = compileSchemas,
            sourceQualifiedNames = shortToQual,
            facts = workloadFacts
          )
        )
      catch {
        case _: OpenIvmCompileException =>
          CompiledRefresh(RefreshTypeCode.FullRefresh, "FULL_REFRESH", "", "")
      }

    val location = mvLocation(spark, name)

    val aggregateHavingDataColumns = computeAggregateHavingDataColumns(spark, compiled, queryText)
    val simpleProjectionHasDataApply =
      computeSimpleProjectionHasDataApply(spark, compiled, name, location, qualSchemas, shortToQual)
    val topKViewSpec = validateTopKViewSpec(
      spark,
      extractTopKViewSpec(spark, queryText),
      compiled,
      analyzed.output.map(_.name)
    )
    val rawHavingPred =
      if (compiled.refreshType == RefreshTypeCode.AggregateHaving) extractHavingPredicateSql(analyzed)
      else None
    val upstreamMvByQual              = computeUpstreamMvByQual(spark, qualNames)
    val upstreamSnapshotTriggerDetail = computeUpstreamSnapshotTriggerDetail(upstreamMvByQual)

    val classification = classifyEffectiveRefreshType(
      compiled = compiled,
      viewShortName = name.table,
      topKViewSpec = topKViewSpec,
      simpleProjectionHasDataApply = simpleProjectionHasDataApply,
      upstreamSnapshotTriggerDetail = upstreamSnapshotTriggerDetail,
      rawHavingPred = rawHavingPred,
      aggregateHavingDataColumns = aggregateHavingDataColumns
    )

    val rewrittenStatements =
      dryRewrite(
        spark,
        name,
        location,
        compiled,
        classification,
        topKViewSpec,
        queryText,
        qualSchemas,
        shortToQual
      )

    DryCompileResult(name, classification, compiled, qualNames, rewrittenStatements, outputSchema)
  }

  /** Produce the Spark-executable statements a REFRESH would run, mirroring
    * [[CreateMaterializedViewCommand]] / [[RefreshMaterializedViewCommand]]:
    * FULL_REFRESH → the single INSERT OVERWRITE envelope; incremental types →
    * the [[SparkRefreshRewriter]] program. Pure string transforms only.
    */
  private def dryRewrite(
      spark: SparkSession,
      name: TableIdentifier,
      location: String,
      compiled: CompiledRefresh,
      classification: MvCommandHelper.EffectiveClassification,
      topKViewSpec: MvCommandHelper.TopKViewSpec,
      queryText: String,
      qualSchemas: Map[String, StructType],
      shortToQual: Map[String, String]
  ): Seq[String] = {
    import MvCommandHelper._
    if (classification.refreshType == RefreshTypeCode.FullRefresh) {
      val body =
        if (compiled.initialLoadSql.nonEmpty) LptsSparkDialect.translate(compiled.initialLoadSql)
        else queryText
      SparkMergeAssembler
        .assemble(
          AssemblyInput(
            refreshType = RefreshTypeCode.FullRefresh,
            refreshTypeName = "FULL_REFRESH",
            deltaSql = body,
            mvName = metaName(name),
            mvLocation = location
          )
        )
        .statements
    } else if (compiled.sql.isEmpty) {
      Seq.empty
    } else {
      val viewDeltaPath = s"${location.stripSuffix("/")}/__openivm_view_delta"
      val writeTarget =
        if (classification.refreshType == RefreshTypeCode.AggregateHaving || topKViewSpec.suffixSql.nonEmpty)
          dataTableId(name)
        else name
      SparkRefreshRewriter
        .rewrite(
          compiledSql = compiled.sql,
          mvName = writeTarget,
          mvLocation = location,
          viewLogicalName = name.table,
          sourceTempViews = Map.empty,
          viewDeltaPath = viewDeltaPath,
          postProcess = LptsSparkDialect.translate,
          sourceSchemas = qualSchemas.map { case (qual, schema) =>
            qual.split("\\.").last -> schema.fieldNames.toSeq
          },
          sourceQualifiedNames = shortToQual,
          semiJoinPruneEnabled = FeatureGate.semiJoinPruneEnabled(spark),
          fkTermPruneEnabled = FeatureGate.fkTermPruneEnabled(spark),
          uniqueJoinSimplifyEnabled = FeatureGate.uniqueJoinSimplifyEnabled(spark),
          windowPartitionSingleDeleteMergeEnabled = FeatureGate.windowPartitionSingleDeleteMergeEnabled(spark)
        )
        .statements
    }
  }
}
