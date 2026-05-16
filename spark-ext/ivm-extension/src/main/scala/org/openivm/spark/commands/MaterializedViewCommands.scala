package org.openivm.spark.commands

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.openivm.spark.analyzer.IvmDmlInterceptorRule
import org.openivm.spark.common._
import org.openivm.spark.compiler.{CompileRequest, OpenIvmCompiler}

import java.sql.Timestamp
import java.util.Collections

// ---------------------------------------------------------------------------
// Compiler singleton — one OpenIvmCompiler per SparkSession, lazily created.
// ---------------------------------------------------------------------------
private[commands] object OpenIvmCompilers {

  private val cache: java.util.Map[SparkSession, OpenIvmCompiler] =
    Collections.synchronizedMap(new java.util.WeakHashMap[SparkSession, OpenIvmCompiler]())

  def forSession(spark: SparkSession): OpenIvmCompiler = {
    val existing = cache.get(spark)
    if (existing != null) return existing
    cache.synchronized {
      val existing2 = cache.get(spark)
      if (existing2 != null) return existing2
      val c = OpenIvmCompiler.build()
      cache.put(spark, c)
      Runtime.getRuntime.addShutdownHook(new Thread(() => c.close()))
      c
    }
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------
private[commands] object MvCommandHelper {

  /** Fully-qualified dot-separated name used in MvMetadata and SQL strings. */
  def metaName(id: TableIdentifier): String =
    id.database.fold(id.table)(db => s"$db.${id.table}")

  /** Backtick-quoted SQL identifier, including optional catalog and database. */
  def sqlIdent(id: TableIdentifier): String = {
    val parts = id.catalog.toSeq ++ id.database.toSeq ++ Seq(id.table)
    parts.map(p => s"`${p.replace("`", "``")}`").mkString(".")
  }

  /** Physical path for the MV's Delta table inside `<warehouse>/_ivm/views/`. */
  def mvLocation(spark: SparkSession, id: TableIdentifier): String = {
    val warehouse = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    val segment   = id.database.fold(id.table)(db => s"$db/${id.table}")
    s"$warehouse/_ivm/views/$segment"
  }

  /**
   * Analyze `querySql` in the current session and return
   * (qualifiedName → fullSchema) and (shortName → fullSchema).
   *
   * qualifiedNames go into MvMetadata.sourceTables and the fingerprint.
   * shortNames go into CompileRequest.sources so DuckDB can resolve them
   * against the view body (which uses unqualified table references).
   */
  def collectSourceSchemas(
      spark: SparkSession,
      querySql: String
  ): (Seq[String], Map[String, StructType], Map[String, StructType]) = {
    val analyzed = spark.sql(querySql).queryExecution.analyzed
    val pairs: Seq[(String, String)] = analyzed
      .collect {
        case r: LogicalRelation if r.catalogTable.isDefined =>
          val id        = r.catalogTable.get.identifier
          val qualified = id.database.fold(id.table)(db => s"$db.${id.table}")
          (qualified, id.table)
        case r: DataSourceV2Relation if r.identifier.isDefined =>
          val ident     = r.identifier.get
          val ns        = ident.namespace()
          val short     = ident.name()
          val qualified = if (ns.nonEmpty) (ns :+ short).mkString(".") else short
          (qualified, short)
      }
      .distinct

    val qualNames   = pairs.map(_._1).distinct
    // Fetch full table schemas from the Spark catalog (not projected/pruned).
    val qualSchemas = qualNames.map(n => n -> spark.table(n).schema).toMap
    val compileSchemas: Map[String, StructType] =
      pairs.map { case (q, s) => s -> qualSchemas(q) }.toMap
    (qualNames, qualSchemas, compileSchemas)
  }

  /** Extract the GROUP BY key column names from an analyzed LogicalPlan. */
  def extractGroupKeys(analyzed: LogicalPlan): Seq[String] =
    analyzed
      .collect { case agg: Aggregate => agg }
      .headOption
      .map(_.groupingExpressions.collect { case ne: NamedExpression => ne.name })
      .getOrElse(Nil)

  /**
   * For incremental refresh types, create a temp view per source table that
   * UNION ALL-s all pending staging Delta paths.  Returns the temp-view names
   * created so they can be dropped after execution.
   */
  def createStagingViews(
      spark: SparkSession,
      deltas: Seq[StagingDelta],
      refreshType: Int
  ): Seq[String] = {
    if (refreshType == RefreshTypeCode.FullRefresh || deltas.isEmpty) return Nil
    val byTable = deltas.groupBy(_.baseTable)
    byTable.toSeq.map { case (qualifiedTable, tableDeltas) =>
      val shortName  = qualifiedTable.split("\\.").last
      val unionParts = tableDeltas.map(d => s"SELECT * FROM delta.`${escapePath(d.stagingPath)}`")
      spark.sql(
        s"CREATE OR REPLACE TEMP VIEW `$shortName` AS ${unionParts.mkString(" UNION ALL ")}"
      )
      shortName
    }
  }

  /** Drop temp views that were created by [[createStagingViews]]. */
  def dropStagingViews(spark: SparkSession, viewNames: Seq[String]): Unit =
    viewNames.foreach(n => spark.sql(s"DROP VIEW IF EXISTS `$n`"))

  private def escapePath(p: String): String = p.replace("`", "``")
}

// ---------------------------------------------------------------------------
// CreateMaterializedViewCommand
// ---------------------------------------------------------------------------

/**
 * Logical plan node for CREATE MATERIALIZED VIEW.
 *
 * @param originalQueryText  Raw SQL of the SELECT body, captured by the parser.
 *                           Stored verbatim in MvMetadata and passed to the compiler.
 */
case class CreateMaterializedViewCommand(
    name: TableIdentifier,
    query: LogicalPlan,
    properties: Map[String, String],
    ifNotExists: Boolean,
    provider: Option[String],
    originalQueryText: String
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._

    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)

    // Existence guard
    MvCatalog.lookup(spark, name) match {
      case Some(_) if ifNotExists => return Seq.empty
      case Some(_) =>
        throw new AnalysisException(
          "TABLE_OR_VIEW_ALREADY_EXISTS",
          Map("relationName" -> sqlIdent(name))
        )
      case None => // proceed
    }

    // Resolve source schemas
    val (qualNames, qualSchemas, compileSchemas) = collectSourceSchemas(spark, originalQueryText)

    // Extract GROUP BY keys and other optional metadata from the analyzed plan
    val analyzed  = spark.sql(originalQueryText).queryExecution.analyzed
    val groupKeys = extractGroupKeys(analyzed)

    // Compile the view via OpenIVM
    val compiler  = OpenIvmCompilers.forSession(spark)
    val compiled  = compiler.compile(CompileRequest(name.table, originalQueryText, compileSchemas))

    // Storage location
    val location = mvLocation(spark, name)

    // Fingerprint the current source schemas
    val fingerprint = MvCatalog.schemaFingerprint(qualSchemas)

    // Persist internal metadata alongside any user-provided properties
    val allProps = properties ++ Map("_ivm_group_keys" -> groupKeys.mkString(","))
    val now      = new Timestamp(System.currentTimeMillis())

    val meta = MvMetadata(
      name                  = name,
      querySql              = originalQueryText,
      refreshType           = compiled.refreshType,
      refreshTypeName       = compiled.refreshTypeName,
      lastVersion           = -1L,
      sourceTables          = qualNames,
      sourceSchemaFingerprint = fingerprint,
      location              = location,
      createdAt             = now,
      properties            = allProps
    )

    // Materialize the MV with an initial full-load CREATE TABLE AS SELECT.
    // Bypass the DML interceptor so the CTAS write is not double-wrapped.
    val escaped = location.replace("'", "\\'")
    val initSql =
      s"CREATE TABLE IF NOT EXISTS ${sqlIdent(name)} USING DELTA LOCATION '$escaped' AS $originalQueryText"
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      spark.sql(initSql)

      // Write metadata catalog entry
      MvCatalog.upsert(spark, meta)

      // Record the Delta version of the initial snapshot
      val version =
        DeltaTable.forPath(spark, location).history(1).collect().head.getAs[Long]("version")
      MvCatalog.advance(spark, name, version)
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
    }

    Seq.empty
  }
}

// ---------------------------------------------------------------------------
// RefreshMaterializedViewCommand
// ---------------------------------------------------------------------------

case class RefreshMaterializedViewCommand(
    name: TableIdentifier
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._

    val meta = MvCatalog.lookup(spark, name).getOrElse(
      throw new AnalysisException(
        "TABLE_OR_VIEW_NOT_FOUND",
        Map("relationName" -> sqlIdent(name))
      )
    )

    val viewNameStr   = metaName(name)
    val stagingDeltas = StagingCatalog.collectFor(spark, viewNameStr, meta.sourceTables)

    // No pending deltas → nothing to do
    if (stagingDeltas.isEmpty) return Seq.empty

    // Resolve current source schemas and check for schema drift
    val freshSchemas     = meta.sourceTables.map(t => t -> spark.table(t).schema).toMap
    val freshFingerprint = MvCatalog.schemaFingerprint(freshSchemas)
    if (freshFingerprint != meta.sourceSchemaFingerprint)
      throw new AnalysisException(
        "INCOMPATIBLE_VIEW_SCHEMA_CHANGE",
        Map(
          "viewName"    -> sqlIdent(name),
          "colName"     -> "source schema fingerprint",
          "expectedNum" -> meta.sourceSchemaFingerprint,
          "actualCols"  -> freshFingerprint,
          "suggestion"  -> "DROP and recreate the materialized view"
        )
      )

    // Use full-recompute semantics: INSERT OVERWRITE TABLE mv SELECT * FROM (querySql).
    // The staging catalog is consulted above to guard against no-op refreshes, but the
    // recompute itself reads from the live source tables rather than the staging delta.
    // Proper incremental execution (using the lpts-compiled SQL with openivm_multiplicity)
    // is deferred to a later surface once the DML interceptor writes the required staging format.
    val input = AssemblyInput(
      refreshType     = RefreshTypeCode.FullRefresh,
      refreshTypeName = "FULL_REFRESH",
      deltaSql        = meta.querySql,
      mvName          = metaName(name),
      mvLocation      = meta.location
    )
    val assembled = SparkMergeAssembler.assemble(input)

    // Execute with interceptor bypass to prevent DeltaStagingExec injection into the MV write.
    IvmDmlInterceptorRule.bypass.set(true)
    try {
      assembled.statements.foreach(sql => spark.sql(sql).collect())

      // Advance the tracked Delta version
      val newVersion =
        DeltaTable.forPath(spark, meta.location).history(1).collect().head.getAs[Long]("version")
      MvCatalog.advance(spark, name, newVersion)

      // Mark all consumed staging paths
      val consumedPaths = stagingDeltas.map(_.stagingPath)
      StagingCatalog.markConsumed(spark, viewNameStr, consumedPaths)

      // Prune staging rows that every dependent MV has now consumed
      val allMvs       = MvCatalog.list(spark)
      val viewsByTable = allMvs
        .flatMap(m => m.sourceTables.map(t => t -> metaName(m.name)))
        .groupBy(_._1)
        .map { case (t, pairs) => t -> pairs.map(_._2) }
      StagingCatalog.pruneFullyConsumed(spark, viewsByTable)
    } catch {
      case t: Throwable =>
        val sqlSnippet = assembled.statements.mkString(";\n---\n")
        throw new RuntimeException(
          s"Refresh of '${sqlIdent(name)}' failed: ${t.getMessage}\nAssembled SQL:\n$sqlSnippet", t
        )
    } finally {
      IvmDmlInterceptorRule.bypass.set(false)
    }

    Seq.empty
  }
}

// ---------------------------------------------------------------------------
// DropMaterializedViewCommand
// ---------------------------------------------------------------------------

case class DropMaterializedViewCommand(
    name: TableIdentifier,
    ifExists: Boolean
) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    import MvCommandHelper._

    MvCatalog.lookup(spark, name) match {
      case None if ifExists =>
        return Seq.empty
      case None =>
        throw new AnalysisException(
          "TABLE_OR_VIEW_NOT_FOUND",
          Map("relationName" -> sqlIdent(name))
        )
      case Some(meta) =>
        // Drop the catalog table entry (Delta table registration in Spark)
        spark.sql(s"DROP TABLE IF EXISTS ${sqlIdent(name)}")

        // Delete the physical Delta files
        val hadoopPath = new Path(meta.location)
        val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
        if (fs.exists(hadoopPath)) fs.delete(hadoopPath, /* recursive = */ true)

        // Remove the tracking row from the MV catalog
        MvCatalog.remove(spark, name)
    }

    Seq.empty
  }
}
