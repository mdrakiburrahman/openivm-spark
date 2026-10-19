package org.openivm.spark.analyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier}
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.openivm.spark.common.{FeatureGate, MvCatalog}
import org.rocksdb.RocksDBException

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID

/**
 * Catalyst resolution rule that intercepts V2 DML operations on base tables
 * tracked by [[MvCatalog]] and injects staging nodes.
 *
 * == Timing ==
 * Registered via `injectResolutionRule`.  Delta's `DeltaAnalysis` fires in the
 * same "Resolution" batch.  Because Delta is registered before openivm-spark,
 * Delta's rules run first in each pass:
 *   - `AppendData` / `OverwriteByExpression` are NOT lowered by Delta → our
 *     rule sees them directly. Overwrites stage both the replaced target rows
 *     and the replacement query so downstream MVs receive a complete signed delta.
 *   - `DeleteFromTable` → `ReplaceData`, `UpdateTable` → `ReplaceData`,
 *     `MergeIntoTable` → `WriteDelta` before our rule fires → our rule matches
 *     the Delta-lowered forms and wraps them in [[StagedDmlNode]].
 *   - `CreateTableAsSelect` / `ReplaceTableAsSelect` / `ReplaceTable` are
 *     wrapped whole in [[StagedTableReplacementNode]]: Spark writes their rows
 *     into an uncommitted `StagedTable` that is neither readable nor committed
 *     when its nested `AppendData` / `OverwriteByExpression` runs.
 *
 * Pre-lowered forms (`DeleteFromTable`, `UpdateTable`, `MergeIntoTable`) are
 * also matched as a fallback in case ordering changes in future Spark/Delta
 * versions.
 *
 * == Bypass ==
 * [[IvmDmlInterceptorRule.bypass]] (thread-local) prevents re-interception when
 * [[StagedDmlNode.run()]] executes the original DML.
 */
class IvmDmlInterceptorRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!FeatureGate.enabled(session) || IvmDmlInterceptorRule.bypass.get()) return plan
    if (alreadyWrapped(plan)) return plan

    plan match {

      // CTAS / RTAS / REPLACE TABLE: the whole command is one table replacement.
      case c: CreateTableAsSelect  => interceptReplacement(c, c.name)
      case r: ReplaceTableAsSelect => interceptReplacement(r, r.name)
      case r: ReplaceTable         => interceptReplacement(r, r.name)

      // -----------------------------------------------------------------------
      // INSERT — pre-read staging: capture incoming rows before write
      // -----------------------------------------------------------------------
      case a: AppendData =>
        val tableName  = extractTableName(a.table)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) a
        else {
          requireCommittedTarget(a.table, tableName)
          val sp  = stagingPath(tableName, "INSERT")
          val ops = Seq((a.query, sp, "INSERT"))
          StagedDmlNode(a, ops, tableName, dependentMaterializedViews = dependents)
        }

      // -----------------------------------------------------------------------
      // OVERWRITE — pre-read staging: retract replaced rows and add replacement rows
      // -----------------------------------------------------------------------
      case o: OverwriteByExpression =>
        val tableName  = extractTableName(o.table)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) o
        else {
          requireCommittedTarget(o.table, tableName)
          // OverwriteByExpression replaces the rows selected by deleteExpr.  The
          // incoming query alone is not a complete signed delta: treating only
          // those rows as +1 leaves every replaced row in dependent projection
          // MVs.  Read both sides before the DML so a full overwrite and a
          // predicate-scoped overwrite produce the same -old/+new delta that CDF
          // exposes after the commit.
          val replacedPlan: LogicalPlan = Filter(o.deleteExpr, o.table)
          val ops = Seq(
            (replacedPlan, stagingPath(tableName, "DELETE"), "DELETE"),
            (o.query, stagingPath(tableName, "OVERWRITE"), "OVERWRITE")
          )
          StagedDmlNode(o, ops, tableName, dependentMaterializedViews = dependents)
        }

      // -----------------------------------------------------------------------
      // DeltaDelete — Delta's native pre-lowered DELETE node
      // -----------------------------------------------------------------------
      case d: DeltaDelete =>
        val tableName  = extractTableName(d.child)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) d
        else {
          val cond                     = d.condition.getOrElse(Literal(true))
          val deletedPlan: LogicalPlan = Filter(cond, d.child)
          val ops                      = Seq((deletedPlan, stagingPath(tableName, "DELETE"), "DELETE"))
          StagedDmlNode(d, ops, tableName, dependentMaterializedViews = dependents)
        }

      // -----------------------------------------------------------------------
      // DeltaUpdateTable — Delta's native pre-lowered UPDATE node
      // -----------------------------------------------------------------------
      case u: DeltaUpdateTable =>
        val tableName  = extractTableName(u.child)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) u
        else {
          // Both staging plans must be executed BEFORE the DML so that the
          // WHERE condition still matches the original rows.
          //
          // beforePlan: rows matching the condition (the "old" values to retract).
          // afterPlan:  the same rows projected through the SET assignments (new
          //             values to add).
          //
          // DeltaUpdateTable.updateColumns/updateExpressions carry only the SET
          // columns (one entry per assignment clause), NOT one entry per table
          // column.  We therefore build an assignment map keyed by column name and
          // project ALL columns: SET columns get their new expression, unchanged
          // columns keep their original AttributeReference.  This mirrors the
          // UpdateTable fallback case below and ensures the staging AFTER file
          // contains every source column so that the StagingDeltaView SELECT can
          // reference any of them (e.g. `SELECT region, amount FROM delta.AFTER`).
          val filteredPlan: LogicalPlan =
            u.condition.map(c => Filter(c, u.child)).getOrElse(u.child)
          val beforePlan: LogicalPlan = filteredPlan
          val assignMap: Map[String, Expression] =
            u.updateColumns
              .zip(u.updateExpressions)
              .flatMap { case (colExpr, valueExpr) =>
                DeltaUpdateTable
                  .getTargetColNameParts(colExpr)
                  .headOption
                  .map(_.toLowerCase -> valueExpr)
              }
              .toMap
          val afterProjections = filteredPlan.output.map { attr =>
            assignMap.get(attr.name.toLowerCase) match {
              case Some(newValue) => Alias(newValue, attr.name)()
              case None           => attr
            }
          }
          val afterPlan: LogicalPlan = Project(afterProjections, filteredPlan)
          val preOps = Seq(
            (beforePlan, stagingPath(tableName, "UPDATE_BEFORE"), "UPDATE_BEFORE"),
            (afterPlan, stagingPath(tableName, "UPDATE_AFTER"), "UPDATE_AFTER")
          )
          StagedDmlNode(u, preOps, tableName, dependentMaterializedViews = dependents)
        }

      // -----------------------------------------------------------------------
      // DeltaMergeInto — Delta's native pre-lowered MERGE node
      // -----------------------------------------------------------------------
      case m: DeltaMergeInto =>
        val tableName  = extractTableName(m.target)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) m
        else {
          val ops = Seq((m.source, stagingPath(tableName, "MERGE_SRC"), "MERGE_SRC"))
          StagedDmlNode(m, ops, tableName, dependentMaterializedViews = dependents)
        }

      // -----------------------------------------------------------------------
      // ReplaceData / WriteDelta — post-Delta-lowering fallbacks
      // (kept for forward compatibility; may fire if Delta ordering changes)
      // -----------------------------------------------------------------------
      case r: ReplaceData =>
        val tableName  = extractTableName(r.originalTable)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) r
        else {
          val ops = if (isUpdateReplaceData(r)) {
            val beforePlan: LogicalPlan = Filter(r.condition, r.originalTable)
            val afterPlan: LogicalPlan  = r.query
            Seq(
              (beforePlan, stagingPath(tableName, "UPDATE_BEFORE"), "UPDATE_BEFORE"),
              (afterPlan, stagingPath(tableName, "UPDATE_AFTER"), "UPDATE_AFTER")
            )
          } else {
            val deletedPlan: LogicalPlan = Filter(r.condition, r.originalTable)
            Seq((deletedPlan, stagingPath(tableName, "DELETE"), "DELETE"))
          }
          StagedDmlNode(r, ops, tableName, dependentMaterializedViews = dependents)
        }

      case w: WriteDelta =>
        val tableName  = extractTableName(w.originalTable)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) w
        else {
          val ops = Seq((w.query, stagingPath(tableName, "MERGE_SRC"), "MERGE_SRC"))
          StagedDmlNode(w, ops, tableName, dependentMaterializedViews = dependents)
        }

      // -----------------------------------------------------------------------
      // Spark-standard fallbacks (fire if Delta's parser is not active)
      // -----------------------------------------------------------------------
      case d: DeleteFromTable =>
        val tableName  = extractTableName(d.table)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) d
        else {
          val deletedPlan: LogicalPlan = Filter(d.condition, d.table)
          val ops                      = Seq((deletedPlan, stagingPath(tableName, "DELETE"), "DELETE"))
          StagedDmlNode(d, ops, tableName, dependentMaterializedViews = dependents)
        }

      case u: UpdateTable =>
        val tableName  = extractTableName(u.table)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) u
        else {
          // Both staging plans must be executed BEFORE the DML (same reason as
          // DeltaUpdateTable above).
          //
          // beforePlan: rows matching the condition (old values to retract).
          // afterPlan:  the same rows projected through the SET assignments (new
          //             values to add).  For columns not mentioned in assignments
          //             the original AttributeReference is kept unchanged.
          val filteredPlan: LogicalPlan =
            u.condition.map(c => Filter(c, u.table)).getOrElse(u.table)
          val beforePlan: LogicalPlan = filteredPlan
          val assignMap: Map[String, Expression] = u.assignments.collect { case Assignment(attr: Attribute, value) =>
            attr.name.toLowerCase -> value
          }.toMap
          val afterProjections = filteredPlan.output.map { attr =>
            assignMap.get(attr.name.toLowerCase) match {
              case Some(newValue) => Alias(newValue, attr.name)()
              case None           => attr
            }
          }
          val afterPlan: LogicalPlan = Project(afterProjections, filteredPlan)
          val preOps = Seq(
            (beforePlan, stagingPath(tableName, "UPDATE_BEFORE"), "UPDATE_BEFORE"),
            (afterPlan, stagingPath(tableName, "UPDATE_AFTER"), "UPDATE_AFTER")
          )
          StagedDmlNode(u, preOps, tableName, dependentMaterializedViews = dependents)
        }

      case m: MergeIntoTable =>
        val tableName  = extractTableName(m.targetTable)
        val dependents = dependentMvs(tableName)
        if (tableName.isEmpty || dependents.isEmpty) m
        else {
          val ops = Seq((m.sourceTable, stagingPath(tableName, "MERGE_SRC"), "MERGE_SRC"))
          StagedDmlNode(m, ops, tableName, dependentMaterializedViews = dependents)
        }

      case _ => plan
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * True when [[ReplaceData.query]] contains a [[Project]] with at least one
   * [[Alias]] node — indicating computed update expressions rather than a plain
   * filter-to-keep scan produced by DELETE.
   */
  private def isUpdateReplaceData(r: ReplaceData): Boolean =
    r.query.find {
      case Project(list, _) => list.exists(_.isInstanceOf[Alias])
      case _                => false
    }.isDefined

  private def alreadyWrapped(p: LogicalPlan): Boolean =
    p.find(node => node.isInstanceOf[StagedDmlNode] || node.isInstanceOf[StagedTableReplacementNode]).isDefined

  /** Wrap a CTAS / RTAS / REPLACE TABLE whose target feeds materialized views. */
  private def interceptReplacement(command: LogicalPlan, name: LogicalPlan): LogicalPlan = name match {
    case ResolvedIdentifier(_, identifier) =>
      val tableName  = qualifiedName(identifier)
      val dependents = dependentMvs(tableName)
      if (dependents.isEmpty) command
      else StagedTableReplacementNode(command, tableName, dependents)
    case _ => command
  }

  /** A write into an uncommitted replacement must arrive through [[interceptReplacement]]:
    * its rows are unreadable, Spark 3.5 presents it as an append, and it is not yet committed.
    */
  private def requireCommittedTarget(target: LogicalPlan, tableName: String): Unit = target match {
    case relation: DataSourceV2Relation if relation.table.isInstanceOf[StagedTable] =>
      throw new IllegalStateException(
        s"[openivm] refusing to capture a write into the uncommitted replacement of $tableName: " +
          "the CREATE/REPLACE TABLE command was not intercepted as a table replacement, so its " +
          "dependent materialized views cannot receive an exact change set"
      )
    case _ => ()
  }

  private def dependentMvs(tableName: String): Seq[String] =
    if (tableName.isEmpty) Seq.empty
    else
      try {
        // Allow calling Spark SQL (DataFrame.collect) from within this analysis rule.
        // Without this, the nested query triggered by MvCatalog re-enters the analyzer,
        // which may be blocked by Spark's re-entrancy guard.
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          MvCatalog.ensureTables(session)
          MvCatalog
            .viewsForSource(session, tableName)
            .map { metadata =>
              metadata.name.database.fold(metadata.name.table)(database => s"$database.${metadata.name.table}")
            }
            .distinct
            .sorted
        }
      } catch {
        case e: RocksDBException =>
          logError(s"[openivm] RocksDB failure resolving dependent MVs for $tableName: ${e.getMessage}", e)
          throw new IllegalStateException(
            s"[openivm] cannot determine dependent MVs for $tableName (RocksDB error: ${e.getMessage}); " +
              "refusing to silently un-tee INSERT. Likely cause: another Spark driver JVM holds the openivm " +
              "RocksDB LOCK. If running under ivm-bench, ensure all callers share one Livy session " +
              "(see services/spark_openivm_sources.py).",
            e
          )
        case e: Exception =>
          logError(s"[openivm] dependentMvs failed for $tableName: ${e.getClass.getName}: ${e.getMessage}", e)
          Seq.empty
      }

  private def extractTableName(relation: LogicalPlan): String = relation match {
    case r: DataSourceV2Relation if r.identifier.isDefined =>
      qualifiedName(r.identifier.get)
    case r: LogicalRelation if r.catalogTable.isDefined =>
      val id = r.catalogTable.get.identifier
      id.database.fold(id.table)(db => s"$db.${id.table}")
    case n: NamedRelation        => n.name
    case SubqueryAlias(_, child) => extractTableName(child)
    case _                       => ""
  }

  private def qualifiedName(identifier: Identifier): String = {
    val ns = identifier.namespace()
    if (ns.nonEmpty) (ns :+ identifier.name()).mkString(".") else identifier.name()
  }

  private def stagingPath(tableName: String, opType: String): String =
    IvmDmlInterceptorRule.stagingPath(session, tableName, opType)
}

object IvmDmlInterceptorRule {

  /** Thread-local bypass flag set by [[StagedDmlNode]] when re-executing the
   *  original DML to prevent infinite re-interception. */
  val bypass: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)

  /** Fresh staging location for one op of a write to `tableName`.
    *
    * @param spark     active session
    * @param tableName qualified name of the written base table
    * @param opType    one of [[org.openivm.spark.common.StagingDelta.OpTypes]]
    */
  private[analyzer] def stagingPath(spark: SparkSession, tableName: String, opType: String): String = {
    val warehouse = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    val safeTable = tableName.replace(".", "_").replace(" ", "_")
    val txnTs = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSS'Z'")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())
    s"$warehouse/_ivm/staging/$safeTable/$opType/$txnTs-${UUID.randomUUID()}"
  }
}
