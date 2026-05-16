package org.openivm.spark.analyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.openivm.spark.common.{FeatureGate, MvCatalog}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/**
 * Catalyst resolution rule that intercepts V2 DML operations on base tables
 * tracked by [[MvCatalog]] and injects staging nodes.
 *
 * == Timing ==
 * Registered via `injectResolutionRule`.  Delta's `DeltaAnalysis` fires in the
 * same "Resolution" batch.  Because Delta is registered before openivm-spark,
 * Delta's rules run first in each pass:
 *   - `AppendData` / `OverwriteByExpression` are NOT lowered by Delta → our
 *     rule sees them directly and wraps their query child with [[WithDeltaStaging]].
 *   - `DeleteFromTable` → `ReplaceData`, `UpdateTable` → `ReplaceData`,
 *     `MergeIntoTable` → `WriteDelta` before our rule fires → our rule matches
 *     the Delta-lowered forms and wraps them in [[StagedDmlNode]].
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

      // -----------------------------------------------------------------------
      // INSERT — pre-read staging: capture incoming rows before write
      // -----------------------------------------------------------------------
      case a: AppendData =>
        val tableName = extractTableName(a.table)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) a
        else {
          val sp  = stagingPath(tableName, "INSERT")
          val ops = Seq((a.query, sp, "INSERT"))
          StagedDmlNode(a, ops, tableName)
        }

      // -----------------------------------------------------------------------
      // OVERWRITE — pre-read staging: capture replacement rows before write
      // -----------------------------------------------------------------------
      case o: OverwriteByExpression =>
        val tableName = extractTableName(o.table)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) o
        else {
          val sp  = stagingPath(tableName, "OVERWRITE")
          val ops = Seq((o.query, sp, "OVERWRITE"))
          StagedDmlNode(o, ops, tableName)
        }

      // -----------------------------------------------------------------------
      // DeltaDelete — Delta's native pre-lowered DELETE node
      // -----------------------------------------------------------------------
      case d: DeltaDelete =>
        val tableName = extractTableName(d.child)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) d
        else {
          val cond = d.condition.getOrElse(Literal(true))
          val deletedPlan: LogicalPlan = Filter(cond, d.child)
          val ops = Seq((deletedPlan, stagingPath(tableName, "DELETE"), "DELETE"))
          StagedDmlNode(d, ops, tableName)
        }

      // -----------------------------------------------------------------------
      // DeltaUpdateTable — Delta's native pre-lowered UPDATE node
      // -----------------------------------------------------------------------
      case u: DeltaUpdateTable =>
        val tableName = extractTableName(u.child)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) u
        else {
          // condition: Option[Expression]
          val beforePlan: LogicalPlan =
            u.condition.map(c => Filter(c, u.child)).getOrElse(u.child)
          val afterPlan: LogicalPlan =
            u.condition.map(c => Filter(c, u.child)).getOrElse(u.child)
          val preOps  = Seq((beforePlan, stagingPath(tableName, "UPDATE_BEFORE"), "UPDATE_BEFORE"))
          val postOps = Seq((afterPlan,  stagingPath(tableName, "UPDATE_AFTER"),  "UPDATE_AFTER"))
          StagedDmlNode(u, preOps, tableName, postOps)
        }

      // -----------------------------------------------------------------------
      // DeltaMergeInto — Delta's native pre-lowered MERGE node
      // -----------------------------------------------------------------------
      case m: DeltaMergeInto =>
        val tableName = extractTableName(m.target)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) m
        else {
          val ops = Seq((m.source, stagingPath(tableName, "MERGE_SRC"), "MERGE_SRC"))
          StagedDmlNode(m, ops, tableName)
        }

      // -----------------------------------------------------------------------
      // ReplaceData / WriteDelta — post-Delta-lowering fallbacks
      // (kept for forward compatibility; may fire if Delta ordering changes)
      // -----------------------------------------------------------------------
      case r: ReplaceData =>
        val tableName = extractTableName(r.originalTable)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) r
        else {
          val ops = if (isUpdateReplaceData(r)) {
            val beforePlan: LogicalPlan = Filter(r.condition, r.originalTable)
            val afterPlan: LogicalPlan  = r.query
            Seq(
              (beforePlan, stagingPath(tableName, "UPDATE_BEFORE"), "UPDATE_BEFORE"),
              (afterPlan,  stagingPath(tableName, "UPDATE_AFTER"),  "UPDATE_AFTER")
            )
          } else {
            val deletedPlan: LogicalPlan = Filter(r.condition, r.originalTable)
            Seq((deletedPlan, stagingPath(tableName, "DELETE"), "DELETE"))
          }
          StagedDmlNode(r, ops, tableName)
        }

      case w: WriteDelta =>
        val tableName = extractTableName(w.originalTable)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) w
        else {
          val ops = Seq((w.query, stagingPath(tableName, "MERGE_SRC"), "MERGE_SRC"))
          StagedDmlNode(w, ops, tableName)
        }

      // -----------------------------------------------------------------------
      // Spark-standard fallbacks (fire if Delta's parser is not active)
      // -----------------------------------------------------------------------
      case d: DeleteFromTable =>
        val tableName = extractTableName(d.table)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) d
        else {
          val deletedPlan: LogicalPlan = Filter(d.condition, d.table)
          val ops = Seq((deletedPlan, stagingPath(tableName, "DELETE"), "DELETE"))
          StagedDmlNode(d, ops, tableName)
        }

      case u: UpdateTable =>
        val tableName = extractTableName(u.table)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) u
        else {
          val beforePlan: LogicalPlan =
            u.condition.map(c => Filter(c, u.table)).getOrElse(u.table)
          val afterPlan: LogicalPlan =
            u.condition.map(c => Filter(c, u.table)).getOrElse(u.table)
          val preOps  = Seq((beforePlan, stagingPath(tableName, "UPDATE_BEFORE"), "UPDATE_BEFORE"))
          val postOps = Seq((afterPlan,  stagingPath(tableName, "UPDATE_AFTER"),  "UPDATE_AFTER"))
          StagedDmlNode(u, preOps, tableName, postOps)
        }

      case m: MergeIntoTable =>
        val tableName = extractTableName(m.targetTable)
        if (tableName.isEmpty || !hasDependentMvs(tableName)) m
        else {
          val ops = Seq((m.sourceTable, stagingPath(tableName, "MERGE_SRC"), "MERGE_SRC"))
          StagedDmlNode(m, ops, tableName)
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
    r.query
      .find {
        case Project(list, _) => list.exists(_.isInstanceOf[Alias])
        case _                => false
      }
      .isDefined

  private def alreadyWrapped(p: LogicalPlan): Boolean =
    p.find(_.isInstanceOf[StagedDmlNode]).isDefined

  private def hasDependentMvs(tableName: String): Boolean =
    try {
      // Allow calling Spark SQL (DataFrame.collect) from within this analysis rule.
      // Without this, the nested query triggered by MvCatalog re-enters the analyzer,
      // which may be blocked by Spark's re-entrancy guard.
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        MvCatalog.ensureTables(session)
        MvCatalog.viewsForSource(session, tableName).nonEmpty
      }
    } catch {
      case e: Exception =>
        logWarning(s"[openivm] hasDependentMvs failed for $tableName: ${e.getClass.getName}: ${e.getMessage}")
        false
    }

  private def extractTableName(relation: LogicalPlan): String = relation match {
    case r: DataSourceV2Relation if r.identifier.isDefined =>
      val id = r.identifier.get
      val ns = id.namespace()
      if (ns.nonEmpty) (ns :+ id.name()).mkString(".") else id.name()
    case r: LogicalRelation if r.catalogTable.isDefined =>
      val id = r.catalogTable.get.identifier
      id.database.fold(id.table)(db => s"$db.${id.table}")
    case n: NamedRelation => n.name
    case SubqueryAlias(_, child) => extractTableName(child)
    case _                => ""
  }

  private def stagingPath(tableName: String, opType: String): String = {
    val warehouse  = session.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    val safeTable  = tableName.replace(".", "_").replace(" ", "_")
    val txnTs      = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSS'Z'")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())
    s"$warehouse/_ivm/staging/$safeTable/$opType/$txnTs"
  }
}

object IvmDmlInterceptorRule {

  /** Thread-local bypass flag set by [[StagedDmlNode]] when re-executing the
   *  original DML to prevent infinite re-interception. */
  val bypass: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)
}
