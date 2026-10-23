package org.openivm.spark.commands

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  Join,
  LogicalPlan,
  Project,
  Sort,
  SubqueryAlias,
  View,
  Window
}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.openivm.spark.common.MvProjectionSource
import org.openivm.spark.compiler.SparkTimeTravelSql

import scala.util.control.NonFatal

/** Refine conservative native lineage only when one join and its window-key origin are proved. */
private[commands] object WindowJoinAffectedKeys extends Logging {
  def query(
      spark: SparkSession,
      querySql: String,
      sources: Seq[String],
      affectedColumn: String
  ): Option[String] = try {
    if (sources.size != 2 || SparkTimeTravelSql.hasSnapshotPin(querySql)) return None
    val resolver = spark.sessionState.conf.resolver
    val ids      = sources.map(source => source -> spark.sessionState.sqlParser.parseTableIdentifier(source))
    if (resolver(ids.head._2.table, ids.last._2.table)) return None

    def sourceFor(id: TableIdentifier): Option[String] =
      ids.collectFirst {
        case (source, candidate) if resolver(candidate.table, id.table) && candidate.database == id.database => source
      }

    def sourceOf(plan: LogicalPlan): Option[String] = plan match {
      case alias: SubqueryAlias                                => sourceOf(alias.child)
      case view: View if MvProjectionSource.isProjection(view) => sourceFor(view.desc.identifier)
      case relation: LogicalRelation => relation.catalogTable.flatMap(t => sourceFor(t.identifier))
      case relation: DataSourceV2Relation =>
        relation.identifier.flatMap { id =>
          val namespace = id.namespace()
          if (namespace.length > 1) None
          else sourceFor(TableIdentifier(id.name(), namespace.headOption))
        }
      case _ => None
    }

    // Public MV views are input boundaries. Their own joins/windows must not be
    // confused with occurrences in the query being incrementally maintained.
    def nodes(plan: LogicalPlan): Seq[LogicalPlan] =
      if (sourceOf(plan).isDefined || plan.isInstanceOf[View]) Seq(plan)
      else plan +: plan.children.flatMap(nodes)

    val plan       = InlineCTE(alwaysInline = true)(spark.sql(querySql).queryExecution.analyzed)
    val queryNodes = nodes(plan)
    if (
      queryNodes.exists { node =>
        !sourceOf(node).isDefined && !(node match {
          case _: Project | _: Filter | _: Sort | _: SubqueryAlias | _: Window | _: Join => true
          case _                                                                         => false
        })
      }
    ) return None
    if (queryNodes.exists(_.expressions.exists(_.exists(_.isInstanceOf[SubqueryExpression])))) return None
    val joins = queryNodes.collect { case join: Join => join }
    if (joins.size != 1) return None
    val join = joins.head
    if (join.joinType != Inner && join.joinType != LeftOuter) return None
    val left  = sourceOf(join.left).getOrElse(return None)
    val right = sourceOf(join.right).getOrElse(return None)
    if (left == right || Set(left, right) != sources.toSet) return None
    val condition = join.condition.getOrElse(return None)
    if (
      !condition.deterministic ||
      !condition.references.subsetOf(join.left.outputSet ++ join.right.outputSet) ||
      !condition.references.exists(join.left.outputSet.contains) ||
      !condition.references.exists(join.right.outputSet.contains)
    ) return None

    // Trace only unchanged attributes through projections/CTE aliases. A cast,
    // derived partition key, aggregate, second join or different window key
    // retains the established conservative program.
    def leftOrigin(node: LogicalPlan, attr: Attribute): Option[Attribute] = node match {
      case current if current eq join => join.left.output.find(_.exprId == attr.exprId)
      case project: Project =>
        project.projectList.find(_.exprId == attr.exprId).flatMap {
          case input: Attribute           => leftOrigin(project.child, input)
          case Alias(input: Attribute, _) => leftOrigin(project.child, input)
          case _                          => None
        }
      case alias: SubqueryAlias => leftOrigin(alias.child, attr)
      case filter: Filter       => leftOrigin(filter.child, attr)
      case sort: Sort           => leftOrigin(sort.child, attr)
      case window: Window       => leftOrigin(window.child, attr)
      case _                    => None
    }

    val windows = queryNodes.collect { case window: Window => window }
    if (windows.isEmpty) return None
    val keys = windows.map { window =>
      window.partitionSpec match {
        case Seq(attr: Attribute) => leftOrigin(window.child, attr)
        case _                    => None
      }
    }
    if (keys.exists(_.isEmpty)) return None
    val key = keys.head.get
    val affectedName = spark.sessionState.sqlParser.parseExpression(affectedColumn) match {
      case attr: UnresolvedAttribute if attr.nameParts.size == 1 => attr.nameParts.head
      case _                                                     => return None
    }
    if (!resolver(key.name, affectedName) || keys.exists(_.get.exprId != key.exprId)) return None

    val predicate = condition.transform { case attr: AttributeReference =>
      val alias = if (join.left.outputSet.contains(attr)) "openivm_key_base" else "openivm_key_lookup"
      UnresolvedAttribute(Seq(alias, attr.name))
    }.sql
    def quote(value: String): String  = "`" + value.replace("`", "``") + "`"
    def table(source: String): String = ids.find(_._1 == source).get._2.quotedString
    def delta(source: String): String = quote("openivm_delta_" + ids.find(_._1 == source).get._2.table)

    // Signed lookup deltas include both old and new join keys. Current left
    // rows cover lookup-side changes; the direct left-delta arm also covers
    // removed rows and changed partition keys absent from the current table.
    Some(s"""SELECT DISTINCT ${quote(affectedName)} FROM (
              |  SELECT ${quote(key.name)} AS ${quote(affectedName)} FROM ${delta(left)}
              |  UNION ALL
              |  SELECT openivm_key_base.${quote(key.name)} AS ${quote(affectedName)}
              |  FROM ${table(left)} AS openivm_key_base
              |  LEFT SEMI JOIN ${delta(right)} AS openivm_key_lookup ON $predicate
              |) openivm_changed_join_partitions""".stripMargin)
  } catch {
    case NonFatal(error) =>
      logDebug("Retaining conservative affected-window keys for an unsupported analyzed join", error)
      None
  }
}
