package org.openivm.spark.common

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import java.util.{IdentityHashMap, Locale}

/** Safe, correctness-preserving extraction of a partition-only predicate for
  * a streaming `UnresolvedRelation`, used by
  * `DeltaStreamStartingVersion.LatestInclusiveWithPredicate` /
  * `EarliestInclusiveWithPredicate` (issue #63 follow-up) to narrow commit
  * discovery without changing what the query actually reads — Spark still
  * applies the complete original predicate at runtime regardless of what is
  * extracted here.
  */
object PartitionPredicateExtraction extends PredicateHelper {

  /** Extracts the conjunction of partition-only conjuncts safely provable
    * from `declaration`'s `WHERE` clause(s) scoped to `relation`, or `None`
    * if nothing usable was found. `relation` is matched by object identity
    * (not structural equality), since two textually-identical `STREAM`
    * references are distinct declared occurrences.
    */
  def extract(
      declaration: LogicalPlan,
      relation: UnresolvedRelation,
      partitionColumns: Set[String]
  ): Option[Expression] = {
    val normalized = partitionColumns.map(_.toLowerCase(Locale.ROOT))
    val scopes     = scopedFiltersByRelation(declaration)
    Option(scopes.get(relation)).flatMap { filters =>
      val supported = filters
        .flatMap(splitConjunctivePredicates)
        .filter(isSupportedPartitionConjunct(_, normalized))
      supported.reduceOption(And(_, _))
    }
  }

  /** Walks `root` top-down, accumulating enclosing `Filter` conjuncts through
    * only "transparent" nodes (`Project`, `SubqueryAlias`, chained `Filter`,
    * `EventTimeWatermark`, and CTE definitions). Any other node — in
    * particular `Join`, `Union`, and `Aggregate` — resets accumulation for
    * each of its children, so a predicate can never leak across sources or
    * survive an operator this extraction doesn't understand: being more
    * conservative only means falling back more often, never narrowing
    * unsafely.
    */
  private def scopedFiltersByRelation(root: LogicalPlan): IdentityHashMap[UnresolvedRelation, Seq[Expression]] = {
    val result = new IdentityHashMap[UnresolvedRelation, Seq[Expression]]()

    def walk(plan: LogicalPlan, filters: Seq[Expression]): Unit = plan match {
      case Filter(condition, child)        => walk(child, filters :+ condition)
      case Project(_, child)               => walk(child, filters)
      case SubqueryAlias(_, child)         => walk(child, filters)
      case EventTimeWatermark(_, _, child) => walk(child, filters)
      case UnresolvedWith(child, cteRelations) =>
        cteRelations.foreach { case (_, alias) => walk(alias, Seq.empty) }
        walk(child, filters)
      case relation: UnresolvedRelation if relation.isStreaming =>
        result.put(relation, filters)
      case other => other.children.foreach(child => walk(child, Seq.empty))
    }

    walk(root, Seq.empty)
    result
  }

  private def isSupportedPartitionConjunct(expr: Expression, partitionColumns: Set[String]): Boolean =
    expr.deterministic && (expr match {
      case EqualTo(left, right)            => isSupportedComparison(left, right, partitionColumns)
      case GreaterThan(left, right)        => isSupportedComparison(left, right, partitionColumns)
      case GreaterThanOrEqual(left, right) => isSupportedComparison(left, right, partitionColumns)
      case LessThan(left, right)           => isSupportedComparison(left, right, partitionColumns)
      case LessThanOrEqual(left, right)    => isSupportedComparison(left, right, partitionColumns)
      case In(value, list)  => isPartitionColumnRef(value, partitionColumns) && list.forall(isLiteralOrCast)
      case IsNull(child)    => isPartitionColumnRef(child, partitionColumns)
      case IsNotNull(child) => isPartitionColumnRef(child, partitionColumns)
      case And(left, right) =>
        isSupportedPartitionConjunct(left, partitionColumns) && isSupportedPartitionConjunct(right, partitionColumns)
      case _ => false
    })

  private def isSupportedComparison(left: Expression, right: Expression, partitionColumns: Set[String]): Boolean =
    (isPartitionColumnRef(left, partitionColumns) && isLiteralOrCast(right)) ||
      (isPartitionColumnRef(right, partitionColumns) && isLiteralOrCast(left))

  private def isPartitionColumnRef(expr: Expression, partitionColumns: Set[String]): Boolean = expr match {
    case u: UnresolvedAttribute => partitionColumns.contains(u.nameParts.last.toLowerCase(Locale.ROOT))
    case a: AttributeReference  => partitionColumns.contains(a.name.toLowerCase(Locale.ROOT))
    case _                      => false
  }

  private def isLiteralOrCast(expr: Expression): Boolean = expr match {
    case _: Literal => true
    case c: Cast    => isLiteralOrCast(c.child)
    case _          => false
  }
}
