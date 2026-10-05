package org.apache.spark.sql.delta.openivm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.{DeltaHistoryManager, DeltaLog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/** Narrow package bridge for Delta's `private[delta]` history-manager APIs
  * needed to resolve symbolic `startingVersion` strategies (issue #63).
  */
object DeltaHistoryAccess {

  /** Delta's sentinel string for a `NULL` partition value, matching Hive's
    * `__HIVE_DEFAULT_PARTITION__` convention (not re-exported publicly by
    * Delta, so duplicated here deliberately rather than depending on a
    * `private[delta]` constant).
    */
  private val NullPartitionValueMarker = "__HIVE_DEFAULT_PARTITION__"

  /** Earliest retained Delta JSON commit version. */
  def earliestDeltaFileVersion(deltaLog: DeltaLog): Long = DeltaHistoryManager.getEarliestDeltaFile(deltaLog)

  /** Earliest version whose snapshot can still be reconstructed from retained
    * commits and checkpoints.
    */
  def earliestRecreatableCommitVersion(deltaLog: DeltaLog): Long = deltaLog.history.getEarliestRecreatableCommit

  /** Newest (`selectLatest = true`) or oldest (`selectLatest = false`)
    * retained commit version containing at least one data-changing `AddFile`
    * whose partition values satisfy `predicate`, or `None` if no retained
    * commit matches.
    *
    * `predicate` is resolved once against a `LocalRelation` shaped like
    * `partitionSchema` — reusing Spark's own analyzer for implicit-cast /
    * type-coercion semantics instead of hand-rolling Delta's partition-value
    * formatting — then re-evaluated per `AddFile` by casting its string
    * partition values to the declared column type and binding them into the
    * resolved expression tree.
    */
  def findMatchingCommit(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicate: Expression,
      partitionSchema: StructType,
      selectLatest: Boolean
  ): Option[Long] = {
    if (partitionSchema.isEmpty) return None
    val relation = LocalRelation(partitionSchema)
    val resolvedCondition = resolvePredicate(spark, predicate, relation) match {
      case Some(condition) => condition
      case None            => return None
    }
    val bound = BindReferences.bindReference(resolvedCondition, relation.output)

    def matches(addFile: AddFile): Boolean =
      try {
        val row = partitionRow(addFile.partitionValues, partitionSchema)
        bound.eval(row) == true
      } catch { case NonFatal(_) => false }

    val start   = earliestRecreatableCommitVersion(deltaLog)
    val changes = deltaLog.getChanges(start, failOnDataLoss = false)

    var found: Option[Long] = None
    changes.foreach { case (version, actions) =>
      val hasMatch = actions.exists {
        case add: AddFile if add.dataChange => matches(add)
        case _                              => false
      }
      if (hasMatch) {
        if (selectLatest) found = Some(version)
        else if (found.isEmpty) found = Some(version)
      }
    }
    found
  }

  /** Resolves `predicate` (whose leaf column references are plain,
    * unqualified names matching `partitionSchema`) against a synthetic
    * `LocalRelation` shaped like `partitionSchema`, applying Spark's normal
    * analyzer type coercion. Returns `None` if analysis fails (e.g. a
    * referenced name is not actually a partition column, or a literal
    * cannot be coerced to the declared type) so the caller can fall back.
    */
  private def resolvePredicate(
      spark: SparkSession,
      predicate: Expression,
      relation: LocalRelation
  ): Option[Expression] =
    try {
      val unresolved = Filter(predicate, relation)
      val analyzed   = spark.sessionState.analyzer.execute(unresolved)
      analyzed match {
        case Filter(condition, _) if condition.resolved => Some(condition)
        case _                                          => None
      }
    } catch { case NonFatal(_) => None }

  private def partitionRow(partitionValues: Map[String, String], partitionSchema: StructType): InternalRow = {
    val values = partitionSchema.fields.map { field =>
      partitionValues.get(field.name) match {
        case None | Some(NullPartitionValueMarker) => null
        case Some(raw) =>
          Cast(Literal(UTF8String.fromString(raw), org.apache.spark.sql.types.StringType), field.dataType)
            .eval(InternalRow.empty)
      }
    }
    InternalRow.fromSeq(values.toIndexedSeq)
  }
}
