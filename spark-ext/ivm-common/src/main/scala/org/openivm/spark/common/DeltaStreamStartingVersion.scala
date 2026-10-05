package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.openivm.DeltaHistoryAccess

import java.util.Locale

/** Symbolic `startingVersion` strategies for Delta streaming sources.
  * Numeric literals and native Delta `latest` are left untouched by
  * [[parse]] and continue to use Delta's own semantics.
  */
sealed trait DeltaStreamStartingVersion {
  def canonicalName: String
}

/** Result of resolving a symbolic strategy to a concrete commit version.
  *
  * `resolution` is `"exact"` when the strategy's own semantics were applied
  * directly (including a predicate-aware match), or `"fallback"` when a
  * predicate-aware strategy could not safely narrow the search and fell back
  * to [[DeltaStreamStartingVersion.EarliestAvailable]] semantics instead —
  * `fallbackReason` then names why (e.g. `"no_partition_predicate"`,
  * `"no_matching_partition_add_file"`). Diagnostic-only: never hashed into
  * the semantic fingerprint.
  */
final case class ResolvedStartingVersion(
    version: Long,
    resolution: String,
    fallbackReason: Option[String] = None,
    effectivePredicate: Option[String] = None
)

object DeltaStreamStartingVersion {

  case object Earliest extends DeltaStreamStartingVersion {
    val canonicalName = "earliest"
  }

  case object EarliestAvailable extends DeltaStreamStartingVersion {
    val canonicalName = "earliestAvailable"
  }

  case object LatestInclusive extends DeltaStreamStartingVersion {
    val canonicalName = "latestInclusive"
  }

  /** Like [[LatestInclusive]], but narrowed to the newest retained commit
    * containing a data-changing `AddFile` whose partition values satisfy a
    * safely extracted partition-only predicate from the query's `WHERE`
    * clause (issue #63 follow-up). Falls back to [[EarliestAvailable]]
    * semantics whenever the predicate cannot be safely used.
    */
  case object LatestInclusiveWithPredicate extends DeltaStreamStartingVersion {
    val canonicalName = "latestInclusiveWithPredicate"
  }

  /** Like [[LatestInclusiveWithPredicate]], but selects the oldest matching
    * retained/reconstructible commit instead of the newest.
    */
  case object EarliestInclusiveWithPredicate extends DeltaStreamStartingVersion {
    val canonicalName = "earliestInclusiveWithPredicate"
  }

  val All: Seq[DeltaStreamStartingVersion] =
    Seq(Earliest, EarliestAvailable, LatestInclusive, LatestInclusiveWithPredicate, EarliestInclusiveWithPredicate)

  /** Recognizes one of the symbolic strategies, case-insensitively. Returns
    * `None` for numeric literals, native `latest`, or any other text so
    * callers can fall through to Delta's own parsing unchanged.
    */
  def parse(value: String): Option[DeltaStreamStartingVersion] =
    Option(value).map(_.trim.toLowerCase(Locale.ROOT)).flatMap { normalized =>
      All.find(_.canonicalName.toLowerCase(Locale.ROOT) == normalized)
    }

  /** `true` for the two predicate-aware strategies. */
  def isPredicateAware(strategy: DeltaStreamStartingVersion): Boolean = strategy match {
    case LatestInclusiveWithPredicate | EarliestInclusiveWithPredicate => true
    case _                                                             => false
  }

  /** Resolves `strategy` to a concrete committed version of `deltaLog`'s
    * current transaction log state. `predicate`/`spark` are required (and
    * only consulted) for the two predicate-aware strategies.
    */
  def resolve(
      spark: SparkSession,
      deltaLog: DeltaLog,
      strategy: DeltaStreamStartingVersion,
      predicate: Option[Expression]
  ): ResolvedStartingVersion = strategy match {
    case Earliest =>
      ResolvedStartingVersion(DeltaHistoryAccess.earliestDeltaFileVersion(deltaLog), resolution = "exact")
    case EarliestAvailable =>
      ResolvedStartingVersion(DeltaHistoryAccess.earliestRecreatableCommitVersion(deltaLog), resolution = "exact")
    case LatestInclusive =>
      ResolvedStartingVersion(deltaLog.update().version, resolution = "exact")
    case LatestInclusiveWithPredicate =>
      resolvePredicateAware(spark, deltaLog, predicate, selectLatest = true)
    case EarliestInclusiveWithPredicate =>
      resolvePredicateAware(spark, deltaLog, predicate, selectLatest = false)
  }

  private def resolvePredicateAware(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicate: Option[Expression],
      selectLatest: Boolean
  ): ResolvedStartingVersion = {
    def fallback(reason: String): ResolvedStartingVersion =
      ResolvedStartingVersion(
        DeltaHistoryAccess.earliestRecreatableCommitVersion(deltaLog),
        resolution = "fallback",
        fallbackReason = Some(reason),
        effectivePredicate = predicate.map(_.sql)
      )

    predicate match {
      case None => fallback("no_partition_predicate")
      case Some(expr) =>
        val snapshot = deltaLog.update()
        DeltaHistoryAccess.findMatchingCommit(
          spark,
          deltaLog,
          expr,
          snapshot.metadata.partitionSchema,
          selectLatest
        ) match {
          case Some(version) =>
            ResolvedStartingVersion(version, resolution = "exact", effectivePredicate = Some(expr.sql))
          case None => fallback("no_matching_partition_add_file")
        }
    }
  }
}
