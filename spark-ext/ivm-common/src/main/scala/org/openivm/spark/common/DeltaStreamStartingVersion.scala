package org.openivm.spark.common

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

  val All: Seq[DeltaStreamStartingVersion] = Seq(Earliest, EarliestAvailable, LatestInclusive)

  /** Recognizes one of the three symbolic strategies, case-insensitively.
    * Returns `None` for numeric literals, native `latest`, or any other text
    * so callers can fall through to Delta's own parsing unchanged.
    */
  def parse(value: String): Option[DeltaStreamStartingVersion] =
    Option(value).map(_.trim.toLowerCase(Locale.ROOT)).flatMap { normalized =>
      All.find(_.canonicalName.toLowerCase(Locale.ROOT) == normalized)
    }

  /** Resolves `strategy` to a concrete committed version of `deltaLog`'s
    * current transaction log state.
    */
  def resolve(deltaLog: DeltaLog, strategy: DeltaStreamStartingVersion): Long = strategy match {
    case Earliest          => DeltaHistoryAccess.earliestDeltaFileVersion(deltaLog)
    case EarliestAvailable => DeltaHistoryAccess.earliestRecreatableCommitVersion(deltaLog)
    case LatestInclusive   => deltaLog.update().version
  }
}
