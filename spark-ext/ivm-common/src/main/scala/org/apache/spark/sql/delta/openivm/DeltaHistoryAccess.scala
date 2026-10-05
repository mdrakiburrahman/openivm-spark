package org.apache.spark.sql.delta.openivm

import org.apache.spark.sql.delta.{DeltaHistoryManager, DeltaLog}

/** Narrow package bridge for Delta's `private[delta]` history-manager APIs
  * needed to resolve symbolic `startingVersion` strategies (issue #63).
  */
object DeltaHistoryAccess {

  /** Earliest retained Delta JSON commit version. */
  def earliestDeltaFileVersion(deltaLog: DeltaLog): Long = DeltaHistoryManager.getEarliestDeltaFile(deltaLog)

  /** Earliest version whose snapshot can still be reconstructed from retained
    * commits and checkpoints.
    */
  def earliestRecreatableCommitVersion(deltaLog: DeltaLog): Long = deltaLog.history.getEarliestRecreatableCommit
}
