package org.openivm.spark.common.rocksdb

import java.io.IOException
import java.util.UUID
import java.util.function.Supplier
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate

/** Explicit driver ingress for an isolated, pre-provisioned guarded checkpoint namespace.
  * This does not select a state-sync mode for SQL commands or authorize other cleanup routes.
  */
final class GuardedCheckpointDriver(
    spark: SparkSession,
    root: String,
    storageCredential: Supplier[String]
) {
  import GuardedSnapshotDecisions.Generation

  private val store     = new OneLakeGuardedStore(OneLakeGuardedPaths(root, () => storageCredential.get()))
  private val decisions = new GuardedSnapshotDecisions(store)

  def publish(): String = {
    requireIsolated()
    val previous   = decisions.latestPublished().map(_.generation.value.take(19).toLong).getOrElse(0L)
    val stamp      = math.max(System.currentTimeMillis(), Math.addExact(previous, 1L))
    val generation = Generation(f"$stamp%019d-${UUID.randomUUID()}")
    OpenIvmStateSync.publishGuarded(spark, generation, store)
    generation.value
  }

  /** Restore into an empty local state root; return the manifest generation actually installed. */
  def restore(): java.util.Optional[String] = {
    requireIsolated()
    OpenIvmStateSync.restoreGuarded(spark, store) match {
      case Some(generation) => java.util.Optional.of(generation.value)
      case None             => java.util.Optional.empty[String]()
    }
  }

  /** A caller must name a finished generation; no age-based sweep or unsealed uploads. */
  def retire(generation: String): Boolean = {
    requireIsolated()
    store.verifyNamespace()
    val receipt = store
      .readFinished(Generation(generation))
      .getOrElse(throw new IOException("guarded checkpoint upload is not sealed"))
    decisions.retire(receipt)
  }

  private def requireIsolated(): Unit = {
    if (spark.sparkContext.isStopped || FeatureGate.stateSyncUri(spark).nonEmpty)
      throw new IOException(
        "guarded checkpoint qualification requires an active Spark application with no legacy state-sync URI"
      )
  }
}
