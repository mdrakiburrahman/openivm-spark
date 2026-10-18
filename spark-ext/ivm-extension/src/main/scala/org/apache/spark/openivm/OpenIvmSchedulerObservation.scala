package org.apache.spark.openivm

import org.apache.spark.SparkContext

/** Keeps listener-backed absence of jobs distinct from unavailable scheduler evidence. */
object OpenIvmSchedulerObservation {

  /** Return confirmed activity, or unknown when the status listener has lost or queued events.
    *
    * @param context Spark context owning the streaming writer
    * @param runId native streaming run's Spark job group
    */
  def workActive(context: SparkContext, runId: String): Option[Boolean] = {
    val metrics = context.listenerBus.metrics.metricRegistry
    def dropped = Option(metrics.getCounters.get("queue.appStatus.numDroppedEvents")).map(_.getCount)
    def caughtUp = Option(metrics.getGauges.get("queue.appStatus.size")).exists { gauge =>
      gauge.getValue match {
        case size: java.lang.Number => size.longValue() == 0L
        case _                      => false
      }
    }

    val before  = dropped
    val tracker = context.statusTracker
    val active  = tracker.getActiveJobIds().toSet
    val grouped = tracker.getJobIdsForGroup(runId)
    if (grouped.exists(active.contains)) Some(true)
    else if (!before.contains(0L) || !caughtUp || !dropped.contains(0L)) None
    else if (active.forall(id => tracker.getJobInfo(id).isDefined)) Some(false)
    else None
  }
}
