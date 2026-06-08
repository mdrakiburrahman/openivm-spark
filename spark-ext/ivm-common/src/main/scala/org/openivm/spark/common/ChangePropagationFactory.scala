package org.openivm.spark.common

import org.apache.spark.sql.SparkSession

import java.util.Collections

/**
 * Per-[[SparkSession]] [[ChangePropagation]] cache.  Mirrors the
 * weak-keyed pattern used by `OpenIvmCompilers.forSession` so two threads in
 * the same session see one instance, while different sessions can race
 * different modes.
 */
object ChangePropagationFactory {

  private val cache: java.util.Map[SparkSession, ChangePropagation] =
    Collections.synchronizedMap(new java.util.WeakHashMap[SparkSession, ChangePropagation]())

  def forSession(spark: SparkSession): ChangePropagation = {
    val existing = cache.get(spark)
    if (existing != null) return existing
    cache.synchronized {
      val again = cache.get(spark)
      if (again != null) return again
      val impl = ChangeFeedMode.fromSession(spark) match {
        case ChangeFeedMode.Intercept => new InterceptChangePropagation
        case ChangeFeedMode.Cdf       => new CdfChangePropagation
      }
      cache.put(spark, impl)
      impl
    }
  }

  /** Test hook: drop the cached entry for `spark` so the next `forSession`
    *  re-reads the current `spark.openivm.changeFeed.mode`. */
  private[openivm] def evict(spark: SparkSession): Unit =
    cache.synchronized {
      cache.remove(spark)
      ()
    }
}
