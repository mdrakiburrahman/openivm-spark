package org.openivm.spark.common

/** Mix this trait into any object/class that needs ergonomic Delta-Lake retry
  * semantics around a block of code.
  *
  * Usage:
  * {{{
  *   object MvCatalog extends DeltaRetrySupport {
  *     def upsert(spark: SparkSession, m: MvMetadata): Unit = withDeltaRetry {
  *       // … Delta MERGE that may hit DELTA_CONCURRENT_APPEND / METADATA_CHANGED …
  *     }
  *   }
  * }}}
  *
  * Backed by [[RetryPolicy.DeltaConflicts]] which retries up to 5 times with
  * linear backoff on any exception (or cause) whose class name or message
  * matches one of [[SparkExceptions.DefaultDeltaRetryPatterns]].
  */
trait DeltaRetrySupport {

  /** Execute `operation` with the default Delta-conflict retry policy. */
  def withDeltaRetry[T](operation: => T): T =
    RetryPolicy.DeltaConflicts.execute(operation)

  /** Execute `operation` with a caller-supplied retry policy. */
  def withRetry[T](policy: RetryPolicy)(operation: => T): T =
    policy.execute(operation)
}
