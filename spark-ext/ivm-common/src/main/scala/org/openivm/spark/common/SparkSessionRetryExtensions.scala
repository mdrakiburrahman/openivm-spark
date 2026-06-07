package org.openivm.spark.common

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

/** Ergonomic retry helpers attached to [[SparkSession]] via an implicit class.
  *
  * Usage:
  * {{{
  *   import org.openivm.spark.common.SparkSessionRetryExtensions._
  *
  *   // Re-run a SQL string with the default Delta-conflict policy:
  *   val df = spark.sqlWithRetry("MERGE INTO mv USING …")
  *
  *   // Run an arbitrary block with custom patterns:
  *   val n = spark.retryOnPatterns(Array("timeout".r, "503".r), maxAttempts = 3, attempt = 1) {
  *     riskyHttpCall()
  *   }
  * }}}
  *
  * Adapted from the upstream `azurearcdata.spark.SparkSessionRetryExtensions`
  * pattern.  The `attempt` argument on `retryOnPatterns` is retained for
  * source-compatibility but ignored — `RetryPolicy` always starts at 1.
  */
object SparkSessionRetryExtensions {

  implicit class SparkSessionOps(spark: SparkSession) {

    /** Run `spark.sql(sqlText)` with retry on the supplied regex patterns.
      *
      * Default `retryPatterns` cover every Delta concurrency conflict that
      * openivm-spark hits in practice (DELTA_METADATA_CHANGED,
      * DELTA_PROTOCOL_CHANGED, DELTA_CONCURRENT_APPEND, DELTA_CONCURRENT_DELETE_*,
      * DELTA_CONCURRENT_TRANSACTION, SparkFileNotFoundException, etc.) —
      * see [[SparkExceptions.DefaultDeltaRetryPatterns]].
      *
      * Default `maxAttempts` is [[RetryPolicy.DefaultMaxAttempts]] (5) with
      * linear 1-s-per-attempt backoff.
      */
    def sqlWithRetry(
        sqlText: String,
        retryPatterns: Array[Regex] = SparkExceptions.DefaultDeltaRetryPatterns.map(_.r),
        maxAttempts: Int = RetryPolicy.DefaultMaxAttempts
    ): DataFrame =
      RetryPolicy(retryPatterns, maxAttempts).execute { spark.sql(sqlText) }

    /** Run an arbitrary by-name operation under a custom retry policy.
      *
      * Provided for compatibility with the upstream API; `attempt` is the
      * caller-visible first-attempt counter and is currently ignored (the
      * underlying [[RetryPolicy]] always starts counting at 1).
      */
    def retryOnPatterns[T](
        patterns: Array[Regex],
        maxAttempts: Int,
        attempt: Int
    )(operation: => T): T = {
      val _ = attempt // reserved for future caller-driven offset
      RetryPolicy(patterns, maxAttempts).execute(operation)
    }
  }
}
