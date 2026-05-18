package org.openivm.spark.common

/** Implicit syntax for `{ … }.withDeltaRetry` / `{ … }.withRetry(policy)`.
  *
  * Usage:
  * {{{
  *   import org.openivm.spark.common.RetryExtensions._
  *
  *   { spark.sql("MERGE INTO …").collect() }.withDeltaRetry
  *   { riskyHttpCall() }.withRetry(RetryPolicy(Array("timeout".r, "503".r)))
  * }}}
  */
object RetryExtensions {

  /** By-name wrapper to hang the retry methods off any expression. */
  implicit class RetryableOps[T](operation: => T) {

    /** Apply the supplied retry policy. */
    def withRetry(policy: RetryPolicy): T = policy.execute(operation)

    /** Apply the default Delta-conflict retry policy. */
    def withDeltaRetry: T = RetryPolicy.DeltaConflicts.execute(operation)
  }
}
