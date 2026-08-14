package org.openivm.spark.common

import org.slf4j.LoggerFactory

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** A retry policy for operations that may fail with transient errors.
  *
  * @param patterns     Compiled regex patterns matched against the exception
  *                     class name AND message of every exception in the cause
  *                     chain.  A match triggers a retry.
  * @param maxAttempts  Maximum total attempts including the first (default 5).
 * @param backoffMs    Per-attempt backoff in milliseconds; multiplied by the
 *                     attempt number for a linear ramp.
 * @param jitterMs     Maximum random delay added to each retry. Jitter keeps
 *                     independent Spark drivers from retrying in lockstep.
 */
final case class RetryPolicy(
    patterns: Array[Regex],
    maxAttempts: Int = RetryPolicy.DefaultMaxAttempts,
    backoffMs: Long = RetryPolicy.DefaultBackoffMs,
    jitterMs: Long = 0L
) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Execute `operation` with this retry policy.  `operation` is captured
    * by-name so each retry re-evaluates the block.
    */
  def execute[T](operation: => T): T = executeInternal(operation, attempt = 1)

  /** Execute `operation` with this retry policy, passing the 1-based attempt
    * number on each invocation. Use this when the body needs to record
    * per-attempt telemetry (e.g. query-log rows tagged with `attempt_idx`).
    *
    * `operation` is invoked as a regular function on each retry, NOT
    * captured by-name. From a retry-semantics standpoint it is identical to
    * [[execute]].
    */
  def executeWithAttempt[T](operation: Int => T): T =
    executeWithAttemptInternal(operation, attempt = 1)

  private def executeWithAttemptInternal[T](operation: Int => T, attempt: Int): T = {
    Try(operation(attempt)) match {
      case Success(result) =>
        if (attempt > 1) {
          log.info(s"openivm-spark retry: operation succeeded on attempt $attempt")
        }
        result

      case Failure(exception) if attempt < maxAttempts && matchesPattern(exception) =>
        val sleep = retryDelayMs(attempt)
        log.warn(
          s"openivm-spark retry: retryable error on attempt $attempt/$maxAttempts " +
            s"— sleeping ${sleep}ms then retrying. Cause: ${exception.getClass.getSimpleName}: " +
            Option(exception.getMessage).getOrElse("<no message>").linesIterator.next()
        )
        Thread.sleep(sleep)
        executeWithAttemptInternal(operation, attempt + 1)

      case Failure(exception) =>
        if (attempt > 1) {
          log.error(s"openivm-spark retry: operation failed after $attempt attempts", exception)
        }
        throw exception
    }
  }

  private def executeInternal[T](operation: => T, attempt: Int): T = {
    Try(operation) match {
      case Success(result) =>
        if (attempt > 1) {
          log.info(s"openivm-spark retry: operation succeeded on attempt $attempt")
        }
        result

      case Failure(exception) if attempt < maxAttempts && matchesPattern(exception) =>
        val sleep = retryDelayMs(attempt)
        log.warn(
          s"openivm-spark retry: retryable error on attempt $attempt/$maxAttempts " +
            s"— sleeping ${sleep}ms then retrying. Cause: ${exception.getClass.getSimpleName}: " +
            Option(exception.getMessage).getOrElse("<no message>").linesIterator.next()
        )
        Thread.sleep(sleep)
        executeInternal(operation, attempt + 1)

      case Failure(exception) =>
        if (attempt > 1) {
          log.error(s"openivm-spark retry: operation failed after $attempt attempts", exception)
        }
        throw exception
    }
  }

  private def matchesPattern(exception: Throwable): Boolean =
    getExceptionChain(exception).exists { ex =>
      val className = ex.getClass.getName
      val message   = Option(ex.getMessage).getOrElse("")
      patterns.exists { p =>
        p.findFirstIn(className).isDefined || p.findFirstIn(message).isDefined
      }
    }

  private def getExceptionChain(exception: Throwable): List[Throwable] = {
    @scala.annotation.tailrec
    def collect(ex: Throwable, acc: List[Throwable]): List[Throwable] =
      if (ex == null) acc else collect(ex.getCause, ex :: acc)
    collect(exception, Nil).reverse
  }

  private def retryDelayMs(attempt: Int): Long = {
    val jitter =
      if (jitterMs <= 0L) 0L
      else java.util.concurrent.ThreadLocalRandom.current().nextLong(jitterMs + 1L)
    backoffMs * attempt + jitter
  }
}

/** Companion object with predefined policies.
  */
object RetryPolicy {

  /** Total attempt cap.  Five attempts × linear 1 s backoff = up to ~15 s of
    * retrying per operation.  Tune via the constructor argument if needed.
    */
  val DefaultMaxAttempts: Int = 5

  /** Per-attempt linear backoff base.  Attempt N waits `backoffMs * N` ms. */
  val DefaultBackoffMs: Long = 1000

  /** Retry policy keyed on Delta + Spark OCC conflict signatures from
    * [[SparkExceptions.DefaultDeltaRetryPatterns]].  This is the default
    * policy used by [[DeltaRetrySupport]] / [[RetryExtensions.RetryableOps.withDeltaRetry]].
    */
  val DeltaConflicts: RetryPolicy =
    RetryPolicy(SparkExceptions.DefaultDeltaRetryPatterns.map(_.r))

  /** Longer, jittered retry window for the small shared Delta catalog tables.
    * Metadata commits are cheap, but many independent Spark drivers can keep
    * the same catalog partition busy for longer than the query-write policy's
    * five attempts.
    */
  val DeltaCatalogConflicts: RetryPolicy =
    RetryPolicy(
      SparkExceptions.DefaultDeltaRetryPatterns.map(_.r),
      maxAttempts = 20,
      backoffMs = 200L,
      jitterMs = 250L
    )
}
