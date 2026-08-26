package org.openivm.spark.testkit

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/** Thread-pool lifecycle shared by every concurrency test.
  *
  * Concurrency tests park production commands inside injected hooks, so a
  * failed assertion can leave a worker thread parked with no one left to
  * release it.  `shutdown()` alone never reclaims such a thread: it only stops
  * new submissions.  A pool drained that way keeps a non-daemon worker — and
  * therefore the whole forked test JVM — alive well past the failure, which is
  * how one bad test turns into a suite that "hangs" under full-suite load.
  *
  * Always pair this with [[ParkedCommandBarrier.use]] NESTED INSIDE the pool
  * scope, so the parked command is released before the pool is drained:
  *
  * {{{
  * TestPools.withPool(2) { implicit ec =>
  *   barrier.use { ... }          // releases here ...
  * }                              // ... then the pool drains cleanly
  * }}}
  */
private[spark] object TestPools {

  val DrainTimeout: FiniteDuration = 30.seconds

  def withPool[A](parallelism: Int, drainTimeout: FiniteDuration = DrainTimeout)(
      body: ExecutionContext => A
  ): A = {
    val pool = Executors.newFixedThreadPool(parallelism)
    try body(ExecutionContext.fromExecutorService(pool))
    finally drain(pool, drainTimeout)
  }

  /** Polite shutdown, then interrupt whatever ignored it. Returns `true` when
    * the pool drained without needing the interrupt.
    */
  def drain(pool: ExecutorService, drainTimeout: FiniteDuration = DrainTimeout): Boolean = {
    val timeoutMs = drainTimeout.toMillis
    pool.shutdown()
    if (pool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) true
    else {
      pool.shutdownNow()
      pool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)
      false
    }
  }
}
