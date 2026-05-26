package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, ThreadFactory, TimeUnit}

/** Off-thread RocksDB flusher for [[RefreshSqlLogCatalog]] so the
  * `spark-openivm` refresh timer (`batch.duration_s` on the bench side) is
  * NOT penalised by query-log persistence.
  *
  * Design (see plan.md "Timer-neutrality"):
  *
  *  - Inline `RefreshSqlLog.record(...)` per statement is ~200 ns
  *    (one `currentTimeMillis` + one `ArrayBuffer.append`). No IO, no
  *    `nanoTime`, no synchronization. The buffer lives on the calling
  *    thread's stack (per-refresh `RefreshSqlLog` instance) so no contention.
  *
  *  - End-of-refresh `RefreshSqlLog.flush()` enqueues the full buffer as a
  *    single `FlushBatch` into this singleton's bounded queue and returns
  *    immediately. The dedicated daemon thread drains the queue and writes
  *    to RocksDB off the hot path.
  *
  *  - `SHOW OPENIVM QUERY LOG` calls `awaitQuiescence(60s)` first to drain
  *    any in-flight batches, then scans `RefreshSqlLogCatalog`. This barrier
  *    runs OUTSIDE the bench timer (bench-server's `_export_spark_openivm_query_log`
  *    is called after `stream_progress` returns, off-timer).
  *
  *  - JVM shutdown hook drains with a 30-second timeout so rows are not
  *    lost on graceful exit.
  *
  *  - Bounded queue (`queueCapacity` batches). If the queue fills up,
  *    the submitting thread falls back to inline-flush so the refresh
  *    NEVER blocks indefinitely on a pathological backlog. The
  *    failure-mode is bounded: at worst we pay the RocksDB write cost
  *    inline for one batch, then continue normally.
  */
object RefreshSqlLogAsyncFlusher {

  private val log = LoggerFactory.getLogger(getClass)

  /** Queue capacity in batches. Each batch is one CREATE/REFRESH lifecycle.
    * 1024 is enough for tens of seconds of arrears at typical bench rates,
    * well beyond what a healthy steady-state needs.
    */
  private val queueCapacity: Int = 1024

  // A flush batch carries everything the worker needs to call
  // RefreshSqlLogCatalog.record. We keep the SparkSession reference so the
  // worker can open the RocksDB on the writer thread (idempotent open).
  private final case class FlushBatch(spark: SparkSession, rows: Seq[RefreshSqlLogRow])
  private case object Sentinel { // marker for awaitQuiescence
    val ticket: Long = 0L
  }
  private final case class Quiesce(ticket: Long)

  /** Each pending message is one of:
    *   - FlushBatch  → write rows
    *   - Quiesce(t)  → signal the corresponding latch in `quiesceAcks` once processed
    */
  private val queue = new ArrayBlockingQueue[AnyRef](queueCapacity)

  // Tickets for awaitQuiescence: we use a monotonic counter + a map of
  // latches indexed by ticket. The worker decrements latches as it processes
  // Quiesce markers.
  private val quiesceTicketCounter = new AtomicLong(0L)
  private val quiesceAcks =
    new java.util.concurrent.ConcurrentHashMap[java.lang.Long, java.util.concurrent.CountDownLatch]()

  // Statistics for observability.
  private val droppedInlineFallbacks = new AtomicLong(0L)
  private val flushedBatches         = new AtomicLong(0L)
  private val flushFailures          = new AtomicLong(0L)

  private val threadFactory: ThreadFactory = new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, "openivm-querylog-flusher")
      t.setDaemon(true)
      t
    }
  }

  // Single-thread worker started lazily — the bulk of openivm-spark sessions
  // don't enable the query-log gate and thus never need this thread.
  private val workerStarted = new java.util.concurrent.atomic.AtomicBoolean(false)

  private def ensureWorker(): Unit =
    if (workerStarted.compareAndSet(false, true)) {
      val worker = threadFactory.newThread(new Runnable {
        override def run(): Unit = workerLoop()
      })
      worker.start()
      // Best-effort drain on graceful shutdown so we don't lose the last batch.
      Runtime.getRuntime.addShutdownHook(
        new Thread(
          () => {
            try {
              // We can't safely call any Spark API here, but the queue is
              // already empty in the common case where the worker keeps up.
              // Block up to 30s for the queue to drain.
              awaitQuiescence(timeoutMs = 30000L)
            } catch { case _: Throwable => () }
          },
          "openivm-querylog-shutdown"
        )
      )
    }

  private def workerLoop(): Unit = {
    while (true) {
      val msg =
        try queue.take()
        catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            return
        }
      msg match {
        case b: FlushBatch =>
          try {
            RefreshSqlLogCatalog.record(b.spark, b.rows)
            flushedBatches.incrementAndGet()
            ()
          } catch {
            case t: Throwable =>
              flushFailures.incrementAndGet()
              log.warn(
                s"[openivm-querylog] async flush failed for ${b.rows.size} rows: " +
                  s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}"
              )
          }
        case Quiesce(ticket) =>
          val latch = quiesceAcks.remove(java.lang.Long.valueOf(ticket))
          if (latch != null) latch.countDown()
        case other =>
          log.warn(s"[openivm-querylog] unexpected message type: ${other.getClass.getName}")
      }
    }
  }

  /** Submit a batch for async write. Returns immediately when the queue has
    * capacity; falls back to inline write when the queue is full so the
    * caller (which is on the refresh hot path) is never blocked.
    *
    * Idempotent for empty `rows` — no work scheduled.
    */
  def submit(spark: SparkSession, rows: Seq[RefreshSqlLogRow]): Unit = {
    if (rows.isEmpty) return
    ensureWorker()
    val batch = FlushBatch(spark, rows)
    if (!queue.offer(batch)) {
      droppedInlineFallbacks.incrementAndGet()
      log.warn(
        s"[openivm-querylog] async queue full (cap=$queueCapacity); falling back to inline flush"
      )
      try RefreshSqlLogCatalog.record(spark, rows)
      catch {
        case t: Throwable =>
          flushFailures.incrementAndGet()
          log.warn(
            s"[openivm-querylog] inline-fallback flush failed for ${rows.size} rows: " +
              s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}"
          )
      }
    }
  }

  /** Block (up to `timeoutMs`) until all batches submitted BEFORE this call
    * have been written. Returns true if the queue drained, false on timeout.
    *
    * Implementation: post a `Quiesce` marker after every currently-queued
    * batch, then wait on the latch. Because the worker is single-threaded
    * and processes in FIFO order, by the time it acks the marker every
    * earlier batch is already persisted.
    *
    * Safe to call when the worker has never started (returns true immediately).
    */
  def awaitQuiescence(timeoutMs: Long): Boolean = {
    if (!workerStarted.get() && queue.isEmpty) return true
    ensureWorker()
    val ticket = quiesceTicketCounter.incrementAndGet()
    val latch  = new java.util.concurrent.CountDownLatch(1)
    quiesceAcks.put(java.lang.Long.valueOf(ticket), latch)
    if (!queue.offer(Quiesce(ticket))) {
      // Queue full — drain by force: spin briefly to wait for capacity.
      // This path is exceedingly rare (submit() also falls back inline) but
      // we cannot fail the caller, so we just keep retrying.
      val deadlineNs = System.nanoTime() + timeoutMs * 1000000L
      var enqueued   = false
      while (!enqueued && System.nanoTime() < deadlineNs) {
        enqueued = queue.offer(Quiesce(ticket), 10L, TimeUnit.MILLISECONDS)
      }
      if (!enqueued) {
        quiesceAcks.remove(java.lang.Long.valueOf(ticket))
        return false
      }
    }
    latch.await(timeoutMs, TimeUnit.MILLISECONDS)
  }

  /** Diagnostics — count of rows lost (none, by design) and stats. */
  def stats: Map[String, Long] = Map(
    "flushedBatches"         -> flushedBatches.get(),
    "flushFailures"          -> flushFailures.get(),
    "droppedInlineFallbacks" -> droppedInlineFallbacks.get(),
    "queueSize"              -> queue.size().toLong
  )

  /** Test-only — reset stats counters. Does NOT reset the queue / worker. */
  private[common] def resetStatsForTesting(): Unit = {
    droppedInlineFallbacks.set(0L)
    flushedBatches.set(0L)
    flushFailures.set(0L)
  }
}
