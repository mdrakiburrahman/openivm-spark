package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, OpenIvmStatePaths}

import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

private[openivm] object OpenIvmStateBarrier {
  private final class GcTombstoneState {
    val epoch: AtomicLong           = new AtomicLong(0L)
    val knownEmptyEpoch: AtomicLong = new AtomicLong(-1L)
  }

  private val barriers     = new ConcurrentHashMap[Path, ReentrantReadWriteLock]()
  private val gcTombstones = new ConcurrentHashMap[Path, GcTombstoneState]()

  private[openivm] def root(spark: SparkSession): Path =
    OpenIvmStatePaths.openIvmRoot(spark).toFile.getCanonicalFile.toPath

  private def forRoot(path: Path): ReentrantReadWriteLock =
    barriers.computeIfAbsent(path, _ => new ReentrantReadWriteLock(true))

  private def gcState(spark: SparkSession): GcTombstoneState =
    gcTombstones.computeIfAbsent(root(spark), _ => new GcTombstoneState)

  private[common] def gcTombstoneEpoch(spark: SparkSession): Long = gcState(spark).epoch.get()

  private[common] def gcTombstonesKnownEmpty(spark: SparkSession, epoch: Long): Boolean =
    !OpenIvmRocksDBConf.fromSpark(spark).multiProcess && {
      val state = gcState(spark)
      state.epoch.get() == epoch && state.knownEmptyEpoch.get() == epoch
    }

  private[common] def markGcTombstonesEmptyIfUnchanged(spark: SparkSession, epoch: Long): Unit =
    if (!OpenIvmRocksDBConf.fromSpark(spark).multiProcess) {
      val state = gcState(spark)
      if (state.epoch.get() == epoch) state.knownEmptyEpoch.set(epoch)
    }

  private[openivm] def markGcTombstonesChanged(spark: SparkSession): Unit = {
    val state = gcState(spark)
    state.epoch.incrementAndGet()
    state.knownEmptyEpoch.set(-1L)
  }

  private def withRead[A](barrier: ReentrantReadWriteLock)(body: => A): A = {
    val lock = barrier.readLock()
    lock.lock()
    try body
    finally lock.unlock()
  }

  def withMutation[A](spark: SparkSession)(body: => A): A =
    if (FeatureGate.stateSyncUri(spark).isDefined) {
      OpenIvmStateSync.maybeRestore(spark)
      withRead(forRoot(root(spark)))(body)
    } else body

  private[rocksdb] def register(spark: SparkSession): Unit = {
    forRoot(root(spark))
    ()
  }

  private[openivm] def withDbOperation[A](dbPath: String)(body: => A): A = {
    var path                            = Paths.get(dbPath).toAbsolutePath.normalize()
    var barrier: ReentrantReadWriteLock = null
    while (path != null && barrier == null) {
      barrier = barriers.get(path)
      path = path.getParent
    }
    if (barrier == null) body else withRead(barrier)(body)
  }

  private[rocksdb] def withCapture[A](spark: SparkSession)(body: => A): A = {
    val barrier = forRoot(root(spark))
    if (barrier.getReadHoldCount > 0 && !barrier.isWriteLockedByCurrentThread)
      throw new IllegalStateException("OpenIVM state capture cannot run inside a state mutation")
    val lock = barrier.writeLock()
    lock.lockInterruptibly()
    try body
    finally lock.unlock()
  }
}
