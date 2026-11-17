package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate

import java.io.File
import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

private[openivm] object OpenIvmStateBarrier {
  private val barriers = new ConcurrentHashMap[Path, ReentrantReadWriteLock]()

  private[openivm] def root(spark: SparkSession): Path =
    new File(FeatureGate.stateWarehouse(spark), "_openivm").getCanonicalFile.toPath

  private def forRoot(path: Path): ReentrantReadWriteLock =
    barriers.computeIfAbsent(path, _ => new ReentrantReadWriteLock(true))

  private def withRead[A](barrier: ReentrantReadWriteLock, allowBarge: Boolean = false)(body: => A): A = {
    val lock = barrier.readLock()
    if (!allowBarge || !lock.tryLock()) lock.lockInterruptibly()
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
    var path = Paths.get(dbPath).toAbsolutePath.normalize()
    var barrier: ReentrantReadWriteLock = null
    while (path != null && barrier == null) {
      barrier = barriers.get(path)
      path = path.getParent
    }
    if (barrier == null) body else withRead(barrier, allowBarge = true)(body)
  }

  private[rocksdb] def withCapture[A](spark: SparkSession)(body: => A): A = {
    val lock = forRoot(root(spark)).writeLock()
    lock.lockInterruptibly()
    try body
    finally lock.unlock()
  }
}
