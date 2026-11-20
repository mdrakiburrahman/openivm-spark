package org.openivm.spark.common.rocksdb

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, OpenIvmStatePaths}

import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

private[openivm] object OpenIvmStateBarrier {
  private final class RootState {
    val lock: ReentrantReadWriteLock   = new ReentrantReadWriteLock(true)
    val mutationGeneration: AtomicLong = new AtomicLong(0L)
    private val mutationMonitor        = new Object
    private var activeMutations        = 0

    def beginMutation(): Unit = mutationMonitor.synchronized {
      activeMutations += 1
    }

    def finishMutation(): Unit = mutationMonitor.synchronized {
      mutationGeneration.incrementAndGet()
      activeMutations -= 1
      mutationMonitor.notifyAll()
    }

    def requestMutationGeneration(): Long = mutationMonitor.synchronized {
      mutationGeneration.get() + activeMutations
    }

    def recordMutation(): Unit = mutationMonitor.synchronized { mutationGeneration.incrementAndGet() }
  }

  private val roots                  = new ConcurrentHashMap[Path, RootState]()
  private val rootOverrideForTesting = new ThreadLocal[Path]()
  @volatile private var stateSyncConfiguredHookForTesting: SparkSession => Boolean = null

  private def stateSyncConfigured(spark: SparkSession): Boolean = {
    val hook = stateSyncConfiguredHookForTesting
    if (hook == null) FeatureGate.stateSyncUri(spark).isDefined else hook(spark)
  }

  private[openivm] def setStateSyncConfiguredHookForTesting(hook: SparkSession => Boolean): Unit =
    stateSyncConfiguredHookForTesting = hook

  private[openivm] def root(spark: SparkSession): Path =
    Option(rootOverrideForTesting.get())
      .getOrElse(OpenIvmStatePaths.openIvmRoot(spark))
      .toFile
      .getCanonicalFile
      .toPath

  private[rocksdb] def withRootForTesting[A](path: Path)(body: => A): A = {
    val previous = rootOverrideForTesting.get()
    rootOverrideForTesting.set(path)
    try body
    finally if (previous == null) rootOverrideForTesting.remove() else rootOverrideForTesting.set(previous)
  }

  private def forRoot(path: Path): RootState =
    roots.computeIfAbsent(path, _ => new RootState)

  private def forDbPath(dbPath: String): RootState = {
    var path             = Paths.get(dbPath).toAbsolutePath.normalize()
    var state: RootState = null
    while (path != null && state == null) {
      state = roots.get(path)
      path = path.getParent
    }
    state
  }

  private def withRead[A](state: RootState)(body: => A): A = {
    val lock = state.lock.readLock()
    lock.lock()
    try body
    finally lock.unlock()
  }

  def withMutation[A](spark: SparkSession)(body: => A): A =
    if (stateSyncConfigured(spark)) {
      OpenIvmStateSync.maybeRestore(spark)
      val state = forRoot(root(spark))
      withRead(state) {
        state.beginMutation()
        try body
        finally state.finishMutation()
      }
    } else body

  private[rocksdb] def withCurrentMutationGeneration[A](spark: SparkSession)(body: Long => A): A =
    if (stateSyncConfigured(spark)) {
      OpenIvmStateSync.maybeRestore(spark)
      val state = forRoot(root(spark))
      withRead(state)(body(state.requestMutationGeneration()))
    } else body(0L)

  private[rocksdb] def currentMutationGeneration(spark: SparkSession): Long =
    forRoot(root(spark)).mutationGeneration.get()

  private[rocksdb] def markRestoredMutationGeneration(spark: SparkSession): Unit =
    forRoot(root(spark)).recordMutation()

  private[rocksdb] def register(spark: SparkSession): Unit = {
    forRoot(root(spark))
    ()
  }

  private[openivm] def withDbOperation[A](dbPath: String)(body: => A): A = {
    val state = forDbPath(dbPath)
    if (state == null) body else withRead(state)(body)
  }

  private[openivm] def withDbMutation[A](dbPath: String)(body: => A): A = {
    val state = forDbPath(dbPath)
    if (state == null) body
    else
      withRead(state) {
        state.beginMutation()
        try body
        finally state.finishMutation()
      }
  }

  private[spark] def currentMutationGenerationForTesting(spark: SparkSession): Long =
    currentMutationGeneration(spark)

  private[common] def captureQueuedForTesting(spark: SparkSession): Boolean =
    forRoot(root(spark)).lock.hasQueuedThreads

  private[rocksdb] def withMutationRootForTesting(path: Path)(body: => Unit): Unit = {
    val state = forRoot(path.toFile.getCanonicalFile.toPath)
    withRead(state) {
      state.beginMutation()
      try body
      finally state.finishMutation()
    }
  }

  private[rocksdb] def withCaptureRootForTesting(path: Path)(body: => Unit): Unit = {
    val state = forRoot(path.toFile.getCanonicalFile.toPath)
    val lock  = state.lock.writeLock()
    lock.lockInterruptibly()
    try body
    finally lock.unlock()
  }

  private[rocksdb] def captureQueuedForTesting(path: Path): Boolean =
    forRoot(path.toFile.getCanonicalFile.toPath).lock.hasQueuedThreads

  private[rocksdb] def mutationGenerationForTesting(path: Path): Long =
    forRoot(path.toFile.getCanonicalFile.toPath).mutationGeneration.get()

  private[rocksdb] def requestMutationGenerationForTesting(path: Path): Long =
    forRoot(path.toFile.getCanonicalFile.toPath).requestMutationGeneration()

  private[rocksdb] def withCapture[A](spark: SparkSession)(body: => A): A = {
    val state = forRoot(root(spark))
    if (state.lock.getReadHoldCount > 0 && !state.lock.isWriteLockedByCurrentThread)
      throw new IllegalStateException("OpenIVM state capture cannot run inside a state mutation")
    val lock = state.lock.writeLock()
    lock.lockInterruptibly()
    try body
    finally lock.unlock()
  }
}
