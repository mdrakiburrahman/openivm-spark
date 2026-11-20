package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, OpenIvmStatePaths}

import java.nio.file.{Path => NioPath, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.NonFatal

private[openivm] object OpenIvmStateBarrier {
  private final class RootState {
    val lock: ReentrantReadWriteLock   = new ReentrantReadWriteLock(true)
    val mutationGeneration: AtomicLong = new AtomicLong(0L)
    @volatile var trackingEnabled     = false
    private val mutationMonitor       = new Object
    private var activeMutations       = 0

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

  private val roots = new ConcurrentHashMap[NioPath, RootState]()
  private val rootOverrideForTesting = new ThreadLocal[NioPath]()
  @volatile private var stateSyncConfiguredHookForTesting: SparkSession => Boolean = null

  private def stateSyncConfigured(spark: SparkSession): Boolean = {
    val hook = stateSyncConfiguredHookForTesting
    if (hook == null) FeatureGate.stateSyncUri(spark).isDefined else hook(spark)
  }

  private[openivm] def setStateSyncConfiguredHookForTesting(hook: SparkSession => Boolean): Unit =
    stateSyncConfiguredHookForTesting = hook

  private def trackingEnabled(spark: SparkSession): Boolean =
    stateSyncConfigured(spark) && FeatureGate.stateSyncCheckpointEnabled(spark)

  private[openivm] def root(spark: SparkSession): NioPath =
    Option(rootOverrideForTesting.get())
      .getOrElse(OpenIvmStatePaths.openIvmRoot(spark))
      .toFile
      .getCanonicalFile
      .toPath

  private[rocksdb] def withRootForTesting[A](path: NioPath)(body: => A): A = {
    val previous = rootOverrideForTesting.get()
    rootOverrideForTesting.set(path)
    try body
    finally if (previous == null) rootOverrideForTesting.remove() else rootOverrideForTesting.set(previous)
  }

  private def forRoot(path: NioPath): RootState =
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

  private def withMutationTracking[A](state: RootState)(body: => A): A =
    if (!state.trackingEnabled) body
    else {
      state.beginMutation()
      try body
      finally state.finishMutation()
    }

  def withMutation[A](spark: SparkSession)(body: => A): A =
    if (stateSyncConfigured(spark)) {
      OpenIvmStateSync.maybeRestore(spark)
      val state = forRoot(root(spark))
      state.trackingEnabled = state.trackingEnabled || trackingEnabled(spark)
      withRead(state)(withMutationTracking(state)(body))
    } else body

  private[rocksdb] def withCurrentMutationGeneration[A](spark: SparkSession)(body: Long => A): A =
    if (trackingEnabled(spark)) {
      OpenIvmStateSync.maybeRestore(spark)
      val state = forRoot(root(spark))
      state.trackingEnabled = true
      withRead(state)(body(state.requestMutationGeneration()))
    } else body(0L)

  private[rocksdb] def currentMutationGeneration(spark: SparkSession): Long =
    forRoot(root(spark)).mutationGeneration.get()

  private[rocksdb] def markRestoredMutationGeneration(spark: SparkSession): Unit =
    forRoot(root(spark)).recordMutation()

  private[rocksdb] def register(spark: SparkSession): Unit = {
    val state = forRoot(root(spark))
    state.trackingEnabled = state.trackingEnabled || trackingEnabled(spark)
    ()
  }

  private[openivm] def withDbOperation[A](dbPath: String)(body: => A): A = {
    val state = forDbPath(dbPath)
    if (state == null) body else withRead(state)(body)
  }

  private[openivm] def withDbMutation[A](dbPath: String)(body: => A): A = {
    val state = forDbPath(dbPath)
    if (state == null) body else withRead(state)(withMutationTracking(state)(body))
  }

  private[rocksdb] def mutationCoverageEnabled(spark: SparkSession): Boolean =
    if (!trackingEnabled(spark) || OpenIvmRocksDBConf.fromSpark(spark).multiProcess) false
    else
      try {
        val stateRoot = root(spark)
        val warehouse = localPath(spark.conf.get("spark.sql.warehouse.dir"))
        val remoteUri = FeatureGate.stateSyncUri(spark).get
        val remoteDisjoint = remoteLocalPath(remoteUri) match {
          case Some(path) => disjoint(stateRoot, path)
          case None       => remoteHasNonLocalScheme(remoteUri)
        }
        val telemetryIsUncaptured = FeatureGate.telemetryUri(spark).isEmpty &&
          FeatureGate.oneLakeTelemetryDir(spark).isEmpty
        FeatureGate.statePath(spark).isDefined &&
        disjoint(stateRoot, warehouse) &&
        remoteDisjoint &&
        telemetryIsUncaptured
      } catch {
        case NonFatal(_) => false
      }

  private def localPath(value: String): NioPath = {
    val uri = new Path(value).toUri
    val path = if (uri.getScheme == null) Paths.get(value) else Paths.get(uri)
    path.toFile.getCanonicalFile.toPath
  }

  private def remoteLocalPath(value: String): Option[NioPath] =
    try {
      val uri = new Path(value).toUri
      Option(uri.getScheme).filter(_.equalsIgnoreCase("file")).map(_ => Paths.get(uri).toFile.getCanonicalFile.toPath)
    } catch {
      case NonFatal(_) => None
    }

  private def remoteHasNonLocalScheme(value: String): Boolean =
    try Option(new Path(value).toUri.getScheme).exists(scheme => !scheme.equalsIgnoreCase("file"))
    catch { case NonFatal(_) => false }

  private def disjoint(left: NioPath, right: NioPath): Boolean =
    !left.startsWith(right) && !right.startsWith(left)

  private[spark] def currentMutationGenerationForTesting(spark: SparkSession): Long =
    currentMutationGeneration(spark)

  private[common] def captureQueuedForTesting(spark: SparkSession): Boolean =
    forRoot(root(spark)).lock.hasQueuedThreads

  private[rocksdb] def withMutationRootForTesting(path: NioPath)(body: => Unit): Unit = {
    val state = forRoot(path.toFile.getCanonicalFile.toPath)
    withRead(state) {
      state.trackingEnabled = true
      state.beginMutation()
      try body
      finally state.finishMutation()
    }
  }

  private[rocksdb] def withCaptureRootForTesting(path: NioPath)(body: => Unit): Unit = {
    val state = forRoot(path.toFile.getCanonicalFile.toPath)
    val lock  = state.lock.writeLock()
    lock.lockInterruptibly()
    try body
    finally lock.unlock()
  }

  private[rocksdb] def captureQueuedForTesting(path: NioPath): Boolean =
    forRoot(path.toFile.getCanonicalFile.toPath).lock.hasQueuedThreads

  private[rocksdb] def mutationGenerationForTesting(path: NioPath): Long =
    forRoot(path.toFile.getCanonicalFile.toPath).mutationGeneration.get()

  private[rocksdb] def requestMutationGenerationForTesting(path: NioPath): Long =
    forRoot(path.toFile.getCanonicalFile.toPath).requestMutationGeneration()

  private[rocksdb] def withCapture[A](spark: SparkSession)(body: => A): A = {
    val state = forRoot(root(spark))
    state.trackingEnabled = state.trackingEnabled || trackingEnabled(spark)
    if (state.lock.getReadHoldCount > 0 && !state.lock.isWriteLockedByCurrentThread)
      throw new IllegalStateException("OpenIVM state capture cannot run inside a state mutation")
    val lock = state.lock.writeLock()
    lock.lockInterruptibly()
    try body
    finally lock.unlock()
  }
}
