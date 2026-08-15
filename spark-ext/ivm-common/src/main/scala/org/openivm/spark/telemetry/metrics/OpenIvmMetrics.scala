package org.openivm.spark.telemetry.metrics

import com.codahale.metrics.{Counter, Gauge, Histogram, Metric, MetricRegistry, MetricSet, Timer}
import org.apache.spark.internal.Logging

import java.util.HashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

/** Process-wide OpenIVM metrics backed by Spark's Dropwizard registry. */
object OpenIvmMetrics extends Logging {

  private val metrics       = TrieMap.empty[String, Metric]
  @volatile private var reg = Option.empty[MetricRegistry]
  private val enabledFlag   = new AtomicBoolean(true)

  val OpenDbHandles: AtomicInteger      = atomicIntegerGauge("rocksdb.registry.open_handles")
  val RefreshInflight: AtomicInteger    = atomicIntegerGauge("refresh.inflight")
  val RefreshQueued: AtomicInteger      = atomicIntegerGauge("refresh.queued")
  val CreateInflight: AtomicInteger     = atomicIntegerGauge("create.inflight")
  val CompilerInflight: AtomicInteger   = atomicIntegerGauge("compiler.inflight")
  val CtasQueueDepth: AtomicInteger     = atomicIntegerGauge("ctas.queue_depth")
  val CtasAdmissionWidth: AtomicInteger = atomicIntegerGauge("ctas.admission_width")
  val CtasActiveThreads: AtomicInteger  = atomicIntegerGauge("ctas.active_threads")
  val DriverAdmissionWidth: AtomicInteger =
    atomicIntegerGauge("driver_admission.width")
  val DriverAdmissionQueued: AtomicInteger =
    atomicIntegerGauge("driver_admission.queued")
  val DriverAdmissionInflight: AtomicInteger =
    atomicIntegerGauge("driver_admission.inflight")
  val RocksDbCommitBatchActiveBytes: AtomicLong =
    atomicLongGauge("rocksdb.commit_batch.active_bytes")
  val RocksDbCommitBatchLastBytes: AtomicLong =
    atomicLongGauge("rocksdb.commit_batch.last_bytes")

  precreateKnownMetrics()

  /** Bind the Spark metrics registry for dynamic metrics created after plugin init. */
  def bindRegistry(registry: MetricRegistry): Unit = {
    reg = Some(registry)
  }

  /** Enable or disable hot-path metric updates. */
  def configure(enabled: Boolean): Unit = enabledFlag.set(enabled)

  def enabled: Boolean = enabledFlag.get()

  def counter(name: String): Counter = metric(name, new Counter).asInstanceOf[Counter]

  def timer(name: String): Timer = metric(name, new Timer).asInstanceOf[Timer]

  def histogram(name: String): Histogram =
    metric(name, new Histogram(new com.codahale.metrics.ExponentiallyDecayingReservoir))
      .asInstanceOf[Histogram]

  def gauge[T](name: String, gauge: Gauge[T]): Gauge[T] = metric(name, gauge).asInstanceOf[Gauge[T]]

  def increment(name: String, value: Long = 1L): Unit = if (enabled) counter(name).inc(value)

  def updateHistogram(name: String, value: Long): Unit = if (enabled) histogram(name).update(value)

  def updateTimer(name: String, nanos: Long): Unit =
    if (enabled && nanos >= 0L) timer(name).update(nanos, TimeUnit.NANOSECONDS)

  def time[A](name: String)(body: => A): A = {
    if (!enabled) return body
    val started = System.nanoTime()
    try body
    finally updateTimer(name, System.nanoTime() - started)
  }

  def recordRocksDbOperation(
      dbScope: String,
      operation: String,
      multiProcess: Boolean,
      failed: Boolean,
      totalNanos: Long,
      jvmLockWaitNanos: Long,
      jvmLockHeldNanos: Long,
      externalLockWaitNanos: Long,
      nativeOpenNanos: Long,
      nativeCloseNanos: Long,
      bodyNanos: Long
  ): Unit = {
    if (!enabled) return
    val prefix = s"rocksdb.scope.${sanitize(dbScope)}.operation.${sanitize(operation)}"
    increment(s"$prefix.count")
    if (failed) increment(s"$prefix.failed")
    updateTimer(s"$prefix.total", totalNanos)
    updateTimer(s"$prefix.lock.wait", jvmLockWaitNanos)
    updateTimer(s"$prefix.lock.hold", jvmLockHeldNanos)
    updateTimer(s"$prefix.external_lock.wait", externalLockWaitNanos)
    updateTimer(s"$prefix.native.open", nativeOpenNanos)
    updateTimer(s"$prefix.native.close", nativeCloseNanos)
    updateTimer(s"$prefix.body", bodyNanos)
    if (nativeOpenNanos > 0L) increment(s"$prefix.native.open.count")
    if (nativeCloseNanos > 0L) increment(s"$prefix.native.close.count")
    if (multiProcess) increment(s"$prefix.multi_process.count")
  }

  def recordRocksDbCommit(dbScope: String, nanos: Long, keys: Long, bytes: Long, sstCount: Int): Unit = {
    if (!enabled) return
    val prefix = s"rocksdb.scope.${sanitize(dbScope)}.commit_batch"
    updateTimer(s"$prefix.latency", nanos)
    updateHistogram(s"$prefix.keys", keys)
    updateHistogram(s"$prefix.bytes", bytes)
    updateHistogram(s"$prefix.sst_files", sstCount.toLong)
    increment(s"$prefix.version_bump")
  }

  def recordColumnFamilyRead(columnFamily: String, bytes: Long): Unit = {
    if (!enabled) return
    val prefix = s"rocksdb.column_family.${sanitize(columnFamily)}.read"
    increment(s"$prefix.count")
    increment(s"$prefix.bytes", math.max(0L, bytes))
  }

  def recordColumnFamilyWrite(columnFamily: String, bytes: Long): Unit = {
    if (!enabled) return
    val prefix = s"rocksdb.column_family.${sanitize(columnFamily)}.write"
    increment(s"$prefix.count")
    increment(s"$prefix.bytes", math.max(0L, bytes))
  }

  def recordLifecyclePhase(mode: String, phase: String, nanos: Long): Unit =
    updateTimer(s"$mode.phase.${sanitize(phase)}", nanos)

  def recordSqlStatement(kind: String, nanos: Long, bytes: Int, retryAttempt: Int): Unit = {
    val safeKind = sanitize(kind)
    updateTimer(s"refresh.sql_stmt.$safeKind", nanos)
    updateHistogram(s"refresh.sql_stmt.$safeKind.bytes", bytes.toLong)
    if (retryAttempt > 0) increment(s"refresh.sql_stmt.$safeKind.retry")
  }

  def recordSparkPlanMetrics(kind: String, planMetrics: Iterable[(String, Long)]): Unit = {
    if (!enabled) return
    val safeKind = sanitize(kind)
    planMetrics.foreach { case (metricName, value) =>
      val safeMetric = sanitize(metricName)
      updateHistogram(s"refresh.sql_stmt.$safeKind.plan.$safeMetric", value)
      val lower = safeMetric.toLowerCase(java.util.Locale.ROOT)
      if (lower.contains("row")) {
        if (lower.contains("source") || lower.contains("scan") || lower.contains("read")) {
          increment(s"refresh.sql_stmt.$safeKind.rows_read", value)
        }
        if (
          lower.contains("output") || lower.contains("insert") || lower
            .contains("update") || lower.contains("delete") ||
          lower.contains("write")
        ) {
          increment(s"refresh.sql_stmt.$safeKind.rows_written", value)
        }
      }
      if (lower.contains("byte")) increment(s"refresh.sql_stmt.$safeKind.bytes_observed", value)
    }
  }

  def recordCompileCache(hit: Boolean): Unit = increment(
    if (hit) "compiler.classification_cache.hit" else "compiler.classification_cache.miss"
  )

  def recordCatalogRetry(catalog: String): Unit = increment(s"catalog.${sanitize(catalog)}.retry")

  def snapshot(prefix: String): Map[String, Metric] =
    metrics.collect { case (name, metric) if name.startsWith(prefix) => name.stripPrefix(prefix) -> metric }.toMap

  private def atomicIntegerGauge(name: String): AtomicInteger = {
    val value = new AtomicInteger(0)
    gauge(name, (() => value.get()): Gauge[Int])
    value
  }

  private def atomicLongGauge(name: String): AtomicLong = {
    val value = new AtomicLong(0L)
    gauge(name, (() => value.get()): Gauge[Long])
    value
  }

  private def metric(name: String, create: => Metric): Metric = {
    val safeName = sanitizePath(name)
    metrics.getOrElseUpdate(
      safeName, {
        val created = create
        reg.foreach(registerDynamic(safeName, created, _))
        created
      }
    )
  }

  private def registerDynamic(name: String, metric: Metric, registry: MetricRegistry): Unit = {
    try registry.register(MetricRegistry.name("openivm", name), metric)
    catch { case NonFatal(_) => () }
  }

  private def precreateKnownMetrics(): Unit = {
    Seq("hit", "miss").foreach(s => counter(s"rocksdb.registry.get_or_open.$s"))
    Seq("run", "compaction").foreach(s => counter(s"rocksdb.maintenance.$s.count"))
    Seq("lock_hold", "run").foreach(s => timer(s"rocksdb.maintenance.$s"))
    Seq("count", "failed", "native.open.count", "native.close.count", "multi_process.count").foreach { suffix =>
      for {
        scope <- Seq("mv", "table", "index", "source", "refresh_profile", "refresh_sql_log", "other")
        op <- Seq(
          "load",
          "get",
          "multi_get",
          "prefix_scan",
          "current_version",
          "with_batch",
          "session",
          "compact_range",
          "sst_file_count"
        )
      } counter(s"rocksdb.scope.$scope.operation.$op.$suffix")
    }
    for {
      scope <- Seq("mv", "table", "index", "source", "refresh_profile", "refresh_sql_log", "other")
      op <- Seq(
        "load",
        "get",
        "multi_get",
        "prefix_scan",
        "current_version",
        "with_batch",
        "session",
        "compact_range",
        "sst_file_count"
      )
      suffix <- Seq("total", "lock.wait", "lock.hold", "external_lock.wait", "native.open", "native.close", "body")
    } timer(s"rocksdb.scope.$scope.operation.$op.$suffix")
    Seq("meta", "properties", "consumed", "cdf_watermarks", "staging", "dependent_mvs").foreach { cf =>
      counter(s"rocksdb.column_family.$cf.read.count")
      counter(s"rocksdb.column_family.$cf.read.bytes")
      counter(s"rocksdb.column_family.$cf.write.count")
      counter(s"rocksdb.column_family.$cf.write.bytes")
    }
    Seq("refresh.lock.wait", "refresh.sql_stmt.merge", "create.phase.create_mv_initial_load", "compiler.compile")
      .foreach(timer)
    Seq("compiler.compile.count", "compiler.classification_cache.hit", "compiler.classification_cache.miss")
      .foreach(counter)
  }

  private def sanitizePath(name: String): String = name.split("\\.").map(sanitize).mkString(".")

  private def sanitize(value: String): String =
    Option(value).getOrElse("null").replaceAll("[^A-Za-z0-9_\\-]", "_").stripPrefix("_").stripSuffix("_") match {
      case "" => "unknown"
      case v  => v.toLowerCase(java.util.Locale.ROOT)
    }
}

/** MetricSet exported through Spark's PluginContext registry. */
final class OpenIvmMetricSet extends MetricSet {
  override def getMetrics: HashMap[String, Metric] = {
    val metrics = new HashMap[String, Metric]
    metrics.put("openivm", new OpenIvmMetricGroupSet)
    metrics
  }
}

final class OpenIvmMetricGroupSet extends MetricSet {
  override def getMetrics: HashMap[String, Metric] = {
    val metrics = new HashMap[String, Metric]
    Seq("rocksdb", "refresh", "create", "compiler", "catalog", "ctas", "driver_admission").foreach { group =>
      metrics.put(group, new OpenIvmPrefixMetricSet(group + "."))
    }
    metrics
  }
}

final class OpenIvmPrefixMetricSet(prefix: String) extends MetricSet {
  override def getMetrics: HashMap[String, Metric] = {
    val metrics = new HashMap[String, Metric]
    OpenIvmMetrics.snapshot(prefix).foreach { case (name, metric) => metrics.put(name, metric) }
    metrics
  }
}
