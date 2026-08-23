package org.openivm.spark.telemetry.metrics

import com.codahale.metrics.MetricRegistry
import org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin
import org.apache.spark.SparkConf
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.plugin.metrics.conf.OpenIvmMetricsConf
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmMetricSetSpec extends AnyFunSpec with Matchers {

  describe("OpenIvmMetricSet") {
    it("registers stable top-level OpenIVM metric names") {
      val registry = new MetricRegistry

      registry.registerAll(new OpenIvmMetricSet)

      registry.getMetrics.containsKey("openivm.rocksdb.registry.open_handles") shouldBe true
      registry.getMetrics.get("openivm.refresh.inflight") should not be null
      registry.getMetrics.get("openivm.create.inflight") should not be null
      registry.getMetrics.get("openivm.compiler.inflight") should not be null
      registry.getMetrics.get("openivm.ctas.queue_depth") should not be null
      registry.getMetrics.get("openivm.create.materialization_admission.inflight") should not be null
      registry.getMetrics.get("openivm.create.materialization_admission.wait") should not be null
    }

    it("updates RocksDB operation timers and counters") {
      OpenIvmMetrics.configure(enabled = true)

      OpenIvmMetrics.recordRocksDbOperation(
        dbScope = "mv",
        operation = "get",
        multiProcess = true,
        failed = false,
        totalNanos = 10L,
        jvmLockWaitNanos = 2L,
        jvmLockHeldNanos = 8L,
        externalLockWaitNanos = 1L,
        nativeOpenNanos = 3L,
        nativeCloseNanos = 4L,
        bodyNanos = 5L
      )

      OpenIvmMetrics.counter("rocksdb.scope.mv.operation.get.count").getCount should be >= 1L
      OpenIvmMetrics.timer("rocksdb.scope.mv.operation.get.lock.wait").getCount should be >= 1L
      OpenIvmMetrics.counter("rocksdb.scope.mv.operation.get.native.open.count").getCount should be >= 1L
    }

    it("separates physical writes, logical commits, and manual flush telemetry") {
      OpenIvmMetrics.configure(enabled = true)
      val scope = s"telemetry_${System.nanoTime()}"

      OpenIvmMetrics.recordRocksDbWrite(scope, nanos = 10L, keys = 2L, bytes = 32L, failed = false)
      OpenIvmMetrics.recordRocksDbFlush(scope, nanos = 20L, columnFamilyCount = 2, failed = false)
      OpenIvmMetrics.recordRocksDbCommit(
        scope,
        nanos = 30L,
        keys = 2L,
        bytes = 32L,
        sstCount = 1,
        failed = false
      )

      OpenIvmMetrics.counter(s"rocksdb.scope.$scope.physical_write.count").getCount shouldBe 1L
      OpenIvmMetrics.timer(s"rocksdb.scope.$scope.physical_write.latency").getCount shouldBe 1L
      OpenIvmMetrics.counter(s"rocksdb.scope.$scope.flush.count").getCount shouldBe 1L
      OpenIvmMetrics.timer(s"rocksdb.scope.$scope.flush.latency").getCount shouldBe 1L
      OpenIvmMetrics.histogram(s"rocksdb.scope.$scope.flush.column_families").getCount shouldBe 1L
      OpenIvmMetrics.timer(s"rocksdb.scope.$scope.commit_batch.latency").getCount shouldBe 1L
      OpenIvmMetrics.counter(s"rocksdb.scope.$scope.commit_batch.version_bump").getCount shouldBe 1L
    }

    it("registers metrics idempotently across multiple registries") {
      OpenIvmMetrics.clearRegistriesForTesting()
      OpenIvmMetrics.configure(enabled = true)
      val driverRegistry   = new MetricRegistry
      val executorRegistry = new MetricRegistry
      val dynamicName      = s"test.dynamic.metric.${System.nanoTime()}"
      val fullDynamicName  = s"openivm.$dynamicName"
      val previousCreate   = OpenIvmMetrics.CreateInflight.get()

      noException shouldBe thrownBy {
        OpenIvmMetricsPlugin.registerMetrics(driverRegistry)
        OpenIvmMetricsPlugin.registerMetrics(driverRegistry)
        OpenIvmMetricsPlugin.registerMetrics(executorRegistry)
      }

      OpenIvmMetrics.increment(dynamicName)

      driverRegistry.getMetrics.containsKey(fullDynamicName) shouldBe true
      executorRegistry.getMetrics.containsKey(fullDynamicName) shouldBe true
      driverRegistry.getCounters.get(fullDynamicName).getCount shouldBe 1L
      executorRegistry.getCounters.get(fullDynamicName).getCount shouldBe 1L

      OpenIvmMetrics.CreateInflight.set(previousCreate + 1)
      try {
        driverRegistry.getGauges.get("openivm.create.inflight").getValue shouldBe (previousCreate + 1)
        executorRegistry.getGauges.get("openivm.create.inflight").getValue shouldBe (previousCreate + 1)
      } finally OpenIvmMetrics.CreateInflight.set(previousCreate)
    }
  }

  describe("OpenIvmMetricsConf") {
    it("defaults metrics on and honours the FeatureGate key") {
      OpenIvmMetricsConf(new SparkConf(false)).enabled shouldBe true
      OpenIvmMetricsConf(new SparkConf(false).set(FeatureGate.MetricsEnabledKey, "false")).enabled shouldBe false
    }
  }
}
