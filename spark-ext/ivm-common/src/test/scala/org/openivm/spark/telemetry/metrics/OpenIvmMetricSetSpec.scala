package org.openivm.spark.telemetry.metrics

import com.codahale.metrics.MetricRegistry
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
  }

  describe("OpenIvmMetricsConf") {
    it("defaults metrics on and honours the FeatureGate key") {
      OpenIvmMetricsConf(new SparkConf(false)).enabled shouldBe true
      OpenIvmMetricsConf(new SparkConf(false).set(FeatureGate.MetricsEnabledKey, "false")).enabled shouldBe false
    }
  }
}
