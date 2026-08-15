package org.openivm.spark.plugin.metrics

import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.openivm.spark.plugin.metrics.conf.OpenIvmMetricsConf
import org.openivm.spark.telemetry.metrics.{OpenIvmMetricSet, OpenIvmMetrics}

/** SparkPlugin that exports OpenIVM metrics through Spark's native registry. */
class OpenIvmMetricsPlugin extends SparkPlugin with Logging {
  override def driverPlugin: OpenIvmMetricsDriverPlugin =
    try new OpenIvmMetricsDriverPlugin
    catch {
      case NonFatal(e) =>
        logWarning("failed to create OpenIvmMetricsDriverPlugin", e)
        null
    }

  override def executorPlugin: OpenIvmMetricsExecutorPlugin =
    try new OpenIvmMetricsExecutorPlugin
    catch {
      case NonFatal(e) =>
        logWarning("failed to create OpenIvmMetricsExecutorPlugin", e)
        null
    }
}

object OpenIvmMetricsPlugin extends Logging {
  def registerMetrics(ctx: PluginContext): Unit =
    try {
      OpenIvmMetrics.bindRegistry(ctx.metricRegistry)
      ctx.metricRegistry.registerAll(new OpenIvmMetricSet)
      logInfo("registered OpenIVM metrics")
    } catch {
      case NonFatal(e) => logWarning("failed to register OpenIVM metrics", e)
    }
}

class OpenIvmMetricsDriverPlugin extends DriverPlugin with Logging {
  override def init(sc: SparkContext, ctx: PluginContext): java.util.Map[String, String] = {
    val config = OpenIvmMetricsConf(ctx.conf)
    OpenIvmMetrics.configure(config.enabled)
    if (!config.enabled) logInfo("OpenIVM metrics are not enabled")
    new java.util.HashMap[String, String]
  }

  override def registerMetrics(appId: String, ctx: PluginContext): Unit =
    if (OpenIvmMetrics.enabled) OpenIvmMetricsPlugin.registerMetrics(ctx)
}

class OpenIvmMetricsExecutorPlugin extends ExecutorPlugin with Logging {
  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    val config = OpenIvmMetricsConf(ctx.conf)
    OpenIvmMetrics.configure(config.enabled)
    if (config.enabled) OpenIvmMetricsPlugin.registerMetrics(ctx)
    else logInfo("OpenIVM metrics are not enabled")
  }
}
