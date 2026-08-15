package org.openivm.spark.plugin.metrics.conf

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.openivm.spark.common.FeatureGate

/** Configuration for OpenIVM Spark-native metrics. */
final case class OpenIvmMetricsConf(enabled: Boolean)

object OpenIvmMetricsConf {
  def apply(conf: SparkConf): OpenIvmMetricsConf =
    OpenIvmMetricsConf(enabled = BooleanOpenIvmMetricsProperty(FeatureGate.MetricsEnabledKey, default = true).get(conf))
}

trait OpenIvmMetricsProperty[T] extends Logging {
  def key: String
  def default: T

  def get(conf: SparkConf): T = {
    val value = getImpl(conf)
    logInfo(s"using $value for $key")
    value
  }

  protected def getImpl(conf: SparkConf): T
}

final case class BooleanOpenIvmMetricsProperty(key: String, default: Boolean) extends OpenIvmMetricsProperty[Boolean] {
  override protected def getImpl(conf: SparkConf): Boolean =
    try conf.getBoolean(key, default)
    catch {
      case NonFatal(e) =>
        logWarning(s"got exception while getting value for $key; assuming $default", e)
        default
    }
}
