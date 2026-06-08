package org.openivm.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}

sealed trait ChangeFeedMode {
  def value: String
}

object ChangeFeedMode {

  case object Intercept extends ChangeFeedMode { val value = "intercept" }
  case object Cdf       extends ChangeFeedMode { val value = "cdf"       }

  val Default: ChangeFeedMode = Intercept

  def parse(raw: String): ChangeFeedMode = raw.trim.toLowerCase(java.util.Locale.ROOT) match {
    case Intercept.value | "" => Intercept
    case Cdf.value            => Cdf
    case other =>
      throw new AnalysisException(
        "INVALID_CONF_VALUE.UNSUPPORTED",
        Map(
          "confName"  -> FeatureGate.ChangeFeedModeKey,
          "confValue" -> other,
          "supported" -> s"${Intercept.value}, ${Cdf.value}"
        )
      )
  }

  def fromConf(conf: SparkConf): ChangeFeedMode =
    parse(conf.get(FeatureGate.ChangeFeedModeKey, Default.value))

  def fromSession(spark: SparkSession): ChangeFeedMode =
    fromConf(spark.sparkContext.getConf)
}
