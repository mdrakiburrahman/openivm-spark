package org.openivm.spark.parity

import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parity.base.InterceptMode

class FingerprintGuardAggregateGroupSumSpec extends AggregateGroupSumScenarios with InterceptMode {
  override protected def extraSparkConf: Map[String, String] =
    Map(FeatureGate.FingerprintGuardEnabledKey -> "true")
}
