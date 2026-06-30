package org.openivm.spark.parity

import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parity.base.InterceptMode

class CompileCachePipelineCascadeSpec extends PipelineCascadeScenarios with InterceptMode {
  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(FeatureGate.CompileClassificationCacheEnabledKey -> "true")
}
