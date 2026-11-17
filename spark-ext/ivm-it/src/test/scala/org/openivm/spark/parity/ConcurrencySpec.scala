package org.openivm.spark.parity

import org.openivm.spark.parity.base.InterceptMode
import org.openivm.spark.common.FeatureGate

import java.io.File
import scala.concurrent.duration._

/** Intercept-mode entry point for [[ConcurrencyScenarios]]. */
class ConcurrencySpec extends ConcurrencyScenarios with InterceptMode

class ConcurrencyStateSyncSpec extends ConcurrencyScenarios with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.StateSyncUriKey -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false"
  )

  override protected val RefreshObservationBudget: FiniteDuration = 45.seconds
}
