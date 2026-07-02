package org.openivm.spark.parity

import org.openivm.spark.parity.base.InterceptMode

/** Step B — running-window aux state backed by the RocksDB
  * [[org.openivm.spark.common.WindowStateCatalog]] (`windowRocksdbState` gate
  * ON, in addition to `windowRunningIncremental`).  Runs the SAME scenarios as
  * [[WindowRunningAuxStateParitySpec]] to prove the RocksDB backing is
  * bag-correct and that the per-partition state is what actually drives the
  * refresh. */
class WindowRunningAuxStateRocksdbParitySpec extends WindowRunningAuxStateScenarios with InterceptMode {
  override protected def rocksdbState: Boolean = true
}
