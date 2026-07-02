package org.openivm.spark.parity

import org.openivm.spark.parity.base.InterceptMode

/** Step A — running-window aux state backed by a Delta table (`openivm_aux_<view>`
  * materialised under `_ivm/aux/`).  RocksDB backing is OFF. */
class WindowRunningAuxStateParitySpec extends WindowRunningAuxStateScenarios with InterceptMode
