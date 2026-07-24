package org.openivm.spark.parity

import org.openivm.spark.parity.base.InterceptMode

class WindowCascadeReuseCompileCacheSpec
    extends WindowCascadeReuseScenarios(compileCacheEnabled = true)
    with InterceptMode
