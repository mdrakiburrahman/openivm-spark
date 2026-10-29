package org.openivm.spark.parity

import org.openivm.spark.parity.base.{CdfMode, InterceptMode}

class CompileCacheSpec extends CompileCacheScenarios with InterceptMode

class CompileCacheCdfSpec extends CompileCacheScenarios with CdfMode
