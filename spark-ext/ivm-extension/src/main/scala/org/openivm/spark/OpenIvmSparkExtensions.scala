package org.openivm.spark

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Entry point wired into Spark via `spark.sql.extensions`.
 *
 * Activation:
 * {{{
 *   spark-shell --conf spark.sql.extensions=org.openivm.spark.OpenIvmSparkExtensions \
 *               --conf spark.openivm.enabled=true \
 *               --jars openivm-spark-0.1.0-SNAPSHOT-assembly.jar
 * }}}
 *
 * This is a deliberately small surface: the heavy lifting (parser, analyzer
 * rules, commands, compiler bridge, refresh assemblers) lives in the
 * `ivm-extension`, `ivm-compiler`, and `ivm-common` modules. The bootstrap
 * registers each via the appropriate `inject*` API after consulting the
 * master feature gate ([[org.openivm.spark.common.FeatureGate.EnabledKey]]
 * = `spark.openivm.enabled`).
 *
 * If the feature gate is off (the default), this class is a no-op — Spark
 * boots up exactly as it would without our jar.
 */
class OpenIvmSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectParser((session, parent) => new parser.IvmParser(session, parent))
    ext.injectResolutionRule(session => new analyzer.IvmDmlInterceptorRule(session))
    ext.injectPlannerStrategy(session => new analyzer.IvmStrategy(session))
  }
}
