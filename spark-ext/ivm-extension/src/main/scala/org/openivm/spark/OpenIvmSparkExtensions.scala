package org.openivm.spark

import org.apache.spark.sql.{SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.openivm.spark.common.ChangePropagationFactory

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
 * The DML interception rule + planner strategy are only meaningful when the
 * active [[org.openivm.spark.common.ChangePropagation]] requires them — i.e.
 * under `spark.openivm.changeFeed.mode = intercept` (the default).  Under
 * `cdf` mode the source-of-truth for changes is the Delta Change Data Feed
 * itself, so we install [[NoOpRule]] / [[NoOpStrategy]] instead so the
 * inject-point still exists but is inert.
 *
 * If the feature gate is off (the default), every injected rule short-circuits
 * via [[org.openivm.spark.common.FeatureGate.enabled]] and Spark boots up
 * exactly as it would without our jar.
 */
class OpenIvmSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectParser((session, parent) => new parser.IvmParser(session, parent))
    ext.injectResolutionRule { session =>
      if (ChangePropagationFactory.forSession(session).requiresDmlInterception)
        new analyzer.IvmDmlInterceptorRule(session)
      else
        new NoOpRule
    }
    ext.injectPlannerStrategy { session =>
      if (ChangePropagationFactory.forSession(session).requiresDmlInterception)
        new analyzer.IvmStrategy(session)
      else
        NoOpStrategy
    }
  }
}

/** A resolution rule that never rewrites its input — used when DML
  * interception is disabled.
  */
private[spark] class NoOpRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

/** A planner strategy that never emits a physical plan — used when DML
  * interception is disabled.
  */
private[spark] object NoOpStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Nil
}
