package org.openivm.spark.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.openivm.spark.common.FeatureGate

/** Extension optimizer hook for runtime delta-size-aware refresh planning.
  *
  * The current prototype keeps Catalyst rewrites side-effect free and performs
  * the row-count probe in REFRESH execution after source delta temp views are
  * materialized. This injected rule is the stable extension point for later
  * statement-level pruning/broadcast rewrites once runtime facts are published
  * into the session.
  */
final class RuntimeDeltaSizeRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (FeatureGate.enabled(session) && FeatureGate.runtimeEmptyDeltaSkipEnabled(session)) plan else plan
}
