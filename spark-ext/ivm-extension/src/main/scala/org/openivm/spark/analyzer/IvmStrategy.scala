package org.openivm.spark.analyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.openivm.spark.executor.DeltaStagingExec

/**
 * Planner strategy that maps [[WithDeltaStaging]] logical nodes to their
 * physical counterpart [[DeltaStagingExec]].
 *
 * Registered via `injectPlannerStrategy`.
 */
class IvmStrategy(session: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WithDeltaStaging(child, stagingPath, opType, baseTable) =>
      DeltaStagingExec(planLater(child), stagingPath, opType, baseTable) :: Nil
    case _ => Nil
  }
}
