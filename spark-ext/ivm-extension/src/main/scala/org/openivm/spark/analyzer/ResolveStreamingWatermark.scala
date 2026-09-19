/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openivm.spark.analyzer

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parser.UnresolvedStreamingWatermark

/**
 * Resolves the extension-owned watermark node to Spark 3.5's native
 * [[EventTimeWatermark]]. Adapted from Apache Spark 4.1's
 * `ResolveEventTimeWatermark`, without its Spark-4 UUID constructor argument.
 */
private[spark] final class ResolveStreamingWatermark(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!FeatureGate.enabled(session)) return plan

    plan.resolveOperatorsUp {
      case unresolved: UnresolvedStreamingWatermark if unresolved.child.resolved =>
        unresolved.eventTimeColExpr match {
          case alias: UnresolvedAlias if alias.child.resolved =>
            throw new AnalysisException(
              "_LEGACY_ERROR_TEMP_2273",
              Map(
                "message" ->
                  s"WATERMARK expression '${alias.child.sql}' requires an explicit column name"
              )
            )

          case eventTime if eventTime.resolved =>
            val projectedEventTime = (eventTime, unresolved.child) match {
              case (alias: Alias, subquery: SubqueryAlias) =>
                val qualified = Alias(alias.child, alias.name)(
                  exprId = alias.exprId,
                  qualifier = subquery.identifier.qualifier :+ subquery.alias,
                  explicitMetadata = alias.explicitMetadata,
                  nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
                )
                qualified.copyTagsFrom(alias)
                qualified
              case _ =>
                eventTime
            }
            val attribute = projectedEventTime.toAttribute
            if (unresolved.child.outputSet.contains(attribute)) {
              EventTimeWatermark(attribute, unresolved.delay, unresolved.child)
            } else {
              val projection =
                Project(projectedEventTime +: unresolved.child.output, unresolved.child)
              EventTimeWatermark(attribute, unresolved.delay, projection)
            }

          case _ =>
            unresolved
        }
    }
  }
}
