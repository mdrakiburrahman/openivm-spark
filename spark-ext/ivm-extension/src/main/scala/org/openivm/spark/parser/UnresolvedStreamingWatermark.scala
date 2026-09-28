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

package org.openivm.spark.parser

import org.apache.spark.sql.catalyst.analysis.UnresolvedUnaryNode
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Spark-3.5-compatible counterpart of Spark 4.1's
 * `UnresolvedEventTimeWatermark`.
 */
private[spark] final case class UnresolvedStreamingWatermark(
    eventTimeColExpr: NamedExpression,
    delay: CalendarInterval,
    child: LogicalPlan
) extends UnresolvedUnaryNode {

  override protected def withNewChildInternal(newChild: LogicalPlan): UnresolvedStreamingWatermark =
    copy(child = newChild)
}
