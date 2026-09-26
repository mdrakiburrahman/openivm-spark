package org.openivm.spark.analyzer

import java.util.UUID

import org.apache.spark.sql.classic.Strategy
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.unsafe.types.CalendarInterval

private[spark] abstract class SparkStrategyCompat extends Strategy

private[analyzer] object EventTimeWatermarkCompat {
  def apply(
      eventTime: Attribute,
      delay: CalendarInterval,
      child: LogicalPlan
  ): EventTimeWatermark =
    EventTimeWatermark(UUID.randomUUID(), eventTime, delay, child)
}
