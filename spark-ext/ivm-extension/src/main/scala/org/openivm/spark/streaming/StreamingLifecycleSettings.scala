package org.openivm.spark.streaming

import java.math.{BigDecimal => Decimal}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate

private[spark] final case class StreamingLifecycleSettings(
    stopTimeout: FiniteDuration,
    firstProgressTimeout: FiniteDuration
)

private[spark] object StreamingLifecycleSettings {

  private val DurationLiteral =
    "([+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?)\\s*([\\p{L}]+)".r
  private val MaxNanos = Decimal.valueOf(Long.MaxValue)

  def fromSpark(spark: SparkSession): StreamingLifecycleSettings =
    parse(key => spark.conf.getOption(key))

  def parse(read: String => Option[String]): StreamingLifecycleSettings =
    StreamingLifecycleSettings(
      stopTimeout = duration(
        FeatureGate.StreamingStopTimeoutKey,
        read(FeatureGate.StreamingStopTimeoutKey).getOrElse(FeatureGate.StreamingStopTimeoutDefault)
      ),
      firstProgressTimeout = duration(
        FeatureGate.StreamingFirstProgressTimeoutKey,
        read(FeatureGate.StreamingFirstProgressTimeoutKey).getOrElse(FeatureGate.StreamingFirstProgressTimeoutDefault)
      )
    )

  private def duration(key: String, value: String): FiniteDuration = {
    val parsed =
      try {
        Option(value).map(_.trim).flatMap {
          case DurationLiteral(amount, unit) =>
            val nanosPerUnit = Duration(1L, unit).unit.toNanos(1L)
            val nanos        = new Decimal(amount).multiply(Decimal.valueOf(nanosPerUnit))
            if (nanos.signum() > 0 && nanos.compareTo(MaxNanos) <= 0 && nanos.stripTrailingZeros().scale() <= 0)
              Some(FiniteDuration(nanos.longValueExact(), TimeUnit.NANOSECONDS))
            else None
          case _ => None
        }
      } catch {
        case NonFatal(_) => None
      }

    parsed.getOrElse(
      StreamingTableErrors.invalid(
        s"Configuration '$key' must be a positive finite duration exactly representable in nanoseconds " +
          s"(at most ${Long.MaxValue}ns), for example '60s' or '5m'."
      )
    )
  }
}
