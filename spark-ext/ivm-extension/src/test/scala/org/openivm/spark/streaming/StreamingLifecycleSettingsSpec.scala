package org.openivm.spark.streaming

import scala.concurrent.duration._
import org.apache.spark.sql.AnalysisException
import org.openivm.spark.common.FeatureGate
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingLifecycleSettingsSpec extends AnyFunSpec with Matchers {

  private val keys = Seq(FeatureGate.StreamingStopTimeoutKey, FeatureGate.StreamingFirstProgressTimeoutKey)

  private def assertInvalid(value: String): Unit =
    keys.foreach { key =>
      val error = intercept[AnalysisException] {
        StreamingLifecycleSettings.parse(requested => if (requested == key) Some(value) else None)
      }
      error.getMessage should include(key)
      error.getMessage should include("positive finite duration")
    }

  describe("strict streaming lifecycle settings") {
    it("uses the FeatureGate keys and defaults only when settings are absent") {
      val requested = scala.collection.mutable.ArrayBuffer.empty[String]
      val settings = StreamingLifecycleSettings.parse { key =>
        requested += key
        None
      }

      requested.toSeq shouldBe keys
      settings.stopTimeout shouldBe 60.seconds
      settings.firstProgressTimeout shouldBe 5.minutes
      settings.stopTimeout shouldBe Duration(FeatureGate.StreamingStopTimeoutDefault)
      settings.firstProgressTimeout shouldBe Duration(FeatureGate.StreamingFirstProgressTimeoutDefault)
    }

    it("parses independent supplied settings without replacing the absent setting") {
      val configured = Map(
        FeatureGate.StreamingStopTimeoutKey          -> "0.25 seconds",
        FeatureGate.StreamingFirstProgressTimeoutKey -> "750 ms"
      )
      val settings = StreamingLifecycleSettings.parse(configured.get)

      settings.stopTimeout shouldBe 250.millis
      settings.firstProgressTimeout shouldBe 750.millis
      StreamingLifecycleSettings
        .parse(Map(FeatureGate.StreamingStopTimeoutKey -> "2s").get)
        .firstProgressTimeout shouldBe 5.minutes
    }

    it("reads the current values on every invocation rather than caching runtime settings") {
      var configured                        = Map(FeatureGate.StreamingStopTimeoutKey -> "1s")
      def read(key: String): Option[String] = configured.get(key)

      StreamingLifecycleSettings.parse(read).stopTimeout shouldBe 1.second
      configured = Map(
        FeatureGate.StreamingStopTimeoutKey          -> "3s",
        FeatureGate.StreamingFirstProgressTimeoutKey -> "4s"
      )
      val changed = StreamingLifecycleSettings.parse(read)
      changed.stopTimeout shouldBe 3.seconds
      changed.firstProgressTimeout shouldBe 4.seconds
    }

    it("accepts standard units and decimals only when they are exact nanoseconds") {
      val examples = Seq(
        "1ns"             -> 1.nanosecond,
        "1 microsecond"   -> 1.microsecond,
        "1 millisecond"   -> 1.millisecond,
        " +2 s "          -> 2.seconds,
        "1.5 minutes"     -> 90.seconds,
        "1e-3 seconds"    -> 1.millisecond,
        "0.125ms"         -> 125.microseconds,
        "0.000000001000s" -> 1.nanosecond
      )
      examples.foreach { case (value, expected) =>
        withClue(s"duration $value: ") {
          val settings = StreamingLifecycleSettings.parse(key =>
            if (key == FeatureGate.StreamingStopTimeoutKey) Some(value) else None
          )
          settings.stopTimeout shouldBe expected
        }
      }
    }

    it("rejects malformed and explicitly null supplied values instead of falling back") {
      Seq("", " ", "banana", "60", "1 fortnight", "10ms junk", "1 second 2 seconds", "NaN", "1_000 ms", null)
        .foreach(assertInvalid)
    }

    it("rejects zero and negative durations for both settings") {
      Seq("0ns", "-1s", "-0.5ms", "0 days", "+0 seconds", "0e100h").foreach(assertInvalid)
    }

    it("rejects every infinite-duration spelling rather than allowing an unbounded wait") {
      Seq("Inf", "PlusInf", "MinusInf", "Duration.Inf", "-Inf", "Infinity", "-Infinity", "infinite")
        .foreach(assertInvalid)
    }

    it("rejects overflow, extreme exponents, and fractional nanoseconds without rounding or saturation") {
      Seq(
        "9223372036854775808ns",
        "9223372036854775807s",
        "9223372036855ms",
        "106752d",
        "1e100s",
        "1e2147483647ns",
        "1e-2147483647ns",
        "1e2147483648ns",
        "0.1ns",
        "1.0000000001s",
        "0.0000000001s",
        "1.00000000000000000000000000000000001ns"
      ).foreach(assertInvalid)
    }

    it("accepts the largest exactly representable finite nanosecond budget") {
      val settings = StreamingLifecycleSettings.parse(_ => Some(s"${Long.MaxValue}ns"))

      settings.stopTimeout.toNanos shouldBe Long.MaxValue
      settings.firstProgressTimeout.toNanos shouldBe Long.MaxValue
    }
  }
}
