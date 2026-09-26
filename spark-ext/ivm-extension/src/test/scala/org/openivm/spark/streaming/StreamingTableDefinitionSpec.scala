package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingTableDefinitionSpec extends AnyFunSpec with Matchers {

  describe("StreamingRuntimeOptions") {
    it("uses native append and zero-interval processing-time defaults") {
      val options = StreamingRuntimeOptions.parse(Map.empty)

      options.outputMode shouldBe "append"
      options.trigger shouldBe "processingtime"
      options.triggerInterval shouldBe Some("0 seconds")
      options.onQueryChange shouldBe "fail"
      options.sinkOptions shouldBe empty
    }

    it("keeps sink options separate from extension-owned controls") {
      val options = StreamingRuntimeOptions.parse(
        Map(
          "outputMode"      -> "complete",
          "trigger"         -> "processingTime",
          "triggerInterval" -> "5 seconds",
          "onQueryChange"   -> "rebuild",
          "mergeSchema"     -> "false"
        )
      )

      options.outputMode shouldBe "complete"
      options.triggerInterval shouldBe Some("5 seconds")
      options.onQueryChange shouldBe "rebuild"
      options.sinkOptions shouldBe Map("mergeschema" -> "false")
    }

    it("supports AvailableNow without an interval") {
      val options = StreamingRuntimeOptions.parse(Map("trigger" -> "availableNow"))

      options.trigger shouldBe "availablenow"
      options.triggerInterval shouldBe None
    }

    it("rejects unsupported writer modes and triggers") {
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        StreamingRuntimeOptions.parse(Map("outputMode" -> "update"))
      }
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        StreamingRuntimeOptions.parse(Map("trigger" -> "continuous"))
      }
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        StreamingRuntimeOptions.parse(
          Map("trigger" -> "availableNow", "triggerInterval" -> "1 second")
        )
      }
    }

    it("rejects extension-owned writer overrides case-insensitively") {
      Seq("path", "checkpointLocation", "queryName").foreach { key =>
        an[org.apache.spark.sql.AnalysisException] should be thrownBy {
          StreamingRuntimeOptions.parse(Map(key -> "forbidden"))
        }
      }
    }
  }

  describe("streaming-table diagnostics") {
    it("redacts quoted secret option values and preserves deterministic hashes") {
      val input = "OPTIONS ('token' = 'secret-value', 'ordinary' = 'visible')"

      StreamingTableDefinition.redactText(input) should not include "secret-value"
      StreamingTableDefinition.sha256("stable") shouldBe StreamingTableDefinition.sha256("stable")
      StreamingTableDefinition.sha256("stable") should not be StreamingTableDefinition.sha256("changed")
    }
  }
}
