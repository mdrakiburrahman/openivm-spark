package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
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
      options.displayName shouldBe None
      options.sinkOptions shouldBe empty
    }

    it("keeps sink options separate from extension-owned controls") {
      val options = StreamingRuntimeOptions.parse(
        Map(
          "outputMode"      -> "complete",
          "trigger"         -> "processingTime",
          "triggerInterval" -> "5 seconds",
          "onQueryChange"   -> "rebuild",
          "displayName"     -> "model.analytics.events",
          "mergeSchema"     -> "false"
        )
      )

      options.outputMode shouldBe "complete"
      options.triggerInterval shouldBe Some("5 seconds")
      options.onQueryChange shouldBe "rebuild"
      options.displayName shouldBe Some("model.analytics.events")
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

    it("rejects unsafe display names") {
      Seq("", "line\nbreak", "x" * 257).foreach { value =>
        an[org.apache.spark.sql.AnalysisException] should be thrownBy {
          StreamingRuntimeOptions.parse(Map("displayName" -> value))
        }
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

    it("classifies every semantic rebuild reason with field-level evidence") {
      val mapper = new ObjectMapper()
      val baseline =
        """{"formatVersion":1,"targetIdentity":"spark_catalog.default.target","declaredLocation":"<catalog-managed>","outputMode":"append","declarationPlan":"project","analyzedPlan":"project","outputSchema":"struct<id:int>","targetSchema":"struct<id:int>","partitionColumns":[],"clusterColumns":[],"tableProperties":{"owner":"one"},"sinkOptions":{"mergeSchema":"false"},"watermarks":[{"eventTime":"ts","delay":"10 seconds"}],"sources":[{"occurrence":0,"identity":"spark_catalog.default.source","provider":"delta","schema":"struct<id:int>","streaming":true,"deltaPath":"file:/source","deltaTableId":"delta-one","options":{"ignoreChanges":"false"}}]}"""
      val baselineDiagnostic = """{"normalizedQuery":"SELECT id FROM STREAM source"}"""
      val baselineHash       = StreamingTableDefinition.sha256(baseline)

      def mutate(change: ObjectNode => Unit): String = {
        val node = mapper.readTree(baseline).deepCopy[ObjectNode]()
        change(node)
        mapper.writeValueAsString(node)
      }

      def decision(semanticJson: String, diagnosticJson: String = baselineDiagnostic): StreamingRebuildDecision =
        StreamingTableDefinition.rebuildDecision(
          baselineHash,
          baseline,
          baselineDiagnostic,
          StreamingTableDefinition(
            formatVersion = 1,
            fingerprint = StreamingTableDefinition.sha256(semanticJson),
            semanticJson = semanticJson,
            diagnosticJson = diagnosticJson,
            operationalJson = "{}",
            operationalHash = StreamingTableDefinition.sha256("{}"),
            sources = Seq.empty,
            sourcePaths = Seq.empty,
            sourceIdentities = Seq.empty
          )
        )

      val cases = Seq(
        "query_plan" -> mutate(_.put("analyzedPlan", "filtered-project")),
        "source_identity" -> mutate(
          _.withArray("sources").get(0).asInstanceOf[ObjectNode].put("deltaTableId", "delta-two")
        ),
        "source_schema" -> mutate(
          _.withArray("sources").get(0).asInstanceOf[ObjectNode].put("schema", "struct<id:bigint>")
        ),
        "target_schema"      -> mutate(_.put("targetSchema", "struct<id:bigint>")),
        "destination_layout" -> mutate(_.withArray("partitionColumns").add("id")),
        "watermark" -> mutate(
          _.withArray("watermarks").get(0).asInstanceOf[ObjectNode].put("delay", "20 seconds")
        ),
        "options" -> mutate(_.put("outputMode", "complete")),
        "properties" -> mutate(
          _.get("tableProperties").asInstanceOf[ObjectNode].put("owner", "two")
        )
      )

      cases.foreach { case (category, semanticJson) =>
        val result = decision(semanticJson)
        withClue(category) {
          result.code shouldBe "SEMANTIC_DEFINITION_CHANGED"
          result.changes.map(_.category) should contain(category)
        }
      }
    }

    it("reports query-text changes separately and hashes plan evidence") {
      val baseline =
        """{"formatVersion":1,"declarationPlan":"secret-plan-one","analyzedPlan":"stable"}"""
      val requested =
        """{"formatVersion":1,"declarationPlan":"secret-plan-two","analyzedPlan":"stable"}"""
      val definition = StreamingTableDefinition(
        formatVersion = 1,
        fingerprint = StreamingTableDefinition.sha256(requested),
        semanticJson = requested,
        diagnosticJson = """{"normalizedQuery":"SELECT id FROM STREAM source WHERE id > 0"}""",
        operationalJson = "{}",
        operationalHash = StreamingTableDefinition.sha256("{}"),
        sources = Seq.empty,
        sourcePaths = Seq.empty,
        sourceIdentities = Seq.empty
      )

      val result = StreamingTableDefinition.rebuildDecision(
        StreamingTableDefinition.sha256(baseline),
        baseline,
        """{"normalizedQuery":"SELECT id FROM STREAM source"}""",
        definition
      )

      result.normalizedQueryChanged shouldBe true
      val planChange = result.changes.find(_.path == "declarationPlan").get
      planChange.category shouldBe "query_plan"
      planChange.previous should startWith("sha256:")
      planChange.requested should startWith("sha256:")
      planChange.previous should not include "secret-plan-one"
      planChange.requested should not include "secret-plan-two"
    }
  }
}
