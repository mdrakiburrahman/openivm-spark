package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import org.openivm.spark.insights.OpenIvmInsightsContract
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingStopInsightsSpec extends AnyFunSpec with Matchers {
  private val observation = StreamingQueryStopObservation(
    "query-id",
    "native-run-id",
    60000L,
    0L,
    false,
    true,
    Some(0L),
    true,
    Some("CancellationFailure"),
    false
  )
  private val mapper = new ObjectMapper()

  describe("terminal native-stop diagnostics") {
    it("emits a typed terminal timeout with bounded identity and uncertainty evidence") {
      val error = new StreamingQueryStopTimeoutException("issue67_child", observation)
      val draft = StreamingInsightEvents.stopFailureDraft(
        "operation",
        "issue67_root",
        "drop",
        60000L,
        Some("S8"),
        error,
        requestId = Some("request")
      )
      draft.eventType shouldBe OpenIvmInsightsContract.EventType.OperationFailed
      draft.code shouldBe OpenIvmInsightsContract.Code.StreamingQueryStopTimeout
      draft.stage shouldBe "query"
      draft.status shouldBe Some("failed")
      draft.terminal shouldBe Some(true)
      draft.operationId shouldBe Some("operation")
      val details = mapper.readTree(draft.detailsJson.get)
      details.path("query_id").asText() shouldBe "query-id"
      details.path("run_id").asText() shouldBe "native-run-id"
      details.path("native_target_relation").asText() shouldBe "issue67_child"
      details.path("termination_confirmed").asBoolean() shouldBe false
      details.path("active").asBoolean() shouldBe false
      details.path("progress_observed").asBoolean() shouldBe true
      details.path("last_batch_id").asLong() shouldBe 0L
      details.path("job_cancellation_attempted").asBoolean() shouldBe true
      details.path("cancellation_error_class").asText() shouldBe "CancellationFailure"
    }

    it("distinguishes native failure from deadline expiry without exporting raw cause text") {
      val secret = "SELECT credentials_and_unbounded_plan"
      val cause  = new java.util.concurrent.TimeoutException(secret * 100)
      val draft = StreamingInsightEvents.stopFailureDraft(
        "operation",
        "issue67_target",
        "stop",
        1L,
        None,
        new StreamingQueryStopFailedException("issue67_target", observation, cause)
      )
      draft.code shouldBe OpenIvmInsightsContract.Code.StreamingQueryStopFailed
      draft.message should not include secret
      draft.detailsJson.get should not include secret
      mapper.readTree(draft.detailsJson.get).path("native_error_class").asText() shouldBe cause.getClass.getName
      draft.detailsJson.get.length should be < 4096
    }

    it("keeps a mixed MV cascade's operation context while identifying its failed streaming writer") {
      val draft = StreamingInsightEvents.stopFailureDraft(
        "operation",
        "issue67_mv",
        "drop",
        60000L,
        None,
        new StreamingQueryStopTimeoutException("issue67_stream", observation),
        operation = OpenIvmInsightsContract.Operation.Drop
      )
      draft.operation shouldBe OpenIvmInsightsContract.Operation.Drop
      val details = mapper.readTree(draft.detailsJson.get)
      details.path("materialization").asText() shouldBe OpenIvmInsightsContract.Materialization.MaterializedView
      details.path("execution_mode").asText() shouldBe OpenIvmInsightsContract.ExecutionMode.MaterializedView
      details.path("native_target_relation").asText() shouldBe "issue67_stream"
      draft.terminal shouldBe Some(true)
    }

    it("reports saturation and interruption distinctly, never as successful stop") {
      Seq(
        new StreamingQueryStopRejectedException("issue67_target", observation) ->
          OpenIvmInsightsContract.Code.StreamingQueryStopRejected,
        new StreamingQueryStopInterruptedException("issue67_target", observation) ->
          OpenIvmInsightsContract.Code.StreamingQueryStopInterrupted
      ).foreach { case (error, code) =>
        val draft = StreamingInsightEvents.stopFailureDraft("operation", "issue67_target", "stop", 1L, None, error)
        draft.code shouldBe code
        draft.status shouldBe Some("failed")
        draft.level shouldBe OpenIvmInsightsContract.Level.Error
        draft.terminal shouldBe Some(true)
      }
    }
  }
}
