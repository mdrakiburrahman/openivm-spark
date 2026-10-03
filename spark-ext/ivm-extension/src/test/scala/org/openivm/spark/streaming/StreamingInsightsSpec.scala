package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.openivm.spark.insights.OpenIvmInsightsContract.{BranchCode, Code, EventRecord, EventType, Operation}
import org.openivm.spark.insights.{OpenIvmInsightsBroker, OpenIvmInsightsContract}
import org.scalatest.funspec.AnyFunSpec

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class StreamingInsightsSpec extends AnyFunSpec with StreamingTableTestFixture {

  private val mapper = new ObjectMapper()

  describe("native streaming insight lifecycle") {
    it("emits S1-S4 with one operation, bounded progress, and terminal-last ordering") {
      val source = "strinsi_branches_source"
      val target = "strinsi_branches_target"
      createSource(source)

      var first: StreamingTableStatus = null
      val s1 = capture("s1", target) {
        first = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      }
      branch(s1, BranchCode.S1)
      codes(s1) should contain allOf (Code.StreamingTargetCreated, Code.StreamingQueryStarted)

      appendRows(source, "(1, 'one', 'p')")
      process(first)

      var reused: StreamingTableStatus = null
      val s2 = capture("s2", target) {
        reused = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      }
      branch(s2, BranchCode.S2)
      reused.queryId shouldBe first.queryId
      val reusedDetails = details(event(s2, Code.StreamingTargetReused))
      reusedDetails.path("checkpoint_location").asText() should not be empty
      reusedDetails.path("query_id").asText() shouldBe first.queryId.get
      reusedDetails.path("run_id").asText() shouldBe first.runId.get
      reusedDetails.path("active").asBoolean() shouldBe true
      val progress = reusedDetails.path("last_progress")
      progress.path("batch_id").isNumber shouldBe true
      progress.path("input_rows").isNumber shouldBe true
      progress.path("output_rows").isNumber shouldBe true
      progress.path("input_rows_per_second").isNumber shouldBe true
      progress.path("processed_rows_per_second").isNumber shouldBe true
      progress.path("batch_duration_ms").isNumber shouldBe true
      progress.path("timestamp").asText() should not be empty

      val stopped = capture("stop", target) {
        StreamingTableManager.stop(spark, Seq(target))
        ()
      }
      codes(stopped) should contain(Code.StreamingQueryStopped)

      var resumed: StreamingTableStatus = null
      val s3 = capture("s3", target) {
        resumed = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      }
      branch(s3, BranchCode.S3)
      codes(s3) should contain(Code.StreamingQueryResumed)
      resumed.queryId shouldBe first.queryId
      resumed.runId should not be first.runId

      var restarted: StreamingTableStatus = null
      val s4 = capture("s4", target) {
        restarted = createStreaming(
          target,
          readStream(source),
          s"SELECT id, value, part FROM STREAM $source",
          options = Map("displayName" -> "strinsi.retuned")
        )
      }
      branch(s4, BranchCode.S4)
      codes(s4) should contain allOf (Code.StreamingQueryStopped, Code.StreamingQueryRestarted)
      restarted.queryId shouldBe first.queryId
      restarted.runId should not be resumed.runId

      Seq(s1, s2, stopped, s3, s4).foreach(assertSingleOperationAndTerminalLast)
    }

    it("emits S5/S6 semantic decisions without dropping on rejection or leaking SQL and plans") {
      val source = "strinsi_semantic_source"
      val target = "strinsi_semantic_target"
      val secret = "strinsi_secret_query_marker"
      createSource(source)
      val original = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p'), (2, 'two', 'p')")
      process(original)
      val originalTarget =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)

      def changedFrame =
        readStream(source).selectExpr("id", s"concat(value, '-$secret') AS value", "part")
      val changedText =
        s"SELECT id, concat(value, '-$secret') AS value, part FROM STREAM $source"

      val s6 = capture("s6", target) {
        createStreaming(target, changedFrame, changedText)
        ()
      }
      s6.failure should not be empty
      val rejected        = details(branch(s6, BranchCode.S6))
      val rejectedChanges = rejected.path("changes")
      rejected.path("policy").asText() shouldBe "fail"
      rejected.path("previous_fingerprint").asText() should have length 64
      rejected.path("requested_fingerprint").asText() should have length 64
      rejected.path("previous_fingerprint_short").asText() should have length 12
      rejected.path("requested_fingerprint_short").asText() should have length 12
      rejected.path("normalizedQueryChanged").asBoolean() shouldBe true
      rejectedChanges.isArray shouldBe true
      rejectedChanges.size() should be > 0
      rejectedChanges.elements().asScala.foreach { change =>
        change.path("path").asText() should not be empty
        change.path("category").asText() should not be empty
        change.has("previous") shouldBe true
        change.has("requested") shouldBe true
      }
      val unchanged = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      unchanged.deltaTableId shouldBe originalTarget.deltaTableId
      original.queryId.flatMap(id => Option(spark.streams.get(id))).exists(_.isActive) shouldBe true
      val rejectedTerminal        = terminalOperation(s6)
      val rejectedTerminalDetails = details(rejectedTerminal)
      rejectedTerminal.eventType shouldBe EventType.OperationFailed
      rejectedTerminal.status shouldBe Some("create_failed")
      rejectedTerminalDetails.path(OpenIvmInsightsContract.DetailField.BranchCode).asText() shouldBe
        BranchCode.S6
      rejectedTerminalDetails.path(OpenIvmInsightsContract.DetailField.ErrorClass).asText() shouldBe
        classOf[org.apache.spark.sql.AnalysisException].getName
      assertNoQueryOrPlanLeakage(s6, secret)
      assertSingleOperationAndTerminalLast(s6)

      var rebuilt: StreamingTableStatus = null
      val s5 = capture("s5", target) {
        rebuilt = createStreaming(
          target,
          changedFrame,
          changedText,
          options = Map("onQueryChange" -> "rebuild")
        )
      }
      val rebuiltDecision = details(branch(s5, BranchCode.S5))
      rebuiltDecision.path("policy").asText() shouldBe "rebuild"
      rebuiltDecision.path("normalizedQueryChanged").asBoolean() shouldBe true
      codes(s5) should contain allOf (
        Code.StreamingCascadePlan,
        Code.StreamingCheckpointArchived,
        Code.StreamingTargetDropped,
        Code.StreamingTargetRecreated,
        Code.StreamingQueryStarted
      )
      val rebuildPlan = details(event(s5, Code.StreamingCascadePlan))
      rebuildPlan.path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "SEMANTIC_DEFINITION_CHANGED"
      rebuildPlan.path("descendant_count").asInt() shouldBe 0
      process(rebuilt)
      assertBagEqual(
        target,
        s"SELECT id, concat(value, '-$secret') AS value, part FROM `$source`"
      )
      assertNoQueryOrPlanLeakage(s5, secret)
      assertSingleOperationAndTerminalLast(s5)
    }

    it("emits S7 recovery with completed and remaining descendant state") {
      val source = "strinsi_recovery_source"
      val root   = "strinsi_recovery_root"
      val child  = "strinsi_recovery_child"
      val leaf   = "strinsi_recovery_leaf"
      createSource(source)
      val rootStatus = createStreaming(root, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(rootStatus)
      val childStatus = createStreaming(child, readStream(root), s"SELECT id, value, part FROM STREAM $root")
      process(childStatus)
      val leafStatus = createStreaming(leaf, readStream(child), s"SELECT id, value, part FROM STREAM $child")
      process(leafStatus)

      StreamingTableManager.setBeforeCascadeDropHookForTesting { target =>
        if (target.name.last == child) throw new IllegalStateException("injected recovery failure")
      }
      val changed     = readStream(source).where("id = 1")
      val changedText = s"SELECT id, value, part FROM STREAM $source WHERE id = 1"
      val interrupted = capture("s7-interrupted", root) {
        createStreaming(
          root,
          changed,
          changedText,
          options = Map("onQueryChange" -> "rebuild")
        )
        ()
      }
      interrupted.failure should not be empty
      branch(interrupted, BranchCode.S5)
      val failedCleanup = event(interrupted, Code.StreamingDescendantCleanupFailed)
      details(failedCleanup).path(OpenIvmInsightsContract.DetailField.ErrorClass).asText() shouldBe
        classOf[IllegalStateException].getName
      val interruptedTerminal = terminalOperation(interrupted)
      details(interruptedTerminal)
        .path(OpenIvmInsightsContract.DetailField.BranchCode)
        .asText() shouldBe BranchCode.S5

      StreamingTableManager.setBeforeCascadeDropHookForTesting((_: StreamingTableCascadeTarget) => ())
      var recovered: StreamingTableStatus = null
      val s7 = capture("s7", root) {
        recovered = createStreaming(
          root,
          readStream(source).where("id = 1"),
          changedText,
          options = Map("onQueryChange" -> "rebuild")
        )
      }

      val recoveryDecision = details(branch(s7, BranchCode.S7))
      recoveryDecision.path("completed_descendant_identities").size() shouldBe 1
      recoveryDecision.path("remaining_descendant_identities").size() shouldBe 1
      val planEvent = event(s7, Code.StreamingCascadePlan)
      val plan      = details(planEvent).path("descendants")
      details(planEvent).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "SEMANTIC_DEFINITION_CHANGED"
      plan.size() shouldBe 2
      plan.get(0).path("target_relation").asText() should endWith(leaf)
      plan.get(0).path("order").asInt() shouldBe 0
      plan.get(0).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "SEMANTIC_DEFINITION_CHANGED"
      plan.get(0).path("recovery_state").asText() shouldBe "completed"
      plan.get(1).path("target_relation").asText() should endWith(child)
      plan.get(1).path("order").asInt() shouldBe 1
      plan.get(1).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe
        "SEMANTIC_DEFINITION_CHANGED"
      plan.get(1).path("recovery_state").asText() shouldBe "remaining"
      val cleanupEvents = s7.events.filter(_.code == Code.StreamingDescendantCleanupCompleted)
      cleanupEvents.map(_.status.get).toSet should contain allOf ("already_completed", "completed")
      cleanupEvents.foreach(_.sequence should be > planEvent.sequence)
      val completedRecovery = s7.events
        .filter(_.code == Code.StreamingRecoveryStateObserved)
        .find(_.status.contains("completed"))
        .getOrElse(fail("Missing completed recovery state"))
      val completedDetails = details(completedRecovery)
      completedDetails.path("remaining_descendant_identities").size() shouldBe 0
      completedDetails.path("upstream_dropped").asBoolean() shouldBe true
      process(recovered)
      spark.catalog.tableExists(child) shouldBe false
      spark.catalog.tableExists(leaf) shouldBe false
      assertSingleOperationAndTerminalLast(s7)
    }

    it("emits S8 with a leaves-first mixed cascade and authoritative causes") {
      val source = "strinsi_drop_source"
      val root   = "strinsi_drop_root"
      val child  = "strinsi_drop_child"
      val mv     = "strinsi_drop_mv"
      createSource(source)
      val rootStatus = createStreaming(root, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      appendRows(source, "(1, 'one', 'p')")
      process(rootStatus)
      val childStatus = createStreaming(child, readStream(root), s"SELECT id, value, part FROM STREAM $root")
      process(childStatus)
      spark.sql(s"CREATE MATERIALIZED VIEW $mv AS SELECT id, value, part FROM $child").collect()

      val rootTarget =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(root), requireTableIdMarker = true)
      val childTarget =
        StreamingTableMetadata.resolveDeltaTarget(spark, Seq(child), requireTableIdMarker = true)

      val s8 = capture("s8", root) {
        StreamingTableManager.drop(spark, Seq(root), ifExists = false)
        ()
      }
      branch(s8, BranchCode.S8)
      val planEvent = event(s8, Code.StreamingCascadePlan)
      val plan      = details(planEvent).path("descendants")
      details(planEvent).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe "explicit_drop"
      plan.size() shouldBe 2
      plan.get(0).path("order").asInt() shouldBe 0
      plan.get(0).path("target_kind").asText() shouldBe "materialized"
      plan.get(0).path("target_relation").asText() should endWith(mv)
      plan.get(0).path("caused_by").asText() shouldBe childTarget.identity
      plan.get(0).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe "explicit_drop"
      plan.get(1).path("order").asInt() shouldBe 1
      plan.get(1).path("target_kind").asText() shouldBe "streaming"
      plan.get(1).path("target_relation").asText() should endWith(child)
      plan.get(1).path("caused_by").asText() shouldBe rootTarget.identity
      plan.get(1).path(OpenIvmInsightsContract.DetailField.Reason).asText() shouldBe "explicit_drop"

      val started  = s8.events.filter(_.code == Code.StreamingDescendantCleanupStarted)
      val finished = s8.events.filter(_.code == Code.StreamingDescendantCleanupCompleted)
      started.map(event => details(event).path("target_identity").asText()) shouldBe
        plan.elements().asScala.toSeq.map(_.path("target_identity").asText())
      finished should have size 2
      started.foreach(_.sequence should be > planEvent.sequence)
      codes(s8) should contain allOf (
        Code.StreamingCheckpointArchived,
        Code.StreamingTargetDropped,
        Code.OperationCompleted
      )
      spark.catalog.tableExists(root) shouldBe false
      spark.catalog.tableExists(child) shouldBe false
      spark.catalog.tableExists(mv) shouldBe false
      assertSingleOperationAndTerminalLast(s8)
    }
  }

  private final class Capture(val events: Vector[EventRecord], val failure: Option[Throwable])

  private def capture(name: String, target: String)(body: => Unit): Capture = {
    val requestId = s"strinsi-$name"
    OpenIvmInsightsBroker.begin(
      spark,
      requestId,
      s"run-$name",
      s"model.streaming.$name",
      Some(OpenIvmInsightsContract.Materialization.StreamingTable),
      Some(s"default.$target")
    )
    var failure = Option.empty[Throwable]
    try body
    catch {
      case NonFatal(error) => failure = Some(error)
    }
    OpenIvmInsightsBroker.end(spark, requestId, succeeded = failure.isEmpty)
    try {
      val result = new Capture(OpenIvmInsightsBroker.page(spark, requestId, 0L, 1000).events, failure)
      result.events.map(_.code) should contain noneOf (
        OpenIvmInsightsContract.Code.RegularSparkCompleted,
        OpenIvmInsightsContract.Code.RegularSparkFailed
      )
      result
    } finally OpenIvmInsightsBroker.release(spark, requestId)
  }

  private def branch(capture: Capture, code: String): EventRecord = {
    val result = capture.events
      .find(event => event.operation == Operation.Streaming && event.code == code)
      .getOrElse(fail(s"Missing streaming branch $code"))
    val branchDetails = details(result)
    branchDetails.path(OpenIvmInsightsContract.DetailField.BranchCode).asText() shouldBe code
    branchDetails.path("branch_label").asText() shouldBe OpenIvmInsightsContract.BranchCode.Labels(code)
    branchDetails.path(OpenIvmInsightsContract.DetailField.ExecutionMode).asText() shouldBe
      OpenIvmInsightsContract.ExecutionMode.Streaming
    branchDetails.path(OpenIvmInsightsContract.DetailField.Materialization).asText() shouldBe
      OpenIvmInsightsContract.Materialization.StreamingTable
    branchDetails.path(OpenIvmInsightsContract.DetailField.TargetRelation).asText() shouldBe
      result.materializedView.get
    result
  }

  private def event(capture: Capture, code: String): EventRecord =
    capture.events.find(_.code == code).getOrElse(fail(s"Missing insight event $code"))

  private def codes(capture: Capture): Set[String] =
    capture.events.map(_.code).toSet

  private def details(event: EventRecord): JsonNode =
    mapper.readTree(event.detailsJson.getOrElse(fail(s"Missing details for ${event.code}")))

  private def terminalOperation(capture: Capture): EventRecord = {
    val operationIds = capture.events.filter(_.operation == Operation.Streaming).flatMap(_.operationId).distinct
    operationIds should have size 1
    capture.events
      .find(event =>
        event.operationId.contains(operationIds.head) &&
          event.terminal.contains(true) &&
          Set(EventType.OperationCompleted, EventType.OperationFailed).contains(event.eventType)
      )
      .getOrElse(fail("Missing terminal streaming operation event"))
  }

  private def assertSingleOperationAndTerminalLast(capture: Capture): Unit = {
    val operationEvents = capture.events.filter(_.operation == Operation.Streaming)
    val operationIds    = operationEvents.flatMap(_.operationId).distinct
    operationIds should have size 1
    val terminal = terminalOperation(capture)
    terminal.sequence shouldBe capture.events.filter(_.operationId.contains(operationIds.head)).map(_.sequence).max
  }

  private def assertNoQueryOrPlanLeakage(capture: Capture, secret: String): Unit = {
    val rendered = capture.events
      .flatMap(event => Seq(event.message) ++ event.detailsJson.toSeq)
      .mkString("\n")
      .toLowerCase(java.util.Locale.ROOT)
    rendered should not include secret.toLowerCase(java.util.Locale.ROOT)
    rendered should not include "select id,"
    rendered should not include "streamingrelation"
    rendered should not include "project ["
  }
}
