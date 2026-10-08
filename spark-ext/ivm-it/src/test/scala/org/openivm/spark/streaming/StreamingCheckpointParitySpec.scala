package org.openivm.spark.streaming

import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funspec.AnyFunSpec

private[streaming] final case class ParityFixture(
    directSource: String,
    managedSource: String,
    directTarget: String,
    managedTarget: String,
    directCheckpoint: String,
    managedDeclaration: String
)

class StreamingCheckpointParitySpec extends AnyFunSpec with StreamingCheckpointParityTestSupport {

  describe("native Structured Streaming checkpoint parity") {
    it("matches direct Spark offset and commit semantics across an unchanged restart") {
      val fixture = createParityFixture("resume")

      val directInitial  = runDirect(fixture)
      val managedInitial = runManaged(fixture)
      assertSucceeded(directInitial)
      assertSucceeded(managedInitial)

      val directInitialCheckpoint  = checkpointSnapshot(fixture.directCheckpoint)
      val managedInitialCheckpoint = checkpointSnapshot(managedInitial.status.checkpointLocation)
      directInitialCheckpoint.semantics shouldBe managedInitialCheckpoint.semantics
      directInitialCheckpoint.semantics.offsetBatches shouldBe Vector(0L)
      directInitialCheckpoint.semantics.commitBatches shouldBe Vector(0L)
      val directTargetVersion  = deltaTableVersion(fixture.directTarget)
      val managedTargetVersion = deltaTableVersion(fixture.managedTarget)

      val directResumed  = runDirect(fixture)
      val managedResumed = runManaged(fixture)
      assertSucceeded(directResumed)
      assertSucceeded(managedResumed)

      directResumed.status.queryId shouldBe directInitial.status.queryId
      directResumed.status.runId should not be directInitial.status.runId
      managedResumed.status.queryId shouldBe managedInitial.status.queryId
      managedResumed.status.runId should not be managedInitial.status.runId
      managedResumed.status.definitionHash shouldBe managedInitial.status.definitionHash
      val directResumedCheckpoint  = checkpointSnapshot(fixture.directCheckpoint)
      val managedResumedCheckpoint = checkpointSnapshot(managedResumed.status.checkpointLocation)
      directResumedCheckpoint shouldBe directInitialCheckpoint
      managedResumedCheckpoint shouldBe managedInitialCheckpoint
      directResumedCheckpoint.semantics shouldBe managedResumedCheckpoint.semantics
      deltaTableVersion(fixture.directTarget) shouldBe directTargetVersion
      deltaTableVersion(fixture.managedTarget) shouldBe managedTargetVersion
      assertFramesBagEqual(spark.table(fixture.directTarget), spark.table(fixture.managedTarget))
    }

    it("matches direct Spark failure without mutating managed state when required history is truncated") {
      val fixture = createParityFixture("truncated")

      val directInitial  = runDirect(fixture)
      val managedInitial = runManaged(fixture)
      assertSucceeded(directInitial)
      assertSucceeded(managedInitial)

      val directCheckpointBefore  = checkpointSnapshot(fixture.directCheckpoint)
      val managedCheckpointBefore = checkpointSnapshot(managedInitial.status.checkpointLocation)
      directCheckpointBefore.semantics shouldBe managedCheckpointBefore.semantics
      directCheckpointBefore.semantics.offsetBatches shouldBe Vector(0L)
      directCheckpointBefore.semantics.commitBatches shouldBe Vector(0L)
      val directRequiredVersion  = requiredHistoricalSnapshot(directCheckpointBefore)
      val managedRequiredVersion = requiredHistoricalSnapshot(managedCheckpointBefore)
      directRequiredVersion shouldBe managedRequiredVersion

      val directTargetId      = deltaTableId(fixture.directTarget)
      val directTargetVersion = deltaTableVersion(fixture.directTarget)
      val managedTarget = StreamingTableMetadata.resolveDeltaTarget(
        spark,
        Seq(fixture.managedTarget),
        requireTableIdMarker = true
      )
      val managedTargetVersion = deltaTableVersion(fixture.managedTarget)
      val managedArchives      = archivedCheckpoints(managedTarget)
      StreamingTableMetadata.pendingResetIntent(spark, managedTarget) shouldBe None

      advanceSource(fixture.directSource)
      advanceSource(fixture.managedSource)
      val directLatest = truncateDeltaHistoryBeforeLatestCheckpoint(
        fixture.directSource,
        directRequiredVersion
      )
      val managedLatest = truncateDeltaHistoryBeforeLatestCheckpoint(
        fixture.managedSource,
        managedRequiredVersion
      )
      directLatest shouldBe managedLatest
      spark.table(fixture.directSource).count() shouldBe 4L
      spark.table(fixture.managedSource).count() shouldBe 4L

      val directFailure  = runDirect(fixture)
      val managedFailure = runManaged(fixture)
      directFailure.exception should not be empty
      managedFailure.exception should not be empty
      val directDeltaErrors  = deltaErrorClasses(directFailure)
      val managedDeltaErrors = deltaErrorClasses(managedFailure)
      directDeltaErrors should not be empty
      managedDeltaErrors shouldBe directDeltaErrors
      directDeltaErrors.exists(isTruncatedHistoryError) shouldBe true

      checkpointSnapshot(fixture.directCheckpoint) shouldBe directCheckpointBefore
      checkpointSnapshot(managedInitial.status.checkpointLocation) shouldBe managedCheckpointBefore
      deltaTableId(fixture.directTarget) shouldBe directTargetId
      deltaTableVersion(fixture.directTarget) shouldBe directTargetVersion
      val managedTargetAfter = StreamingTableMetadata.resolveDeltaTarget(
        spark,
        Seq(fixture.managedTarget),
        requireTableIdMarker = true
      )
      managedTargetAfter.deltaTableId shouldBe managedTarget.deltaTableId
      managedTargetAfter.dataPath shouldBe managedTarget.dataPath
      deltaTableVersion(fixture.managedTarget) shouldBe managedTargetVersion
      archivedCheckpoints(managedTargetAfter) shouldBe managedArchives
      StreamingTableMetadata.pendingResetIntent(spark, managedTargetAfter) shouldBe None
      spark.catalog.tableExists(fixture.managedTarget) shouldBe true
    }
  }

  private def createParityFixture(suffix: String): ParityFixture = {
    val directSource     = s"strsql_checkpoint_${suffix}_direct_source"
    val managedSource    = s"strsql_checkpoint_${suffix}_managed_source"
    val directTarget     = s"strsql_checkpoint_${suffix}_direct_target"
    val managedTarget    = s"strsql_checkpoint_${suffix}_managed_target"
    val directCheckpoint = scratchPath(s"checkpoint-$suffix-direct")
    createCheckpointedSource(directSource)
    createCheckpointedSource(managedSource)
    spark.sql(s"CREATE TABLE ${quoteIdentifier(directTarget)} (id INT, value STRING) USING DELTA").collect()
    val declaration =
      s"""CREATE STREAMING TABLE ${quoteIdentifier(managedTarget)}
         |USING DELTA
         |OPTIONS ('trigger' = 'availableNow')
         |AS SELECT id, value
         |FROM STREAM ${quoteIdentifier(managedSource)}
         |WITH ('maxFilesPerTrigger' = '1000')""".stripMargin
    ParityFixture(
      directSource,
      managedSource,
      directTarget,
      managedTarget,
      directCheckpoint,
      declaration
    )
  }

  private def createCheckpointedSource(name: String): Unit = {
    spark
      .sql(
        s"""CREATE TABLE ${quoteIdentifier(name)} (id INT, value STRING)
           |USING DELTA
           |TBLPROPERTIES ('delta.checkpointInterval' = '1')""".stripMargin
      )
      .collect()
    insertRows(name, "(1, 'initial')")
  }

  private def advanceSource(name: String): Unit =
    (2 to 4).foreach(id => insertRows(name, s"($id, 'later-$id')"))

  private def runDirect(fixture: ParityFixture): SqlCompletedRun =
    awaitStreamingRun {
      val query = track(
        spark.readStream
          .format("delta")
          .option("maxFilesPerTrigger", "1000")
          .table(fixture.directSource)
          .select("id", "value")
          .writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", fixture.directCheckpoint)
          .trigger(Trigger.AvailableNow())
          .toTable(fixture.directTarget)
      )
      SqlStreamingStatus(
        tableName = fixture.directTarget,
        queryId = query.id.toString,
        runId = query.runId.toString,
        status = "direct",
        checkpointLocation = fixture.directCheckpoint,
        definitionHash = "direct"
      )
    }

  private def runManaged(fixture: ParityFixture): SqlCompletedRun =
    awaitStreamingRun(createStreamingSql(fixture.managedDeclaration))

  private def assertSucceeded(run: SqlCompletedRun): Unit =
    run.exception shouldBe None

  private def requiredHistoricalSnapshot(snapshot: StreamingCheckpointSnapshot): Long = {
    snapshot.semantics.sourceOffsets should have size 1
    val offset = snapshot.semantics.sourceOffsets.head
    offset.index shouldBe -1L
    offset.reservoirVersion - 1L
  }

  private def isTruncatedHistoryError(errorClass: String): Boolean =
    errorClass.contains("TRUNCATED_TRANSACTION_LOG") ||
      errorClass.contains("LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE") ||
      errorClass.contains("MISSING_FILES")
}
