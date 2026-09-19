package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.scalatest.funspec.AnyFunSpec

private[streaming] final case class CompletedTargetState(
    path: String,
    deltaTableId: String,
    version: Long,
    activeFiles: Set[String]
)

class StreamingSqlCompletedRunSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("reissuing CREATE after AvailableNow completion") {
    it("performs no source-row or target-data work when the source is unchanged") {
      val source = "strsql_completed_noop_source"
      val target = "strsql_completed_noop_target"
      createDeltaSource(
        source,
        "id INT, value STRING, part STRING",
        partitions = Seq("part")
      )
      insertRows(source, "(1, 'one', 'east'), (2, 'two', 'west')")
      val declaration = projectionDeclaration(source, target)

      val initial              = createStreamingSqlToCompletion(declaration)
      val initialSourceVersion = sourceVersion(source)
      val initialTarget        = targetState(target)
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'east' AS part UNION ALL " +
          "SELECT 2, 'two', 'west'"
      )

      val replayOne       = createStreamingSqlToCompletion(declaration)
      val replayOneTarget = targetState(target)
      assertStableResume(initial, replayOne, initialTarget, replayOneTarget)
      sourceVersion(source) shouldBe initialSourceVersion
      replayOne.inputRows shouldBe 0L
      dataFileActions(initialTarget, replayOneTarget) shouldBe 0L
      replayOneTarget.activeFiles shouldBe initialTarget.activeFiles
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'east' AS part UNION ALL " +
          "SELECT 2, 'two', 'west'"
      )

      val replayTwo       = createStreamingSqlToCompletion(declaration)
      val replayTwoTarget = targetState(target)
      assertStableResume(initial, replayTwo, initialTarget, replayTwoTarget)
      replayTwo.status.runId should not be replayOne.status.runId
      sourceVersion(source) shouldBe initialSourceVersion
      replayTwo.inputRows shouldBe 0L
      dataFileActions(replayOneTarget, replayTwoTarget) shouldBe 0L
      replayTwoTarget.activeFiles shouldBe replayOneTarget.activeFiles
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'east' AS part UNION ALL " +
          "SELECT 2, 'two', 'west'"
      )
    }

    it("processes only new duplicate-valued inserts after completion") {
      val source = "strsql_completed_insert_source"
      val target = "strsql_completed_insert_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'seed', 'p')")
      val declaration = projectionDeclaration(source, target)

      val initial            = createStreamingSqlToCompletion(declaration)
      val initialTarget      = targetState(target)
      val sourceBeforeInsert = sourceVersion(source)
      insertRows(source, "(2, 'duplicate', 'p'), (2, 'duplicate', 'p')")
      val sourceAfterInsert = sourceVersion(source)
      sourceAfterInsert should be > sourceBeforeInsert

      val inserted       = createStreamingSqlToCompletion(declaration)
      val insertedTarget = targetState(target)
      assertStableResume(initial, inserted, initialTarget, insertedTarget)
      inserted.inputRows shouldBe 2L
      dataFileActions(initialTarget, insertedTarget) should be > 0L
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'seed' AS value, 'p' AS part UNION ALL " +
          "SELECT 2, 'duplicate', 'p' UNION ALL SELECT 2, 'duplicate', 'p'"
      )

      val replay       = createStreamingSqlToCompletion(declaration)
      val replayTarget = targetState(target)
      assertStableResume(initial, replay, initialTarget, replayTarget)
      replay.status.runId should not be inserted.status.runId
      sourceVersion(source) shouldBe sourceAfterInsert
      replay.inputRows shouldBe 0L
      dataFileActions(insertedTarget, replayTarget) shouldBe 0L
      replayTarget.activeFiles shouldBe insertedTarget.activeFiles
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'seed' AS value, 'p' AS part UNION ALL " +
          "SELECT 2, 'duplicate', 'p' UNION ALL SELECT 2, 'duplicate', 'p'"
      )
    }

    it("consumes a partition DELETE with ignoreChanges and accepts later inserts") {
      val source = "strsql_completed_delete_source"
      val target = "strsql_completed_delete_target"
      createDeltaSource(
        source,
        "id INT, value STRING, part STRING",
        partitions = Seq("part")
      )
      insertRows(source, "(1, 'retained-history', 'expired'), (2, 'steady', 'active')")
      val declaration = projectionDeclaration(
        source,
        target,
        readerOptions = Some(
          "'ignoreChanges' = 'true', 'skipChangeCommits' = 'false'"
        )
      )

      val initial            = createStreamingSqlToCompletion(declaration)
      val initialTarget      = targetState(target)
      val sourceBeforeDelete = sourceVersion(source)
      spark.sql(s"DELETE FROM ${quoteIdentifier(source)} WHERE part = 'expired'").collect()
      val sourceAfterDelete = sourceVersion(source)
      sourceAfterDelete should be > sourceBeforeDelete
      assertFramesBagEqual(
        spark.table(source),
        spark.sql("SELECT 2 AS id, 'steady' AS value, 'active' AS part")
      )

      val deleted       = createStreamingSqlToCompletion(declaration)
      val deletedTarget = targetState(target)
      assertStableResume(initial, deleted, initialTarget, deletedTarget)
      deleted.inputRows shouldBe 0L
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'retained-history' AS value, 'expired' AS part UNION ALL " +
          "SELECT 2, 'steady', 'active'"
      )

      insertRows(source, "(3, 'later', 'active'), (3, 'later', 'active')")
      val sourceAfterInsert = sourceVersion(source)
      sourceAfterInsert should be > sourceAfterDelete
      val inserted       = createStreamingSqlToCompletion(declaration)
      val insertedTarget = targetState(target)
      assertStableResume(initial, inserted, initialTarget, insertedTarget)
      inserted.inputRows shouldBe 2L
      dataFileActions(deletedTarget, insertedTarget) should be > 0L
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'retained-history' AS value, 'expired' AS part UNION ALL " +
          "SELECT 2, 'steady', 'active' UNION ALL " +
          "SELECT 3, 'later', 'active' UNION ALL SELECT 3, 'later', 'active'"
      )

      val replay       = createStreamingSqlToCompletion(declaration)
      val replayTarget = targetState(target)
      assertStableResume(initial, replay, initialTarget, replayTarget)
      replay.status.runId should not be inserted.status.runId
      sourceVersion(source) shouldBe sourceAfterInsert
      replay.inputRows shouldBe 0L
      dataFileActions(insertedTarget, replayTarget) shouldBe 0L
      replayTarget.activeFiles shouldBe insertedTarget.activeFiles
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'retained-history' AS value, 'expired' AS part UNION ALL " +
          "SELECT 2, 'steady', 'active' UNION ALL " +
          "SELECT 3, 'later', 'active' UNION ALL SELECT 3, 'later', 'active'"
      )
    }
  }

  private def projectionDeclaration(source: String, target: String, readerOptions: Option[String] = None): String = {
    val withClause = readerOptions.map(options => s" WITH ($options)").getOrElse("")
    s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
       |OPTIONS ('trigger' = 'availableNow')
       |AS SELECT id, value, part
       |FROM STREAM ${quoteIdentifier(source)}$withClause""".stripMargin
  }

  private def targetState(target: String): CompletedTargetState = {
    val path     = targetPath(target)
    val snapshot = DeltaLog.forTable(spark, new Path(path)).update()
    val activeFiles = snapshot.allFiles
      .collect()
      .map { file =>
        s"${file.path}\n${file.size}\n${file.modificationTime}"
      }
      .toSet
    CompletedTargetState(path, snapshot.metadata.id, snapshot.version, activeFiles)
  }

  private def dataFileActions(before: CompletedTargetState, after: CompletedTargetState): Long = {
    if (after.version <= before.version) 0L
    else {
      DeltaLog
        .forTable(spark, new Path(after.path))
        .getChanges(before.version + 1L, failOnDataLoss = true)
        .takeWhile(_._1 <= after.version)
        .flatMap(_._2.iterator)
        .count {
          case add: AddFile       => add.dataChange
          case remove: RemoveFile => remove.dataChange
          case _                  => false
        }
        .toLong
    }
  }

  private def assertStableResume(
      initial: SqlCompletedRun,
      resumed: SqlCompletedRun,
      initialTarget: CompletedTargetState,
      resumedTarget: CompletedTargetState
  ): Unit = {
    resumed.status.tableName shouldBe initial.status.tableName
    resumed.status.queryId shouldBe initial.status.queryId
    resumed.status.runId should not be initial.status.runId
    resumed.status.checkpointLocation shouldBe initial.status.checkpointLocation
    resumed.status.definitionHash shouldBe initial.status.definitionHash
    resumedTarget.path shouldBe initialTarget.path
    resumedTarget.deltaTableId shouldBe initialTarget.deltaTableId
  }
}
