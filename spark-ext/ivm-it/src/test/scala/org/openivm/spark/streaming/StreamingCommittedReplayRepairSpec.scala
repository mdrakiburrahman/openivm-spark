package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.openivm.spark.common.{DeltaTableVersion, FeatureGate}
import org.scalatest.funspec.AnyFunSpec

import scala.concurrent.duration._

class StreamingCommittedReplayRepairSpec extends AnyFunSpec with StreamingSqlTestSupport {
  private val repairKey = FeatureGate.StreamingRepairExpiredCommittedBatchReplayKey

  describe("expired committed-batch replay") {
    it("recovers a three-source UNION ALL without duplicates and consumes retained new data") {
      val sources     = Seq("strreplay_union_a", "strreplay_union_b", "strreplay_union_c")
      val target      = "strreplay_union_target"
      val declaration = seed(sources, target)
      val checkpoint  = completedCheckpoint(declaration)
      val before      = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
      sources.foreach { source =>
        insertRows(source, "(3)")
        expireBefore(source, 3L)
      }
      val listener = new SqlCompletionListener
      spark.streams.addListener(listener)
      spark.conf.set(repairKey, false)
      try {
        val failed = listener.await(createStreamingSql(declaration), 90.seconds)
        failed.exception.get should include("DELTA_MISSING_FILES_UNEXPECTED_VERSION")
      } finally {
        spark.conf.unset(repairKey)
        spark.streams.removeListener(listener)
      }

      val recovered = createStreamingSqlToCompletion(declaration)
      recovered.inputRows shouldBe 3L
      checkpointText(checkpoint, s"offsets/${before.batchId + 1L}") shouldBe
        checkpointText(checkpoint, s"offsets/${before.batchId}")
      checkpointText(checkpoint, s"commits/${before.batchId + 1L}") shouldBe
        checkpointText(checkpoint, s"commits/${before.batchId}")
      assertBagEqual(target, expected(sources.size, Seq(1, 2, 3)))
      val after = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
      after.batchId should be >= (before.batchId + 2L)
      val unchanged = createStreamingSqlToCompletion(declaration)
      unchanged.inputRows shouldBe 0L
      StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get.batchId shouldBe after.batchId
      assertBagEqual(target, expected(sources.size, Seq(1, 2, 3)))
      sources.foreach(insertRows(_, "(4)"))
      createStreamingSqlToCompletion(declaration).inputRows shouldBe 3L
      assertBagEqual(target, expected(sources.size, Seq(1, 2, 3, 4)))
    }

    it("recovers the production three-source CTE UNION ALL without changing source positions or losing history") {
      val sources     = Seq("strreplay_cte_a", "strreplay_cte_b", "strreplay_cte_c")
      val target      = "strreplay_cte_target"
      val declaration = seed(sources, target, withCtes = true)
      val checkpoint  = completedCheckpoint(declaration)
      val before      = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
      sources.foreach { source =>
        insertRows(source, "(3)")
        expireBefore(source, 3L)
      }

      createStreamingSqlToCompletion(declaration).inputRows shouldBe 3L
      checkpointText(checkpoint, s"offsets/${before.batchId + 1L}") shouldBe
        checkpointText(checkpoint, s"offsets/${before.batchId}")
      checkpointText(checkpoint, s"commits/${before.batchId + 1L}") shouldBe
        checkpointText(checkpoint, s"commits/${before.batchId}")
      assertBagEqual(target, expected(sources.size, Seq(1, 2, 3)))
      createStreamingSqlToCompletion(declaration).inputRows shouldBe 0L
      assertBagEqual(target, expected(sources.size, Seq(1, 2, 3)))
      sources.foreach(insertRows(_, "(4)"))
      createStreamingSqlToCompletion(declaration).inputRows shouldBe 3L
      assertBagEqual(target, expected(sources.size, Seq(1, 2, 3, 4)))
    }

    it("adds an identical horizon only once and safely resumes an interrupted empty batch") {
      val source      = "strreplay_idempotent_source"
      val target      = "strreplay_idempotent_target"
      val declaration = seed(Seq(source), target)
      val checkpoint  = completedCheckpoint(declaration)
      val before      = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
      insertRows(source, "(3)")
      expireBefore(source, 3L)
      val frame    = spark.readStream.format("delta").table(source)
      val resolved = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      StreamingCommittedReplayRepair.beforeStart(spark, frame, resolved)
      val repaired = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
      repaired.batchId shouldBe before.batchId + 1L
      repaired.previous shouldBe before.current
      repaired.current shouldBe before.current
      StreamingCommittedReplayRepair.beforeStart(spark, frame, resolved)
      StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get.batchId shouldBe repaired.batchId
      // Simulate interruption after the offset add but before the commit add.
      val commit = new Path(checkpoint, s"commits/${repaired.batchId}")
      assert(commit.getFileSystem(spark.sessionState.newHadoopConf()).delete(commit, false))
      createStreamingSqlToCompletion(declaration)
      assertBagEqual(target, expected(1, Seq(1, 2, 3)))
    }

    it("fails closed when a committed END offset has also expired") {
      val source      = "strreplay_lost_source"
      val target      = "strreplay_lost_target"
      val declaration = seed(Seq(source), target)
      val checkpoint  = completedCheckpoint(declaration)
      val before      = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
      insertRows(source, "(3), (4)")
      insertRows(source, "(5)")
      expireBefore(source, 4L)
      val error = intercept[Exception] { createStreamingSql(declaration) }
      error.getMessage should include("committed END offset")
      error.getMessage should include("full refresh")
      StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get.batchId shouldBe before.batchId
      assertBagEqual(target, expected(1, Seq(1, 2)))
    }

    it("does not modify a checkpoint for an aggregate even before a state directory exists") {
      assertNotRepaired("aggregate", stateful = true)
    }

    it("does not modify a checkpoint for an aggregate hidden inside a CTE") {
      assertNotRepaired("cte_aggregate", stateful = true, withCte = true)
    }

    it("does not modify a checkpoint with a state directory even if empty") {
      assertNotRepaired("state", stateDirectory = true)
    }

    it("does not modify a checkpoint whose latest offset batch is uncommitted") {
      assertNotRepaired("uncommitted", deleteCommit = true)
    }

    it("does not modify a checkpoint without the previous offset record") {
      assertNotRepaired("previous", deletePrevious = true)
    }
  }

  private def seed(sources: Seq[String], target: String, withCtes: Boolean = false): String = {
    sources.foreach { source =>
      createDeltaSource(source, "id INT")
      insertRows(source, "(1)")
    }
    val branches = sources.map(source => s"SELECT id FROM STREAM ${quoteIdentifier(source)}")
    val query =
      if (withCtes) {
        val definitions = branches.zipWithIndex.map { case (branch, index) =>
          s"branch_$index AS ($branch WHERE id >= 1)"
        }
        val selections = sources.indices.map(index => s"SELECT * FROM branch_$index")
        s"WITH ${definitions.mkString(", ")} ${selections.mkString(" UNION ALL ")}"
      } else branches.mkString(" UNION ALL ")
    val declaration =
      s"CREATE STREAMING TABLE ${quoteIdentifier(target)} OPTIONS ('trigger' = 'availableNow') AS $query"
    createStreamingSqlToCompletion(declaration)
    sources.foreach(insertRows(_, "(2)"))
    declaration
  }

  private def completedCheckpoint(declaration: String): String = {
    val checkpoint = createStreamingSqlToCompletion(declaration).status.checkpointLocation
    StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get.batchId should be >= 1L
    checkpoint
  }

  private def expireBefore(source: String, version: Long): Unit = {
    val log      = DeltaTableVersion.deltaLogOption(spark, source).get
    val snapshot = log.update()
    // Keep a complete source snapshot but remove the early JSON commits, as real log cleanup does.
    log.checkpoint(snapshot)
    val fs = log.logPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.listStatus(log.logPath).foreach { file =>
      val name = file.getPath.getName
      if (name.matches("[0-9]{20}\\.json") && name.take(20).toLong < version)
        assert(fs.delete(file.getPath, false))
    }
  }

  private def assertNotRepaired(
      suffix: String,
      stateful: Boolean = false,
      stateDirectory: Boolean = false,
      deleteCommit: Boolean = false,
      deletePrevious: Boolean = false,
      withCte: Boolean = false
  ): Unit = {
    val source      = s"strreplay_${suffix}_source"
    val target      = s"strreplay_${suffix}_target"
    val declaration = seed(Seq(source), target)
    val checkpoint  = completedCheckpoint(declaration)
    val batch       = StreamingReplayCheckpointLog.latestCommitted(spark, checkpoint).get
    val fs          = new Path(checkpoint).getFileSystem(spark.sessionState.newHadoopConf())
    if (stateDirectory) assert(fs.mkdirs(new Path(checkpoint, "state")))
    if (deleteCommit) assert(fs.delete(new Path(checkpoint, s"commits/${batch.batchId}"), false))
    if (deletePrevious) assert(fs.delete(new Path(checkpoint, s"offsets/${batch.batchId - 1L}"), false))
    insertRows(source, "(3)")
    expireBefore(source, 3L)
    val input = spark.readStream.format("delta").table(source)
    val frame =
      if (withCte) {
        val view = s"strreplay_${suffix}_input"
        input.createOrReplaceTempView(view)
        spark.sql(s"WITH counts AS (SELECT id, count(*) AS count FROM $view GROUP BY id) SELECT * FROM counts")
      } else if (stateful) input.groupBy("id").count()
      else input
    val resolved = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
    StreamingCommittedReplayRepair.beforeStart(spark, frame, resolved)
    fs.exists(new Path(checkpoint, s"offsets/${batch.batchId + 1L}")) shouldBe false
    fs.exists(new Path(checkpoint, s"commits/${batch.batchId + 1L}")) shouldBe false
  }

  private def checkpointText(checkpoint: String, relative: String): String = {
    val path   = new Path(checkpoint, relative)
    val fs     = path.getFileSystem(spark.sessionState.newHadoopConf())
    val source = scala.io.Source.fromInputStream(fs.open(path), "UTF-8")
    try source.mkString
    finally source.close()
  }

  private def expected(copies: Int, ids: Seq[Int]): String =
    Seq.fill(copies)(ids.map(id => s"SELECT $id AS id")).flatten.mkString(" UNION ALL ")
}
