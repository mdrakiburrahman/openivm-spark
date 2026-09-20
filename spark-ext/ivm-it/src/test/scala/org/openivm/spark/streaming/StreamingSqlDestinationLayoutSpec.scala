package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.scalatest.funspec.AnyFunSpec

import scala.io.Source

private[streaming] final case class DestinationLayoutState(
    path: String,
    deltaTableId: String,
    version: Long,
    activeFiles: Set[String],
    partitionColumns: Seq[String],
    clusteringColumns: Seq[Seq[String]],
    writerFeatures: Set[String]
)

class StreamingSqlDestinationLayoutSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("streaming destination layouts") {
    it("maintains a Hive-partitioned destination across completed runs and a new partition") {
      val source = "strsql_layout_partition_source"
      val target = "strsql_layout_partition_target"
      createLayoutSource(source)
      insertRows(
        source,
        "(1, 'east', DATE '2024-01-01', 'one'), " +
          "(2, 'west', DATE '2024-01-01', 'two')"
      )
      val declaration = partitionedDeclaration(source, target)
      val initialExpected =
        "SELECT 1 AS id, 'east' AS region, DATE '2024-01-01' AS event_date, 'one' AS value " +
          "UNION ALL SELECT 2, 'west', DATE '2024-01-01', 'two'"
      val insertedExpected =
        initialExpected + " UNION ALL SELECT 3, 'north', DATE '2024-01-02', 'three'"

      val initial       = createStreamingSqlToCompletion(declaration)
      val initialSource = sourceLayoutState(source)
      val initialTarget = targetLayoutState(target)
      initialSource.partitionColumns shouldBe empty
      initialSource.clusteringColumns shouldBe empty
      initialTarget.partitionColumns shouldBe Seq("event_date")
      initialTarget.clusteringColumns shouldBe empty
      targetPartitionValues(target) shouldBe Set("2024-01-01")
      assertBagEqual(target, initialExpected)

      val sourceBeforeReplay = sourceVersion(source)
      val replay             = createStreamingSqlToCompletion(declaration)
      val replayTarget       = targetLayoutState(target)
      assertStableLayoutResume(initial, replay, initialTarget, replayTarget)
      sourceVersion(source) shouldBe sourceBeforeReplay
      replay.inputRows shouldBe 0L
      dataFileActions(initialTarget, replayTarget) shouldBe 0L
      replayTarget.activeFiles shouldBe initialTarget.activeFiles

      insertRows(source, "(3, 'north', DATE '2024-01-02', 'three')")
      val inserted       = createStreamingSqlToCompletion(declaration)
      val insertedTarget = targetLayoutState(target)
      assertStableLayoutResume(initial, inserted, initialTarget, insertedTarget)
      inserted.inputRows shouldBe 1L
      insertedTarget.partitionColumns shouldBe Seq("event_date")
      targetPartitionValues(target) shouldBe Set("2024-01-01", "2024-01-02")
      assertBagEqual(target, insertedExpected)

      val finalReplay       = createStreamingSqlToCompletion(declaration)
      val finalReplayTarget = targetLayoutState(target)
      assertStableLayoutResume(initial, finalReplay, initialTarget, finalReplayTarget)
      finalReplay.inputRows shouldBe 0L
      dataFileActions(insertedTarget, finalReplayTarget) shouldBe 0L
      finalReplayTarget.activeFiles shouldBe insertedTarget.activeFiles
      assertBagEqual(target, insertedExpected)
    }

    it("maintains a multi-key liquid-clustered destination through OPTIMIZE and resume") {
      val source = "strsql_layout_cluster_source"
      val target = "strsql_layout_cluster_target"
      createLayoutSource(source)
      insertRows(
        source,
        "(1, 'east', DATE '2024-02-01', 'one'), " +
          "(2, 'west', DATE '2024-02-01', 'two')"
      )
      val declaration = clusteredDeclaration(source, target, "region, event_date")
      val initialExpected =
        "SELECT 1 AS id, 'east' AS region, DATE '2024-02-01' AS event_date, 'one' AS value " +
          "UNION ALL SELECT 2, 'west', DATE '2024-02-01', 'two'"
      val insertedExpected =
        initialExpected + " UNION ALL SELECT 3, 'north', DATE '2024-02-02', 'three'"
      val resumedExpected =
        insertedExpected + " UNION ALL SELECT 4, 'south', DATE '2024-02-03', 'four'"

      val initial       = createStreamingSqlToCompletion(declaration)
      val initialSource = sourceLayoutState(source)
      val initialTarget = targetLayoutState(target)
      initialSource.partitionColumns shouldBe empty
      initialSource.clusteringColumns shouldBe empty
      assertClustered(initialTarget, Seq(Seq("region"), Seq("event_date")))
      assertBagEqual(target, initialExpected)

      val sourceBeforeReplay = sourceVersion(source)
      val replay             = createStreamingSqlToCompletion(declaration)
      val replayTarget       = targetLayoutState(target)
      assertStableLayoutResume(initial, replay, initialTarget, replayTarget)
      sourceVersion(source) shouldBe sourceBeforeReplay
      replay.inputRows shouldBe 0L
      dataFileActions(initialTarget, replayTarget) shouldBe 0L
      replayTarget.activeFiles shouldBe initialTarget.activeFiles

      insertRows(source, "(3, 'north', DATE '2024-02-02', 'three')")
      val inserted       = createStreamingSqlToCompletion(declaration)
      val insertedTarget = targetLayoutState(target)
      assertStableLayoutResume(initial, inserted, initialTarget, insertedTarget)
      inserted.inputRows shouldBe 1L
      assertClustered(insertedTarget, Seq(Seq("region"), Seq("event_date")))
      assertBagEqual(target, insertedExpected)

      val noChange       = createStreamingSqlToCompletion(declaration)
      val noChangeTarget = targetLayoutState(target)
      noChange.inputRows shouldBe 0L
      dataFileActions(insertedTarget, noChangeTarget) shouldBe 0L
      noChangeTarget.activeFiles shouldBe insertedTarget.activeFiles
      assertBagEqual(target, insertedExpected)
      noException should be thrownBy {
        spark.sql(s"OPTIMIZE ${quoteIdentifier(target)}").collect()
      }
      val optimizedTarget = targetLayoutState(target)
      optimizedTarget.deltaTableId shouldBe initialTarget.deltaTableId
      assertClustered(optimizedTarget, Seq(Seq("region"), Seq("event_date")))
      assertBagEqual(target, insertedExpected)

      insertRows(source, "(4, 'south', DATE '2024-02-03', 'four')")
      val resumed       = createStreamingSqlToCompletion(declaration)
      val resumedTarget = targetLayoutState(target)
      assertStableLayoutResume(initial, resumed, initialTarget, resumedTarget)
      resumed.status.runId should not be noChange.status.runId
      resumed.inputRows shouldBe 1L
      assertClustered(resumedTarget, Seq(Seq("region"), Seq("event_date")))
      assertBagEqual(target, resumedExpected)
    }

    it("supports one quoted liquid-clustering key without requiring OPTIMIZE") {
      val source = "strsql_layout_quoted_source"
      val target = "strsql_layout_quoted_target"
      createLayoutSource(source)
      insertRows(source, "(1, 'east', DATE '2024-03-01', 'one')")
      val declaration =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |CLUSTER BY (`region.name`)
           |OPTIONS ('trigger' = 'availableNow')
           |AS
           |SELECT
           |  id,
           |  source_region AS `region.name`,
           |  source_event_date AS event_date,
           |  value
           |FROM STREAM ${quoteIdentifier(source)}""".stripMargin

      val initial       = createStreamingSqlToCompletion(declaration)
      val initialTarget = targetLayoutState(target)
      assertClustered(initialTarget, Seq(Seq("region.name")))
      assertFramesBagEqual(
        spark.table(target),
        spark.sql(
          "SELECT 1 AS id, 'east' AS `region.name`, DATE '2024-03-01' AS event_date, 'one' AS value"
        )
      )

      val sourceBeforeReplay = sourceVersion(source)
      val replay             = createStreamingSqlToCompletion(declaration)
      val replayTarget       = targetLayoutState(target)
      assertStableLayoutResume(initial, replay, initialTarget, replayTarget)
      sourceVersion(source) shouldBe sourceBeforeReplay
      replay.inputRows shouldBe 0L
      dataFileActions(initialTarget, replayTarget) shouldBe 0L
      replayTarget.activeFiles shouldBe initialTarget.activeFiles
      assertFramesBagEqual(
        spark.table(target),
        spark.sql(
          "SELECT 1 AS id, 'east' AS `region.name`, DATE '2024-03-01' AS event_date, 'one' AS value"
        )
      )

      insertRows(source, "(2, 'west', DATE '2024-03-02', 'two')")
      val inserted       = createStreamingSqlToCompletion(declaration)
      val insertedTarget = targetLayoutState(target)
      assertStableLayoutResume(initial, inserted, initialTarget, insertedTarget)
      inserted.inputRows shouldBe 1L
      assertClustered(insertedTarget, Seq(Seq("region.name")))
      assertFramesBagEqual(
        spark.table(target),
        spark.sql(
          """SELECT 1 AS id, 'east' AS `region.name`, DATE '2024-03-01' AS event_date, 'one' AS value
            |UNION ALL
            |SELECT 2, 'west', DATE '2024-03-02', 'two'""".stripMargin
        )
      )
    }

    it("rejects mixed, unknown, and duplicate layout keys before target mutation") {
      val source = "strsql_layout_invalid_source"
      createLayoutSource(source)
      val baseSelect = layoutSelect(source)

      val invalidDeclarations = Seq(
        "strsql_layout_invalid_mixed" ->
          s"""CREATE STREAMING TABLE strsql_layout_invalid_mixed
             |PARTITIONED BY (event_date)
             |CLUSTER BY (region)
             |OPTIONS ('trigger' = 'availableNow')
             |AS $baseSelect""".stripMargin,
        "strsql_layout_invalid_unknown" ->
          s"""CREATE STREAMING TABLE strsql_layout_invalid_unknown
             |CLUSTER BY (missing_output)
             |OPTIONS ('trigger' = 'availableNow')
             |AS $baseSelect""".stripMargin,
        "strsql_layout_invalid_duplicate" ->
          s"""CREATE STREAMING TABLE strsql_layout_invalid_duplicate
             |CLUSTER BY (region, REGION)
             |OPTIONS ('trigger' = 'availableNow')
             |AS $baseSelect""".stripMargin
      )

      invalidDeclarations.foreach { case (target, declaration) =>
        an[org.apache.spark.sql.AnalysisException] should be thrownBy {
          createStreamingSqlToCompletion(declaration)
        }
        spark.catalog.tableExists(target) shouldBe false
      }
    }

    it("fails a clustering change by default and applies an explicit owned rebuild") {
      val source = "strsql_layout_change_source"
      val target = "strsql_layout_change_target"
      createLayoutSource(source)
      insertRows(
        source,
        "(1, 'east', DATE '2024-04-01', 'one'), " +
          "(2, 'west', DATE '2024-04-02', 'two')"
      )
      val originalDeclaration = clusteredDeclaration(source, target, "region, event_date")
      val changedDeclaration  = clusteredDeclaration(source, target, "region")

      val initial       = createStreamingSqlToCompletion(originalDeclaration)
      val initialTarget = targetLayoutState(target)
      assertClustered(initialTarget, Seq(Seq("region"), Seq("event_date")))
      val expected =
        """SELECT 1 AS id, 'east' AS region, DATE '2024-04-01' AS event_date, 'one' AS value
          |UNION ALL
          |SELECT 2, 'west', DATE '2024-04-02', 'two'""".stripMargin
      assertBagEqual(target, expected)

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSqlToCompletion(changedDeclaration)
      }
      val rejectedTarget = targetLayoutState(target)
      rejectedTarget shouldBe initialTarget
      pathExists(initial.status.checkpointLocation) shouldBe true
      assertBagEqual(target, expected)

      val rebuildDeclaration =
        changedDeclaration.replace(
          "OPTIONS ('trigger' = 'availableNow')",
          "OPTIONS ('trigger' = 'availableNow', 'onQueryChange' = 'rebuild')"
        )
      val rebuilt       = createStreamingSqlToCompletion(rebuildDeclaration)
      val rebuiltTarget = targetLayoutState(target)
      rebuilt.status.queryId should not be initial.status.queryId
      rebuiltTarget.deltaTableId should not be initialTarget.deltaTableId
      assertClustered(rebuiltTarget, Seq(Seq("region")))
      assertBagEqual(target, expected)
    }

    it("resumes an unclustered v1 manifest with no clustering field") {
      val source = "strsql_layout_legacy_source"
      val target = "strsql_layout_legacy_target"
      createLayoutSource(source)
      insertRows(source, "(1, 'east', DATE '2024-05-01', 'one')")
      val declaration =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |OPTIONS ('trigger' = 'availableNow')
           |AS ${layoutSelect(source)}""".stripMargin

      val initial       = createStreamingSqlToCompletion(declaration)
      val initialTarget = targetLayoutState(target)
      initialTarget.clusteringColumns shouldBe empty
      val sourceBeforeResume = sourceVersion(source)
      val legacyFingerprint  = rewriteAsLegacyUnclusteredManifest(target)

      val resumed       = createStreamingSqlToCompletion(declaration)
      val resumedTarget = targetLayoutState(target)
      resumed.status.queryId shouldBe initial.status.queryId
      resumed.status.runId should not be initial.status.runId
      resumed.status.checkpointLocation shouldBe initial.status.checkpointLocation
      resumed.status.definitionHash shouldBe legacyFingerprint
      resumedTarget.deltaTableId shouldBe initialTarget.deltaTableId
      resumedTarget.clusteringColumns shouldBe empty
      sourceVersion(source) shouldBe sourceBeforeResume
      resumed.inputRows shouldBe 0L
      dataFileActions(initialTarget, resumedTarget) shouldBe 0L
      resumedTarget.activeFiles shouldBe initialTarget.activeFiles
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'east' AS region, DATE '2024-05-01' AS event_date, 'one' AS value"
      )
    }
  }

  private def createLayoutSource(source: String): Unit =
    createDeltaSource(
      source,
      "id INT, source_region STRING, source_event_date DATE, value STRING"
    )

  private def layoutSelect(source: String): String =
    s"""SELECT
       |  id,
       |  source_region AS region,
       |  source_event_date AS event_date,
       |  value
       |FROM STREAM ${quoteIdentifier(source)}""".stripMargin

  private def partitionedDeclaration(source: String, target: String): String =
    s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
       |PARTITIONED BY (event_date)
       |OPTIONS ('trigger' = 'availableNow')
       |AS ${layoutSelect(source)}""".stripMargin

  private def clusteredDeclaration(source: String, target: String, columns: String): String =
    s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
       |CLUSTER BY ($columns)
       |OPTIONS ('trigger' = 'availableNow')
       |AS ${layoutSelect(source)}""".stripMargin

  private def tablePath(table: String): String =
    spark
      .sql(s"DESCRIBE DETAIL ${quoteIdentifier(table)}")
      .select("location")
      .head()
      .getString(0)

  private def sourceLayoutState(source: String): DestinationLayoutState =
    layoutState(tablePath(source))

  private def targetLayoutState(target: String): DestinationLayoutState =
    layoutState(targetPath(target))

  private def layoutState(path: String): DestinationLayoutState = {
    val snapshot = DeltaLog.forTable(spark, new Path(path)).update()
    val clustering = ClusteringMetadataDomain
      .fromSnapshot(snapshot)
      .map(_.clusteringColumns)
      .getOrElse(Seq.empty)
    val activeFiles = snapshot.allFiles
      .collect()
      .map(file => s"${file.path}\n${file.size}\n${file.modificationTime}")
      .toSet
    DestinationLayoutState(
      path = path,
      deltaTableId = snapshot.metadata.id,
      version = snapshot.version,
      activeFiles = activeFiles,
      partitionColumns = snapshot.metadata.partitionColumns,
      clusteringColumns = clustering,
      writerFeatures = snapshot.protocol.writerFeatures.getOrElse(Set.empty)
    )
  }

  private def assertClustered(state: DestinationLayoutState, expectedColumns: Seq[Seq[String]]): Unit = {
    state.partitionColumns shouldBe empty
    state.clusteringColumns shouldBe expectedColumns
    state.writerFeatures should contain("clustering")
    state.writerFeatures should contain("domainMetadata")
  }

  private def targetPartitionValues(target: String): Set[String] =
    DeltaLog
      .forTable(spark, new Path(targetPath(target)))
      .update()
      .allFiles
      .collect()
      .flatMap(_.partitionValues.get("event_date"))
      .toSet

  private def dataFileActions(before: DestinationLayoutState, after: DestinationLayoutState): Long = {
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

  private def assertStableLayoutResume(
      initial: SqlCompletedRun,
      resumed: SqlCompletedRun,
      initialTarget: DestinationLayoutState,
      resumedTarget: DestinationLayoutState
  ): Unit = {
    resumed.status.tableName shouldBe initial.status.tableName
    resumed.status.queryId shouldBe initial.status.queryId
    resumed.status.runId should not be initial.status.runId
    resumed.status.checkpointLocation shouldBe initial.status.checkpointLocation
    resumed.status.definitionHash shouldBe initial.status.definitionHash
    resumedTarget.path shouldBe initialTarget.path
    resumedTarget.deltaTableId shouldBe initialTarget.deltaTableId
    resumedTarget.partitionColumns shouldBe initialTarget.partitionColumns
    resumedTarget.clusteringColumns shouldBe initialTarget.clusteringColumns
  }

  private def rewriteAsLegacyUnclusteredManifest(target: String): String = {
    val targetInfo = StreamingTableMetadata.resolveDeltaTarget(
      spark,
      Seq(target),
      requireTableIdMarker = true
    )
    val path   = StreamingTableMetadata.definitionPath(targetInfo)
    val fs     = path.getFileSystem(spark.sessionState.newHadoopConf())
    val input  = fs.open(path)
    val source = Source.fromInputStream(input, "UTF-8")
    val json =
      try source.mkString
      finally source.close()
    val mapper     = new ObjectMapper()
    val root       = mapper.readTree(json).asInstanceOf[ObjectNode]
    val definition = root.get("definition").asInstanceOf[ObjectNode]
    val semantic   = definition.get("semantic").asInstanceOf[ObjectNode]
    semantic.remove("clusterColumns")
    semantic.remove("clusteringColumns")
    val fingerprint = StreamingTableDefinition.sha256(semantic.toString)
    definition.put("fingerprint", fingerprint)
    val output = fs.create(path, true)
    try output.write(mapper.writeValueAsBytes(root))
    finally output.close()
    fingerprint
  }
}
