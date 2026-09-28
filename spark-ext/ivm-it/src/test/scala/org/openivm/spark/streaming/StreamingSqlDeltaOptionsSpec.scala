package org.openivm.spark.streaming

import org.apache.spark.sql.streaming.StreamingQueryException
import org.scalatest.funspec.AnyFunSpec

class StreamingSqlDeltaOptionsSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("Delta streaming source options through SQL") {
    it("reads the current snapshot after historical mutations and fails historical replay by default") {
      val source        = "strsql_opts_snapshot_source"
      val currentTarget = "strsql_opts_snapshot_current"
      val historyTarget = "strsql_opts_snapshot_history"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      spark
        .sql(
          s"""ALTER TABLE ${quoteIdentifier(source)}
             |SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')""".stripMargin
        )
        .collect()
      insertRows(source, "(1, 'one', 'p'), (2, 'before', 'p'), (3, 'deleted', 'p')")

      val history = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(historyTarget)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = '0')""".stripMargin
      )
      val historyQuery = process(history)
      spark.sql(s"UPDATE ${quoteIdentifier(source)} SET value = 'after' WHERE id = 2").collect()
      spark.sql(s"DELETE FROM ${quoteIdentifier(source)} WHERE id = 3").collect()

      an[StreamingQueryException] should be thrownBy {
        process(historyQuery)
      }

      val current = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(currentTarget)}
           |USING DELTA
           |AS SELECT id, value, part FROM STREAM ${quoteIdentifier(source)}""".stripMargin
      )
      process(current)
      assertBagEqual(
        currentTarget,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'after', 'p'"
      )
    }

    it("matches native ignoreChanges duplicate semantics exactly") {
      val source = "strsql_opts_ignore_changes_source"
      val target = "strsql_opts_ignore_changes_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      spark
        .sql(
          s"""ALTER TABLE ${quoteIdentifier(source)}
             |SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')""".stripMargin
        )
        .collect()
      insertRows(source, "(1, 'stable', 'p'), (2, 'before', 'p')")
      spark.sql(s"UPDATE ${quoteIdentifier(source)} SET value = 'after' WHERE id = 2").collect()

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |PARTITIONED BY (part)
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = '0', 'ignoreChanges' = 'true')""".stripMargin
      )
      val nativePath       = scratchPath("ignore-changes-native")
      val nativeCheckpoint = scratchPath("ignore-changes-checkpoint")
      val native = track(
        spark.readStream
          .format("delta")
          .option("startingVersion", "0")
          .option("ignoreChanges", "true")
          .table(source)
          .select("id", "value", "part")
          .writeStream
          .format("delta")
          .option("checkpointLocation", nativeCheckpoint)
          .start(nativePath)
      )

      process(status)
      process(native)
      assertFramesBagEqual(
        spark.table(target),
        spark.read.format("delta").load(nativePath)
      )
      spark.table(target).where("id = 1").count() should be > 1L
    }

    it("skips data-changing commits with skipChangeCommits and continues with later appends") {
      val source = "strsql_opts_skip_changes_source"
      val target = "strsql_opts_skip_changes_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one-original', 'p'), (2, 'two', 'p')")
      spark.sql(s"UPDATE ${quoteIdentifier(source)} SET value = 'one-updated' WHERE id = 1").collect()
      insertRows(source, "(3, 'three', 'p')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = '0', 'skipChangeCommits' = 'true')""".stripMargin
      )
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one-original' AS value, 'p' AS part UNION ALL " +
          "SELECT 2, 'two', 'p' UNION ALL SELECT 3, 'three', 'p'"
      )
    }

    it("retains a deleted partition and processes the next append with one ignoreDeletes checkpoint") {
      val source = "strsql_opts_partition_delete_source"
      val target = "strsql_opts_partition_delete_target"
      createDeltaSource(
        source,
        "id INT, value STRING, part STRING",
        partitions = Seq("part")
      )
      insertRows(source, "(1, 'retained', 'expired'), (2, 'steady', 'active')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('ignoreDeletes' = 'true')""".stripMargin
      )
      val query = process(status)
      spark.sql(s"DELETE FROM ${quoteIdentifier(source)} WHERE part = 'expired'").collect()
      insertRows(source, "(3, 'later', 'active')")
      process(query)

      query.id.toString shouldBe status.queryId
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'retained' AS value, 'expired' AS part UNION ALL " +
          "SELECT 2, 'steady', 'active' UNION ALL SELECT 3, 'later', 'active'"
      )
    }

    it("starts from the Delta commit selected by startingTimestamp") {
      val source = "strsql_opts_timestamp_source"
      val target = "strsql_opts_timestamp_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'before', 'p')")
      insertRows(source, "(2, 'at-start', 'p')")
      val startingVersion   = sourceVersion(source)
      val startingTimestamp = sourceVersionTimestamp(source, startingVersion).toInstant.toString

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingTimestamp' = '$startingTimestamp')""".stripMargin
      )
      insertRows(source, "(3, 'after-start', 'p')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 2 AS id, 'at-start' AS value, 'p' AS part UNION ALL " +
          "SELECT 3, 'after-start', 'p'"
      )
    }

    it("limits each batch to one file while treating maxBytesPerTrigger as a soft bound") {
      val source = "strsql_opts_rate_source"
      val target = "strsql_opts_rate_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      insertRows(source, "(2, 'two', 'p')")
      insertRows(source, "(3, 'three', 'p')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH (
           |  'startingVersion' = '1',
           |  'maxFilesPerTrigger' = '1',
           |  'maxBytesPerTrigger' = '1b'
           |)""".stripMargin
      )
      val query        = process(status)
      val inputBatches = query.recentProgress.toSeq.map(_.numInputRows).filter(_ > 0L)

      inputBatches.size should be >= 3
      inputBatches.foreach(rows => rows shouldBe 1L)
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL " +
          "SELECT 2, 'two', 'p' UNION ALL SELECT 3, 'three', 'p'"
      )
    }
  }
}
