package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

class StreamingSqlJoinsSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("streaming joins submitted through SQL") {
    it("keeps reader options isolated for repeated stream sources") {
      val left   = "strsql_join_options_left"
      val right  = "strsql_join_options_right"
      val target = "strsql_join_options_target"
      createDeltaSource(left, "id INT, value STRING")
      createDeltaSource(right, "id INT, value STRING")
      spark
        .sql(
          s"""ALTER TABLE ${quoteIdentifier(left)}
             |SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')""".stripMargin
        )
        .collect()
      insertRows(left, "(1, 'left-original'), (2, 'left-two')")
      spark.sql(s"UPDATE ${quoteIdentifier(left)} SET value = 'left-updated' WHERE id = 1").collect()
      insertRows(right, "(1, 'right-one'), (2, 'right-two')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT a.id, a.value AS left_value, b.value AS right_value
           |FROM STREAM ${quoteIdentifier(left)}
           |WITH ('startingVersion' = '0', 'skipChangeCommits' = 'true') AS a
           |JOIN STREAM(${quoteIdentifier(right)})
           |WITH ('startingVersion' = '0', 'maxFilesPerTrigger' = '1') AS b
           |ON a.id = b.id""".stripMargin
      )
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'left-original' AS left_value, 'right-one' AS right_value UNION ALL " +
          "SELECT 2, 'left-two', 'right-two'"
      )
    }

    it("executes a stream-static inner join with the static snapshot") {
      val events = "strsql_join_static_events"
      val labels = "strsql_join_static_labels"
      val target = "strsql_join_static_target"
      createDeltaSource(events, "id INT, value STRING")
      createDeltaSource(labels, "id INT, label STRING")
      insertRows(labels, "(1, 'one'), (2, 'two')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT e.id, e.value, d.label
           |FROM STREAM ${quoteIdentifier(events)} AS e
           |JOIN ${quoteIdentifier(labels)} AS d ON e.id = d.id""".stripMargin
      )
      insertRows(events, "(1, 'first'), (3, 'unmatched')")
      process(status)

      assertBagEqual(target, "SELECT 1 AS id, 'first' AS value, 'one' AS label")
    }

    it("executes a bounded watermarked stream-stream inner join") {
      val left   = "strsql_join_watermark_inner_left"
      val right  = "strsql_join_watermark_inner_right"
      val target = "strsql_join_watermark_inner_target"
      createDeltaSource(left, "id INT, event_time TIMESTAMP, value STRING")
      createDeltaSource(right, "id INT, event_time TIMESTAMP, value STRING")
      insertRows(
        left,
        "(1, TIMESTAMP '2024-01-01 00:05:00', 'left-one'), " +
          "(2, TIMESTAMP '2024-01-01 00:30:00', 'left-two')"
      )
      insertRows(
        right,
        "(1, TIMESTAMP '2024-01-01 00:08:00', 'right-one'), " +
          "(2, TIMESTAMP '2024-01-01 01:30:00', 'too-late')"
      )

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT a.id, a.value AS left_value, b.value AS right_value
           |FROM STREAM ${quoteIdentifier(left)}
           |WATERMARK a.event_time DELAY OF INTERVAL 1 MINUTE AS a
           |JOIN STREAM ${quoteIdentifier(right)}
           |WATERMARK b.event_time DELAY OF INTERVAL 1 MINUTE AS b
           |ON a.id = b.id
           |AND a.event_time BETWEEN
           |  b.event_time - INTERVAL 5 MINUTES
           |  AND b.event_time + INTERVAL 5 MINUTES""".stripMargin
      )
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'left-one' AS left_value, 'right-one' AS right_value"
      )
    }

    it("emits an unmatched row from a bounded watermarked left outer join") {
      val left   = "strsql_join_watermark_outer_left"
      val right  = "strsql_join_watermark_outer_right"
      val target = "strsql_join_watermark_outer_target"
      createDeltaSource(left, "id INT, event_time TIMESTAMP, value STRING")
      createDeltaSource(right, "id INT, event_time TIMESTAMP, value STRING")
      insertRows(left, "(1, TIMESTAMP '2024-01-01 00:00:00', 'left-only')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT a.id, a.value AS left_value, b.value AS right_value
           |FROM STREAM ${quoteIdentifier(left)}
           |WATERMARK a.event_time DELAY OF INTERVAL 1 MINUTE AS a
           |LEFT OUTER JOIN STREAM ${quoteIdentifier(right)}
           |WATERMARK b.event_time DELAY OF INTERVAL 1 MINUTE AS b
           |ON a.id = b.id
           |AND a.event_time >= b.event_time
           |AND a.event_time <= b.event_time + INTERVAL 10 MINUTES""".stripMargin
      )
      val query = process(status)
      insertRows(left, "(2, TIMESTAMP '2024-01-01 01:00:00', 'left-advance-one')")
      insertRows(right, "(2, TIMESTAMP '2024-01-01 01:00:00', 'advance-one')")
      process(query)
      insertRows(left, "(3, TIMESTAMP '2024-01-01 02:00:00', 'left-advance-two')")
      insertRows(right, "(3, TIMESTAMP '2024-01-01 02:00:00', 'advance-two')")
      process(query)

      assertFramesBagEqual(
        spark.table(target).where("id = 1"),
        spark.sql(
          "SELECT 1 AS id, 'left-only' AS left_value, CAST(NULL AS STRING) AS right_value"
        )
      )
    }

    it("rejects an unbounded stream-stream full outer join before target creation") {
      val left   = "strsql_join_invalid_left"
      val right  = "strsql_join_invalid_right"
      val target = "strsql_join_invalid_target"
      createDeltaSource(left, "id INT, value STRING")
      createDeltaSource(right, "id INT, value STRING")

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(
          s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
             |AS
             |SELECT a.id, a.value AS left_value, b.value AS right_value
             |FROM STREAM ${quoteIdentifier(left)} AS a
             |FULL OUTER JOIN STREAM ${quoteIdentifier(right)} AS b
             |ON a.id = b.id""".stripMargin
        )
      }
      spark.catalog.tableExists(target) shouldBe false
    }
  }
}
