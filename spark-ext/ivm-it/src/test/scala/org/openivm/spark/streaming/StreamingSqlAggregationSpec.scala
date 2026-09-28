package org.openivm.spark.streaming

import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.funspec.AnyFunSpec

class StreamingSqlAggregationSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("stateful streaming SQL") {
    it("emits a closed event-time window after deterministic watermark advancement") {
      val source = "strsql_agg_window_source"
      val target = "strsql_agg_window_target"
      createDeltaSource(source, "id INT, event_time TIMESTAMP, part STRING")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT
           |  window(e.event_time, '10 minutes').start AS window_start,
           |  window(e.event_time, '10 minutes').end AS window_end,
           |  e.part,
           |  COUNT(*) AS event_count
           |FROM STREAM ${quoteIdentifier(source)}
           |WATERMARK e.event_time DELAY OF INTERVAL 1 MINUTE AS e
           |GROUP BY window(e.event_time, '10 minutes'), e.part""".stripMargin
      )
      val query = process(status)
      insertRows(
        source,
        "(1, TIMESTAMP '2024-01-01 00:01:00', 'east'), " +
          "(2, TIMESTAMP '2024-01-01 00:02:00', 'east')"
      )
      process(query)
      insertRows(source, "(100, TIMESTAMP '2024-01-01 01:00:00', 'advance')")
      process(query)
      insertRows(source, "(101, TIMESTAMP '2024-01-01 02:00:00', 'advance')")
      process(query)

      assertFramesBagEqual(
        spark.table(target).where("window_start = TIMESTAMP '2024-01-01 00:00:00'"),
        spark.sql(
          """SELECT
            |  TIMESTAMP '2024-01-01 00:00:00' AS window_start,
            |  TIMESTAMP '2024-01-01 00:10:00' AS window_end,
            |  'east' AS part,
            |  CAST(2 AS BIGINT) AS event_count""".stripMargin
        )
      )
    }

    it("completes a closed daily window with an AvailableNow three-day watermark") {
      val source        = "strsql_agg_available_daily_source"
      val target        = "strsql_agg_available_daily_target"
      val watermarkDays = 3
      createDeltaSource(source, "id INT, event_time TIMESTAMP, part STRING")
      insertRows(
        source,
        "(1, TIMESTAMP '2026-09-10 01:00:00', 'east'), " +
          "(2, TIMESTAMP '2026-09-10 23:00:00', 'east'), " +
          "(100, TIMESTAMP '2026-09-15 00:00:00', 'advance')"
      )

      val completed = createStreamingSqlToCompletion(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |OPTIONS ('trigger' = 'availableNow')
           |AS
           |SELECT
           |  window(e.event_time, '1 day').start AS window_start,
           |  window(e.event_time, '1 day').end AS window_end,
           |  e.part,
           |  COUNT(*) AS event_count
           |FROM STREAM ${quoteIdentifier(source)}
           |WATERMARK e.event_time DELAY OF INTERVAL $watermarkDays DAYS AS e
           |GROUP BY window(e.event_time, '1 day'), e.part""".stripMargin
      )

      completed.inputRows shouldBe 3L
      assertFramesBagEqual(
        spark.table(target),
        spark.sql(
          """SELECT
            |  TIMESTAMP '2026-09-10 00:00:00' AS window_start,
            |  TIMESTAMP '2026-09-11 00:00:00' AS window_end,
            |  'east' AS part,
            |  CAST(2 AS BIGINT) AS event_count""".stripMargin
        )
      )
    }

    it("keeps one deterministic latest payload per key in a closed event-time window") {
      val source = "strsql_agg_latest_source"
      val target = "strsql_agg_latest_target"
      createLatestRowSource(source)

      val status = createStreamingSql(latestRowDeclaration(source, target))
      val query  = process(status)
      insertRows(
        source,
        "('alpha', TIMESTAMP '2024-01-01 00:01:00', TIMESTAMP '2024-01-01 10:00:00', 1, 'alpha-old'), " +
          "('alpha', TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 11:00:00', 1, 'alpha-newer'), " +
          "('beta', TIMESTAMP '2024-01-01 00:03:00', TIMESTAMP '2024-01-01 09:00:00', 5, 'beta-latest')"
      )
      process(query)
      insertRows(
        source,
        "('alpha', TIMESTAMP '2024-01-01 00:04:00', TIMESTAMP '2024-01-01 11:00:00', 2, 'alpha-latest'), " +
          "('alpha', TIMESTAMP '2024-01-01 00:05:00', TIMESTAMP '2024-01-01 08:00:00', 99, 'alpha-stale'), " +
          "('beta', TIMESTAMP '2024-01-01 00:06:00', TIMESTAMP '2024-01-01 08:00:00', 99, 'beta-stale')"
      )
      process(query)

      spark.table(target).count() shouldBe 0L
      advanceLatestRowWatermark(source, query)

      val closedWindow = spark.table(target).where("window_start = TIMESTAMP '2024-01-01 00:00:00'")
      val expected = spark.sql(
        """SELECT
          |  TIMESTAMP '2024-01-01 00:00:00' AS window_start,
          |  TIMESTAMP '2024-01-01 00:10:00' AS window_end,
          |  'alpha' AS entity_key,
          |  TIMESTAMP '2024-01-01 00:04:00' AS event_time,
          |  TIMESTAMP '2024-01-01 11:00:00' AS version_time,
          |  CAST(2 AS BIGINT) AS source_sequence,
          |  'alpha-latest' AS payload
          |UNION ALL
          |SELECT
          |  TIMESTAMP '2024-01-01 00:00:00',
          |  TIMESTAMP '2024-01-01 00:10:00',
          |  'beta',
          |  TIMESTAMP '2024-01-01 00:03:00',
          |  TIMESTAMP '2024-01-01 09:00:00',
          |  CAST(5 AS BIGINT),
          |  'beta-latest'""".stripMargin
      )
      assertFramesBagEqual(closedWindow, expected)

      insertRows(
        source,
        "('alpha', TIMESTAMP '2024-01-01 00:07:00', TIMESTAMP '2024-01-01 12:00:00', 3, 'too-late')"
      )
      process(query)

      assertFramesBagEqual(
        spark.table(target).where("window_start = TIMESTAMP '2024-01-01 00:00:00'"),
        expected
      )
    }

    it("restores pending latest-row state from the native checkpoint") {
      val source = "strsql_agg_latest_resume_source"
      val target = "strsql_agg_latest_resume_target"
      createLatestRowSource(source)
      val declaration = latestRowDeclaration(source, target)

      val initial      = createStreamingSql(declaration)
      val initialQuery = process(initial)
      insertRows(
        source,
        "('alpha', TIMESTAMP '2024-01-01 00:01:00', TIMESTAMP '2024-01-01 10:00:00', 1, 'before-stop'), " +
          "('beta', TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 09:00:00', 1, 'beta-before-stop')"
      )
      process(initialQuery)
      spark.table(target).count() shouldBe 0L

      stopStreamingSql(target)
      val resumed = createStreamingSql(declaration)
      resumed.queryId shouldBe initial.queryId
      resumed.runId should not be initial.runId
      resumed.checkpointLocation shouldBe initial.checkpointLocation
      resumed.definitionHash shouldBe initial.definitionHash

      val resumedQuery = process(resumed)
      insertRows(
        source,
        "('alpha', TIMESTAMP '2024-01-01 00:03:00', TIMESTAMP '2024-01-01 11:00:00', 2, 'after-resume')"
      )
      process(resumedQuery)
      advanceLatestRowWatermark(source, resumedQuery)

      assertFramesBagEqual(
        spark.table(target).where("window_start = TIMESTAMP '2024-01-01 00:00:00'"),
        spark.sql(
          """SELECT
            |  TIMESTAMP '2024-01-01 00:00:00' AS window_start,
            |  TIMESTAMP '2024-01-01 00:10:00' AS window_end,
            |  'alpha' AS entity_key,
            |  TIMESTAMP '2024-01-01 00:03:00' AS event_time,
            |  TIMESTAMP '2024-01-01 11:00:00' AS version_time,
            |  CAST(2 AS BIGINT) AS source_sequence,
            |  'after-resume' AS payload
            |UNION ALL
            |SELECT
            |  TIMESTAMP '2024-01-01 00:00:00',
            |  TIMESTAMP '2024-01-01 00:10:00',
            |  'beta',
            |  TIMESTAMP '2024-01-01 00:02:00',
            |  TIMESTAMP '2024-01-01 09:00:00',
            |  CAST(1 AS BIGINT),
            |  'beta-before-stop'""".stripMargin
        )
      )
    }

    it("maintains an explicit complete-mode aggregate") {
      val source = "strsql_agg_complete_source"
      val target = "strsql_agg_complete_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |OPTIONS ('outputMode' = 'complete')
           |AS
           |SELECT part, COUNT(*) AS event_count
           |FROM STREAM ${quoteIdentifier(source)}
           |GROUP BY part""".stripMargin
      )
      val query = process(status)
      insertRows(source, "(1, 'one', 'east'), (2, 'two', 'west')")
      process(query)
      insertRows(source, "(3, 'three', 'east')")
      process(query)

      assertBagEqual(
        target,
        "SELECT 'east' AS part, CAST(2 AS BIGINT) AS event_count UNION ALL " +
          "SELECT 'west', CAST(1 AS BIGINT)"
      )
    }

    it("projects an explicitly named derived timestamp watermark") {
      val source = "strsql_agg_derived_source"
      val target = "strsql_agg_derived_target"
      createDeltaSource(source, "id INT, epoch_seconds BIGINT")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT e.id, e.event_time
           |FROM STREAM ${quoteIdentifier(source)}
           |WATERMARK timestamp_seconds(epoch_seconds) AS event_time
           |DELAY OF INTERVAL 5 MINUTES AS e""".stripMargin
      )
      insertRows(source, "(1, 1704067200)")
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, TIMESTAMP '2024-01-01 00:00:00' AS event_time"
      )
    }

    it("rejects a distinct aggregate before creating the target") {
      val source = "strsql_agg_distinct_source"
      val target = "strsql_agg_distinct_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(
          s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
             |OPTIONS ('outputMode' = 'complete')
             |AS
             |SELECT part, COUNT(DISTINCT value) AS unique_values
             |FROM STREAM ${quoteIdentifier(source)}
             |GROUP BY part""".stripMargin
        )
      }
      spark.catalog.tableExists(target) shouldBe false
    }

    it("rejects a non-time analytic window before creating the target") {
      val source = "strsql_agg_analytic_source"
      val target = "strsql_agg_analytic_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(
          s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
             |AS
             |SELECT
             |  id,
             |  ROW_NUMBER() OVER (PARTITION BY part ORDER BY id) AS row_number
             |FROM STREAM ${quoteIdentifier(source)}""".stripMargin
        )
      }
      spark.catalog.tableExists(target) shouldBe false
    }

    it("rejects append-mode aggregation without a watermark before target creation") {
      val source = "strsql_agg_append_source"
      val target = "strsql_agg_append_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")

      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(
          s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
             |AS
             |SELECT part, COUNT(*) AS event_count
             |FROM STREAM ${quoteIdentifier(source)}
             |GROUP BY part""".stripMargin
        )
      }
      spark.catalog.tableExists(target) shouldBe false
    }
  }

  private def createLatestRowSource(source: String): Unit =
    createDeltaSource(
      source,
      "entity_key STRING, event_time TIMESTAMP, version_time TIMESTAMP, source_sequence BIGINT, payload STRING"
    )

  private def latestRowDeclaration(source: String, target: String): String =
    s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
       |AS
       |WITH latest_by_window AS (
       |  SELECT
       |    window(e.event_time, '10 minutes').start AS window_start,
       |    window(e.event_time, '10 minutes').end AS window_end,
       |    e.entity_key,
       |    MAX_BY(
       |      NAMED_STRUCT(
       |        'event_time', e.event_time,
       |        'version_time', e.version_time,
       |        'source_sequence', e.source_sequence,
       |        'payload', e.payload
       |      ),
       |      NAMED_STRUCT(
       |        'version_time', e.version_time,
       |        'source_sequence', e.source_sequence
       |      )
       |    ) AS latest
       |  FROM STREAM ${quoteIdentifier(source)}
       |  WATERMARK e.event_time DELAY OF INTERVAL 1 MINUTE AS e
       |  GROUP BY window(e.event_time, '10 minutes'), e.entity_key
       |)
       |SELECT
       |  window_start,
       |  window_end,
       |  entity_key,
       |  latest.event_time AS event_time,
       |  latest.version_time AS version_time,
       |  latest.source_sequence AS source_sequence,
       |  latest.payload AS payload
       |FROM latest_by_window""".stripMargin

  private def advanceLatestRowWatermark(source: String, query: StreamingQuery): Unit = {
    insertRows(
      source,
      "('watermark-one', TIMESTAMP '2024-01-01 01:00:00', TIMESTAMP '2024-01-01 01:00:00', 1, 'advance')"
    )
    process(query)
    insertRows(
      source,
      "('watermark-two', TIMESTAMP '2024-01-01 02:00:00', TIMESTAMP '2024-01-01 02:00:00', 1, 'advance')"
    )
    process(query)
  }
}
