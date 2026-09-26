package org.openivm.spark.streaming

import org.apache.spark.sql.functions.expr
import org.scalatest.funspec.AnyFunSpec

class StreamingSqlCteSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("nested SQL plans and definition reconciliation") {
    it("executes a streaming source inside nested CTE and subquery plans") {
      val source = "strsql_cte_nested_source"
      val target = "strsql_cte_nested_target"
      createDeltaSource(source, "id INT, value STRING")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH outer_rows AS (
           |  WITH inner_rows AS (
           |    SELECT id, value FROM STREAM ${quoteIdentifier(source)}
           |  )
           |  SELECT nested.id, nested.value
           |  FROM (
           |    SELECT id, value FROM inner_rows WHERE id >= 2
           |  ) AS nested
           |)
           |SELECT id, value FROM outer_rows""".stripMargin
      )
      insertRows(source, "(1, 'skip'), (2, 'two'), (3, 'three')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 2 AS id, 'two' AS value UNION ALL SELECT 3, 'three'"
      )
    }

    it("does not rebuild when new source commits arrive under the same SQL definition") {
      val source = "strsql_cte_commits_source"
      val target = "strsql_cte_commits_target"
      createDeltaSource(source, "id INT, value STRING")
      insertRows(source, "(1, 'before')")
      val createSql =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value FROM STREAM ${quoteIdentifier(source)}""".stripMargin

      val first = createStreamingSql(createSql)
      val query = process(first)
      insertRows(source, "(2, 'after')")
      val repeated = createStreamingSql(createSql)

      repeated.queryId shouldBe first.queryId
      repeated.runId shouldBe first.runId
      repeated.definitionHash shouldBe first.definitionHash
      process(query)
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'before' AS value UNION ALL SELECT 2, 'after'"
      )
    }

    it("treats formatting and comments as harmless redeclaration changes") {
      val source = "strsql_cte_format_source"
      val target = "strsql_cte_format_target"
      createDeltaSource(source, "id INT, value STRING")
      val compact =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value FROM STREAM ${quoteIdentifier(source)}
           |WHERE id >= 1""".stripMargin
      val formatted =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT
           |  id,
           |  value -- formatting-only comment
           |FROM STREAM ${quoteIdentifier(source)}
           |WHERE id >= 1""".stripMargin

      val first  = createStreamingSql(compact)
      val second = createStreamingSql(formatted)

      second.queryId shouldBe first.queryId
      second.runId shouldBe first.runId
      second.definitionHash shouldBe first.definitionHash
    }

    it("rejects a changed nested CTE filter instead of treating it as the same definition") {
      val source = "strsql_cte_changed_source"
      val target = "strsql_cte_changed_target"
      createDeltaSource(source, "id INT, value STRING")
      val original =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH filtered AS (
           |  SELECT id, value FROM STREAM ${quoteIdentifier(source)} WHERE id >= 1
           |)
           |SELECT id, value FROM filtered""".stripMargin
      val changed =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH filtered AS (
           |  SELECT id, value FROM STREAM ${quoteIdentifier(source)} WHERE id >= 2
           |)
           |SELECT id, value FROM filtered""".stripMargin

      createStreamingSql(original)
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(changed)
      }
    }

    it("rejects a changed source hidden inside a CTE definition") {
      val sourceA = "strsql_cte_source_change_a"
      val sourceB = "strsql_cte_source_change_b"
      val target  = "strsql_cte_source_change_target"
      createDeltaSource(sourceA, "id INT, value STRING")
      createDeltaSource(sourceB, "id INT, value STRING")
      val original =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH source_rows AS (
           |  SELECT id, value FROM STREAM ${quoteIdentifier(sourceA)}
           |)
           |SELECT id, value FROM source_rows""".stripMargin
      val changed =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH source_rows AS (
           |  SELECT id, value FROM STREAM ${quoteIdentifier(sourceB)}
           |)
           |SELECT id, value FROM source_rows""".stripMargin

      createStreamingSql(original)
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(changed)
      }
    }

    it("rejects changed hash-number literal content in a top-level filter") {
      val source = "strsql_cte_literal_source"
      val target = "strsql_cte_literal_target"
      createDeltaSource(source, "id INT, code STRING")
      val original =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, code FROM STREAM ${quoteIdentifier(source)}
           |WHERE code = 'ticket#123'""".stripMargin
      val changed =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, code FROM STREAM ${quoteIdentifier(source)}
           |WHERE code = 'ticket#456'""".stripMargin

      createStreamingSql(original)
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(changed)
      }
    }

    it("rejects changed comment-like literal content in a top-level filter") {
      val source = "strsql_cte_comment_literal_source"
      val target = "strsql_cte_comment_literal_target"
      createDeltaSource(source, "id INT, code STRING")
      val original =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, code FROM STREAM ${quoteIdentifier(source)}
           |WHERE code = 'left /* alpha */ right'""".stripMargin
      val changed =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, code FROM STREAM ${quoteIdentifier(source)}
           |WHERE code = 'left /* beta */ right'""".stripMargin

      createStreamingSql(original)
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(changed)
      }
    }

    it("rejects changed significant whitespace inside a string literal") {
      val source = "strsql_cte_space_literal_source"
      val target = "strsql_cte_space_literal_target"
      createDeltaSource(source, "id INT, code STRING")
      val original =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, code FROM STREAM ${quoteIdentifier(source)}
           |WHERE code = 'left  right'""".stripMargin
      val changed =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, code FROM STREAM ${quoteIdentifier(source)}
           |WHERE code = 'left right'""".stripMargin

      createStreamingSql(original)
      an[org.apache.spark.sql.AnalysisException] should be thrownBy {
        createStreamingSql(changed)
      }
    }

    it("matches native behavior when one table is used in streaming and static roles") {
      val source = "strsql_cte_mixed_role_source"
      val target = "strsql_cte_mixed_role_target"
      createDeltaSource(source, "id INT, value STRING")
      insertRows(source, "(1, 'initial')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT stream_side.id, stream_side.value
           |FROM STREAM ${quoteIdentifier(source)} AS stream_side
           |JOIN ${quoteIdentifier(source)} AS static_side
           |ON stream_side.id = static_side.id""".stripMargin
      )
      val nativePath       = scratchPath("mixed-role-native")
      val nativeCheckpoint = scratchPath("mixed-role-checkpoint")
      val streamSide       = spark.readStream.table(source).as("stream_side")
      val staticSide       = spark.table(source).as("static_side")
      val native = track(
        streamSide
          .join(staticSide, expr("stream_side.id = static_side.id"))
          .selectExpr("stream_side.id AS id", "stream_side.value AS value")
          .writeStream
          .format("delta")
          .option("checkpointLocation", nativeCheckpoint)
          .start(nativePath)
      )

      val query = process(status)
      process(native)
      insertRows(source, "(2, 'later')")
      process(query)
      process(native)

      assertFramesBagEqual(
        spark.table(target),
        spark.read.format("delta").load(nativePath)
      )
    }
  }
}
