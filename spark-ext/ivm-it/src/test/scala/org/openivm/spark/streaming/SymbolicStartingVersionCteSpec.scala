package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

/** Regression coverage for the CTE/subquery-aware traversal fix in
  * `StreamingTableDefinition.resolveSymbolicStartingVersions` (PR #64 review
  * comment). Before the fix, `plan.transformUp` silently skipped streaming
  * relations nested inside a `WITH` CTE or a scalar subquery because
  * `UnresolvedWith.cteRelations` and `SubqueryExpression` plans are not
  * reachable through ordinary `children`, so a symbolic `startingVersion`
  * reached native Delta unresolved and failed with `DELTA_ILLEGAL_OPTION`.
  */
class SymbolicStartingVersionCteSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("symbolic startingVersion resolution through CTEs and subqueries") {
    it("resolves a symbolic startingVersion for a streaming source nested inside a CTE") {
      val source = "symcte_cte_source"
      val target = "symcte_cte_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p'), (2, 'two', 'p')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH source_events AS (
           |  SELECT id, value, part
           |  FROM STREAM ${quoteIdentifier(source)}
           |  WITH ('startingVersion' = 'latestInclusive')
           |)
           |SELECT id, value, part FROM source_events""".stripMargin
      )
      insertRows(source, "(3, 'three', 'p')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL " +
          "SELECT 2, 'two', 'p' UNION ALL SELECT 3, 'three', 'p'"
      )
    }

    it("resolves symbolic startingVersion independently for two streaming sources nested in separate CTEs") {
      val sourceA = "symcte_multi_source_a"
      val sourceB = "symcte_multi_source_b"
      val target  = "symcte_multi_target"
      createDeltaSource(sourceA, "id INT, value STRING, part STRING")
      createDeltaSource(sourceB, "id INT, value STRING, part STRING")
      insertRows(sourceA, "(1, 'a-before', 'p')")
      insertRows(sourceB, "(10, 'b-before', 'p')")
      // Advance each source to a different current version before CREATE, so
      // 'latestInclusive' must resolve two distinct numeric versions.
      insertRows(sourceA, "(2, 'a-at-start', 'p')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH events_a AS (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(sourceA)}
           |  WITH ('startingVersion' = 'latestInclusive')
           |),
           |events_b AS (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(sourceB)}
           |  WITH ('startingVersion' = 'latestInclusive')
           |)
           |SELECT * FROM events_a UNION ALL SELECT * FROM events_b""".stripMargin
      )
      insertRows(sourceA, "(3, 'a-after', 'p')")
      insertRows(sourceB, "(11, 'b-after', 'p')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 2 AS id, 'a-at-start' AS value, 'p' AS part UNION ALL " +
          "SELECT 3, 'a-after', 'p' UNION ALL " +
          "SELECT 10, 'b-before', 'p' UNION ALL " +
          "SELECT 11, 'b-after', 'p'"
      )
    }

    it("resolves a symbolic startingVersion for a streaming source nested inside a CTE referenced by another CTE") {
      val source = "symcte_nested_source"
      val target = "symcte_nested_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH inner_events AS (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(source)}
           |  WITH ('startingVersion' = 'latestInclusive')
           |),
           |outer_events AS (
           |  SELECT id, value, part FROM inner_events WHERE id >= 1
           |)
           |SELECT id, value, part FROM outer_events""".stripMargin
      )
      insertRows(source, "(2, 'two', 'p')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p'"
      )
    }

    it("still resolves a symbolic startingVersion for a direct top-level STREAM (control case)") {
      val source = "symcte_control_source"
      val target = "symcte_control_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliest')""".stripMargin
      )
      insertRows(source, "(2, 'two', 'p')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p'"
      )
    }

    it("retains the declared symbolic value in the semantic fingerprint while diagnostics show the resolved version") {
      val source = "symcte_diag_source"
      val target = "symcte_diag_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")

      createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH source_events AS (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(source)}
           |  WITH ('startingVersion' = 'latestInclusive')
           |)
           |SELECT id, value, part FROM source_events""".stripMargin
      )

      val mapper   = new com.fasterxml.jackson.databind.ObjectMapper()
      val target_  = StreamingTableMetadata.resolveDeltaTarget(spark, Seq(target), requireTableIdMarker = true)
      val manifest = StreamingTableMetadata.readManifest(spark, target_)
      val semanticSourceOptions =
        mapper.readTree(manifest.semanticJson).get("sources").get(0).get("options")
      val diagnosticSourceOptions =
        mapper.readTree(manifest.diagnosticJson).get("sources").get(0).get("rawOptions")

      semanticSourceOptions.get("startingversion").asText() shouldBe "latestInclusive"
      diagnosticSourceOptions.get("startingversion").asText() should fullyMatch regex "[0-9]+"
    }

    it("reissues the identical symbolic declaration after the source advances without drifting the definition hash") {
      val source = "symcte_drift_source"
      val target = "symcte_drift_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      val createSql =
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH source_events AS (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(source)}
           |  WITH ('startingVersion' = 'latestInclusive')
           |)
           |SELECT id, value, part FROM source_events""".stripMargin

      val first = createStreamingSql(createSql)
      val query = process(first)
      // Advance the source so 'latestInclusive' resolves to a different
      // numeric commit than it did on the first CREATE.
      insertRows(source, "(2, 'two', 'p')")
      val repeated = createStreamingSql(createSql)

      repeated.queryId shouldBe first.queryId
      repeated.runId shouldBe first.runId
      repeated.definitionHash shouldBe first.definitionHash
      process(query)
      assertBagEqual(
        target,
        "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p'"
      )
    }

    it("rejects a user-supplied reserved marker option instead of trusting it as the declared symbolic value") {
      val source = "symcte_reserved_source"
      val target = "symcte_reserved_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")

      val ex = intercept[org.apache.spark.sql.AnalysisException] {
        createStreamingSql(
          s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
             |AS SELECT id, value, part
             |FROM STREAM ${quoteIdentifier(source)}
             |WITH ('startingVersion' = '0', '__openivm_declared_startingversion' = 'latestInclusive')""".stripMargin
        )
      }
      ex.getMessage should include("reserved for internal use")
    }
  }
}
