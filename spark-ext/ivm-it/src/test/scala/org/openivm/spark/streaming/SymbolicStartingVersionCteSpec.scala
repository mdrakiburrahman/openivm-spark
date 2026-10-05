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
  }
}
