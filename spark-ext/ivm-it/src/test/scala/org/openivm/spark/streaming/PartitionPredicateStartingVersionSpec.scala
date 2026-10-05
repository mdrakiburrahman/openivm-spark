package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

/** Acceptance coverage for the partition-predicate-aware symbolic
  * `startingVersion` strategies (PR #64 follow-up comment
  * issuecomment-5995359203): `latestInclusiveWithPredicate` /
  * `earliestInclusiveWithPredicate`. Covers the "happy path" scenarios where
  * a usable partition-only predicate is extracted and a matching commit is
  * found; fallback/safety scenarios live in
  * `PartitionPredicateStartingVersionFallbackSpec`.
  */
class PartitionPredicateStartingVersionSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("partition-predicate-aware symbolic startingVersion resolution") {
    it("resolves latestInclusiveWithPredicate to the newest commit matching the partition predicate") {
      val source = "predsv_latest_source"
      val target = "predsv_latest_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p1-a', 'p1')")
      insertRows(source, "(2, 'p2-a', 'p2')")
      insertRows(source, "(3, 'p1-b', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'latestInclusiveWithPredicate')
           |WHERE part = 'p1'""".stripMargin
      )
      insertRows(source, "(4, 'p1-c', 'p1')")
      process(status)

      // Resolution should land at-or-after the newest commit touching
      // part='p1' (version adding id=3), so id=3's write and everything
      // after is replayed; id=1/2 (strictly older commits) are not.
      assertBagEqual(target, "SELECT 3 AS id, 'p1-b' AS value, 'p1' AS part UNION ALL SELECT 4, 'p1-c', 'p1'")
    }

    it("resolves earliestInclusiveWithPredicate to the oldest retained commit matching the partition predicate") {
      val source = "predsv_earliest_source"
      val target = "predsv_earliest_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p2-a', 'p2')")
      insertRows(source, "(2, 'p1-a', 'p1')")
      insertRows(source, "(3, 'p2-b', 'p2')")
      insertRows(source, "(4, 'p1-b', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part = 'p1'""".stripMargin
      )
      process(status)

      assertBagEqual(
        target,
        "SELECT 2 AS id, 'p1-a' AS value, 'p1' AS part UNION ALL SELECT 4, 'p1-b', 'p1'"
      )
    }

    it("extracts only the partition-only conjunct from a mixed partition/non-partition AND predicate") {
      val source = "predsv_mixed_source"
      val target = "predsv_mixed_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p2-a', 'p2')")
      insertRows(source, "(2, 'p1-a', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part = 'p1' AND value = 'p1-a'""".stripMargin
      )
      insertRows(source, "(3, 'p1-b', 'p1')")
      process(status)

      // Discovery only used `part = 'p1'`; Spark still fully applies the
      // declared WHERE clause, including the non-partition conjunct.
      assertBagEqual(target, "SELECT 2 AS id, 'p1-a' AS value, 'p1' AS part")
    }

    it("extracts an IN predicate over partition values for commit discovery") {
      val source = "predsv_in_source"
      val target = "predsv_in_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p3-a', 'p3')")
      insertRows(source, "(2, 'p1-a', 'p1')")
      insertRows(source, "(3, 'p2-a', 'p2')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part IN ('p1', 'p2')""".stripMargin
      )
      process(status)

      assertBagEqual(
        target,
        "SELECT 2 AS id, 'p1-a' AS value, 'p1' AS part UNION ALL SELECT 3, 'p2-a', 'p2'"
      )
    }

    it("resolves a partition predicate for a streaming source nested inside a CTE") {
      val source = "predsv_cte_source"
      val target = "predsv_cte_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p2-a', 'p2')")
      insertRows(source, "(2, 'p1-a', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |WITH source_events AS (
           |  SELECT id, value, part
           |  FROM STREAM ${quoteIdentifier(source)}
           |  WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |  WHERE part = 'p1'
           |)
           |SELECT id, value, part FROM source_events""".stripMargin
      )
      process(status)

      assertBagEqual(target, "SELECT 2 AS id, 'p1-a' AS value, 'p1' AS part")
    }

    it("resolves partition predicates independently for two streaming sources in the same query") {
      val sourceA = "predsv_multi_source_a"
      val sourceB = "predsv_multi_source_b"
      val target  = "predsv_multi_target"
      createDeltaSource(sourceA, "id INT, value STRING, part STRING", partitions = Seq("part"))
      createDeltaSource(sourceB, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(sourceA, "(1, 'a-p2', 'p2')")
      insertRows(sourceA, "(2, 'a-p1', 'p1')")
      insertRows(sourceB, "(10, 'b-p9', 'p9')")
      insertRows(sourceB, "(11, 'b-p1', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS
           |SELECT * FROM (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(sourceA)}
           |  WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |  WHERE part = 'p1'
           |) UNION ALL SELECT * FROM (
           |  SELECT id, value, part FROM STREAM ${quoteIdentifier(sourceB)}
           |  WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |  WHERE part = 'p1'
           |)""".stripMargin
      )
      process(status)

      assertBagEqual(
        target,
        "SELECT 2 AS id, 'a-p1' AS value, 'p1' AS part UNION ALL SELECT 11, 'b-p1', 'p1'"
      )
    }
  }
}
