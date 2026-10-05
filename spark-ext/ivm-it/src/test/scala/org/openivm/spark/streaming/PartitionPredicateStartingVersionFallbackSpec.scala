package org.openivm.spark.streaming

import org.scalatest.funspec.AnyFunSpec

/** Safety/fallback acceptance coverage for the partition-predicate-aware
  * symbolic `startingVersion` strategies (PR #64 follow-up comment
  * issuecomment-5995359203): every case here must degrade to
  * `earliestAvailable` semantics rather than silently narrowing past what
  * is provably safe. Happy-path matching coverage lives in
  * `PartitionPredicateStartingVersionSpec`.
  */
class PartitionPredicateStartingVersionFallbackSpec extends AnyFunSpec with StreamingSqlTestSupport {

  describe("partition-predicate-aware symbolic startingVersion fallback safety") {
    it("falls back to earliestAvailable semantics when no AddFile matches the partition predicate") {
      val source = "predsv_nomatch_source"
      val target = "predsv_nomatch_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p1-a', 'p1')")
      insertRows(source, "(2, 'p2-a', 'p2')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part = 'p9'""".stripMargin
      )
      process(status)

      // No commit ever touches part='p9'; falls back to earliestAvailable,
      // so every pre-existing row is replayed by Spark, then filtered out
      // by the declared WHERE clause, leaving the target empty.
      spark.table(target).count() shouldBe 0L
    }

    it("falls back to earliestAvailable semantics when no usable partition predicate can be extracted") {
      val source = "predsv_nopredicate_source"
      val target = "predsv_nopredicate_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p1-a', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')""".stripMargin
      )
      insertRows(source, "(2, 'p2-a', 'p2')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'p1-a' AS value, 'p1' AS part UNION ALL SELECT 2, 'p2-a', 'p2'"
      )
    }

    it("does not unsafely narrow past an OR predicate over partition values; falls back instead") {
      val source = "predsv_or_source"
      val target = "predsv_or_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p1-a', 'p1')")

      val status = createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(source)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part = 'p1' OR part = 'p2'""".stripMargin
      )
      insertRows(source, "(2, 'p2-a', 'p2')")
      process(status)

      assertBagEqual(
        target,
        "SELECT 1 AS id, 'p1-a' AS value, 'p1' AS part UNION ALL SELECT 2, 'p2-a', 'p2'"
      )
    }

    it(
      "reports resolution=fallback and a reason in diagnostics, and resolution=exact with the effective predicate otherwise"
    ) {
      val matchedSource   = "predsv_diag_match_source"
      val unmatchedSource = "predsv_diag_nomatch_source"
      val matchedTarget   = "predsv_diag_match_target"
      val unmatchedTarget = "predsv_diag_nomatch_target"
      createDeltaSource(matchedSource, "id INT, value STRING, part STRING", partitions = Seq("part"))
      createDeltaSource(unmatchedSource, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(matchedSource, "(1, 'p1-a', 'p1')")
      insertRows(unmatchedSource, "(1, 'p1-a', 'p1')")

      createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(matchedTarget)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(matchedSource)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part = 'p1'""".stripMargin
      )
      createStreamingSql(
        s"""CREATE STREAMING TABLE ${quoteIdentifier(unmatchedTarget)}
           |AS SELECT id, value, part
           |FROM STREAM ${quoteIdentifier(unmatchedSource)}
           |WITH ('startingVersion' = 'earliestInclusiveWithPredicate')
           |WHERE part = 'p9'""".stripMargin
      )

      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()

      val matchedManifest =
        StreamingTableMetadata.readManifest(
          spark,
          StreamingTableMetadata.resolveDeltaTarget(spark, Seq(matchedTarget), requireTableIdMarker = true)
        )
      val matchedOptions = mapper.readTree(matchedManifest.diagnosticJson).get("sources").get(0).get("rawOptions")
      matchedOptions.get("__openivm_predicate_resolution").asText() shouldBe "exact"
      matchedOptions.get("__openivm_effective_partition_predicate").asText() should include("part")
      matchedOptions.has("__openivm_predicate_fallback_reason") shouldBe false

      val unmatchedManifest =
        StreamingTableMetadata.readManifest(
          spark,
          StreamingTableMetadata.resolveDeltaTarget(spark, Seq(unmatchedTarget), requireTableIdMarker = true)
        )
      val unmatchedOptions = mapper.readTree(unmatchedManifest.diagnosticJson).get("sources").get(0).get("rawOptions")
      unmatchedOptions.get("__openivm_predicate_resolution").asText() shouldBe "fallback"
      unmatchedOptions.get("__openivm_predicate_fallback_reason").asText().nonEmpty shouldBe true
    }

    it("rejects user-supplied diagnostic marker options the same way the declared-strategy marker is rejected") {
      val source = "predsv_reserved_source"
      val target = "predsv_reserved_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'p1-a', 'p1')")

      val ex = intercept[org.apache.spark.sql.AnalysisException] {
        createStreamingSql(
          s"""CREATE STREAMING TABLE ${quoteIdentifier(target)}
             |AS SELECT id, value, part
             |FROM STREAM ${quoteIdentifier(source)}
             |WITH ('startingVersion' = '0', '__openivm_predicate_resolution' = 'exact')""".stripMargin
        )
      }
      ex.getMessage should include("reserved for internal use")
    }
  }
}
