package org.openivm.spark.streaming

import org.apache.spark.sql.streaming.StreamingQueryException
import org.scalatest.funspec.AnyFunSpec

/** Delta change-commit tolerances (`ignoreDeletes`, `ignoreChanges`, `skipChangeCommits`) tune how a
  * query consumes its source, so changing them restarts the same checkpoint instead of rebuilding.
  */
class StreamingSqlOperationalOptionsSpec extends AnyFunSpec with StreamingSqlTestSupport {

  private def target(name: String): StreamingTableTarget =
    StreamingTableMetadata.resolveDeltaTarget(spark, Seq(name), requireTableIdMarker = true)

  private def manifest(name: String): StreamingTableManifest =
    StreamingTableMetadata.readManifest(spark, target(name))

  private def declaration(source: String, targetName: String, readerOptions: String, filter: String = ""): String =
    s"""CREATE STREAMING TABLE ${quoteIdentifier(targetName)}
       |OPTIONS ('onQueryChange' = 'rebuild')
       |AS SELECT id, value, part
       |FROM STREAM ${quoteIdentifier(source)}
       |WITH ($readerOptions)$filter""".stripMargin

  describe("operational Delta change-commit tolerances") {

    it("resumes the same checkpoint when ignoreDeletes is added after a retention delete fails the query") {
      val source = "strsql_tolerance_delete_source"
      val name   = "strsql_tolerance_delete_target"
      createDeltaSource(source, "id INT, value STRING, part STRING", partitions = Seq("part"))
      insertRows(source, "(1, 'retained', 'expired'), (2, 'steady', 'active')")

      val first      = createStreamingSql(declaration(source, name, "'maxFilesPerTrigger' = '100'"))
      val firstQuery = process(first)
      val generation = target(name).deltaTableId
      spark.sql(s"DELETE FROM ${quoteIdentifier(source)} WHERE part = 'expired'").collect()
      insertRows(source, "(3, 'later', 'active')")
      val failure = the[StreamingQueryException] thrownBy process(firstQuery)
      failure.getMessage should (include("DELTA_SOURCE_IGNORE_DELETE") or include("ignoreDeletes"))

      val resumed = createStreamingSql(
        declaration(source, name, "'maxFilesPerTrigger' = '100', 'ignoreDeletes' = 'true'")
      )
      resumed.queryId shouldBe first.queryId
      resumed.runId should not be first.runId
      resumed.checkpointLocation shouldBe first.checkpointLocation
      resumed.definitionHash shouldBe first.definitionHash
      target(name).deltaTableId shouldBe generation

      process(resumed)
      assertBagEqual(
        name,
        "SELECT 1 AS id, 'retained' AS value, 'expired' AS part UNION ALL " +
          "SELECT 2, 'steady', 'active' UNION ALL SELECT 3, 'later', 'active'"
      )
      val persisted = manifest(name)
      persisted.definitionHash shouldBe first.definitionHash
      persisted.semanticJson should not include "ignoredeletes"
      persisted.operationalJson should include("\"ignoredeletes\":\"true\"")
      persisted.diagnosticJson should include("\"ignoredeletes\":\"true\"")
      showStreamingSql()
        .filter(row => row.getAs[String]("table_name") == first.tableName)
        .map(_.getAs[String]("definition_hash")) shouldBe Seq(first.definitionHash)
    }

    it("resumes a stream whose definition an earlier version persisted with ignoreChanges as semantic") {
      val source = "strsql_tolerance_legacy_source"
      val name   = "strsql_tolerance_legacy_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      val sql = declaration(source, name, "'ignoreChanges' = 'true'")

      val legacy = StreamingTableDefinition.withLegacyOperationalOptions(createStreamingSql(sql))
      process(legacy)
      val legacyManifest = manifest(name)
      legacyManifest.definitionHash shouldBe legacy.definitionHash
      legacyManifest.semanticJson should include("ignorechanges")
      legacyManifest.operationalJson should not include "ignorechanges"
      val generation = target(name).deltaTableId
      stopStreamingSql(name)
      insertRows(source, "(2, 'two', 'p')")

      val resumed = createStreamingSql(sql)
      resumed.queryId shouldBe legacy.queryId
      resumed.definitionHash should not be legacy.definitionHash
      target(name).deltaTableId shouldBe generation
      val upgraded = manifest(name)
      upgraded.definitionHash shouldBe resumed.definitionHash
      upgraded.semanticJson should not include "ignorechanges"
      upgraded.operationalJson should include("\"ignorechanges\":\"true\"")

      process(resumed)
      assertBagEqual(name, "SELECT 1 AS id, 'one' AS value, 'p' AS part UNION ALL SELECT 2, 'two', 'p'")
    }

    it("still rebuilds when startingVersion or the query changes") {
      val source = "strsql_tolerance_semantic_source"
      val name   = "strsql_tolerance_semantic_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p')")
      insertRows(source, "(2, 'two', 'p')")

      val first = createStreamingSql(declaration(source, name, "'ignoreChanges' = 'true'"))
      process(first)
      val firstGeneration = target(name).deltaTableId

      val startingVersion =
        createStreamingSql(declaration(source, name, "'ignoreChanges' = 'true', 'startingVersion' = '0'"))
      startingVersion.queryId should not be first.queryId
      startingVersion.definitionHash should not be first.definitionHash
      target(name).deltaTableId should not be firstGeneration
      process(startingVersion)
      val startingGeneration = target(name).deltaTableId

      val filtered = createStreamingSql(
        declaration(source, name, "'ignoreChanges' = 'true', 'startingVersion' = '0'", filter = "\nWHERE id > 1")
      )
      filtered.queryId should not be startingVersion.queryId
      filtered.definitionHash should not be startingVersion.definitionHash
      target(name).deltaTableId should not be startingGeneration
      process(filtered)
      assertBagEqual(name, "SELECT 2 AS id, 'two' AS value, 'p' AS part")
    }
  }
}
