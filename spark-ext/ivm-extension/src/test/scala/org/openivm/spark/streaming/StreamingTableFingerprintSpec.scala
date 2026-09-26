package org.openivm.spark.streaming

import org.apache.spark.sql.openivm.StreamingDatasetAccess
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.scalatest.funspec.AnyFunSpec

class StreamingTableFingerprintSpec extends AnyFunSpec with StreamingTableTestFixture {

  describe("streaming-table definition fingerprints") {
    it("ignores declaration formatting and comments while retaining plan semantics") {
      val source = "strt_fingerprint_source"
      createSource(source)
      val query    = readStream(source).select("id", "value", "part")
      val identity = StreamingTableMetadata.canonicalIdentity(spark, Seq("strt_fingerprint_target"))
      val runtime  = StreamingRuntimeOptions.parse(Map.empty)
      val compact = StreamingTableSpec(
        Seq("strt_fingerprint_target"),
        s"SELECT id, value, part FROM STREAM $source",
        query.queryExecution.logical
      )
      val formatted = compact.copy(
        queryText = s"""|SELECT id,
              |       value,
              |       part -- presentation-only comment
              |FROM STREAM $source
              |""".stripMargin
      )

      val first = StreamingTableDefinition.build(
        spark,
        compact,
        query.queryExecution.analyzed,
        identity,
        runtime
      )
      val second = StreamingTableDefinition.build(
        spark,
        formatted,
        query.queryExecution.analyzed,
        identity,
        runtime
      )

      first.fingerprint shouldBe second.fingerprint
    }

    it("separates semantic changes from trigger and source-rate operational changes") {
      val source = "strt_fingerprint_operational_source"
      createSource(source)
      val baseQuery    = readStream(source).select("id", "value", "part")
      val changedQuery = readStream(source).where("id > 0").select("id", "value", "part")
      val identity     = StreamingTableMetadata.canonicalIdentity(spark, Seq("strt_fingerprint_operational_target"))
      val baseSpec = StreamingTableSpec(
        Seq("strt_fingerprint_operational_target"),
        s"SELECT id, value, part FROM STREAM $source",
        baseQuery.queryExecution.logical
      )
      val base = StreamingTableDefinition.build(
        spark,
        baseSpec,
        baseQuery.queryExecution.analyzed,
        identity,
        StreamingRuntimeOptions.parse(Map.empty)
      )
      val retuned = StreamingTableDefinition.build(
        spark,
        baseSpec,
        baseQuery.queryExecution.analyzed,
        identity,
        StreamingRuntimeOptions.parse(Map("triggerInterval" -> "1 second"))
      )
      val changed = StreamingTableDefinition.build(
        spark,
        baseSpec.copy(query = changedQuery.queryExecution.logical),
        changedQuery.queryExecution.analyzed,
        identity,
        StreamingRuntimeOptions.parse(Map.empty)
      )

      retuned.fingerprint shouldBe base.fingerprint
      retuned.operationalHash should not be base.operationalHash
      changed.fingerprint should not be base.fingerprint
    }

    it("preserves literal payloads and captures direct native Delta reader options") {
      val source          = "strt_fingerprint_literals_source"
      val alternateSource = "strt_fingerprint_literals_alternate"
      createSource(source)
      createSource(alternateSource, "id INT, value INT, part STRING")
      val identity = StreamingTableMetadata.canonicalIdentity(spark, Seq("strt_fingerprint_literals_target"))
      val runtime  = StreamingRuntimeOptions.parse(Map.empty)

      def definition(query: org.apache.spark.sql.DataFrame): StreamingTableDefinition =
        StreamingTableDefinition.build(
          spark,
          StreamingTableSpec(
            Seq("strt_fingerprint_literals_target"),
            s"SELECT id, value, part FROM STREAM $source",
            query.queryExecution.logical
          ),
          query.queryExecution.analyzed,
          identity,
          runtime
        )

      val spaced         = definition(readStream(source).where("value = 'a  b'"))
      val compact        = definition(readStream(source).where("value = 'a b'"))
      val numberedOne    = definition(readStream(source).where("value = '#1'"))
      val numberedTwo    = definition(readStream(source).where("value = '#2'"))
      val commentLikeOne = definition(readStream(source).where("value = '/*one*/'"))
      val commentLikeTwo = definition(readStream(source).where("value = '/*two*/'"))

      spaced.fingerprint should not be compact.fingerprint
      numberedOne.fingerprint should not be numberedTwo.fingerprint
      commentLikeOne.fingerprint should not be commentLikeTwo.fingerprint

      val oneFile = definition(
        spark.readStream.format("delta").option("maxFilesPerTrigger", "1").table(source)
      )
      val twoFiles = definition(
        spark.readStream.format("delta").option("maxFilesPerTrigger", "2").table(source)
      )
      oneFile.fingerprint shouldBe twoFiles.fingerprint
      oneFile.operationalHash should not be twoFiles.operationalHash
      oneFile.sourcePaths should not be empty

      val alternate = definition(readStream(alternateSource).selectExpr("id", "CAST(value AS STRING) AS value", "part"))
      alternate.fingerprint should not be definition(readStream(source)).fingerprint
    }

    it("captures streaming sources inside CTE and subquery-plan boundaries") {
      val source = "strt_fingerprint_cte_source"
      val dim    = "strt_fingerprint_cte_dim"
      createSource(source)
      spark.sql(s"CREATE TABLE `$dim` (id INT) USING DELTA").collect()
      val command = spark.sessionState.sqlParser
        .parsePlan(
          s"""CREATE STREAMING TABLE strt_fingerprint_cte_target AS
           |WITH source_cte AS (
           |  SELECT * FROM STREAM $source WITH ('maxFilesPerTrigger' = '1')
           |)
           |SELECT id, value, part
           |FROM source_cte
           |WHERE id IN (SELECT id FROM $dim)""".stripMargin
        )
        .asInstanceOf[CreateStreamingTableCommand]
      val frame = StreamingDatasetAccess.ofRows(spark, command.spec.query)
      val definition = StreamingTableDefinition.build(
        spark,
        command.spec,
        frame.queryExecution.analyzed,
        StreamingTableMetadata.canonicalIdentity(spark, command.spec.name),
        StreamingRuntimeOptions.parse(command.spec.options)
      )

      definition.sourcePaths should not be empty
      definition.semanticJson should include("source_cte")
      definition.operationalJson should include("maxfilespertrigger")
    }

    it("ignores ephemeral Spark common-expression IDs and migrates legacy fingerprints") {
      val prefix =
        """{"formatVersion":1,"declarationPlan":"stable","analyzedPlan":"before:"""
      val suffix = """:after","outputSchema":"stable"}"""
      val legacy127 =
        prefix +
          """product:org.apache.spark.sql.catalyst.expressions.CommonExpressionId(""" +
          """[value:java.lang.Long:"127",value:java.lang.Boolean:"false"])""" +
          suffix
      val legacy103 =
        prefix +
          """product:org.apache.spark.sql.catalyst.expressions.CommonExpressionId(""" +
          """[value:java.lang.Long:"103",value:java.lang.Boolean:"false"])""" +
          suffix
      val normalized = prefix + "<common-expression-id>" + suffix

      StreamingTableDefinition.semanticallyEquivalent(legacy127, legacy103) shouldBe true
      StreamingTableDefinition.semanticallyEquivalent(legacy127, normalized) shouldBe true
      StreamingTableDefinition.semanticallyEquivalent(
        legacy127,
        normalized.replace("outputSchema\":\"stable", "outputSchema\":\"changed")
      ) shouldBe false
    }
  }
}
