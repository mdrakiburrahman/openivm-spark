package org.openivm.spark.streaming

import org.apache.spark.sql.openivm.StreamingDatasetAccess
import org.openivm.spark.commands.CreateStreamingTableCommand
import org.scalatest.funspec.AnyFunSpec

class StreamingDefinitionDriftSpec extends AnyFunSpec with StreamingTableTestFixture {

  private def command(sql: String): CreateStreamingTableCommand =
    spark.sessionState.sqlParser.parsePlan(sql).asInstanceOf[CreateStreamingTableCommand]

  private def define(sql: String): StreamingTableDefinition = {
    val parsed = command(sql)
    val frame  = StreamingDatasetAccess.ofRows(spark, parsed.spec.query)
    StreamingTableDefinition.build(
      spark,
      parsed.spec,
      frame.queryExecution.analyzed,
      StreamingTableMetadata.canonicalIdentity(spark, parsed.spec.name),
      StreamingRuntimeOptions.parse(parsed.spec.options)
    )
  }

  private def requested(semanticJson: String): StreamingTableDefinition =
    StreamingTableDefinition(
      formatVersion = StreamingTableDefinition.FormatVersion,
      fingerprint = StreamingTableDefinition.sha256(semanticJson),
      semanticJson = semanticJson,
      diagnosticJson = "{}",
      operationalJson = "{}",
      operationalHash = "operational",
      sources = Seq.empty,
      sourcePaths = Seq.empty,
      sourceIdentities = Seq.empty
    )

  private val idColumn    = "\\\"id\\\":int:true:[s]:\\\"{}\\\""
  private val wideId      = "\\\"id\\\":bigint:true:[s]:\\\"{}\\\""
  private val extraColumn = "\\\"extra\\\":string:true:[s]:\\\"{}\\\""

  private def semantic(
      columns: Seq[String],
      projection: String = "attribute:id",
      declaration: String = "decl",
      outputSchema: String = "[id:int]",
      identity: String = "s|delta:file:/s#table-one"
  ): String = {
    val list = columns.mkString("[", ",", "]")
    s"""{"formatVersion":1,"declarationPlan":"$declaration",""" +
      s""""analyzedPlan":"plan:Project([$projection],plan:Relation(output=$list))",""" +
      s""""outputSchema":"$outputSchema","sources":[{"occurrence":0,"identity":"$identity",""" +
      s""""schema":"$list","options":{}}]}"""
  }

  describe("streaming-table definition drift") {
    it("keeps CURRENT_DATE unevaluated so the fingerprint does not change with the calendar day") {
      val source = "strt_drift_date_source"
      createSource(source)
      val sql =
        s"""CREATE STREAMING TABLE strt_drift_date_target AS
           |SELECT id, value, part FROM STREAM $source
           |WHERE part >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 29), 'yyyyMMdd')""".stripMargin

      val first = define(sql)

      first.semanticJson should include("org.apache.spark.sql.catalyst.expressions.CurrentDate")
      define(sql).fingerprint shouldBe first.fingerprint
    }

    it("resumes the owned checkpoint when a source gains a column the declaration does not read") {
      val source = "strt_drift_column_source"
      val target = "strt_drift_column_target"
      createSource(source)
      val sql   = s"CREATE STREAMING TABLE $target AS SELECT id, value, part FROM STREAM $source"
      val first = StreamingTableManager.create(spark, command(sql).spec)
      try {
        appendRows(source, "(1, 'before', 'p')")
        process(first)
        StreamingTableManager.stop(spark, Seq(target)).status shouldBe "stopped"

        spark.sql(s"ALTER TABLE `$source` ADD COLUMNS (extra STRING)").collect()
        appendRows(source, "(2, 'after', 'p', 'x')")
        val second = StreamingTableManager.create(spark, command(sql).spec)

        second.queryId shouldBe first.queryId
        second.status shouldBe "initializing"
        process(second)
        assertBagEqual(
          target,
          "SELECT 1 AS id, 'before' AS value, 'p' AS part UNION ALL SELECT 2, 'after', 'p'"
        )
        StreamingTableManager.stop(spark, Seq(target)).status shouldBe "stopped"

        val third = StreamingTableManager.create(spark, command(sql).spec)
        third.queryId shouldBe first.queryId
        third.definitionHash shouldBe second.definitionHash
      } finally StreamingTableManager.stop(spark, Seq(target))
    }

    it("still rejects a changed column the declaration reads") {
      val source = "strt_drift_read_column_source"
      val target = "strt_drift_read_column_target"
      createSource(source)
      val sql   = s"CREATE STREAMING TABLE $target AS SELECT id, value, part FROM STREAM $source"
      val first = StreamingTableManager.create(spark, command(sql).spec)
      try {
        appendRows(source, "(1, 'before', 'p')")
        process(first)
        StreamingTableManager.stop(spark, Seq(target)).status shouldBe "stopped"

        spark.sql(s"ALTER TABLE `$source` ALTER COLUMN value COMMENT 'changed'").collect()

        an[org.apache.spark.sql.AnalysisException] should be thrownBy {
          StreamingTableManager.create(spark, command(sql).spec)
        }
      } finally StreamingTableManager.stop(spark, Seq(target))
    }

    it("accepts only additive source columns that cannot reach the query") {
      val stored = semantic(Seq(idColumn))

      StreamingDefinitionDrift.resumable(
        stored,
        requested(semantic(Seq(idColumn, extraColumn))),
        expandsViews = false,
        ignoreAnalyzedPlan = false
      ) shouldBe true
      StreamingDefinitionDrift.resumable(
        stored,
        requested(semantic(Seq(wideId))),
        expandsViews = false,
        ignoreAnalyzedPlan = false
      ) shouldBe false
      StreamingDefinitionDrift.resumable(
        stored,
        requested(semantic(Seq(idColumn, extraColumn), outputSchema = "[id:int,extra:string]")),
        expandsViews = false,
        ignoreAnalyzedPlan = false
      ) shouldBe false
      StreamingDefinitionDrift.resumable(
        stored,
        requested(semantic(Seq(idColumn), declaration = "changed")),
        expandsViews = false,
        ignoreAnalyzedPlan = true
      ) shouldBe false
      StreamingDefinitionDrift.resumable(
        stored,
        requested(semantic(Seq(idColumn), identity = "s|delta:file:/s#table-two")),
        expandsViews = false,
        ignoreAnalyzedPlan = true
      ) shouldBe false
      StreamingDefinitionDrift.resumable(
        stored,
        requested(semantic(Seq(idColumn, extraColumn))),
        expandsViews = true,
        ignoreAnalyzedPlan = true
      ) shouldBe false
    }

    it("accepts analyzer encoding drift of an unchanged declaration only when opted in") {
      val stored  = semantic(Seq(idColumn))
      val drifted = requested(semantic(Seq(idColumn), projection = "attribute:id,<new-runtime-field>"))

      StreamingDefinitionDrift.resumable(
        stored,
        drifted,
        expandsViews = false,
        ignoreAnalyzedPlan = false
      ) shouldBe false
      StreamingDefinitionDrift.resumable(stored, drifted, expandsViews = false, ignoreAnalyzedPlan = true) shouldBe true
    }

    it("splits signature lists around nested types and quoted metadata") {
      val struct  = "\"a\":struct<x:int,y:array<string>>:true:[q,r]:\"{\\\"c\\\":\\\"],<\\\"}\""
      val decimal = "\"b\":decimal(10,2):false:[]:\"{}\""

      StreamingDefinitionDrift.topLevelElements(s"[$struct,$decimal]") shouldBe Some(Seq(struct, decimal))
      StreamingDefinitionDrift.topLevelElements("[]") shouldBe Some(Seq.empty)
      StreamingDefinitionDrift.topLevelElements("[\"a\":int:true:[q:\"{}\"]") shouldBe None
      StreamingDefinitionDrift.withoutRelationOutputs(
        s"plan:A(output=[$struct],children=[plan:B(output=[])])"
      ) shouldBe Some("plan:A(output=<relation-output>,children=[plan:B(output=<relation-output>)])")
    }
  }
}
